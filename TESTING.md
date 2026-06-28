# Testing

## Schema / data contract verification

Run this after deploying or when you see “Could not find the table 'public.current_sessions'”.

1. **Apply schema** — paste full [`supabase/schema.sql`](supabase/schema.sql) into Supabase SQL editor → Run.
2. **Confirm table** — Supabase Table Editor → `current_sessions` exists.
3. **Schema health**:

```bash
curl -s http://localhost:3000/api/schema/health | jq '{missingTables, currentSessionsAvailable, schemaActionRequired}'
```

4. **Status**:

```bash
curl -s http://localhost:3000/api/status | jq '{dataSource, currentSessionsTableAvailable, schemaMissingTables, schemaActionRequired, currentSessionsCount}'
```

5. **Selected date** (replace date as needed):

```bash
curl -s "http://localhost:3000/api/sessions?date=2026-06-28" | jq '{isoDate, sessionsCount, dataSource, statusReason, error, schemaError}'
```

6. **Open app** — Browse should show sessions, empty states (“Not checked yet”), or **Database schema missing. Run supabase/schema.sql.** — not an infinite “Loading…” or raw PostgREST error.

If `current_sessions` is missing and no snapshot fallback exists, `/api/sessions` returns HTTP 503 with:

```json
{ "error": "Missing Supabase table current_sessions. Run supabase/schema.sql in the Supabase SQL editor.", "statusReason": "schema_error" }
```

See [`AUDIT.md`](AUDIT.md) for the full table/column/API contract.

---

## Supabase availability collector

### Prerequisites

1. Run [`supabase/schema.sql`](supabase/schema.sql) in the Supabase SQL editor (creates `current_sessions`, `availability_snapshots`, `scrape_runs`, and related tables).
2. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in `.env` or Railway.

### Cold start / instant load

1. Ensure `current_sessions` has rows (from a prior successful scrape, or seed manually).
2. Restart the server: `npm start`.
3. Open http://localhost:3000 — sessions should appear on first load without waiting for a scrape.
4. Confirm startup log: `Serving N saved session(s) (supabase)`.

### Stabilization / never-blank refresh

1. With saved rows in `current_sessions`, hard-refresh the browser.
2. Sessions should appear immediately (from API or local cache) — not a blank list while `scrapeInProgress` is true.
3. Header while scraping: `checked Xm ago · refreshing…`.
4. Simulate API returning empty sessions during scrape — UI should keep last cached sessions visible.
5. Confirm `/api/status` fields:

```bash
curl -s http://localhost:3000/api/status | jq '{dataSource, currentSessionsCount, scrapeInProgress, minutesSinceLastScrape, coveragePercent, missingDatesInScrapeWindow, watchlistSideDebug}'
```

6. Pick a future date (e.g. Monday Jun 29 or Thu Jul 2) with arrows or calendar — saved sessions for that date should display if scraped; otherwise **Not checked yet** or **No sessions found for this date** (never **Still checking** when saved rows exist).
7. Debug a date:

```bash
curl -s http://localhost:3000/api/debug/date/2026-06-29 | jq '{isoDate, currentSessionsCountForDate, wasDateChecked, uiReason, uiReasonText}'
curl -s "http://localhost:3000/api/status?selected_date=2026-06-29" | jq '{selectedDateDebug, currentSessionsByDate, sessionsForSelectedDateCount}'
```

8. Trigger Tier 1 refresh (wait ~5 min or restart) — future-date sessions must **not** disappear from Browse.
9. Lineup/watchlist wave side should match Browse (canonical `current_sessions` side, not stale watchlist row).
10. Level chips order: PRG, INT, AT, AB, ET, EB, PT, PB.
11. Price appears on cards when modal scrape captured it.

### Selected-date sessions (Supabase source of truth)

Browse loads **`GET /api/sessions?date=YYYY-MM-DD`** on every date change. That endpoint queries `current_sessions` directly for the exact `iso_date` and returns:

- `sessions` — rows for that date
- `statusReason` — `saved_sessions_found` | `checked_no_sessions` | `not_checked` | `error`
- `dataSource` — `supabase/current_sessions`

The UI renders the returned sessions directly — it does not filter the full `/api/status` payload client-side for the selected date.

```bash
curl -s "http://localhost:3000/api/sessions?date=2026-06-29" | jq '{isoDate, sessionsCount, statusReason, dataSource, hasSavedSessions}'
```

- Sessions load **before** watchlist on startup — Lineup fetch cannot block Browse rendering.
- `/api/status` response is normalized (`sessions`, `currentSessions`, `isoDate`/`dateKey`) before render.
- If `/api/status` returns zero sessions, client retries `/api/sessions`.
- Stuck on **Loading…** → check Settings **App state** for boot state and session counts; use **Reset local app state** if needed.

1. Open Railway URL on desktop and phone — **Settings → App state** should show the same **App version** and **Build time** (after deploy propagates).
2. Compare session counts:

```bash
curl -s -D - "https://YOUR-APP/api/status" -o /dev/null | grep -i cache-control
curl -s "https://YOUR-APP/api/status" | jq '{appVersion, buildTime, currentSessionsCount, dataSource, lastSuccessfulScrape}'
```

3. Hard refresh desktop (Cmd+Shift+R) — future dates should match phone.
4. If desktop shows stale selected date: **Settings → Reset local app state** — should refetch without wiping Lineup.
5. DevTools → Application → Service Workers — confirm `/api/status` requests are **not** served from SW (network / no-store).
6. Fresh app open should show sessions within a few seconds — not stuck on **Loading…** while `/api/status` returns `sessionsCount > 0`.
7. If stuck, open DevTools Console — a script syntax error prevents all JS from running; use **Settings → Reset local app cache**.
8. Old caches `lineup-v1` / `lineup-static-v2` are deleted on activate; active cache is `lineup-static-v3` (API and index.html not cached).

### Profile Sync Code (cross-device Lineup)

1. On desktop: **Settings** → confirm sync code `ap-surf-connor-2026` → **Save code**.
2. Add a session to Lineup via 🔔.
3. Hard-refresh — Lineup should still show watched sessions (loads from `GET /api/watchlist`, not empty localStorage).
4. Close and reopen the app — same Lineup should appear after “Loading your lineup…”.
5. On phone (Safari or installed PWA): same code → **Save code** → confirm same watched sessions (use **Sync now** if needed).
6. Remove a watch on one device → **Sync now** on the other → confirm it updates.

Verify watchlist debug fields:

```bash
curl -s "http://localhost:3000/api/status?user_key=YOUR_PROFILE_USER_KEY&profile_code=ap-surf-connor-2026" | jq '{user_key, watchlistCount, watchlistRowsLoaded, watchlistLastError, supabaseAvailable}'
curl -s "http://localhost:3000/api/watchlist?user_key=YOUR_PROFILE_USER_KEY" | jq '{watchlistCount, watchlistLastError, items: .items | length}'
```

Profile codes derive a stable `user_key` client-side (`profile:` + SHA-256). Watches are stored in Supabase by that key. Startup must **not** call `/api/watchlist/sync` with an empty list (that would have wiped server rows before this fix).

### Mobile layout (iPhone Safari / PWA)

1. Test at ~375px width (iPhone SE) and ~390px (standard iPhone).
2. Confirm header is compact, date heading is centered and not clipped.
3. Wave/level chips scroll horizontally with no page-level horizontal scroll.
4. Bottom nav sits above Safari home indicator / PWA safe area.
5. Session cards: status shows compact spot count (e.g. `1 spot left`) without ellipses; optional second line (`Closing out`).
6. Header open count matches the visible filtered list for the selected date.
7. Lineup cards include **Book at Atlantic Park** link opening the official booking page.
8. Add to Home Screen → launch standalone → confirm safe areas still look correct.

### Background scrapes

1. With Supabase configured, wait for a tier 1 scrape (default every 5 min) or restart to trigger bootstrap.
2. Check server logs for:
   - `Supabase: upserted N current_sessions row(s)`
   - `Supabase: saved N availability snapshot row(s)`
3. In Supabase Table Editor:
   - **`current_sessions`** — rows updated with recent `last_scraped_at`
   - **`availability_snapshots`** — new rows appended each scrape
   - **`scrape_runs`** — row with `success = true`, `sessions_found`, `dates_covered`

### Failed scrape safety

1. Simulate failure (e.g. block network temporarily or stop Playwright).
2. Confirm `/api/status` still returns previously saved sessions.
3. Confirm `scrape_runs` has a row with `success = false` and `error` populated.
4. Confirm `current_sessions` was **not** wiped.

### API checks

```bash
curl -s http://localhost:3000/api/status | jq '{scrapeInProgress, currentSessionsCount, currentSessionsByDate, dataSource, lastScrapeAttempt, lastSuccessfulScrape, lastFullCoverageScrape, tierCoverage, minutesSinceLastScrape, coveragePercent, scrapeScheduleEnabled, serverStartedAt, waveSideDebug, watchlistSideDebug}'
curl -s http://localhost:3000/api/debug/date/2026-07-02 | jq '{currentSessionsCountForDate, wasDateChecked, uiReason, uiReasonText, supabaseCurrentSessionsCountForDate}'
curl -s http://localhost:3000/api/analytics/availability-summary | jq '.snapshotCount, .averageSlotsByWeekday'
curl -s -X POST http://localhost:3000/api/notify/test -H 'Content-Type: application/json' -d '{"ntfy_topic":"your-topic"}'
```

### Collector health (Railway)

- **Disable App Sleep / Serverless sleep** on the Railway service — background tier-1 scrapes must run every ~5 minutes.
- If `minutesSinceLastScrape` is much larger than `CHECK_EVERY_MINS`, the service may be sleeping or the scraper crashed.
- Compare `serverStartedAt` with `lastScrapeAttempt` to distinguish a fresh deploy from a stalled scraper.

### Wave side mapping

- Set `DEBUG_WAVE_SIDE=1` to log parsed side for each session tile during scrapes.
- Check `/api/status` → `waveSideDebug.ambiguousSamples` if Left/Right labels look wrong.
- Side is parsed from tile tooltip text and column headers first; wave index is fallback only.

### Alert priority (non-noisy)

Per scrape, at most **one** alert fires per watched session, in priority order: `opened` → `low_slots` → `selling_fast` → `last_call`.

| Transition | Expected |
|---|---|
| unavailable → 1–4 slots | `opened` |
| 5 → 2 slots (already available) | `low_slots` |
| 2 → 4 slots | **nothing** |
| 8 → 4 slots | `selling_fast` (drop ≥ 3) |
| 4 → 4 slots | **nothing** |

Adding a watch does **not** send an immediate alert — only real transitions on the next scrape.

### Saved data on app open

1. Hard-refresh with rows in `current_sessions`.
2. Sessions render immediately from cache/API — not blank while `scrapeInProgress`.
3. Status shows `checked Xm ago · refreshing…` when a scrape is running.
4. Unchecked dates show **Not checked yet**, not **No sessions found**.
5. `/api/status?selected_date=YYYY-MM-DD` returns `selectedDateDebug` with coverage fields.

```bash
curl -s 'http://localhost:3000/api/status?selected_date=2026-07-02' | jq '{dataSource, currentSessionsCount, scrapeInProgress, selectedDateDebug}'
```

### Opened vs low-slot alerts

- Watching a **packed/sold-out** session should produce an **`opened`** alert when spots appear — not `low_slots`.
- If a session opens with 1–2 spots, only **`opened`** fires on that scrape (not both).
- `notification_events` rows include `previous_available`, `current_available`, `previous_slots`, `current_slots`, and `event_reason`.

### Frontend behavior

- Header shows filtered open count for the active date (not global total).
- While a scrape runs: `checked Xm ago · refreshing…` — sessions stay visible.
- Today hides sessions whose `start_ts` has passed; past sessions remain in Supabase for analytics.
- Unchecked dates: **Not checked yet.**
- Checked dates with no sessions: **No sessions found for this date.**

---

## Internal founder demo (ntfy only)

ntfy is **not** the public onboarding flow. It is temporary demo infrastructure for testing watchlist alert logic before native push / web push / SMS / email after login.

### Enable demo alerts

Set in Railway (or `.env` locally):

```bash
INTERNAL_BETA_NOTIFICATIONS=true
NTFY_TOPIC=ap-surf-connor-2026   # optional server fallback
```

When enabled:

- The **Demo Alerts** nav tab appears (hidden for normal users).
- Copy explains this is internal demo only.
- The ntfy topic field prefills with `ap-surf-connor-2026` until you save a different topic in localStorage.
- Subscribe to that exact topic in the ntfy app.

When **not** enabled:

- The Demo Alerts block in Settings is hidden.
- Bell buttons show “Alerts coming soon” and do not add watches.
- **Profile Sync Code** in Settings still works for cross-device Lineup sync.

### Quick test checklist

1. **Settings** → save Profile Sync Code `ap-surf-connor-2026` on each device.
2. Install [ntfy](https://ntfy.sh) and subscribe to `ap-surf-connor-2026`.
3. Open **Settings** → **Demo Alerts** → confirm the topic is prefilled.
4. Tap **Send test notification** — you should receive “AP Session Alert”.
5. Browse a session → tap 🔔 → confirm it appears on **Lineup** on both devices after **Sync now**.
6. Wait for a scrape cycle and verify alerts on open/low-slot/selling-fast changes.

### Notes

- Profile Sync Code derives a stable `user_key` stored in localStorage; watches sync to Supabase with that key.
- ntfy topics are stored in **localStorage** per device.
- `NTFY_TOPIC` is only used as a **server fallback** when `INTERNAL_BETA_NOTIFICATIONS=true` and a watch has no topic saved.
- Do not expose `SUPABASE_SERVICE_ROLE_KEY` in the frontend — it stays server-side only.
