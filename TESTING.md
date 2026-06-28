# Testing

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

6. Pick a future date (e.g. July 2) with arrows or calendar — saved sessions for that date should display if scraped; otherwise **Not checked yet** or **No sessions found for this date**.
7. Lineup/watchlist wave side should match Browse (canonical `current_sessions` side, not stale watchlist row).
8. Level chips order: PRG, INT, AT, AB, ET, EB, PT, PB.
9. Price appears on cards when modal scrape captured it.

### Profile Sync Code (cross-device Lineup)

1. On desktop: **Settings** → confirm sync code `ap-surf-connor-2026` → **Save code**.
2. Add a session to Lineup via 🔔.
3. On phone (Safari or installed PWA): same code → **Save code** → **Sync now**.
4. Confirm the same watched sessions appear on both devices.
5. Remove a watch on one device → **Sync now** on the other → confirm it updates.

Profile codes derive a stable `user_key` client-side (`profile:` + SHA-256). Watches are stored in Supabase by that key. Future public release should replace this with Supabase Auth.

### Mobile layout (iPhone Safari / PWA)

1. Test at ~375px width (iPhone SE) and ~390px (standard iPhone).
2. Confirm header is compact, date heading is centered and not clipped.
3. Wave/level chips scroll horizontally with no page-level horizontal scroll.
4. Bottom nav sits above Safari home indicator / PWA safe area.
5. Session cards: readable text, tappable bell (44px), no overlap with bottom nav when scrolled to end.
6. Add to Home Screen → launch standalone → confirm safe areas still look correct.

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
curl -s http://localhost:3000/api/status | jq '{scrapeInProgress, currentSessionsCount, dataSource, lastScrapeAttempt, lastSuccessfulScrape, minutesSinceLastScrape, coveragePercent, scrapeScheduleEnabled, serverStartedAt, waveSideDebug, watchlistSideDebug}'
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
