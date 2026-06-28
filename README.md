# Lineup

Atlantic Park session availability dashboard — monitors [Atlantic Park Surf](https://booking.atlanticparksurf.com/activity-agenda) sessions, collects historical availability in Supabase, and sends demo watchlist alerts via ntfy when enabled.

## Branding (demo pass)

The UI uses the **Lineup** wordmark with the circular wave mark from `public/brand/lineup-logo-source.png`. App icons are generated from that source; the checkerboard matte is a temporary placeholder until we export a true transparent mark.

## Architecture

- **Background scraper** runs on the server on a tiered schedule — the frontend never triggers scrapes.
- **`current_sessions`** in Supabase holds the latest known state of every session (instant load on app open).
- **`availability_snapshots`** stores one row per session per scrape for heat maps and fill-rate analytics.
- **`scrape_runs`** logs every scrape attempt (success or failure).
- Failed scrapes do **not** wipe saved data — the app keeps serving the last known good sessions.

### Scrape schedule

| Tier | Coverage | Frequency |
|------|----------|-----------|
| 1 | Today + tomorrow (with slot counts) | Every `CHECK_EVERY_MINS` (default 5 min) |
| 2 | Next 7 days | Every 30 min |
| 3 | Weeks 2–3 | Every 6 hours |
| 4 | Weeks 4+ | Daily at midnight |

Overlapping scrapes are skipped (not queued) so the server stays responsive.

## Local setup

```bash
npm install
npm start
```

Open http://localhost:3000 to browse sessions and add watches.

### Supabase setup

1. Create a Supabase project.
2. Run the full SQL in [`supabase/schema.sql`](supabase/schema.sql) in the Supabase SQL editor (idempotent — safe to re-run).
3. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in your environment (server only — never expose to the frontend).
4. Verify: `GET /api/schema/health` → `missingTables: []`.

If the schema was not applied, the app still boots but Browse shows **Database schema missing. Run supabase/schema.sql.** See [`AUDIT.md`](AUDIT.md).

On startup the server loads saved sessions from `current_sessions` before accepting requests. The UI treats Supabase as source of truth: saved sessions render immediately, background scrapes refresh in place, and failed scrapes never wipe visible data.

### Stabilization behavior

- **`GET /api/sessions?date=YYYY-MM-DD`** queries Supabase `current_sessions` for that exact date and returns saved sessions with `statusReason` (`saved_sessions_found`, `checked_no_sessions`, `not_checked`). Browse uses this as the display source of truth on every date change.
- All dates use **`America/New_York`** (Atlantic Park). Today/Tomorrow labels are computed at render time, not stored.
- On app open, Browse fetches saved sessions for park-today **before** global status — no scrape required to display cards.
- **`GET /api/status`** returns global scrape meta and full session list; background refresh only.
- While a scrape runs, the header shows **`checked Xm ago · refreshing…`** and sessions stay on screen.
- If a refresh fails, the app keeps last known good data with **`showing saved data · refresh failed`**.
- **`dataSource`**: `"supabase/current_sessions"` when serving persisted rows, `"memory-fallback"` when Supabase is unavailable.
- `/api/status` reloads `current_sessions` from Supabase on every request (without disturbing in-progress scrape date tracking).
- Tier 1 (today/tomorrow) **upserts only** — it never deletes future rows from `current_sessions`.
- The serve window uses `SCRAPE_WEEKS_AHEAD` so saved future dates (Mon, Jul 2, etc.) stay visible even when the booking site exposes fewer weeks.
- Status includes `selectedDateDebug` when `selected_date` query param is passed.
- Status fields include `currentSessionsCount`, `currentSessionsByDate`, `tierCoverage`, `lastFullCoverageScrape`, `lastTier1Scrape`–`lastTier4Scrape`, `datesCheckedEmpty`, `lastSuccessfulScrape`, `lastScrapeAttempt`, `minutesSinceLastScrape`, `scrapeInProgress`, `missingDatesInScrapeWindow`, `coveragePercent`, and `watchlistSideDebug`.
- Debug a single date: `GET /api/debug/date/YYYY-MM-DD` — session count, scrape runs, snapshots, and UI reason.
- Session cards show price when scraped (`price_text` / min–max) and booked counts only when capacity is known.

### Full-window session coverage

The app serves sessions across the full `SCRAPE_WEEKS_AHEAD` window (default 4 weeks), not just today/tomorrow.

- **Tier 1** (every 5 min): today/tomorrow — upserts only those dates; never wipes future rows.
- **Tier 2** (every 30 min): next 7 days.
- **Tier 3** (every 6 hours): weeks 2–4.
- **`scrape_snapshots`** merges with existing snapshot data so Tier 1 cannot shrink saved future dates.
- On startup, if `current_sessions` is sparse, the server **auto-backfills** from `scrape_snapshots` / `availability_snapshots` or schedules Tier 2.

**Manual backfill** (after schema reset):

```bash
curl -X POST https://YOUR-APP/api/admin/backfill-current-sessions
curl https://YOUR-APP/api/debug/coverage
```

**Railway:** Disable **Serverless / App Sleep** — otherwise Tier 2/3 scrapes pause until someone opens the app. Long-term: separate web API service + scraper worker.

### Future session detail enrichment

Two scrape levels run in parallel:

1. **Basic calendar scrape** (Tiers 1–4) — fast tile discovery across `SCRAPE_WEEKS_AHEAD`. Finds session identity, open/packed, wave side, level, and optional tile price text.
2. **Detail enrichment** — slower modal (or network JSON when available) scrape for `slots_available`, `capacity`, `estimated_booked`, `fill_rate`, and price. Runs on a priority queue in the background.

**Rules:**

- Saved/basic sessions render immediately — enrichment never blocks the UI.
- Basic scrapes **never overwrite** detailed slot/capacity/price fields with null.
- Browse reads saved Supabase data only — **no frontend-triggered scraping**. The selected date is used only to raise background enrichment priority.
- Detail enrichment runs only when data is missing, stale, watched, within 48h, or a basic scrape detected an availability change.
- `POST /api/admin/enrich-date` with `{ "isoDate": "2026-07-02" }` forces detail collection for a date (admin only).

**Schedule:**

| Priority | Coverage | Detail check frequency |
|----------|----------|------------------------|
| P1 | Watched, selected date, today/tomorrow, next 48h | Every `CHECK_EVERY_MINS` (default 5 min), offset from Tier 1 |
| P2 | Next 7 days | Every `ENRICHMENT_TIER2_EVERY_MINS` (default 45 min) |
| P3 | Weeks 2–3 | Every `ENRICHMENT_TIER3_STALE_HOURS` (default 12 h); basic Tier 3 scrape every 6 h |

**Optimizations:** persistent enrichment browser (reused Chromium context), week-grouped navigation, network JSON preferred over modals, images/fonts/media blocked during enrichment, per-session upsert + `availability_snapshots` insert.

**Debug:** `GET /api/debug/enrichment` — queue size, stale/missing counts, average run duration, recent errors.

**Status fields:** `detailCoveragePercent`, `sessionsWithSlotsCount`, `sessionsWithCapacityCount`, `sessionsWithPriceCount`, `enrichmentQueuePending`, `detailEnrichmentInProgress`, `lastDetailEnrichmentAt`.

**Railway:** Disable App Sleep / Serverless sleep so background scrapes and enrichment run on schedule. Playwright/Chromium is CPU-heavy — **split into web/API + background worker services** when possible, and increase CPU/memory if enrichment is slow.

See [`AUDIT.md`](AUDIT.md) for schema columns (`detail_status`, `last_detailed_check_at`, `session_enrichment_queue`).

### Browser cache and local state

- **API routes** (`/api/*`) send `Cache-Control: no-store` — responses are never cached by the browser or service worker.
- **Service worker** (`public/sw.js`) uses versioned cache `lineup-static-v2`; only static assets are cached. `/api/*` is never intercepted.
- **`index.html`** is network-first so deploys land quickly on desktop PWA/Safari.
- **localStorage session cache** is not loaded on startup — fresh Supabase data from `/api/status` wins. Sticky merge only runs during an active scrape.
- **Settings → App state** shows build version, `user_key`, selected date, watchlist counts, and `dataSource`.
- **Reset local app state** clears cached UI/selected date only; Supabase Lineup rows are kept.
- App refetches on tab focus / visibility change (debounced 5s).

### Profile Sync Code (internal beta)

Cross-device Lineup sync uses a **Profile Sync Code** instead of full login:

1. Open **Settings** → enter the same code on phone and computer (default internal code: `ap-surf-connor-2026`).
2. The app derives a stable `user_key` from the code (SHA-256 hash, prefixed `profile:`).
3. Watchlist rows in Supabase are keyed by that `user_key`, so Lineup and bell states sync across devices.
4. **Sync now** pulls the server watchlist first, then uploads any local-only rows (never wipes server with an empty client list).

localStorage caches the watchlist as a fallback. On startup the app loads Lineup from `GET /api/watchlist` before rendering an empty state. `/api/watchlist/sync` only removes server rows when `replace: true` is sent explicitly.

### Mobile / PWA

- Viewport uses `viewport-fit=cover` with iOS safe-area insets for header and bottom nav.
- Bottom nav is fixed above the home indicator; scroll panes include extra bottom padding.
- Filter chips scroll horizontally without clipping; session cards use 44px tap targets.
- Install as a PWA via Safari **Add to Home Screen** or Chrome install prompt.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `SUPABASE_URL` | — | Supabase project URL (server only) |
| `SUPABASE_SERVICE_ROLE_KEY` | — | Supabase service role key (server only) |
| `CHECK_EVERY_MINS` | `5` | Tier 1 scrape interval |
| `ENRICHMENT_TIER2_EVERY_MINS` | `45` | P2 detail enrichment interval |
| `ENRICHMENT_TIER3_STALE_HOURS` | `12` | P3 detail enrichment interval |
| `DETAIL_ENRICH_MAX_PER_RUN` | `25` | Max sessions per enrichment run |
| `ENRICHMENT_DELAY_MS` | `350` | Pause between modal/network detail checks |
| `ENRICHMENT_BROWSER_IDLE_MS` | `300000` | Close idle enrichment browser after 5 min |
| `HISTORY_SNAPSHOTS` | `true` | Set `false` to disable `availability_snapshots` inserts |
| `INTERNAL_BETA_NOTIFICATIONS` | — | Set `true` for founder demo only — shows **Demo Alerts** in Settings and enables ntfy testing |
| `NTFY_TOPIC` | — | Optional server fallback when internal beta is on |
| `LOW_SLOTS_THRESHOLD` | `2` | Notify when watched sessions drop to this many slots or fewer |
| `SCRAPE_WEEKS_AHEAD` | `4` | Calendar weeks to scrape ahead |
| `DEBUG_WAVE_SIDE` | — | Set `1` to log every parsed wave side during scrapes |
| `APP_VERSION` | `package.json` version | Exposed in `/api/status` for cross-device build checks |
| `BUILD_TIME` | server boot ISO time | Exposed in `/api/status` |

See [TESTING.md](TESTING.md) for verification steps.

### Collector scheduler

```bash
curl -s http://localhost:3000/api/debug/collector | jq '{scrapeScheduleEnabled,lastTier1Scrape,minutesSinceLastTier1,recommendedAction,recentScrapeRuns}'
curl -s -X POST http://localhost:3000/api/admin/run-tier1 | jq
```

After deploy, within 5–10 minutes `lastTier1Scrape` should be set and `scrapeScheduleEnabled` should be `true`.

---

### Railway / background collector health

The scraper must run continuously — **disable Railway Serverless/App Sleep** on this service. If the process sleeps, `lastScrapeAttempt` in `/api/status` will go stale and availability snapshots stop accumulating.

Check collector health via `/api/status` and `/api/debug/collector`:

- `likelySleepingOrRestarted` — true if Tier 1 hasn't run in 3× `CHECK_EVERY_MINS` since boot
- `minutesSinceLastTier1` — should stay below ~10 when Railway is awake
- `lastApiSessionsDurationMs` — `/api/sessions?date=` should respond in milliseconds, not wait on Playwright

**Railway Serverless/App Sleep must be disabled.** If enabled, scraping and notifications pause while the service sleeps, and morning boot depends entirely on saved Supabase rows until the next cron wake.

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | Live sessions + scrape state (`scrapeInProgress`, `currentSessionsCount`, `dataSource`, `coveragePercent`, `missingDatesInScrapeWindow`, `watchlistSideDebug`, date coverage) |
| `GET /api/analytics/availability-summary` | Aggregate stats from recent `availability_snapshots` |
| `POST /api/notify/test` | Send a test ntfy notification |

## Deploy to Railway

1. Push this repo to GitHub.
2. Create a new Railway project from the repo.
3. Railway builds using the included `Dockerfile` (Playwright base image).
4. Set env vars (Supabase keys required for persistence, optional `INTERNAL_BETA_NOTIFICATIONS`, etc.).
5. Deploy — the service exposes port 3000 via `PORT`.

## Push notifications

**ntfy is internal demo infrastructure only.** It lets founders test watchlist alert logic before a real public notification channel exists. Do not expose ntfy setup to normal users.

For founder/demo testing (`INTERNAL_BETA_NOTIFICATIONS=true`):

1. Install the [ntfy app](https://ntfy.sh) and subscribe to `ap-surf-connor-2026`.
2. Open the app → **Demo Alerts** tab → confirm the topic is prefilled → **Save**.
3. Tap 🔔 on sessions to watch; demo alerts go to that shared topic.

**Future public MVP** should use native push, web push, SMS, or email after login — not ntfy topic configuration in the UI.
