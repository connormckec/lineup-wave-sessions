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
| `HISTORY_SNAPSHOTS` | `true` | Set `false` to disable `availability_snapshots` inserts |
| `INTERNAL_BETA_NOTIFICATIONS` | — | Set `true` for founder demo only — shows **Demo Alerts** in Settings and enables ntfy testing |
| `NTFY_TOPIC` | — | Optional server fallback when internal beta is on |
| `LOW_SLOTS_THRESHOLD` | `2` | Notify when watched sessions drop to this many slots or fewer |
| `SCRAPE_WEEKS_AHEAD` | `4` | Calendar weeks to scrape ahead |
| `DEBUG_WAVE_SIDE` | — | Set `1` to log every parsed wave side during scrapes |
| `APP_VERSION` | `package.json` version | Exposed in `/api/status` for cross-device build checks |
| `BUILD_TIME` | server boot ISO time | Exposed in `/api/status` |

See [TESTING.md](TESTING.md) for verification steps.

### Railway / background collector health

The scraper must run continuously — **disable Railway Serverless/App Sleep** on this service. If the process sleeps, `lastScrapeAttempt` in `/api/status` will go stale and availability snapshots stop accumulating.

Check collector health via `/api/status`:

- `lastScrapeAttempt` / `lastSuccessfulScrape` — should update every few minutes (tier 1)
- `minutesSinceLastScrape` — alert if this grows beyond ~2× `CHECK_EVERY_MINS`
- `scrapeScheduleEnabled` — should be `true`
- `serverStartedAt` — when the current process booted
- `waveSideDebug` — recent side parses and any ambiguous mappings

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
