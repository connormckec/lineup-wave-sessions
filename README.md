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
2. Run the full SQL in [`supabase/schema.sql`](supabase/schema.sql) in the Supabase SQL editor.
3. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in your environment (server only — never expose to the frontend).

On startup the server loads saved sessions from `current_sessions` before accepting requests. The UI treats Supabase as source of truth: saved sessions render immediately, background scrapes refresh in place, and failed scrapes never wipe visible data.

### Stabilization behavior

- **`GET /api/status`** returns saved sessions immediately; if memory is empty it reloads from Supabase first.
- While a scrape runs, the header shows **`checked Xm ago · refreshing…`** and sessions stay on screen.
- If a refresh fails, the app keeps last known good data with **`showing saved data · refresh failed`**.
- **`dataSource`**: `"supabase"` when serving persisted data, `"memory-fallback"` when Supabase is unavailable.
- Status fields include `currentSessionsCount`, `lastSuccessfulScrape`, `lastScrapeAttempt`, `minutesSinceLastScrape`, `scrapeInProgress`, `missingDatesInScrapeWindow`, `coveragePercent`, and `watchlistSideDebug` (stored vs canonical wave side).
- Session cards show price when scraped (`price_text` / min–max) and booked counts only when capacity is known.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `SUPABASE_URL` | — | Supabase project URL (server only) |
| `SUPABASE_SERVICE_ROLE_KEY` | — | Supabase service role key (server only) |
| `CHECK_EVERY_MINS` | `5` | Tier 1 scrape interval |
| `HISTORY_SNAPSHOTS` | `true` | Set `false` to disable `availability_snapshots` inserts |
| `INTERNAL_BETA_NOTIFICATIONS` | — | Set `true` for founder demo only — shows **Demo Alerts** tab and enables ntfy testing |
| `NTFY_TOPIC` | — | Optional server fallback when internal beta is on |
| `LOW_SLOTS_THRESHOLD` | `2` | Notify when watched sessions drop to this many slots or fewer |
| `SCRAPE_WEEKS_AHEAD` | `4` | Calendar weeks to scrape ahead |
| `DEBUG_WAVE_SIDE` | — | Set `1` to log every parsed wave side during scrapes |

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
