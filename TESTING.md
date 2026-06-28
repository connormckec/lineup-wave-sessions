# Testing

## Supabase availability collector

### Prerequisites

1. Run [`supabase/schema.sql`](supabase/schema.sql) in the Supabase SQL editor (creates `current_sessions`, `availability_snapshots`, `scrape_runs`, and related tables).
2. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in `.env` or Railway.

### Cold start / instant load

1. Ensure `current_sessions` has rows (from a prior successful scrape, or seed manually).
2. Restart the server: `npm start`.
3. Open http://localhost:3000 — sessions should appear on first load without waiting for a scrape.
4. Confirm startup log: `Serving N saved session(s) (supabase-current)`.

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
curl -s http://localhost:3000/api/status | jq '{scrapeInProgress, currentSessionsCount, lastScrapeAttempt, lastSuccessfulScrape, minutesSinceLastScrape, scrapeScheduleEnabled, serverStartedAt, waveSideDebug}'
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

- The Demo Alerts tab and ntfy setup UI are hidden.
- Bell buttons show “Alerts coming soon” and do not add watches.

### Quick test checklist

1. Install [ntfy](https://ntfy.sh) and subscribe to `ap-surf-connor-2026`.
2. Open the app → **Demo Alerts** tab → confirm the topic is prefilled.
3. Tap **Send test notification** — you should receive “AP Session Alert”.
4. Browse a session → tap 🔔 → confirm it appears on **Lineup**.
5. Wait for a scrape cycle and verify alerts on open/low-slot/selling-fast changes.

### Notes

- Demo user topics are stored in **localStorage** on the device; watches sync to Supabase with `user_key`.
- `NTFY_TOPIC` is only used as a **server fallback** when `INTERNAL_BETA_NOTIFICATIONS=true` and a watch has no topic saved.
- Do not expose `SUPABASE_SERVICE_ROLE_KEY` in the frontend — it stays server-side only.
