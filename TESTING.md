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
curl -s http://localhost:3000/api/status | jq '{scrapeInProgress, currentSessionsCount, snapshotRowsInsertedLastRun, lastSuccessfulScrape, source}'
curl -s http://localhost:3000/api/analytics/availability-summary | jq '.snapshotCount, .averageSlotsByWeekday'
curl -s -X POST http://localhost:3000/api/notify/test -H 'Content-Type: application/json' -d '{"ntfy_topic":"your-topic"}'
```

### Frontend behavior

- Header shows filtered open count for the active date (not global total).
- While a scrape runs: `checked Xm ago · refreshing…` — sessions stay visible.
- Today hides sessions whose `start_ts` has passed; past sessions remain in Supabase for analytics.
- Unchecked dates: **Not checked yet.**
- Checked dates with no sessions: **No sessions found for this date.**

---

## Internal founder testing (notifications)

### Enable internal beta notifications

Set in Railway (or `.env` locally):

```bash
INTERNAL_BETA_NOTIFICATIONS=true
NTFY_TOPIC=ap-surf-connor-2026   # optional server fallback
```

When enabled:

- The **Alerts** tab is highlighted in the bottom nav.
- The ntfy topic field prefills with `ap-surf-connor-2026` until you save a different topic in localStorage.
- Helper text reminds you to subscribe to that exact topic in the ntfy app.

### Quick test checklist

1. Install [ntfy](https://ntfy.sh) and subscribe to `ap-surf-connor-2026`.
2. Open the app → **Alerts** tab → confirm the topic is prefilled.
3. Tap **Send test notification** — you should receive “AP Session Alert”.
4. Browse a session → tap 🔔 → confirm it appears on **Lineup**.
5. Wait for a scrape cycle and verify alerts on open/low-slot/selling-fast changes.

### Notes

- Each user’s topic is stored in **localStorage** on the device; watches sync to Supabase with `user_key`.
- `NTFY_TOPIC` is only used as a **server fallback** when `INTERNAL_BETA_NOTIFICATIONS=true` and a watch has no topic saved.
- Do not expose `SUPABASE_SERVICE_ROLE_KEY` in the frontend — it stays server-side only.
