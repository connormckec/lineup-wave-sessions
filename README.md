# AP Session Watcher

Monitors [Atlantic Park Surf](https://booking.atlanticparksurf.com/activity-agenda) session availability and sends push notifications via [ntfy.sh](https://ntfy.sh) when watched sessions open or slots run low.

## Local setup

```bash
npm install
npm start
```

Open http://localhost:3000 to browse sessions and add watches.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `INTERNAL_BETA_NOTIFICATIONS` | — | Set `true` for founder testing (prefills topic, highlights Alerts tab) |
| `NTFY_TOPIC` | — | Optional server fallback when internal beta is on (e.g. `ap-surf-connor-2026`) |
| `SUPABASE_URL` | — | Supabase project URL (server only) |
| `SUPABASE_SERVICE_ROLE_KEY` | — | Supabase service role key (server only — never expose to frontend) |
| `CHECK_EVERY_MINS` | `5` | How often to scrape the booking page |
| `LOW_SLOTS_THRESHOLD` | `2` | Notify when watched sessions drop to this many slots or fewer |

See [TESTING.md](TESTING.md) for internal founder testing steps.

## Deploy to Railway

1. Push this repo to GitHub.
2. Create a new Railway project from the repo.
3. Railway will build using the included `Dockerfile` (Playwright base image).
4. Set env vars in Railway (Supabase, optional `INTERNAL_BETA_NOTIFICATIONS`, etc.).
5. Deploy — the service exposes port 3000 automatically via `PORT`.

## Push notifications

1. Install the [ntfy app](https://ntfy.sh) on your phone.
2. Open the app → **Alerts** tab → enter your private topic → **Save topic**.
3. Subscribe to the same topic in the ntfy app.
4. Tap 🔔 on sessions to watch; alerts go to **your** topic (not a shared global topic).

For internal testing, set `INTERNAL_BETA_NOTIFICATIONS=true` — the default topic `ap-surf-connor-2026` is prefilled until you save your own.
