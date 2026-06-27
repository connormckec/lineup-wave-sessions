# AP Session Watcher

Monitors [Atlantic Park Surf](https://booking.atlanticparksurf.com/activity-agenda) session availability and sends push notifications via [ntfy.sh](https://ntfy.sh) when spots open or slots run low.

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
| `NTFY_TOPIC` | — | ntfy.sh topic for push notifications (required for alerts) |
| `CHECK_EVERY_MINS` | `5` | How often to scrape the booking page |
| `LOW_SLOTS_THRESHOLD` | `2` | Notify when watched sessions drop to this many slots or fewer |

## Deploy to Railway

1. Push this repo to GitHub.
2. Create a new Railway project from the repo.
3. Railway will build using the included `Dockerfile` (Playwright base image).
4. Set `NTFY_TOPIC` (and optionally other env vars) in Railway settings.
5. Deploy — the service exposes port 3000 automatically via `PORT`.

## Push notifications

1. Install the [ntfy app](https://ntfy.sh) on your phone.
2. Subscribe to a private topic name (e.g. `my-ap-surf-alerts-xyz123`).
3. Set `NTFY_TOPIC` to that topic name in your environment.
