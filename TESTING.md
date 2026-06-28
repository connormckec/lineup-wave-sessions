# Internal founder testing

## Enable internal beta notifications

Set in Railway (or `.env` locally):

```bash
INTERNAL_BETA_NOTIFICATIONS=true
NTFY_TOPIC=ap-surf-connor-2026   # optional server fallback
```

When enabled:

- The **Alerts** tab is highlighted in the bottom nav.
- The ntfy topic field prefills with `ap-surf-connor-2026` until you save a different topic in localStorage.
- Helper text reminds you to subscribe to that exact topic in the ntfy app.

## Quick test checklist

1. Install [ntfy](https://ntfy.sh) and subscribe to `ap-surf-connor-2026`.
2. Open the app → **Alerts** tab → confirm the topic is prefilled.
3. Tap **Send test notification** — you should receive “AP Session Alert”.
4. Browse a session → tap 🔔 → confirm it appears on **Lineup**.
5. Wait for a scrape cycle (or trigger locally) and verify alerts on open/low-slot changes.

## Notes

- Each user’s topic is stored in **localStorage** on the device; watches sync to Supabase with `user_key`.
- `NTFY_TOPIC` is only used as a **server fallback** when `INTERNAL_BETA_NOTIFICATIONS=true` and a watch has no topic saved.
- Do not expose `SUPABASE_SERVICE_ROLE_KEY` in the frontend — it stays server-side only.
