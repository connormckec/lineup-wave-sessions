# Lineup release checklist

Use this before tagging a stable beta release.

## Automated checks

- [ ] Run `npm test` (includes beta production smoke with Playwright fixtures)
- [ ] Confirm the beta smoke test passes on the target branch

## Browser sanity

- [ ] Open production/staging in a clean browser tab
- [ ] Confirm no uncaught errors in the browser console on Browse load
- [ ] Confirm no `SyntaxError` or `ReferenceError` on initial render

## API health

- [ ] Confirm `GET /api/sessions?date=<today>` returns HTTP 200
- [ ] Confirm response includes `sessions` array (or an intentional empty-state reason)

## Core flows

- [ ] Browse: date rail, filters, session cards, Live now, and sidebar render
- [ ] Browse: open and full sessions both appear when present in data
- [ ] Browse: Show lessons / Hide lessons toggles lesson rows
- [ ] Your Lineup: watched sessions join to current session records (not stale snapshot-only data)
- [ ] Settings: profile sync and notification blocks render
- [ ] Send a test notification from Settings (with a saved profile sync code)

## Layout & deployment

- [ ] Test at mobile width (~390px): header, Browse list, and footer remain usable
- [ ] Verify the current Railway deployment matches the intended commit
- [ ] Confirm workers, cron schedules, scraping, threshold scanning, and Supabase persistence were **not** unintentionally changed

## Release

- [ ] Beta footer and Report an issue flow visible in the deployed build
- [ ] Tag the stable release (e.g. `v1.x-beta`) after the above checks pass
