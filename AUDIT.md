# Supabase schema / API data contract audit

This document records what the Lineup app expects from Supabase and the HTTP API. Use it when debugging “schema cache” errors or mismatches between frontend and database.

## Quick fix: missing `current_sessions`

If you see:

> Could not find the table 'public.current_sessions' in the schema cache

1. Open Supabase → **SQL** → paste the full [`supabase/schema.sql`](supabase/schema.sql) → **Run**.
2. Confirm **`current_sessions`** appears in **Table Editor**.
3. Restart the server (`npm start`).
4. Check `GET /api/schema/health` — `missingTables` should be `[]`.
5. Open the app — Browse should load or show a clear schema message (not hang).

---

## Pipeline diagnosis (saved + live data)

Use these endpoints to diagnose before changing code:

```bash
curl -s /api/debug/coverage | jq
curl -s /api/debug/date/2026-06-29 | jq '{statusReason,uiMessage,apiContract,sessionsCount,currentSessionsCountForDate,fallbackSnapshotCount}'
curl -s "/api/sessions?date=2026-06-29" | jq '{sessionsCount,statusReason,dataSource,hasSavedSessions,isFallback}'
```

### Where is saved full-window data stored?

| Source | Role |
|--------|------|
| **`current_sessions`** | **Display table** — latest row per session; Browse reads this first |
| **`scrape_snapshots`** (`id='latest'`) | Merged JSON backup; fallback when `current_sessions` sparse for a date |
| **`availability_snapshots`** | Append-only history; fallback = latest row per `session_key` for a date |
| **In-memory `sessionsByKey`** | Server cache; reloaded on startup from `current_sessions` |

### Does `current_sessions` have rows beyond tomorrow?

Check `GET /api/debug/coverage` → `datesInCurrentSessions`, `currentSessionsByDate`, `missingDatesFromCurrentSessions`.

If only today/tomorrow appear: Tier 2/3 have not populated yet, or backfill has not run. Run `POST /api/admin/backfill-current-sessions` if `scrape_snapshots` or `availability_snapshots` have broader data.

### Are Tier 2/Tier 3 running?

Check `lastTier2Scrape`, `lastTier3Scrape`, `relevantScrapeRuns` in debug endpoints. Tier 2 every 30 min, Tier 3 every 6 h. **Railway App Sleep must be disabled** or scrapes pause until someone opens the app.

### Is Tier 1 overwriting future-date rows?

**No** — Tier 1 upserts today/tomorrow only. It must not reload-wipe memory (fixed: no full `loadCurrentSessionsFromSupabase` after tier scrape). `saveLatestSnapshotToSupabase` merges with existing snapshot JSON so Tier 1 cannot shrink saved future dates.

### Is the frontend using `/api/sessions?date=`?

**Yes** — Browse calls `GET /api/sessions?date=YYYY-MM-DD` on every date change and renders `selectedDateSessions` directly.

**Bug that caused “waiting for first scrape”:** The status bar used global `/api/status` session counts (`data.sessions`) instead of selected-date sessions. Future dates could have rows from `/api/sessions?date=` while the header still said “waiting for first scrape”. Fixed: status bar and empty states now respect `selectedDateSessions` and API `statusReason`.

### Why “not checked yet” when saved data exists?

Only if **all** sources return zero rows for that date: `current_sessions`, `scrape_snapshots`, `availability_snapshots`, memory.

If fallback rows exist, API returns `statusReason: fallback_sessions_found` — UI must not show “Not checked yet”.

---

## Canonical display contract

**Only path for Browse:** `GET /api/sessions?date=YYYY-MM-DD`

```json
{
  "isoDate": "YYYY-MM-DD",
  "sessions": [],
  "sessionsCount": 0,
  "dataSource": "supabase/current_sessions",
  "statusReason": "saved_sessions_found | fallback_sessions_found | checked_no_sessions | not_checked | schema_error | error",
  "hasSavedSessions": true,
  "lastBasicCheckAt": "...",
  "lastDetailedCheckAt": "...",
  "isScrapeInProgress": false,
  "isFallback": false,
  "error": null
}
```

| statusReason | When |
|--------------|------|
| `saved_sessions_found` | Rows from `current_sessions` |
| `fallback_sessions_found` | Rows from snapshot/history/memory fallback |
| `checked_no_sessions` | Date was scraped, zero sessions |
| `not_checked` | No source has rows for this date |
| `schema_error` | Table missing — run schema.sql |

---

| Table | Used by app | Purpose |
|-------|-------------|---------|
| `current_sessions` | **Yes — primary UI source** | Latest row per session; Browse date queries filter by `iso_date` |
| `scrape_snapshots` | Yes | Meta + JSON session blob (`id='latest'`); fallback if `current_sessions` missing |
| `availability_snapshots` | Yes | Append-only scrape history for analytics |
| `scrape_runs` | Yes | Scrape attempt audit log |
| `watchlist_items` | Yes | Per-user Lineup / ntfy watches |
| `notification_events` | Yes | Alert dedupe / debug log |
| `session_enrichment_queue` | Yes | Background detail enrichment queue |
| `date_coverage` | **No** | Not referenced in code |

---

## Column contracts

### `current_sessions`

| Column | Type | Notes |
|--------|------|-------|
| `park` | text | Default `atlantic_park`; PK part |
| `session_key` | text | PK part; maps to session `key` |
| `iso_date` | date | Browse filter field |
| `start_ts` | bigint | Unix ms |
| `start_time` | text | Display time |
| `display_date` | text | Human date string |
| `weekday` | text | |
| `wave_side` | text | Left / Right |
| `session_type` | text | Level code (PRG, INT, …) |
| `available` | boolean | |
| `slots_available` | integer | |
| `capacity` | integer | Optional |
| `estimated_booked` | integer | Optional |
| `fill_rate` | numeric | Optional |
| `price_text` | text | Optional |
| `price_min` / `price_max` | numeric | Optional |
| `currency` | text | Default USD |
| `status_label` | text | |
| `source_tier` | integer | Scrape tier |
| `raw` | jsonb | Raw tile payload |
| `first_seen_at` | timestamptz | |
| `last_seen_at` | timestamptz | |
| `last_scraped_at` | timestamptz | |
| `last_basic_check_at` | timestamptz | Set on basic tile scrape |
| `last_detailed_check_at` | timestamptz | Set on modal/network detail scrape |
| `detail_status` | text | `pending`, `checking`, `checked_with_slots`, `checked_packed`, `checked_open_no_slots_visible`, `checked_available_no_slot_count`, `failed_modal_mismatch`, `failed_modal_open`, `failed_selector`, `failed_cookie_overlay`, `failed_parse`, `failed_timeout`, `unknown` (legacy: `checked_packed_no_slots`, `checked_no_slots_visible`, `checked`, `failed`) |
| `detail_error` | text | e.g. `slots_exceed_capacity`, `modal_mismatch: …` |

**Verification fields (stored in `raw` jsonb):** `detailVerified`, `detailConfidence` (`exact_match`, `weak_match`, `mismatch`, `default_suppressed`), `detailSourceSessionKey`, `detailSourceIsoDate`, `detailSourceStartTime`, `detailSourceSessionType`, `detailSourceWaveSide`, `detailParseOutput`.

**Critical rule:** Basic scrapes must not null out verified detail fields (`detailVerified: true`) for the same `session_key`. API responses suppress unverified slots/capacity/booked/price — UI shows *details pending* instead of inferred defaults (10/12/2).

### `scrape_snapshots`

| Column | Notes |
|--------|-------|
| `id` | `'latest'` row holds current JSON cache |
| `sessions` | jsonb array of session objects |
| `scrape_meta` | jsonb: `datesCheckedDuringScrape`, `datesCheckedEmpty`, tier timestamps |
| `last_successful_scrape` | timestamptz |
| `last_scrape_attempt` | timestamptz |
| `last_scrape_error` | text |

### `watchlist_items`

All columns in [`supabase/schema.sql`](supabase/schema.sql). Server reads/writes via `reloadWatchlistFromSupabase`, `/api/watchlist`, `/api/watchlist/sync`.

### `notification_events`

Insert fields: `user_key`, `session_key`, `event_type`, `ntfy_topic`, `message`, `sent_ok`, `error`, `previous_available`, `current_available`, `previous_slots`, `current_slots`, `event_reason`.

### `availability_snapshots` / `scrape_runs`

Defined in schema.sql; written during scrapes, read by analytics/debug endpoints.

---

All dates use **`America/New_York`** (Atlantic Park local). `iso_date` in Supabase is the park calendar day; `start_ts` is absolute UTC epoch. UI computes Today/Tomorrow at render time — never from stored `day_label`.

---

## API routes

| Route | Purpose |
|-------|---------|
| `GET /api/status` | Global scrape meta + full session list (after `ensureSessionsForStatus`) |
| `GET /api/sessions?date=YYYY-MM-DD` | **Canonical Browse source** — rows for one date |
| `GET /api/sessions` | Legacy: same as status payload when no `date` param |
| `GET /api/schema/health` | Table probe results, `missingTables`, actionable message |
| `GET /api/watchlist?user_key=` | User watchlist |
| `GET /api/debug/date/:isoDate` | Date-level debug + enrichment queue rows |
| `GET /api/debug/collector` | Scheduler state: `tier1IntervalConfigured`, `tier1LastAttemptAt`, `tier1LastCompletedAt`, `tier1LastSkipReason`, `tier1TargetDates`, `tier1LastResult`, recent scrape_runs |
| `POST /api/admin/run-tier1` | Manually trigger Tier 1 (park-local today + tomorrow). Use `?wait=true` for `targetDates`, `sessionsFound`, `rowsUpserted`, `skipReason`, `blockingScrapeTier`. |
| `POST /api/admin/run-tier2` | Manually trigger Tier 2 scrape (next 7 days) |
| `POST /api/admin/run-tier3` | Manually trigger Tier 3 scrape (weeks 2–3) |
| `GET /api/debug/boot` | Park timezone, saved session counts, why Browse would show not_checked |
| `GET /api/debug/enrichment` | Enrichment queue, stale/missing counts, run duration, recent errors |
| `POST /api/admin/backfill-current-sessions` | Restore `current_sessions` from snapshot/history after schema reset |
| `POST /api/admin/enrich-date` | Force detail enrichment for all open sessions on a date. Returns reconciled outcome counts (`sessionsUpdatedWithSlots`, `sessionsFailedCookieOverlay`, etc.), `skipReason` when skipped, cookie diagnostics, and `unchangedReasons`. |
| `POST /api/admin/repair-detail-data` | Clear unverified/default-like/mismatched detail metrics while preserving basic session rows and verified detail. Optional `{ isoDate, dryRun }`. |
| `GET /api/debug/coverage` | Expected vs actual dates across sources |

---

## `GET /api/sessions?date=YYYY-MM-DD` response contract

```json
{
  "isoDate": "2026-06-28",
  "sessions": [],
  "sessionsCount": 0,
  "dataSource": "supabase/current_sessions",
  "statusReason": "saved_sessions_found | fallback_sessions_found | checked_no_sessions | not_checked | schema_error | error",
  "lastCheckedForDate": "2026-06-27T12:00:00.000Z",
  "wasDateChecked": true,
  "isScrapeInProgress": false,
  "hasSavedSessions": false,
  "lastSuccessfulScrape": "2026-06-27T12:00:00.000Z",
  "error": null,
  "schemaError": null,
  "dateCoverage": {},
  "schemaHealth": { "missingTables": [], "currentSessionsAvailable": true }
}
```

### `dataSource` values

| Value | Meaning |
|-------|---------|
| `supabase/current_sessions` | Rows from `current_sessions` |
| `supabase/scrape_snapshots_fallback` | Legacy JSON blob from `scrape_snapshots` (table missing or empty) |
| `memory-fallback` | In-memory only (no Supabase) |
| `schema-missing` | `current_sessions` absent and no fallback data |

### `statusReason` values

| Value | UI message |
|-------|------------|
| `saved_sessions_found` | Sessions list from `current_sessions` |
| `fallback_sessions_found` | Sessions from snapshot/history fallback |
| `checked_no_sessions` | No sessions found for this date |
| `not_checked` | Not checked yet — **only when no source has rows** |
| `schema_error` | Database schema missing. Run supabase/schema.sql. |
| `error` | Actionable error string from `error` field |

---

## Frontend dependencies

Browse rendering uses **only** `GET /api/sessions?date=` for the selected date:

- `payload.sessions` → `selectedDateSessions`
- `payload.statusReason`, `payload.dataSource`, `payload.sessionsCount`
- `payload.error` / `payload.schemaError` → user-facing schema message

`/api/status` is used for header meta, scrape progress, watchlist counts, and background refresh — not for filtering sessions by date client-side.

---

## Data source alignment

Both endpoints prefer the same chain:

1. **`current_sessions`** (canonical)
2. **`scrape_snapshots`** JSON fallback (labeled `supabase/scrape_snapshots_fallback`)
3. In-memory cache (labeled `memory-fallback`)

On startup the server runs `auditSupabaseSchema()` and logs missing tables. `/api/status` includes `schemaHealth`, `schemaMissingTables`, and `schemaActionRequired`.

---

## Schema idempotency

[`supabase/schema.sql`](supabase/schema.sql) uses:

- `create table if not exists`
- `create index if not exists`
- `alter table … add column if not exists`

Run the **entire file** in the SQL editor. Partial runs (migration section only) will fail if base tables were never created.

---

## Verification checklist

See [TESTING.md — Schema / data contract verification](TESTING.md#schema--data-contract-verification).
