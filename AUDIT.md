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

## Required Supabase tables

| Table | Used by app | Purpose |
|-------|-------------|---------|
| `current_sessions` | **Yes — primary UI source** | Latest row per session; Browse date queries filter by `iso_date` |
| `scrape_snapshots` | Yes | Meta + JSON session blob (`id='latest'`); fallback if `current_sessions` missing |
| `availability_snapshots` | Yes | Append-only scrape history for analytics |
| `scrape_runs` | Yes | Scrape attempt audit log |
| `watchlist_items` | Yes | Per-user Lineup / ntfy watches |
| `notification_events` | Yes | Alert dedupe / debug log |
| `date_coverage` | **No** | Not referenced in code |
| `session_enrichment_queue` | **No** | Not referenced in code |

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

## API routes

| Route | Purpose |
|-------|---------|
| `GET /api/status` | Global scrape meta + full session list (after `ensureSessionsForStatus`) |
| `GET /api/sessions?date=YYYY-MM-DD` | **Canonical Browse source** — rows for one date |
| `GET /api/sessions` | Legacy: same as status payload when no `date` param |
| `GET /api/schema/health` | Table probe results, `missingTables`, actionable message |
| `GET /api/watchlist?user_key=` | User watchlist |
| `GET /api/debug/date/:isoDate` | Date-level debug |

---

## `GET /api/sessions?date=YYYY-MM-DD` response contract

```json
{
  "isoDate": "2026-06-28",
  "sessions": [],
  "sessionsCount": 0,
  "dataSource": "supabase/current_sessions",
  "statusReason": "saved_sessions_found | checked_no_sessions | not_checked | schema_error | error",
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
| `saved_sessions_found` | Sessions list |
| `checked_no_sessions` | No sessions found for this date |
| `not_checked` | Not checked yet |
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
