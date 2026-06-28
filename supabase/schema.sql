-- Lineup / Atlantic Park Surf Dashboard — Supabase schema
-- Safe to run repeatedly in the Supabase SQL editor (idempotent).
--
-- Step 1: Paste this entire file into Supabase → SQL → New query → Run.
-- Step 2: Confirm tables in Table Editor (especially current_sessions).
-- Step 3: Hit GET /api/schema/health and GET /api/sessions?date=YYYY-MM-DD on your server.
--
-- NOT used by the app (do not create unless you add features later):
--   date_coverage, session_enrichment_queue

-- Operational scrape state (meta only — sessions live in current_sessions)
create table if not exists scrape_snapshots (
  id text primary key,
  sessions jsonb not null default '[]'::jsonb,
  scrape_meta jsonb not null default '{}'::jsonb,
  last_successful_scrape timestamptz,
  last_scrape_attempt timestamptz,
  last_scrape_error text,
  updated_at timestamptz not null default now()
);

-- Latest known state of every session (source of truth for live UI)
create table if not exists current_sessions (
  park text not null default 'atlantic_park',
  session_key text not null,
  iso_date date,
  start_ts bigint,
  start_time text,
  display_date text,
  weekday text,
  wave_side text,
  session_type text,
  available boolean,
  slots_available integer,
  capacity integer,
  estimated_booked integer,
  fill_rate numeric,
  price_text text,
  price_min numeric,
  price_max numeric,
  currency text default 'USD',
  status_label text,
  source_tier integer,
  raw jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_scraped_at timestamptz not null default now(),
  primary key (park, session_key)
);

create index if not exists current_sessions_iso_date_idx
  on current_sessions (iso_date);

create index if not exists current_sessions_last_scraped_at_idx
  on current_sessions (last_scraped_at desc);

-- Historical availability for heat-map / fill-rate analytics (append-only)
create table if not exists availability_snapshots (
  id uuid primary key default gen_random_uuid(),
  scraped_at timestamptz not null default now(),
  park text not null default 'atlantic_park',
  session_key text not null,
  iso_date date not null,
  start_ts bigint,
  start_time text,
  weekday text,
  wave_side text,
  session_type text,
  available boolean,
  slots_available integer,
  capacity integer,
  estimated_booked integer,
  fill_rate numeric,
  price_text text,
  price_min numeric,
  price_max numeric,
  currency text default 'USD',
  status_label text,
  source_tier integer,
  raw jsonb
);

create index if not exists availability_snapshots_iso_date_idx
  on availability_snapshots (iso_date);

create index if not exists availability_snapshots_scraped_at_idx
  on availability_snapshots (scraped_at desc);

create index if not exists availability_snapshots_session_type_idx
  on availability_snapshots (session_type, iso_date);

-- Audit log of every scrape attempt
create table if not exists scrape_runs (
  id uuid primary key default gen_random_uuid(),
  park text not null default 'atlantic_park',
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  success boolean,
  tier text,
  sessions_found integer,
  dates_covered integer,
  missing_dates text[],
  coverage_percent integer,
  error text,
  error_stack text
);

create index if not exists scrape_runs_started_at_idx
  on scrape_runs (started_at desc);

create index if not exists scrape_runs_success_idx
  on scrape_runs (success, started_at desc);

-- Per-user session watchlist for ntfy alerts
create table if not exists watchlist_items (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  user_key text not null,
  ntfy_topic text,
  session_key text not null,
  iso_date date,
  start_ts bigint,
  start_time text,
  time text,
  date text,
  day_label text,
  wave integer,
  wave_side text,
  session_type text,
  alert_when_opens boolean not null default true,
  alert_when_low_slots boolean not null default true,
  low_slots_threshold integer not null default 2,
  alert_when_selling_fast boolean not null default true,
  fast_drop_threshold integer not null default 3,
  alert_last_call boolean not null default true,
  last_call_minutes_before integer not null default 120,
  watched_at timestamptz,
  initial_available boolean,
  initial_slots_available integer,
  last_seen_available boolean,
  last_seen_slots_available integer,
  last_seen_at timestamptz,
  active boolean not null default true
);

create unique index if not exists watchlist_items_user_session_idx
  on watchlist_items (user_key, session_key);

create index if not exists watchlist_items_user_key_idx
  on watchlist_items (user_key)
  where active = true;

-- Audit log of sent notifications (dedupe / debugging)
create table if not exists notification_events (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  user_key text,
  session_key text,
  event_type text,
  ntfy_topic text,
  message text,
  sent_ok boolean,
  error text,
  previous_available boolean,
  current_available boolean,
  previous_slots integer,
  current_slots integer,
  event_reason text
);

create index if not exists notification_events_session_idx
  on notification_events (session_key, created_at desc);

-- Column migrations for existing deployments (safe when tables exist from above)
alter table watchlist_items add column if not exists alert_when_selling_fast boolean not null default true;
alter table watchlist_items add column if not exists fast_drop_threshold integer not null default 3;
alter table watchlist_items add column if not exists alert_last_call boolean not null default true;
alter table watchlist_items add column if not exists last_call_minutes_before integer not null default 120;
alter table watchlist_items add column if not exists watched_at timestamptz;
alter table watchlist_items add column if not exists initial_available boolean;
alter table watchlist_items add column if not exists initial_slots_available integer;
alter table watchlist_items add column if not exists last_seen_available boolean;
alter table watchlist_items add column if not exists last_seen_slots_available integer;
alter table watchlist_items add column if not exists last_seen_at timestamptz;

alter table current_sessions add column if not exists capacity integer;
alter table current_sessions add column if not exists estimated_booked integer;
alter table current_sessions add column if not exists fill_rate numeric;
alter table current_sessions add column if not exists price_text text;
alter table current_sessions add column if not exists price_min numeric;
alter table current_sessions add column if not exists price_max numeric;
alter table current_sessions add column if not exists currency text default 'USD';

alter table availability_snapshots add column if not exists price_text text;
alter table availability_snapshots add column if not exists price_min numeric;
alter table availability_snapshots add column if not exists price_max numeric;
alter table availability_snapshots add column if not exists currency text default 'USD';

alter table scrape_runs add column if not exists coverage_percent integer;

alter table notification_events add column if not exists previous_available boolean;
alter table notification_events add column if not exists current_available boolean;
alter table notification_events add column if not exists previous_slots integer;
alter table notification_events add column if not exists current_slots integer;
alter table notification_events add column if not exists event_reason text;

-- scrape_meta jsonb in scrape_snapshots may include:
--   datesCheckedDuringScrape text[] — ISO dates the scraper has visited
--   datesCheckedEmpty text[] — ISO dates checked with zero sessions
--   lastFullCoverageScrape timestamptz — last tier 2+ successful run
