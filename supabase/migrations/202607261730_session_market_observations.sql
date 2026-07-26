-- Append-only session market observations (price + trusted inventory history)
-- Safe to run repeatedly in Supabase SQL editor.

create table if not exists session_market_observations (
  id uuid primary key default gen_random_uuid(),
  park text not null default 'atlantic_park',
  session_key text not null,
  observed_at timestamptz not null,
  session_start_at timestamptz,
  iso_date date,
  wave_side text,
  session_type text,
  available boolean,
  trusted_spots_remaining integer,
  availability_source text,
  threshold_scanned_at timestamptz,
  schedule_scanned_at timestamptz,
  inventory_freshness text,
  price_freshness text,
  price_display_text text,
  price_exact_cents integer,
  price_min_cents integer,
  price_max_cents integer,
  currency text default 'USD',
  price_source text,
  price_verified_at timestamptz,
  hours_until_session numeric,
  source_job_id uuid,
  observation_run_id uuid,
  observation_reason text,
  raw_evidence jsonb,
  dedupe_key text not null unique,
  observed_net_booking_delta integer,
  observation_interval_seconds numeric,
  price_changed_during_interval boolean,
  previous_price_display_text text,
  previous_trusted_spots_remaining integer,
  created_at timestamptz not null default now()
);

create index if not exists session_market_observations_session_key_idx
  on session_market_observations (session_key, observed_at desc);

create index if not exists session_market_observations_observed_at_idx
  on session_market_observations (observed_at desc);

create index if not exists session_market_observations_iso_date_idx
  on session_market_observations (iso_date, observed_at desc);

create index if not exists session_market_observations_observation_run_id_idx
  on session_market_observations (observation_run_id, observed_at desc);

create table if not exists session_product_price_observations (
  id uuid primary key default gen_random_uuid(),
  observation_id uuid not null references session_market_observations(id) on delete cascade,
  session_key text not null,
  product_key text not null,
  product_label text,
  price_cents integer,
  original_price_cents integer,
  available boolean default true,
  raw_text text,
  observed_at timestamptz not null,
  source text,
  confidence text,
  dedupe_key text not null unique,
  created_at timestamptz not null default now()
);

create index if not exists session_product_price_observations_observation_id_idx
  on session_product_price_observations (observation_id);

create index if not exists session_product_price_observations_session_key_idx
  on session_product_price_observations (session_key, observed_at desc);

-- Security: service_role only (no browser/anon access)
alter table if exists session_market_observations enable row level security;
alter table if exists session_product_price_observations enable row level security;

revoke all on table session_market_observations from public, anon, authenticated;
revoke all on table session_product_price_observations from public, anon, authenticated;

grant all on table session_market_observations to service_role;
grant all on table session_product_price_observations to service_role;
