-- Latest scrape cache for fast cold starts (existing)
create table if not exists scrape_snapshots (
  id text primary key,
  sessions jsonb not null default '[]'::jsonb,
  scrape_meta jsonb not null default '{}'::jsonb,
  last_successful_scrape timestamptz,
  last_scrape_attempt timestamptz,
  last_scrape_error text,
  updated_at timestamptz not null default now()
);

-- Historical availability for heat-map / analytics (append-only)
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
