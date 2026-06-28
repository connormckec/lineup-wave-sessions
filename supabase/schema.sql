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
  error text
);

create index if not exists notification_events_session_idx
  on notification_events (session_key, created_at desc);
