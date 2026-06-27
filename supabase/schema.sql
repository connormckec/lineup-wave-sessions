create table if not exists scrape_snapshots (
  id text primary key,
  sessions jsonb not null default '[]'::jsonb,
  scrape_meta jsonb not null default '{}'::jsonb,
  last_successful_scrape timestamptz,
  last_scrape_attempt timestamptz,
  last_scrape_error text,
  updated_at timestamptz not null default now()
);
