-- Notification pipeline: profiles, change events, deliveries, claim RPC
-- Run in Supabase SQL editor after review.

create table if not exists notification_profiles (
  user_key text primary key,
  ntfy_topic text,
  topic_valid boolean not null default false,
  topic_updated_at timestamptz,
  auth_version integer not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists session_change_events (
  id uuid primary key default gen_random_uuid(),
  park text not null default 'atlantic_park',
  session_key text not null,
  iso_date date,
  event_type text not null,
  previous_available boolean,
  new_available boolean,
  previous_slots integer,
  new_slots integer,
  threshold_scanned_at timestamptz not null,
  source_job_id uuid,
  dedupe_key text not null unique,
  test_event boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists session_change_events_session_created_idx
  on session_change_events (session_key, created_at desc);

create index if not exists session_change_events_created_idx
  on session_change_events (created_at desc);

create table if not exists notification_deliveries (
  id uuid primary key default gen_random_uuid(),
  change_event_id uuid not null references session_change_events(id) on delete cascade,
  watch_id uuid not null references watchlist_items(id) on delete cascade,
  user_key text not null,
  provider text not null default 'ntfy',
  destination text not null,
  status text not null default 'pending',
  attempts integer not null default 0,
  next_attempt_at timestamptz,
  claimed_at timestamptz,
  claimed_by text,
  last_attempt_at timestamptz,
  sent_at timestamptz,
  provider_status integer,
  last_error text,
  dedupe_key text not null unique,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists notification_deliveries_status_next_attempt_idx
  on notification_deliveries (status, next_attempt_at);

create index if not exists notification_deliveries_change_event_idx
  on notification_deliveries (change_event_id);

create index if not exists notification_deliveries_user_key_idx
  on notification_deliveries (user_key, created_at desc);

create index if not exists watchlist_items_session_active_idx
  on watchlist_items (session_key)
  where active = true;

-- Backfill notification_profiles from existing watch rows (first non-null topic per user).
insert into notification_profiles (user_key, ntfy_topic, topic_valid, topic_updated_at, updated_at)
select distinct on (user_key)
  user_key,
  trim(ntfy_topic) as ntfy_topic,
  true as topic_valid,
  now() as topic_updated_at,
  now() as updated_at
from watchlist_items
where active = true
  and ntfy_topic is not null
  and trim(ntfy_topic) <> ''
order by user_key, updated_at desc nulls last, created_at desc
on conflict (user_key) do update
set ntfy_topic = excluded.ntfy_topic,
    topic_valid = excluded.topic_valid,
    topic_updated_at = excluded.topic_updated_at,
    updated_at = excluded.updated_at
where notification_profiles.ntfy_topic is null
   or trim(notification_profiles.ntfy_topic) = '';

create or replace function claim_notification_deliveries(
  batch_size integer,
  worker_id text,
  stale_seconds integer default 300
)
returns setof notification_deliveries
language plpgsql
as $$
begin
  return query
  with candidates as (
    select d.id
    from notification_deliveries d
    where (
      d.status = 'pending'
      and (d.next_attempt_at is null or d.next_attempt_at <= now())
    ) or (
      d.status = 'retryable'
      and (d.next_attempt_at is null or d.next_attempt_at <= now())
    ) or (
      d.status = 'claimed'
      and d.claimed_at is not null
      and d.claimed_at < now() - make_interval(secs => stale_seconds)
    )
    order by d.next_attempt_at nulls first, d.created_at asc
    limit batch_size
    for update skip locked
  )
  update notification_deliveries d
  set status = 'claimed',
      claimed_at = now(),
      claimed_by = worker_id,
      updated_at = now()
  from candidates c
  where d.id = c.id
  returning d.*;
end;
$$;
