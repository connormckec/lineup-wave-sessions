-- Notification pipeline hardening: RLS, grants, RPC permissions, FK behavior
-- Run after 202607251400_notification_pipeline.sql

alter table if exists notification_profiles enable row level security;
alter table if exists session_change_events enable row level security;
alter table if exists notification_deliveries enable row level security;

revoke all on table notification_profiles from public, anon, authenticated;
revoke all on table session_change_events from public, anon, authenticated;
revoke all on table notification_deliveries from public, anon, authenticated;

grant all on table notification_profiles to service_role;
grant all on table session_change_events to service_role;
grant all on table notification_deliveries to service_role;

alter table if exists notification_deliveries
  drop constraint if exists notification_deliveries_watch_id_fkey;

alter table if exists notification_deliveries
  add constraint notification_deliveries_watch_id_fkey
  foreign key (watch_id) references watchlist_items(id) on delete set null;

alter table if exists notification_deliveries
  alter column watch_id drop not null;

create or replace function claim_notification_deliveries(
  batch_size integer,
  worker_id text,
  stale_seconds integer default 300
)
returns setof notification_deliveries
language plpgsql
security definer
set search_path = public
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

revoke all on function claim_notification_deliveries(integer, text, integer) from public;
revoke all on function claim_notification_deliveries(integer, text, integer) from anon;
revoke all on function claim_notification_deliveries(integer, text, integer) from authenticated;
grant execute on function claim_notification_deliveries(integer, text, integer) to service_role;
