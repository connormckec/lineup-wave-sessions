-- Web Push subscriptions + delivery fan-out (additive; preserves ntfy history)

create table if not exists push_subscriptions (
  id uuid primary key default gen_random_uuid(),
  user_key text not null,
  device_install_id text not null,
  endpoint text not null,
  endpoint_hash text not null unique,
  p256dh text not null,
  auth text not null,
  user_agent text,
  device_label text,
  active boolean not null default true,
  permission_state text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_success_at timestamptz,
  last_failure_at timestamptz,
  last_error_code text,
  consecutive_failures integer not null default 0,
  disabled_at timestamptz
);

create unique index if not exists push_subscriptions_user_device_install_idx
  on push_subscriptions (user_key, device_install_id);

create index if not exists push_subscriptions_user_active_idx
  on push_subscriptions (user_key)
  where active = true;

create index if not exists push_subscriptions_device_install_idx
  on push_subscriptions (device_install_id);

alter table if exists notification_deliveries
  add column if not exists push_subscription_id uuid references push_subscriptions(id) on delete set null;

create index if not exists notification_deliveries_push_subscription_idx
  on notification_deliveries (push_subscription_id)
  where push_subscription_id is not null;

alter table if exists push_subscriptions enable row level security;

revoke all on table push_subscriptions from public, anon, authenticated;
grant all on table push_subscriptions to service_role;
