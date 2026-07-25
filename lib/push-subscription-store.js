'use strict';

const crypto = require('crypto');

const MAX_ENDPOINT_LENGTH = 2048;
const MAX_KEY_LENGTH = 512;
const MAX_DEVICE_INSTALL_ID_LENGTH = 64;
const MAX_DEVICE_LABEL_LENGTH = 64;
const MAX_USER_AGENT_LENGTH = 512;
const MAX_PERMISSION_STATE_LENGTH = 32;
const ENDPOINT_HASH_PREFIX_LEN = 8;

function hashEndpoint(endpoint) {
  return crypto.createHash('sha256').update(String(endpoint)).digest('hex');
}

function endpointHashPrefix(endpointHash) {
  const text = String(endpointHash || '');
  if (!text) return '—';
  return text.slice(0, ENDPOINT_HASH_PREFIX_LEN);
}

function validateDeviceInstallId(raw) {
  const id = String(raw || '').trim();
  if (!id) return { ok: false, error: 'device_install_id_required' };
  if (id.length > MAX_DEVICE_INSTALL_ID_LENGTH) return { ok: false, error: 'device_install_id_too_long' };
  if (!/^[0-9a-f-]{8,64}$/i.test(id)) return { ok: false, error: 'device_install_id_invalid' };
  return { ok: true, deviceInstallId: id };
}

function validateSubscriptionShape(subscription) {
  if (!subscription || typeof subscription !== 'object') {
    return { ok: false, error: 'subscription_required' };
  }
  const endpoint = String(subscription.endpoint || '').trim();
  if (!endpoint) return { ok: false, error: 'endpoint_required' };
  if (endpoint.length > MAX_ENDPOINT_LENGTH) return { ok: false, error: 'endpoint_too_long' };
  try {
    const parsed = new URL(endpoint);
    if (parsed.protocol !== 'https:') return { ok: false, error: 'endpoint_must_be_https' };
  } catch {
    return { ok: false, error: 'endpoint_invalid' };
  }

  const keys = subscription.keys;
  if (!keys || typeof keys !== 'object') return { ok: false, error: 'subscription_keys_required' };
  const p256dh = String(keys.p256dh || '').trim();
  const auth = String(keys.auth || '').trim();
  if (!p256dh) return { ok: false, error: 'p256dh_required' };
  if (!auth) return { ok: false, error: 'auth_required' };
  if (p256dh.length > MAX_KEY_LENGTH || auth.length > MAX_KEY_LENGTH) {
    return { ok: false, error: 'subscription_keys_too_long' };
  }

  return {
    ok: true,
    subscription: {
      endpoint,
      expirationTime: subscription.expirationTime ?? null,
      keys: { p256dh, auth },
    },
  };
}

function validateSubscribePayload(body) {
  const sub = validateSubscriptionShape(body?.subscription);
  if (!sub.ok) return sub;
  const device = validateDeviceInstallId(body?.deviceInstallId);
  if (!device.ok) return device;

  const deviceLabel = body?.deviceLabel == null
    ? null
    : String(body.deviceLabel).trim().slice(0, MAX_DEVICE_LABEL_LENGTH) || null;
  const permissionState = body?.permissionState == null
    ? null
    : String(body.permissionState).trim().slice(0, MAX_PERMISSION_STATE_LENGTH) || null;
  const userAgent = body?.userAgent == null
    ? null
    : String(body.userAgent).trim().slice(0, MAX_USER_AGENT_LENGTH) || null;

  return {
    ok: true,
    subscription: sub.subscription,
    deviceInstallId: device.deviceInstallId,
    deviceLabel,
    permissionState,
    userAgent,
  };
}

function toSafeSubscriptionResponse(row) {
  if (!row) return null;
  return {
    subscriptionId: row.id,
    deviceInstallId: row.device_install_id,
    deviceLabel: row.device_label || null,
    active: row.active === true,
    permissionState: row.permission_state || null,
    endpointHashPrefix: endpointHashPrefix(row.endpoint_hash),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastSuccessAt: row.last_success_at || null,
    lastFailureAt: row.last_failure_at || null,
    lastErrorCode: row.last_error_code || null,
    consecutiveFailures: row.consecutive_failures ?? 0,
    disabledAt: row.disabled_at || null,
  };
}

function toSendSubscription(row) {
  if (!row || row.active === false) return null;
  return {
    id: row.id,
    endpoint: row.endpoint,
    keys: {
      p256dh: row.p256dh,
      auth: row.auth,
    },
    endpointHash: row.endpoint_hash,
    userKey: row.user_key,
    deviceInstallId: row.device_install_id,
  };
}

async function upsertPushSubscription(supabase, userKey, payload) {
  const validated = validateSubscribePayload(payload);
  if (!validated.ok) return validated;

  const now = new Date().toISOString();
  const endpointHash = hashEndpoint(validated.subscription.endpoint);

  const { data: existingDevice, error: deviceLookupError } = await supabase
    .from('push_subscriptions')
    .select('*')
    .eq('user_key', userKey)
    .eq('device_install_id', validated.deviceInstallId)
    .maybeSingle();
  if (deviceLookupError) throw deviceLookupError;

  const row = {
    user_key: userKey,
    device_install_id: validated.deviceInstallId,
    endpoint: validated.subscription.endpoint,
    endpoint_hash: endpointHash,
    p256dh: validated.subscription.keys.p256dh,
    auth: validated.subscription.keys.auth,
    user_agent: validated.userAgent,
    device_label: validated.deviceLabel,
    active: true,
    permission_state: validated.permissionState,
    updated_at: now,
    disabled_at: null,
    last_error_code: null,
    consecutive_failures: 0,
  };

  if (existingDevice?.id) {
    const { data, error } = await supabase
      .from('push_subscriptions')
      .update(row)
      .eq('id', existingDevice.id)
      .select('*')
      .single();
    if (error) throw error;
    return { ok: true, subscription: data, created: false };
  }

  const { data: byHash, error: hashError } = await supabase
    .from('push_subscriptions')
    .select('*')
    .eq('endpoint_hash', endpointHash)
    .maybeSingle();
  if (hashError) throw hashError;

  if (byHash?.id) {
    const { data, error } = await supabase
      .from('push_subscriptions')
      .update({ ...row, created_at: byHash.created_at })
      .eq('id', byHash.id)
      .select('*')
      .single();
    if (error) throw error;
    return { ok: true, subscription: data, created: false };
  }

  const { data, error } = await supabase
    .from('push_subscriptions')
    .insert({ ...row, created_at: now })
    .select('*')
    .single();
  if (error) throw error;
  return { ok: true, subscription: data, created: true };
}

async function listActiveSubscriptionsForUser(supabase, userKey) {
  const { data, error } = await supabase
    .from('push_subscriptions')
    .select('*')
    .eq('user_key', userKey)
    .eq('active', true)
    .order('updated_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

async function findSubscriptionById(supabase, id) {
  if (!id) return null;
  const { data, error } = await supabase
    .from('push_subscriptions')
    .select('*')
    .eq('id', id)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function findActiveSubscriptionForDevice(supabase, userKey, deviceInstallId) {
  const device = validateDeviceInstallId(deviceInstallId);
  if (!device.ok) return null;
  const { data, error } = await supabase
    .from('push_subscriptions')
    .select('*')
    .eq('user_key', userKey)
    .eq('device_install_id', device.deviceInstallId)
    .eq('active', true)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function deactivateSubscriptionForDevice(supabase, userKey, deviceInstallId) {
  const device = validateDeviceInstallId(deviceInstallId);
  if (!device.ok) return device;
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from('push_subscriptions')
    .update({
      active: false,
      disabled_at: now,
      updated_at: now,
      permission_state: 'unsubscribed',
    })
    .eq('user_key', userKey)
    .eq('device_install_id', device.deviceInstallId)
    .select('*')
    .maybeSingle();
  if (error) throw error;
  if (data?.id) {
    await cancelPendingDeliveriesForSubscription(supabase, data.id, 'subscription_deactivated');
  }
  return { ok: true, subscription: data || null };
}

async function deactivateSubscriptionById(supabase, subscriptionId, errorCode = 'expired') {
  if (!subscriptionId) return null;
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from('push_subscriptions')
    .update({
      active: false,
      disabled_at: now,
      updated_at: now,
      last_error_code: errorCode,
      last_failure_at: now,
    })
    .eq('id', subscriptionId)
    .select('*')
    .single();
  if (error) throw error;
  await cancelPendingDeliveriesForSubscription(supabase, subscriptionId, errorCode);
  return data;
}

async function recordSubscriptionSuccess(supabase, subscriptionId) {
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from('push_subscriptions')
    .update({
      last_success_at: now,
      updated_at: now,
      last_error_code: null,
      consecutive_failures: 0,
    })
    .eq('id', subscriptionId)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function recordSubscriptionFailure(supabase, subscriptionId, errorCode) {
  const existing = await findSubscriptionById(supabase, subscriptionId);
  if (!existing) return null;
  const now = new Date().toISOString();
  const failures = (existing.consecutive_failures || 0) + 1;
  const { data, error } = await supabase
    .from('push_subscriptions')
    .update({
      last_failure_at: now,
      updated_at: now,
      last_error_code: errorCode || 'delivery_failed',
      consecutive_failures: failures,
    })
    .eq('id', subscriptionId)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function cancelPendingDeliveriesForSubscription(supabase, subscriptionId, reason = 'subscription_inactive') {
  if (!subscriptionId) return { cancelled: 0 };
  const { data, error } = await supabase
    .from('notification_deliveries')
    .update({
      status: 'cancelled',
      last_error: reason,
      updated_at: new Date().toISOString(),
    })
    .eq('push_subscription_id', subscriptionId)
    .in('status', ['pending', 'retryable', 'claimed'])
    .select('id');
  if (error) throw error;
  return { cancelled: (data || []).length };
}

async function buildPushStatusForUser(supabase, userKey, deviceInstallId, { webPushConfigured = false } = {}) {
  const active = await listActiveSubscriptionsForUser(supabase, userKey);
  const deviceSub = deviceInstallId
    ? await findActiveSubscriptionForDevice(supabase, userKey, deviceInstallId)
    : null;
  const repairNeeded = !!deviceInstallId && !deviceSub && active.length > 0;

  return {
    configured: webPushConfigured,
    activeDeviceCount: active.length,
    thisDeviceActive: !!deviceSub,
    repairNeeded,
    device: toSafeSubscriptionResponse(deviceSub),
    devices: active.map(toSafeSubscriptionResponse),
  };
}

module.exports = {
  MAX_ENDPOINT_LENGTH,
  ENDPOINT_HASH_PREFIX_LEN,
  hashEndpoint,
  endpointHashPrefix,
  validateDeviceInstallId,
  validateSubscriptionShape,
  validateSubscribePayload,
  toSafeSubscriptionResponse,
  toSendSubscription,
  upsertPushSubscription,
  listActiveSubscriptionsForUser,
  findSubscriptionById,
  findActiveSubscriptionForDevice,
  deactivateSubscriptionForDevice,
  deactivateSubscriptionById,
  recordSubscriptionSuccess,
  recordSubscriptionFailure,
  cancelPendingDeliveriesForSubscription,
  buildPushStatusForUser,
};
