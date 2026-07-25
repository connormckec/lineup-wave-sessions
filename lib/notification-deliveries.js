'use strict';

const sessionChangeEvents = require('./session-change-events');
const notificationProfileStore = require('./notification-profile-store');

const RETRY_DELAYS_MS = [
  0,
  60_000,
  5 * 60_000,
  15 * 60_000,
  60 * 60_000,
  6 * 60 * 60_000,
];

const MAX_ATTEMPTS = RETRY_DELAYS_MS.length;

function isUniqueViolation(error) {
  const code = error?.code || '';
  const msg = String(error?.message || '').toLowerCase();
  return code === '23505' || msg.includes('duplicate') || msg.includes('unique');
}

function nextAttemptAt(attemptNumber) {
  const delay = RETRY_DELAYS_MS[Math.min(attemptNumber, RETRY_DELAYS_MS.length - 1)] ?? RETRY_DELAYS_MS[RETRY_DELAYS_MS.length - 1];
  return new Date(Date.now() + delay).toISOString();
}

async function insertSessionChangeEvent(supabase, eventRow) {
  const { data, error } = await supabase
    .from('session_change_events')
    .insert(eventRow)
    .select('*')
    .single();
  if (error) {
    if (isUniqueViolation(error)) {
      return { ok: true, duplicate: true, event: null };
    }
    throw error;
  }
  return { ok: true, duplicate: false, event: data };
}

async function createDeliveriesForEvent(supabase, changeEvent, watches, {
  resolveDestination,
} = {}) {
  const created = [];
  const skipped = [];

  for (const watch of watches) {
    if (!watch?.id || watch.active === false) {
      skipped.push({ watchId: watch?.id, reason: 'inactive' });
      continue;
    }

    const dest = await resolveDestination(watch.user_key);
    if (!dest?.ok || !dest.destination) {
      skipped.push({ watchId: watch.id, reason: dest?.error || 'no_destination' });
      continue;
    }

    const dedupeKey = sessionChangeEvents.buildDeliveryDedupeKey({
      changeEventId: changeEvent.id,
      watchId: watch.id,
      provider: 'ntfy',
    });

    const row = {
      change_event_id: changeEvent.id,
      watch_id: watch.id,
      user_key: watch.user_key,
      provider: 'ntfy',
      destination: dest.destination,
      status: 'pending',
      attempts: 0,
      next_attempt_at: new Date().toISOString(),
      dedupe_key: dedupeKey,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from('notification_deliveries')
      .insert(row)
      .select('*')
      .single();

    if (error) {
      if (isUniqueViolation(error)) {
        skipped.push({ watchId: watch.id, reason: 'duplicate_delivery' });
        continue;
      }
      throw error;
    }
    created.push(data);
  }

  return { created, skipped };
}

async function findActiveWatchesForSession(supabase, sessionKey, inMemoryWatches = []) {
  const memoryMatches = (inMemoryWatches || []).filter(
    (w) => w.active !== false && w.session_key === sessionKey,
  );
  if (!supabase) return memoryMatches;

  const { data, error } = await supabase
    .from('watchlist_items')
    .select('*')
    .eq('session_key', sessionKey)
    .eq('active', true);
  if (error) throw error;

  const byId = new Map();
  for (const row of data || []) byId.set(row.id, row);
  for (const row of memoryMatches) byId.set(row.id, row);
  return [...byId.values()];
}

async function recordChangeEventAndDeliveries({
  supabase,
  eventRow,
  watches,
  resolveDestination,
}) {
  const insertResult = await insertSessionChangeEvent(supabase, eventRow);
  if (insertResult.duplicate) {
    return { ok: true, duplicateEvent: true, event: null, deliveries: [] };
  }

  const deliveryResult = await createDeliveriesForEvent(
    supabase,
    insertResult.event,
    watches,
    { resolveDestination },
  );

  return {
    ok: true,
    duplicateEvent: false,
    event: insertResult.event,
    deliveries: deliveryResult.created,
    skippedDeliveries: deliveryResult.skipped,
  };
}

function classifyDeliveryFailure(result, { invalidDestination = false } = {}) {
  if (invalidDestination) {
    return { status: 'terminal_failed', transient: false };
  }
  if (result?.ok) {
    return { status: 'sent', transient: false };
  }
  if (result?.transient) {
    return { status: 'retryable', transient: true };
  }
  return { status: 'terminal_failed', transient: false };
}

async function updateDeliveryAfterAttempt(supabase, delivery, result, {
  invalidDestination = false,
} = {}) {
  const now = new Date().toISOString();
  const attempts = (delivery.attempts || 0) + 1;
  const classification = classifyDeliveryFailure(result, { invalidDestination });

  if (classification.status === 'sent') {
    const { data, error } = await supabase
      .from('notification_deliveries')
      .update({
        status: 'sent',
        attempts,
        last_attempt_at: now,
        sent_at: now,
        provider_status: result.providerStatus,
        last_error: null,
        updated_at: now,
      })
      .eq('id', delivery.id)
      .select('*')
      .single();
    if (error) throw error;
    return data;
  }

  const exhausted = attempts >= MAX_ATTEMPTS;
  const nextStatus = exhausted ? 'terminal_failed' : classification.status;
  const patch = {
    status: nextStatus,
    attempts,
    last_attempt_at: now,
    provider_status: result?.providerStatus ?? null,
    last_error: result?.error || 'delivery_failed',
    next_attempt_at: exhausted ? null : nextAttemptAt(attempts),
    updated_at: now,
  };

  const { data, error } = await supabase
    .from('notification_deliveries')
    .update(patch)
    .eq('id', delivery.id)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

async function cancelDelivery(supabase, delivery, reason = 'cancelled') {
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from('notification_deliveries')
    .update({
      status: 'cancelled',
      last_error: reason,
      updated_at: now,
    })
    .eq('id', delivery.id)
    .select('*')
    .single();
  if (error) throw error;
  return data;
}

module.exports = {
  RETRY_DELAYS_MS,
  MAX_ATTEMPTS,
  isUniqueViolation,
  nextAttemptAt,
  insertSessionChangeEvent,
  createDeliveriesForEvent,
  findActiveWatchesForSession,
  recordChangeEventAndDeliveries,
  classifyDeliveryFailure,
  updateDeliveryAfterAttempt,
  cancelDelivery,
};
