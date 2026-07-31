'use strict';

const { maskDestination } = require('./notification-topic');
const pushSubscriptionStore = require('./push-subscription-store');

async function fetchRecentChangeEvents(supabase, { limit = 50 } = {}) {
  const { data, error } = await supabase
    .from('session_change_events')
    .select('id, park, session_key, iso_date, event_type, previous_available, new_available, previous_slots, new_slots, threshold_scanned_at, source_job_id, test_event, created_at, dedupe_key')
    .order('created_at', { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function fetchDeliveriesByStatus(supabase, status, { limit = 50, provider = null } = {}) {
  let query = supabase
    .from('notification_deliveries')
    .select('id, change_event_id, watch_id, user_key, provider, push_subscription_id, status, attempts, next_attempt_at, claimed_at, claimed_by, last_attempt_at, sent_at, provider_status, last_error, dedupe_key, created_at, updated_at')
    .eq('status', status)
    .order('created_at', { ascending: false })
    .limit(limit);
  if (provider) query = query.eq('provider', provider);
  const { data, error } = await query;
  if (error) throw error;
  return (data || []).map(maskDeliveryRow);
}

async function fetchDeliverySummary(supabase) {
  const statuses = ['pending', 'claimed', 'retryable', 'sent', 'terminal_failed'];
  const counts = {};
  for (const status of statuses) {
    const { count, error } = await supabase
      .from('notification_deliveries')
      .select('*', { count: 'exact', head: true })
      .eq('status', status);
    if (error) throw error;
    counts[status] = count || 0;
  }
  return counts;
}

function maskDeliveryRow(row) {
  if (!row) return row;
  return {
    ...row,
    destination: maskDestination(row.destination),
    user_key: row.user_key ? `${String(row.user_key).slice(0, 12)}…` : null,
  };
}

async function fetchPushSubscriptionSummary(supabase) {
  const { count: activeCount, error: activeError } = await supabase
    .from('push_subscriptions')
    .select('*', { count: 'exact', head: true })
    .eq('active', true);
  if (activeError) throw activeError;

  const { count: inactiveCount, error: inactiveError } = await supabase
    .from('push_subscriptions')
    .select('*', { count: 'exact', head: true })
    .eq('active', false);
  if (inactiveError) throw inactiveError;

  const { data: recent, error: recentError } = await supabase
    .from('push_subscriptions')
    .select('id, user_key, device_install_id, device_label, active, endpoint_hash, last_success_at, last_failure_at, last_error_code, consecutive_failures, disabled_at, updated_at')
    .order('updated_at', { ascending: false })
    .limit(25);
  if (recentError) throw recentError;

  return {
    activeSubscriptions: activeCount || 0,
    inactiveSubscriptions: inactiveCount || 0,
    recentSubscriptions: (recent || []).map((row) => ({
      subscriptionId: row.id,
      userKeyPrefix: row.user_key ? `${String(row.user_key).slice(0, 12)}…` : null,
      deviceInstallId: row.device_install_id,
      deviceLabel: row.device_label,
      active: row.active,
      endpointHashPrefix: pushSubscriptionStore.endpointHashPrefix(row.endpoint_hash),
      lastSuccessAt: row.last_success_at,
      lastFailureAt: row.last_failure_at,
      lastErrorCode: row.last_error_code,
      consecutiveFailures: row.consecutive_failures,
      disabledAt: row.disabled_at,
      updatedAt: row.updated_at,
    })),
  };
}

async function fetchProfilesWithWatchesButNoPush(supabase, { limit = 50 } = {}) {
  const { data: watches, error } = await supabase
    .from('watchlist_items')
    .select('user_key')
    .eq('active', true)
    .limit(limit);
  if (error) throw error;
  const userKeys = [...new Set((watches || []).map((w) => w.user_key).filter(Boolean))];
  const missing = [];
  for (const userKey of userKeys) {
    const subs = await pushSubscriptionStore.listActiveSubscriptionsForUser(supabase, userKey);
    if (!subs.length) {
      missing.push({ userKeyPrefix: `${String(userKey).slice(0, 12)}…` });
    }
  }
  return missing;
}

async function fetchWatchesWithoutDestinations(supabase, { limit = 50, deliveryProvider = 'ntfy' } = {}) {
  const { data: watches, error } = await supabase
    .from('watchlist_items')
    .select('id, user_key, session_key, iso_date, active, ntfy_topic')
    .eq('active', true)
    .limit(limit);
  if (error) throw error;

  const missing = [];
  for (const watch of watches || []) {
    if (deliveryProvider === 'webpush') {
      const subs = await pushSubscriptionStore.listActiveSubscriptionsForUser(supabase, watch.user_key);
      if (!subs.length) {
        missing.push({
          watchId: watch.id,
          session_key: watch.session_key,
          user_key: `${String(watch.user_key).slice(0, 12)}…`,
          reason: 'no_push_subscriptions',
        });
      }
      continue;
    }

    const { data: profile } = await supabase
      .from('notification_profiles')
      .select('ntfy_topic, topic_valid')
      .eq('user_key', watch.user_key)
      .maybeSingle();
    const topic = (profile?.ntfy_topic || watch.ntfy_topic || '').trim();
    if (!topic || profile?.topic_valid === false) {
      missing.push({
        watchId: watch.id,
        session_key: watch.session_key,
        user_key: `${String(watch.user_key).slice(0, 12)}…`,
      });
    }
  }
  return missing;
}

async function buildDiagnosticsPayload(supabase, { limit = 25, deliveryProvider = 'ntfy' } = {}) {
  const [
    recentEvents,
    pending,
    claimed,
    retryable,
    sent,
    terminalFailed,
    summary,
    watchesWithoutDestinations,
    pushSummary,
    webPushPending,
    webPushRetryable,
    webPushSent,
    webPushTerminal,
  ] = await Promise.all([
    fetchRecentChangeEvents(supabase, { limit }),
    fetchDeliveriesByStatus(supabase, 'pending', { limit }),
    fetchDeliveriesByStatus(supabase, 'claimed', { limit }),
    fetchDeliveriesByStatus(supabase, 'retryable', { limit }),
    fetchDeliveriesByStatus(supabase, 'sent', { limit }),
    fetchDeliveriesByStatus(supabase, 'terminal_failed', { limit }),
    fetchDeliverySummary(supabase),
    fetchWatchesWithoutDestinations(supabase, { limit, deliveryProvider }),
    fetchPushSubscriptionSummary(supabase).catch(() => ({
      activeSubscriptions: null,
      inactiveSubscriptions: null,
      recentSubscriptions: [],
    })),
    fetchDeliveriesByStatus(supabase, 'pending', { limit, provider: 'webpush' }),
    fetchDeliveriesByStatus(supabase, 'retryable', { limit, provider: 'webpush' }),
    fetchDeliveriesByStatus(supabase, 'sent', { limit, provider: 'webpush' }),
    fetchDeliveriesByStatus(supabase, 'terminal_failed', { limit, provider: 'webpush' }),
  ]);

  return {
    summary,
    recentChangeEvents: recentEvents,
    pendingDeliveries: pending,
    claimedDeliveries: claimed,
    retryableDeliveries: retryable,
    sentDeliveries: sent,
    terminalFailedDeliveries: terminalFailed,
    watchesWithoutDestinations,
    pushSubscriptions: pushSummary,
    webPushDeliveries: {
      pending: webPushPending,
      retryable: webPushRetryable,
      sent: webPushSent,
      terminalFailed: webPushTerminal,
    },
  };
}

async function buildWatchSessionDiagnostics(supabase, {
  userKey,
  sessionKey,
  deliveryProvider = 'webpush',
} = {}) {
  if (!userKey || !sessionKey) {
    return { ok: false, error: 'userKey_and_sessionKey_required' };
  }

  const occupancyView = require('./browse-availability-view');
  const pushSubscriptionStore = require('./push-subscription-store');

  const { data: watchRows, error: watchError } = await supabase
    .from('watchlist_items')
    .select('id, user_key, session_key, iso_date, active, created_at, updated_at')
    .eq('user_key', userKey)
    .eq('session_key', sessionKey)
    .order('updated_at', { ascending: false })
    .limit(5);
  if (watchError) throw watchError;

  const watch = (watchRows || []).find((row) => row.active) || watchRows?.[0] || null;

  const { data: sessionRows, error: sessionError } = await supabase
    .from('current_sessions')
    .select('*')
    .eq('session_key', sessionKey)
    .limit(1);
  if (sessionError && !isMissingTableError(sessionError)) throw sessionError;
  const sessionRow = sessionRows?.[0] || null;
  const occupancy = sessionRow ? occupancyView.getSessionOccupancyViewModel(sessionRow) : null;

  const { data: events, error: eventsError } = await supabase
    .from('session_change_events')
    .select('id, event_type, previous_slots, new_slots, previous_available, new_available, threshold_scanned_at, created_at, dedupe_key')
    .eq('session_key', sessionKey)
    .order('created_at', { ascending: false })
    .limit(10);
  if (eventsError) throw eventsError;

  let deliveries = [];
  if (watch?.id && events?.length) {
    const eventIds = events.map((e) => e.id);
    const { data: deliveryRows, error: deliveryError } = await supabase
      .from('notification_deliveries')
      .select('id, change_event_id, watch_id, status, attempts, last_attempt_at, sent_at, provider_status, last_error, created_at, updated_at, provider, push_subscription_id')
      .eq('watch_id', watch.id)
      .in('change_event_id', eventIds)
      .order('created_at', { ascending: false })
      .limit(20);
    if (deliveryError) throw deliveryError;
    deliveries = (deliveryRows || []).map(maskDeliveryRow);
  }

  const pushSubs = await pushSubscriptionStore.listActiveSubscriptionsForUser(supabase, userKey).catch(() => []);

  const lastEvent = events?.[0] || null;
  const lastDelivery = deliveries[0] || null;
  let noNotificationReason = null;
  if (!watch) noNotificationReason = 'no_watchlist_item';
  else if (!watch.active) noNotificationReason = 'watch_inactive';
  else if (!occupancy?.hasTrustedSlots) noNotificationReason = 'no_trusted_occupancy';
  else if (!lastEvent) noNotificationReason = 'no_change_event_recorded';
  else if (!deliveries.length) noNotificationReason = 'no_delivery_rows_for_watch';
  else if (lastDelivery?.status === 'sent') noNotificationReason = null;
  else if (lastDelivery?.status === 'terminal_failed') noNotificationReason = `delivery_failed:${lastDelivery.last_error || 'unknown'}`;
  else if (!pushSubs.length && deliveryProvider === 'webpush') noNotificationReason = 'no_active_push_subscriptions';

  return {
    ok: true,
    watchlistItem: watch ? {
      id: watch.id,
      active: watch.active,
      sessionKey: watch.session_key,
      isoDate: watch.iso_date,
      updatedAt: watch.updated_at,
    } : null,
    sessionKey,
    occupancy: occupancy ? {
      spotsLeft: occupancy.spotsLeft,
      capacity: occupancy.capacity,
      bookedCount: occupancy.bookedCount,
      isOpen: occupancy.isOpen,
      isFull: occupancy.isFull,
      hasTrustedSlots: occupancy.hasTrustedSlots,
      verifiedAt: occupancy.verifiedAt,
      source: occupancy.source,
      confidence: occupancy.confidence,
      diagnosticWarnings: occupancy.diagnosticWarnings,
    } : null,
    scheduleStatus: sessionRow?.available,
    lastThresholdVerification: occupancy?.verifiedAt || null,
    previousCanonicalAvailability: lastEvent ? {
      previousSlots: lastEvent.previous_slots,
      newSlots: lastEvent.new_slots,
      previousAvailable: lastEvent.previous_available,
      newAvailable: lastEvent.new_available,
      eventType: lastEvent.event_type,
      createdAt: lastEvent.created_at,
    } : null,
    lastDetectedTransition: lastEvent,
    notificationEventCreated: !!lastEvent,
    lastDeliveryAttempt: lastDelivery,
    activePushSubscriptionCount: pushSubs.length,
    pushSubscriptions: pushSubs.map((sub) => ({
      id: sub.id,
      active: sub.active !== false,
      endpointHashPrefix: pushSubscriptionStore.endpointHashPrefix(sub.endpoint_hash),
      lastSuccessAt: sub.last_success_at,
      lastFailureAt: sub.last_failure_at,
      lastErrorCode: sub.last_error_code,
    })),
    noNotificationReason,
  };
}

function isMissingTableError(error) {
  const code = error?.code || '';
  const msg = String(error?.message || '').toLowerCase();
  return code === '42P01' || msg.includes('does not exist');
}

module.exports = {
  maskDeliveryRow,
  fetchRecentChangeEvents,
  fetchDeliveriesByStatus,
  fetchDeliverySummary,
  fetchPushSubscriptionSummary,
  fetchProfilesWithWatchesButNoPush,
  fetchWatchesWithoutDestinations,
  buildDiagnosticsPayload,
  buildWatchSessionDiagnostics,
};
