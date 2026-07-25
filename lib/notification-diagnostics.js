'use strict';

const { maskDestination } = require('./notification-topic');

async function fetchRecentChangeEvents(supabase, { limit = 50 } = {}) {
  const { data, error } = await supabase
    .from('session_change_events')
    .select('id, park, session_key, iso_date, event_type, previous_available, new_available, previous_slots, new_slots, threshold_scanned_at, source_job_id, test_event, created_at, dedupe_key')
    .order('created_at', { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function fetchDeliveriesByStatus(supabase, status, { limit = 50 } = {}) {
  const { data, error } = await supabase
    .from('notification_deliveries')
    .select('id, change_event_id, watch_id, user_key, provider, status, attempts, next_attempt_at, claimed_at, claimed_by, last_attempt_at, sent_at, provider_status, last_error, dedupe_key, created_at, updated_at')
    .eq('status', status)
    .order('created_at', { ascending: false })
    .limit(limit);
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

async function fetchWatchesWithoutDestinations(supabase, { limit = 50 } = {}) {
  const { data: watches, error } = await supabase
    .from('watchlist_items')
    .select('id, user_key, session_key, iso_date, active, ntfy_topic')
    .eq('active', true)
    .limit(limit);
  if (error) throw error;

  const missing = [];
  for (const watch of watches || []) {
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

async function buildDiagnosticsPayload(supabase, { limit = 25 } = {}) {
  const [
    recentEvents,
    pending,
    claimed,
    retryable,
    sent,
    terminalFailed,
    summary,
    watchesWithoutDestinations,
  ] = await Promise.all([
    fetchRecentChangeEvents(supabase, { limit }),
    fetchDeliveriesByStatus(supabase, 'pending', { limit }),
    fetchDeliveriesByStatus(supabase, 'claimed', { limit }),
    fetchDeliveriesByStatus(supabase, 'retryable', { limit }),
    fetchDeliveriesByStatus(supabase, 'sent', { limit }),
    fetchDeliveriesByStatus(supabase, 'terminal_failed', { limit }),
    fetchDeliverySummary(supabase),
    fetchWatchesWithoutDestinations(supabase, { limit }),
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
  };
}

module.exports = {
  maskDeliveryRow,
  fetchRecentChangeEvents,
  fetchDeliveriesByStatus,
  fetchDeliverySummary,
  fetchWatchesWithoutDestinations,
  buildDiagnosticsPayload,
};
