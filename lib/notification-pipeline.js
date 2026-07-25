'use strict';

const sessionChangeEvents = require('./session-change-events');
const notificationDeliveries = require('./notification-deliveries');
const notificationProvider = require('./notification-provider');
const pushSubscriptionStore = require('./push-subscription-store');
const { validateNtfyTopic } = require('./notification-topic');

function buildLineupClickUrl(isoDate, { lineupAppUrl } = {}) {
  const base = String(lineupAppUrl || '').replace(/\/$/, '');
  if (!base) return null;
  if (!isoDate) return base;
  return `${base}/?date=${encodeURIComponent(isoDate)}`;
}

function buildRelativeClickPath(isoDate) {
  if (!isoDate) return '/';
  return `/?date=${encodeURIComponent(isoDate)}`;
}

async function processThresholdSessionNotifications({
  supabase,
  previousSession,
  nextSession,
  watches = [],
  resolveDestination,
  listPushSubscriptions,
  deliveryProvider = 'ntfy',
  sourceJobId = null,
  dryRun = false,
  writeSucceeded = true,
  park = 'atlantic_park',
} = {}) {
  const eventRow = sessionChangeEvents.deriveSessionChangeEvent({
    previousSession,
    nextSession,
    park,
    sourceJobId,
    dryRun,
    writeSucceeded,
  });

  if (!eventRow) {
    return { ok: true, skipped: true, reason: 'no_event' };
  }

  if (!supabase) {
    return { ok: true, skipped: true, reason: 'supabase_unconfigured' };
  }

  if (deliveryProvider === 'disabled') {
    return { ok: true, skipped: true, reason: 'provider_disabled' };
  }

  try {
    const sessionKey = nextSession.key;
    const matchedWatches = watches.length
      ? watches.filter((w) => w.active !== false && w.session_key === sessionKey)
      : await notificationDeliveries.findActiveWatchesForSession(supabase, sessionKey, watches);

    const result = await notificationDeliveries.recordChangeEventAndDeliveries({
      supabase,
      eventRow,
      watches: matchedWatches,
      resolveDestination,
      deliveryProvider,
      listPushSubscriptions,
    });

    return { ok: true, ...result };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

async function processDeliveryBatch({
  supabase,
  workerId,
  batchSize = 25,
  staleClaimSeconds = 300,
  lineupAppUrl,
  loadEventById,
  loadWatchById,
  resolveDestination,
  loadPushSubscriptionById,
  onVapidConfigError,
} = {}) {
  if (!supabase) {
    return { ok: false, error: 'supabase_unconfigured' };
  }

  const { data: claimed, error: claimError } = await supabase.rpc('claim_notification_deliveries', {
    batch_size: batchSize,
    worker_id: workerId,
    stale_seconds: staleClaimSeconds,
  });
  if (claimError) throw claimError;

  const results = [];
  for (const delivery of claimed || []) {
    try {
      const watch = await loadWatchById(delivery.watch_id);
      if (!watch || watch.active === false) {
        const cancelled = await notificationDeliveries.cancelDelivery(
          supabase,
          delivery,
          'watch_inactive_or_missing',
        );
        results.push({ id: delivery.id, status: cancelled.status, error: cancelled.last_error });
        continue;
      }

      const event = await loadEventById(delivery.change_event_id);
      const copy = sessionChangeEvents.buildNotificationCopy(event, watch);
      const clickUrl = buildLineupClickUrl(event?.iso_date, { lineupAppUrl });
      const relativeClickPath = buildRelativeClickPath(event?.iso_date);

      if (delivery.provider === 'webpush') {
        const subRow = delivery.push_subscription_id
          ? await loadPushSubscriptionById(delivery.push_subscription_id)
          : null;
        const sendSub = pushSubscriptionStore.toSendSubscription(subRow);
        if (!sendSub) {
          const cancelled = await notificationDeliveries.cancelDelivery(
            supabase,
            delivery,
            'push_subscription_inactive',
          );
          results.push({ id: delivery.id, status: cancelled.status, error: cancelled.last_error });
          continue;
        }

        const sendResult = await notificationProvider.sendNotification({
          provider: 'webpush',
          subscription: sendSub,
          title: copy.title,
          message: copy.message,
          clickUrl: relativeClickPath,
          eventType: event?.event_type,
          sessionKey: event?.session_key,
          isoDate: event?.iso_date,
          deliveryId: delivery.id,
          testEvent: event?.test_event === true,
          lineupOrigin: lineupAppUrl,
        });

        if (sendResult.ok) {
          await pushSubscriptionStore.recordSubscriptionSuccess(supabase, sendSub.id);
        } else if (sendResult.deactivateSubscription) {
          await pushSubscriptionStore.deactivateSubscriptionById(supabase, sendSub.id, sendResult.error);
        } else if (!sendResult.ok) {
          await pushSubscriptionStore.recordSubscriptionFailure(supabase, sendSub.id, sendResult.error);
        }
        if (sendResult.vapidConfigError && typeof onVapidConfigError === 'function') {
          onVapidConfigError(sendResult.error);
        }

        const updated = await notificationDeliveries.updateDeliveryAfterAttempt(supabase, delivery, sendResult, {
          vapidConfigError: sendResult.vapidConfigError,
        });
        results.push({
          id: delivery.id,
          status: updated.status,
          error: updated.last_error || null,
        });
        continue;
      }

      if (delivery.provider !== 'ntfy') {
        const cancelled = await notificationDeliveries.cancelDelivery(
          supabase,
          delivery,
          'unsupported_provider',
        );
        results.push({ id: delivery.id, status: cancelled.status, error: cancelled.last_error });
        continue;
      }

      const dest = await resolveDestination(delivery.user_key);
      if (!dest?.ok || !dest.destination) {
        const cancelled = await notificationDeliveries.cancelDelivery(
          supabase,
          delivery,
          dest?.error || 'no_destination',
        );
        results.push({ id: delivery.id, status: cancelled.status, error: cancelled.last_error });
        continue;
      }

      const topicValidation = validateNtfyTopic(dest.destination);
      if (!topicValidation.ok) {
        await notificationDeliveries.updateDeliveryAfterAttempt(supabase, delivery, {
          ok: false,
          error: topicValidation.error,
          transient: false,
        }, { invalidDestination: true });
        results.push({ id: delivery.id, status: 'terminal_failed', error: topicValidation.error });
        continue;
      }

      const sendResult = await notificationProvider.sendNotification({
        provider: 'ntfy',
        destination: dest.destination,
        title: copy.title,
        message: copy.message,
        clickUrl,
        deliveryId: delivery.id,
        testEvent: event?.test_event === true,
      });

      const updated = await notificationDeliveries.updateDeliveryAfterAttempt(supabase, delivery, sendResult);
      results.push({
        id: delivery.id,
        status: updated.status,
        error: updated.last_error || null,
      });
    } catch (err) {
      results.push({ id: delivery.id, status: 'error', error: err.message || String(err) });
    }
  }

  return {
    ok: true,
    claimed: (claimed || []).length,
    results,
  };
}

module.exports = {
  buildLineupClickUrl,
  buildRelativeClickPath,
  processThresholdSessionNotifications,
  processDeliveryBatch,
};
