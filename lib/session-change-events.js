'use strict';

const crypto = require('crypto');
const thresholdMaintenance = require('./threshold-maintenance');
const trustedState = require('./trusted-session-state');

const EVENT_BECAME_AVAILABLE = 'became_available';
const EVENT_SPOTS_OPENED = 'spots_opened';

function buildEventDedupeKey({
  park = 'atlantic_park',
  sessionKey,
  eventType,
  thresholdScannedAt,
  newAvailable,
  newSlots,
}) {
  const canonical = [
    park,
    sessionKey,
    eventType,
    thresholdScannedAt,
    newAvailable === true ? '1' : newAvailable === false ? '0' : 'null',
    newSlots == null ? 'null' : String(newSlots),
  ].join('|');
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function buildDeliveryDedupeKey({ changeEventId, watchId, provider = 'ntfy' }) {
  return `${changeEventId}:${watchId}:${provider}`;
}

function hasAffirmativePackedEvidence(previousSession, prev) {
  if (previousSession?.available === false) return true;
  if (!prev?.trusted) return false;
  if (prev.available === false) return true;
  if (prev.slots === 0) return true;
  return false;
}

function deriveSessionChangeEvent({
  previousSession,
  nextSession,
  park = 'atlantic_park',
  sourceJobId = null,
  dryRun = false,
  writeSucceeded = true,
} = {}) {
  if (dryRun || !writeSucceeded || !nextSession?.key) {
    return null;
  }

  const prev = trustedState.extractTrustedAvailabilityState(previousSession);
  const next = trustedState.extractTrustedAvailabilityState(nextSession);

  if (!next.trusted || !next.thresholdScannedAt) {
    return null;
  }

  if (previousSession && !thresholdMaintenance.shouldApplyPreparedThresholdUpdate(previousSession, {
    threshold_scanned_at: next.thresholdScannedAt,
    thresholdScanAt: next.thresholdScannedAt,
  })) {
    return null;
  }

  if (trustedState.statesEqual(prev, next)) {
    return null;
  }

  const isoDate = nextSession.isoDate || nextSession.dateKey || trustedState.thresholdFieldOnSession(nextSession, 'iso_date') || null;

  if (next.available === true && next.slots != null && next.slots > 0) {
    if (hasAffirmativePackedEvidence(previousSession, prev)) {
      return buildEventRecord({
        park,
        sessionKey: nextSession.key,
        isoDate,
        eventType: EVENT_BECAME_AVAILABLE,
        prev,
        next,
        sourceJobId,
      });
    }

    if (
      prev.trusted
      && prev.slots != null
      && prev.slots >= 0
      && next.slots > prev.slots
    ) {
      return buildEventRecord({
        park,
        sessionKey: nextSession.key,
        isoDate,
        eventType: EVENT_SPOTS_OPENED,
        prev,
        next,
        sourceJobId,
      });
    }
  }

  return null;
}

function buildEventRecord({
  park,
  sessionKey,
  isoDate,
  eventType,
  prev,
  next,
  sourceJobId,
  testEvent = false,
}) {
  const dedupeKey = buildEventDedupeKey({
    park,
    sessionKey,
    eventType,
    thresholdScannedAt: next.thresholdScannedAt,
    newAvailable: next.available,
    newSlots: next.slots,
  });

  return {
    park,
    session_key: sessionKey,
    iso_date: isoDate,
    event_type: eventType,
    previous_available: prev?.available ?? null,
    new_available: next.available,
    previous_slots: prev?.slots ?? null,
    new_slots: next.slots,
    threshold_scanned_at: next.thresholdScannedAt,
    source_job_id: sourceJobId,
    dedupe_key: dedupeKey,
    test_event: testEvent,
  };
}

function buildNotificationCopy(event, sessionMeta = {}) {
  const level = sessionMeta.session_type || sessionMeta.level || 'Session';
  const waveSide = sessionMeta.wave_side || sessionMeta.waveSide || 'Wave';
  const time = sessionMeta.time || sessionMeta.start_time || '';
  const dayLabel = sessionMeta.day_label || sessionMeta.dayLabel || sessionMeta.iso_date || event.iso_date || '';
  const slots = event.new_slots ?? 0;

  if (event.event_type === EVENT_SPOTS_OPENED) {
    return {
      title: `${level} spots opened`,
      message: `${slots} spots are available on ${waveSide}, ${dayLabel}${time ? ` at ${time}` : ''}.`,
    };
  }

  return {
    title: `${level} opened up`,
    message: `${slots} spot${slots === 1 ? '' : 's'} ${slots === 1 ? 'is' : 'are'} available on ${waveSide}, ${dayLabel}${time ? ` at ${time}` : ''}.`,
  };
}

module.exports = {
  EVENT_BECAME_AVAILABLE,
  EVENT_SPOTS_OPENED,
  buildEventDedupeKey,
  buildDeliveryDedupeKey,
  wasPreviouslyPacked: hasAffirmativePackedEvidence,
  hasAffirmativePackedEvidence,
  deriveSessionChangeEvent,
  buildEventRecord,
  buildNotificationCopy,
};
