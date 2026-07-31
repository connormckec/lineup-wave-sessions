'use strict';

const thresholdMaintenance = require('./threshold-maintenance');

function thresholdFieldOnSession(s, field) {
  if (!s) return null;
  if (s[field] != null) return s[field];
  if (s.raw && s.raw[field] != null) return s.raw[field];
  return null;
}

function sessionThresholdScanVerified(s) {
  return thresholdMaintenance.sessionThresholdScanVerified(s);
}

function isTrustedThresholdSlotResult(session) {
  return thresholdMaintenance.isThresholdSlotsTrusted(session);
}

function resolveSlotStatus(session) {
  return thresholdFieldOnSession(session, 'slot_status')
    ?? thresholdFieldOnSession(session, 'thresholdConfidence')
    ?? thresholdFieldOnSession(session, 'threshold_confidence');
}

function hasTrustedSlotStatus(session) {
  const status = resolveSlotStatus(session);
  return status === 'exact' || status === 'at_least';
}

function readNumericField(session, field) {
  const value = thresholdFieldOnSession(session, field);
  if (value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function resolveCanonicalSlotCount(session) {
  if (!session) return null;
  if (!sessionThresholdScanVerified(session)) return null;
  if (!hasTrustedSlotStatus(session)) return null;

  const slotSource = thresholdFieldOnSession(session, 'slot_source');
  if (slotSource && slotSource !== 'entries_left_threshold_scan') return null;

  const scanTs = thresholdMaintenance.getThresholdScanTimestamp(session);
  const suspendedTs = thresholdMaintenance.getThresholdSuspendedTimestamp(session);
  if (suspendedTs != null && (scanTs == null || scanTs <= suspendedTs)) return null;

  if (session.available === false) {
    const zeroCandidates = [
      readNumericField(session, 'available_entries'),
      readNumericField(session, 'thresholdInferredSlots'),
      readNumericField(session, 'threshold_inferred_slots'),
    ].filter((v) => v != null);
    if (zeroCandidates.some((v) => v === 0)) return 0;
    return 0;
  }

  const directCandidates = [
    'available_entries',
    'thresholdInferredSlots',
    'threshold_inferred_slots',
  ];
  for (const field of directCandidates) {
    const num = readNumericField(session, field);
    if (num != null) return num;
  }

  const slotStatus = resolveSlotStatus(session);
  const maxVisible = readNumericField(session, 'thresholdMaxVisible')
    ?? readNumericField(session, 'threshold_max_visible');
  if (maxVisible != null && (slotStatus === 'exact' || slotStatus === 'at_least')) {
    return maxVisible;
  }

  return null;
}

function hasCanonicalSlotCount(session) {
  return resolveCanonicalSlotCount(session) != null;
}

function applyCanonicalSlots(session, { overwrite = false } = {}) {
  if (!session) return session;
  const canonical = resolveCanonicalSlotCount(session);
  if (canonical == null) return session;
  const out = { ...session };
  if (overwrite || out.slots == null) out.slots = canonical;
  if (out.thresholdSlots == null) out.thresholdSlots = canonical;
  return out;
}

function getTrustedSlots(session) {
  if (!isTrustedThresholdSlotResult(session)) return null;
  return resolveCanonicalSlotCount(session);
}

function getThresholdScannedAt(session) {
  const raw = thresholdFieldOnSession(session, 'threshold_scanned_at')
    ?? thresholdFieldOnSession(session, 'thresholdScanAt')
    ?? thresholdFieldOnSession(session, 'threshold_scan_at');
  if (!raw) return null;
  const ts = new Date(raw);
  return Number.isFinite(ts.getTime()) ? ts.toISOString() : null;
}

function extractTrustedAvailabilityState(session) {
  if (!session) {
    return {
      trusted: false,
      available: null,
      slots: null,
      thresholdScannedAt: null,
    };
  }

  const thresholdScannedAt = getThresholdScannedAt(session);
  const trusted = isTrustedThresholdSlotResult(session) && hasTrustedSlotStatus(session);
  const slots = trusted ? resolveCanonicalSlotCount(session) : null;

  if (session.available === false) {
    return {
      trusted,
      available: false,
      slots: slots ?? 0,
      thresholdScannedAt,
    };
  }

  if (!trusted) {
    return {
      trusted: false,
      available: null,
      slots: null,
      thresholdScannedAt,
    };
  }

  const numericSlots = slots == null ? null : Number(slots);
  const available = numericSlots != null && numericSlots > 0;
  return {
    trusted: true,
    available,
    slots: numericSlots,
    thresholdScannedAt,
  };
}

function statesEqual(a, b) {
  return !!a && !!b
    && a.available === b.available
    && a.slots === b.slots
    && a.thresholdScannedAt === b.thresholdScannedAt;
}

module.exports = {
  thresholdFieldOnSession,
  isTrustedThresholdSlotResult,
  resolveSlotStatus,
  hasTrustedSlotStatus,
  resolveCanonicalSlotCount,
  hasCanonicalSlotCount,
  applyCanonicalSlots,
  getTrustedSlots,
  getThresholdScannedAt,
  extractTrustedAvailabilityState,
  statesEqual,
};
