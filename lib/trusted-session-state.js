'use strict';

const thresholdMaintenance = require('./threshold-maintenance');

function thresholdFieldOnSession(s, field) {
  if (!s) return null;
  if (s[field] != null) return s[field];
  if (s.raw && s.raw[field] != null) return s.raw[field];
  return null;
}

function getTrustedSlots(session) {
  if (!thresholdMaintenance.isThresholdSlotsTrusted(session)) return null;
  const exact = thresholdFieldOnSession(session, 'available_entries');
  if (exact != null) return Number(exact);
  const atLeast = thresholdFieldOnSession(session, 'available_entries_at_least');
  if (atLeast != null) return Number(atLeast);
  const inferred = thresholdFieldOnSession(session, 'thresholdInferredSlots');
  return inferred == null ? null : Number(inferred);
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
  const trusted = thresholdMaintenance.isThresholdSlotsTrusted(session);
  const slots = trusted ? getTrustedSlots(session) : null;

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
  getTrustedSlots,
  getThresholdScannedAt,
  extractTrustedAvailabilityState,
  statesEqual,
};
