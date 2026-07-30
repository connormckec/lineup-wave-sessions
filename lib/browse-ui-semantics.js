'use strict';

const DOT_COUNT = 10;
const SCARCE_THRESHOLD = 3;
const DEFAULT_VERIFICATION_FRESH_MINUTES = 15;
const SEMANTICS_API_VERSION = '2';

function isThresholdTrusted(s) {
  if (!s) return false;
  if (s.available === false) {
    return !!(s.threshold_scan_verified || s.thresholdScanVerified)
      && (s.slot_source === 'entries_left_threshold_scan' || !s.slot_source);
  }
  return !!(s.threshold_scan_verified || s.thresholdScanVerified)
    && (s.slot_source === 'entries_left_threshold_scan' || !s.slot_source);
}

function trustedSlots(s) {
  if (!isThresholdTrusted(s)) return null;
  return s.available_entries ?? s.thresholdInferredSlots ?? s.slots ?? null;
}

function inventoryState(s) {
  if (!isThresholdTrusted(s) && s.slots == null && s.available !== false) {
    return { kind: 'unverified', label: 'Not verified', slots: null };
  }
  if (!s.available || trustedSlots(s) === 0) {
    return { kind: 'full', label: 'Full', slots: 0 };
  }
  const slots = trustedSlots(s);
  if (slots == null) return { kind: 'unverified', label: 'Not verified', slots: null };
  if (slots >= 10) return { kind: 'healthy', label: '10+ open', slots };
  if (slots === 1) return { kind: 'packed', label: 'Packed', slots };
  if (slots <= SCARCE_THRESHOLD) return { kind: 'scarce', label: `${slots} open`, slots };
  return { kind: 'healthy', label: `${slots} open`, slots };
}

function verificationAgeMinutes(s, nowMs = Date.now()) {
  const ts = s?.threshold_scanned_at || s?.thresholdScanAt;
  if (!ts) return null;
  return Math.max(0, Math.round((nowMs - new Date(ts).getTime()) / 60000));
}

function isTrustedVerificationFresh(s, maxAgeMinutes = DEFAULT_VERIFICATION_FRESH_MINUTES, nowMs = Date.now()) {
  if (!isThresholdTrusted(s) || !(s.threshold_scanned_at || s.thresholdScanAt)) return false;
  const age = verificationAgeMinutes(s, nowMs);
  return age != null && age <= maxAgeMinutes;
}

function countTrustedOpenSessions(sessions, isoDate, {
  freshWithinMinutes = DEFAULT_VERIFICATION_FRESH_MINUTES,
  nowMs = Date.now(),
} = {}) {
  return (sessions || []).filter((s) => (
    s.isoDate === isoDate
    && s.available !== false
    && isThresholdTrusted(s)
    && (trustedSlots(s) ?? 0) > 0
    && isTrustedVerificationFresh(s, freshWithinMinutes, nowMs)
  )).length;
}

function computeDayRailOpenCounts(sessions, dayRail, options = {}) {
  return (dayRail || []).map((day) => ({
    ...day,
    openCount: countTrustedOpenSessions(sessions, day.isoDate, options),
  }));
}

function computeSelectedDayVerificationSummary(sessions, isoDate, {
  freshWithinMinutes = DEFAULT_VERIFICATION_FRESH_MINUTES,
  nowMs = Date.now(),
} = {}) {
  const daySessions = (sessions || []).filter((s) => s.isoDate === isoDate);
  const verifiedSessions = daySessions.filter((s) => isThresholdTrusted(s) && (s.threshold_scanned_at || s.thresholdScanAt));
  const freshVerified = verifiedSessions.filter((s) => isTrustedVerificationFresh(s, freshWithinMinutes, nowMs));
  let oldestVerifiedAt = null;
  let oldestAgeMinutes = null;
  for (const s of verifiedSessions) {
    const ts = s.threshold_scanned_at || s.thresholdScanAt;
    const ms = new Date(ts).getTime();
    if (!oldestVerifiedAt || ms < new Date(oldestVerifiedAt).getTime()) {
      oldestVerifiedAt = ts;
      oldestAgeMinutes = verificationAgeMinutes(s, nowMs);
    }
  }
  const totalEligible = daySessions.length;
  const verifiedCount = verifiedSessions.length;
  const freshCount = freshVerified.length;
  const staleCount = verifiedCount - freshCount;
  let headerText;
  if (verifiedCount === 0) {
    headerText = totalEligible > 0 ? 'None verified yet' : 'No sessions';
  } else if (freshCount === verifiedCount) {
    headerText = `${freshCount} of ${totalEligible} verified within ${freshWithinMinutes}m`;
  } else {
    headerText = `${freshCount} of ${totalEligible} verified within ${freshWithinMinutes}m`;
  }
  let subText = null;
  if (staleCount > 0 && oldestVerifiedAt) {
    subText = `Oldest verified ${formatAgeMinutes(oldestAgeMinutes)}`;
  }
  return {
    totalEligible,
    verifiedCount,
    freshCount,
    staleCount,
    freshWithinMinutes,
    oldestVerifiedAt,
    oldestAgeMinutes,
    headerText,
    subText,
  };
}

function findNextTrustedOpenSession(sessions, isoDate, {
  freshWithinMinutes = DEFAULT_VERIFICATION_FRESH_MINUTES,
  nowMs = Date.now(),
} = {}) {
  return (sessions || [])
    .filter((s) => (
      s.isoDate === isoDate
      && s.available !== false
      && isThresholdTrusted(s)
      && (trustedSlots(s) ?? 0) > 0
      && isTrustedVerificationFresh(s, freshWithinMinutes, nowMs)
    ))
    .sort((a, b) => a.ts - b.ts)[0] || null;
}

function formatAgeMinutes(mins) {
  if (mins == null) return '—';
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.round(hrs / 24)}d ago`;
}

function sessionStartMs(s) {
  const ts = s?.start_ts ?? s?.ts ?? s?.startTs;
  if (ts == null) return null;
  const ms = Number(ts) * 1000;
  return Number.isFinite(ms) ? ms : null;
}

function sessionDurationMs(s) {
  return (s?.durationMinutes || 90) * 60 * 1000;
}

function isSessionPast(s, nowMs = Date.now()) {
  const startMs = sessionStartMs(s);
  if (startMs == null) return false;
  return nowMs > startMs + sessionDurationMs(s);
}

function isSessionLive(s, nowMs = Date.now()) {
  const startMs = sessionStartMs(s);
  if (startMs == null) return false;
  return nowMs >= startMs && nowMs < startMs + sessionDurationMs(s);
}

function waveSideKey(s) {
  if (s?.wave === 1 || /^left/i.test(String(s?.waveSide || ''))) return 'left';
  if (s?.wave === 2 || /^right/i.test(String(s?.waveSide || ''))) return 'right';
  return null;
}

function formatClockRange(s) {
  const startMs = sessionStartMs(s);
  if (startMs == null) return s?.time || '—';
  const endMs = startMs + sessionDurationMs(s);
  const fmt = new Intl.DateTimeFormat('en-US', {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
  const start = fmt.format(new Date(startMs)).replace(/\s*(AM|PM)/i, (m) => m.toLowerCase());
  const end = fmt.format(new Date(endMs)).replace(/\s*(AM|PM)/i, (m) => m.toLowerCase());
  const startBare = start.replace(/\s*(am|pm)$/i, '').trim();
  const endBare = end.replace(/\s*(am|pm)$/i, '').trim();
  const mer = (start.match(/(am|pm)$/i) || end.match(/(am|pm)$/i) || ['', ''])[0];
  return `${startBare}–${endBare} ${mer}`.trim();
}

function liveInventoryLabel(s) {
  const state = inventoryState(s);
  if (state.kind === 'full' || !s?.available || (trustedSlots(s) ?? 0) === 0) return 'Full';
  if (state.kind === 'scarce' && (trustedSlots(s) ?? 0) === 1) return 'Packed';
  if (state.kind === 'scarce') return `${trustedSlots(s)} open`;
  if (state.kind === 'healthy') return `${state.label}`;
  return null;
}

function computeEstimatedBooked(s) {
  const capacity = s?.capacity;
  const remaining = trustedSlots(s);
  const trusted = isThresholdTrusted(s);
  if (trusted && capacity != null && remaining != null) {
    return {
      estimatedBooked: Math.max(0, capacity - remaining),
      capacity,
      remaining,
      canEstimate: true,
    };
  }
  if (trusted && remaining != null) {
    return {
      estimatedBooked: null,
      capacity: capacity ?? null,
      remaining,
      canEstimate: false,
    };
  }
  return {
    estimatedBooked: null,
    capacity: capacity ?? null,
    remaining: null,
    canEstimate: false,
  };
}

function buildLiveSidePresentation(s, nowMs = Date.now()) {
  if (!s) return null;
  const booked = computeEstimatedBooked(s);
  const verifiedAt = s.threshold_scanned_at || s.thresholdScanAt || null;
  const verifiedAge = verifiedAt ? verificationAgeMinutes(s, nowMs) : null;
  const stateLabel = liveInventoryLabel(s);
  let occupancyLine;
  if (booked.canEstimate) {
    occupancyLine = `Estimated ${booked.estimatedBooked} surfer${booked.estimatedBooked === 1 ? '' : 's'}`;
    if (stateLabel === 'Full' || stateLabel === 'Packed') {
      occupancyLine += ` · ${stateLabel}`;
    }
  } else if (booked.remaining != null) {
    occupancyLine = 'Occupancy estimate unavailable';
  } else {
    occupancyLine = 'Occupancy estimate unavailable';
  }
  return {
    waveSide: s.waveSide || (s.wave === 1 ? 'Left Wave' : 'Right Wave'),
    level: s.level || s.session_type,
    timeRange: formatClockRange(s),
    occupancyLine,
    spotsRemainingLine: !booked.canEstimate && booked.remaining != null
      ? `Last verified: ${booked.remaining} spot${booked.remaining === 1 ? '' : 's'} remaining`
      : null,
    verifiedLine: verifiedAt ? `Verified ${formatAgeMinutes(verifiedAge)}` : 'Not verified',
    verifiedAgeMinutes: verifiedAge,
    stateLabel,
    stale: verifiedAge != null && verifiedAge > DEFAULT_VERIFICATION_FRESH_MINUTES,
    session: s,
  };
}

function findNextUpcomingSession(sessions, nowMs = Date.now()) {
  return (sessions || [])
    .filter((s) => {
      const startMs = sessionStartMs(s);
      return startMs != null && startMs > nowMs && !isSessionPast(s, nowMs);
    })
    .sort((a, b) => sessionStartMs(a) - sessionStartMs(b))[0] || null;
}

function computeLiveNowSummary(sessions, nowMs = Date.now()) {
  const liveSessions = (sessions || []).filter((s) => isSessionLive(s, nowMs));
  const leftLive = liveSessions.find((s) => waveSideKey(s) === 'left') || null;
  const rightLive = liveSessions.find((s) => waveSideKey(s) === 'right') || null;
  const helpText = 'Based on booking inventory; may differ from surfers physically in the water due to no-shows or operational changes.';

  if (!leftLive && !rightLive) {
    const next = findNextUpcomingSession(sessions, nowMs);
    return {
      mode: 'idle',
      title: 'NO SESSION LIVE',
      windowLabel: null,
      left: null,
      right: null,
      nextSession: next ? {
        time: next.time,
        level: next.level || next.session_type,
        waveSide: next.waveSide || (next.wave === 1 ? 'Left Wave' : 'Right Wave'),
      } : null,
      helpText,
    };
  }

  const anchor = leftLive || rightLive;
  const sameWindow = leftLive && rightLive
    && formatClockRange(leftLive) === formatClockRange(rightLive);

  return {
    mode: 'live',
    title: 'LIVE NOW',
    windowLabel: sameWindow ? formatClockRange(anchor) : null,
    left: buildLiveSidePresentation(leftLive, nowMs),
    right: buildLiveSidePresentation(rightLive, nowMs),
    nextSession: null,
    helpText,
  };
}

function watchLabelForSession(s, watched, alertsEnabled = true) {
  if (!alertsEnabled) return { label: 'Alerts off', disabled: true };
  return {
    label: watched ? 'Watching' : 'Watch',
    disabled: false,
  };
}

const api = {
  SEMANTICS_API_VERSION,
  DOT_COUNT,
  SCARCE_THRESHOLD,
  DEFAULT_VERIFICATION_FRESH_MINUTES,
  isThresholdTrusted,
  trustedSlots,
  inventoryState,
  verificationAgeMinutes,
  isTrustedVerificationFresh,
  countTrustedOpenSessions,
  computeDayRailOpenCounts,
  computeSelectedDayVerificationSummary,
  findNextTrustedOpenSession,
  formatAgeMinutes,
  sessionStartMs,
  sessionDurationMs,
  isSessionPast,
  isSessionLive,
  waveSideKey,
  formatClockRange,
  liveInventoryLabel,
  computeEstimatedBooked,
  buildLiveSidePresentation,
  findNextUpcomingSession,
  computeLiveNowSummary,
  watchLabelForSession,
};

if (typeof window !== 'undefined') {
  window.BrowseUiSemantics = api;
}
if (typeof module !== 'undefined' && module.exports) {
  module.exports = api;
}
