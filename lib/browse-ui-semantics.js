'use strict';

(function initBrowseUiSemantics() {
  const liveSchedule = typeof require !== 'undefined'
    ? require('./browse-live-schedule')
    : (typeof window !== 'undefined' ? window.LineupBrowse?.liveSchedule : null);
  const availabilityView = typeof require !== 'undefined'
    ? require('./browse-availability-view')
    : (typeof window !== 'undefined' ? window.LineupBrowse?.availabilityView : null);

const DOT_COUNT = 10;
const SCARCE_THRESHOLD = 3;
const DEFAULT_VERIFICATION_FRESH_MINUTES = 15;
const SEMANTICS_API_VERSION = '4';

function isThresholdTrusted(s) {
  if (!s) return false;
  const verified = !!(s.threshold_scan_verified || s.thresholdScanVerified);
  const source = s.slot_source || s.slotsSource;
  const conf = s.slot_status || s.thresholdConfidence || s.threshold_confidence;
  const suspendedAt = s.thresholdTrustedSuspendedAt || s.raw?.thresholdTrustedSuspendedAt;
  const verifiedAt = s.threshold_scanned_at || s.thresholdScanAt || s.threshold_scan_at;
  if (suspendedAt && verifiedAt && new Date(verifiedAt) <= new Date(suspendedAt)) return false;
  if (suspendedAt && !verifiedAt) return false;
  if (!verified) return false;
  if (source && source !== 'entries_left_threshold_scan') return false;
  return conf === 'exact' || conf === 'at_least';
}

function readSlotNumber(s, field) {
  const value = s?.[field] ?? s?.raw?.[field];
  if (value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function trustedSlots(s) {
  const vm = availabilityView?.getSessionAvailabilityViewModel?.(s);
  if (vm?.hasTrustedCount) return vm.spotsLeft;
  if (!isThresholdTrusted(s)) return null;
  if (s.available === false) {
    const zeroish = [
      readSlotNumber(s, 'available_entries'),
      readSlotNumber(s, 'thresholdInferredSlots'),
      readSlotNumber(s, 'threshold_inferred_slots'),
    ].find((v) => v != null);
    return zeroish != null ? zeroish : 0;
  }
  const direct = [
    readSlotNumber(s, 'available_entries'),
    readSlotNumber(s, 'thresholdInferredSlots'),
    readSlotNumber(s, 'threshold_inferred_slots'),
  ];
  for (const num of direct) {
    if (num != null) return num;
  }
  const slotStatus = s.slot_status || s.thresholdConfidence || s.threshold_confidence;
  const maxVisible = readSlotNumber(s, 'thresholdMaxVisible') ?? readSlotNumber(s, 'threshold_max_visible');
  if (maxVisible != null && (slotStatus === 'exact' || slotStatus === 'at_least')) return maxVisible;
  return s.slots ?? null;
}

function inventoryDisplayLabel(s, slots) {
  const vm = availabilityView?.getSessionAvailabilityViewModel?.(s);
  if (vm?.spotsLabel) return vm.spotsLabel;
  if (!s.available || slots === 0) return 'Full';
  if (slots == null) return 'Open';
  const atLeast = s.thresholdConfidence === 'at_least' || s.slot_status === 'at_least' || s.slotsAtLeast === true;
  if (atLeast) {
    const max = s.thresholdScanMaxTested || slots;
    return `${max}+ spots left`;
  }
  if (slots === 1) return '1 spot left';
  return `${slots} spots left`;
}

function inventoryState(s) {
  const vm = availabilityView?.getSessionAvailabilityViewModel?.(s);
  if (vm) {
    return {
      kind: vm.inventoryKind,
      label: vm.statusLabel,
      slots: vm.spotsLeft,
      viewModel: vm,
    };
  }
  const slots = trustedSlots(s);
  if (!s.available || slots === 0) {
    return { kind: 'full', label: 'Full', slots: 0 };
  }
  if (!isThresholdTrusted(s) && s.slots == null && s.available !== false) {
    return { kind: 'unverified', label: 'Open', slots: null };
  }
  if (slots == null) return { kind: 'unverified', label: 'Open', slots: null };
  const label = inventoryDisplayLabel(s, slots);
  if (slots >= 10) return { kind: 'healthy', label, slots };
  if (slots === 1) return { kind: 'packed', label, slots };
  if (slots <= SCARCE_THRESHOLD) return { kind: 'scarce', label, slots };
  return { kind: 'healthy', label, slots };
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
  nowMs = Date.now(),
} = {}) {
  return (sessions || []).filter((s) => (
    s.isoDate === isoDate
    && availabilityView?.getSessionAvailabilityViewModel
      ? availabilityView.getSessionAvailabilityViewModel(s).isOpen
      : ((trustedSlots(s) ?? 0) > 0 && s.available !== false)
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
  if (liveSchedule?.sessionStartMs) return liveSchedule.sessionStartMs(s);
  const ts = s?.start_ts ?? s?.ts ?? s?.startTs;
  if (ts == null) return null;
  const ms = Number(ts) * 1000;
  return Number.isFinite(ms) ? ms : null;
}

function sessionDurationMs(s) {
  if (liveSchedule?.sessionDurationMs) return liveSchedule.sessionDurationMs(s);
  return (s?.durationMinutes || 60) * 60 * 1000;
}

function isSessionPast(s, nowMs = Date.now()) {
  if (liveSchedule?.isSessionPastAt) return liveSchedule.isSessionPastAt(s, nowMs);
  const startMs = sessionStartMs(s);
  if (startMs == null) return false;
  return nowMs >= startMs + sessionDurationMs(s);
}

function isSessionLive(s, nowMs = Date.now()) {
  if (liveSchedule?.isSessionLiveAt) return liveSchedule.isSessionLiveAt(s, nowMs);
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
  if (liveSchedule?.formatClockRange) return liveSchedule.formatClockRange(s);
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
  const level = s.level || s.session_type || 'Session';
  const verifiedAt = s.threshold_scanned_at || s.thresholdScanAt || null;
  const verifiedAge = verifiedAt ? verificationAgeMinutes(s, nowMs) : null;
  const fresh = isTrustedVerificationFresh(s, DEFAULT_VERIFICATION_FRESH_MINUTES, nowMs);
  const trusted = isThresholdTrusted(s);
  const remaining = trusted ? trustedSlots(s) : null;
  const scheduleFull = s?.available === false;

  let summaryLine;
  let detailLine = null;

  if (fresh && trusted && remaining != null) {
    summaryLine = remaining === 0
      ? `${level} · Full`
      : `${level} · ${inventoryDisplayLabel(s, remaining)}`;
    if (verifiedAt) detailLine = `Verified ${formatAgeMinutes(verifiedAge)}`;
  } else if (scheduleFull) {
    summaryLine = `${level} · Full`;
    if (!fresh && remaining != null && remaining > 0) {
      detailLine = `Currently listed as full · Last verified ${remaining} spot${remaining === 1 ? '' : 's'} remaining`;
    } else if (verifiedAt) {
      detailLine = `Verified ${formatAgeMinutes(verifiedAge)}`;
    } else {
      detailLine = 'Not verified';
    }
  } else if (trusted && remaining != null && remaining > 0) {
    summaryLine = `${level} · ${inventoryDisplayLabel(s, remaining)}`;
    detailLine = verifiedAt
      ? (fresh
        ? `Verified ${formatAgeMinutes(verifiedAge)}`
        : `Last verified ${remaining} spot${remaining === 1 ? '' : 's'} remaining · ${formatAgeMinutes(verifiedAge)} ago`)
      : 'Not verified';
  } else {
    summaryLine = `${level} · ${inventoryState(s).label}`;
    detailLine = verifiedAt ? `Verified ${formatAgeMinutes(verifiedAge)}` : 'Not verified';
  }

  return {
    waveSide: s.waveSide || (s.wave === 1 ? 'Left Wave' : 'Right Wave'),
    level,
    timeRange: formatClockRange(s),
    summaryLine,
    detailLine,
    verifiedLine: detailLine,
    verifiedAgeMinutes: verifiedAge,
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
  const leftLive = liveSchedule?.pickLiveSessionForSide
    ? liveSchedule.pickLiveSessionForSide(sessions, 'left', nowMs)
    : liveSessions.find((s) => waveSideKey(s) === 'left') || null;
  const rightLive = liveSchedule?.pickLiveSessionForSide
    ? liveSchedule.pickLiveSessionForSide(sessions, 'right', nowMs)
    : liveSessions.find((s) => waveSideKey(s) === 'right') || null;
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

const semantics = {
  SEMANTICS_API_VERSION,
  DOT_COUNT,
  SCARCE_THRESHOLD,
  DEFAULT_VERIFICATION_FRESH_MINUTES,
  isThresholdTrusted,
  trustedSlots,
  inventoryState,
  inventoryDisplayLabel,
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

if (typeof module !== 'undefined' && module.exports) {
  module.exports = semantics;
}

if (typeof window !== 'undefined') {
  window.LineupBrowse = window.LineupBrowse || {};
  window.LineupBrowse.semantics = semantics;
}
}());
