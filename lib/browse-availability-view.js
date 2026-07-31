'use strict';

(function initBrowseAvailabilityView() {
  let resolveCanonicalSlotCount;
  let getThresholdScannedAt;
  let capacityConfig;
  let sessionFilters;

  if (typeof require !== 'undefined') {
    const trustedSessionState = require('./trusted-session-state');
    resolveCanonicalSlotCount = trustedSessionState.resolveCanonicalSlotCount;
    getThresholdScannedAt = trustedSessionState.getThresholdScannedAt;
    capacityConfig = require('./session-capacity-config');
    sessionFilters = require('./browse-session-filters');
  }

  function thresholdFieldOnSession(session, field) {
    if (!session) return null;
    if (session[field] != null) return session[field];
    if (session.raw && session.raw[field] != null) return session.raw[field];
    return null;
  }

  function readNumericField(session, field) {
    const value = thresholdFieldOnSession(session, field);
    if (value == null) return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function sessionThresholdScanVerified(session) {
    if (!session) return false;
    return session.thresholdScanVerified === true
      || session.raw?.thresholdScanVerified === true
      || session.threshold_scan_verified === true
      || session.raw?.threshold_scan_verified === true;
  }

  function resolveSlotStatus(session) {
    return thresholdFieldOnSession(session, 'slot_status')
      ?? thresholdFieldOnSession(session, 'thresholdConfidence')
      ?? thresholdFieldOnSession(session, 'threshold_confidence');
  }

  function resolveConfidence(session) {
    return resolveSlotStatus(session);
  }

  function hasTrustedSlotStatus(session) {
    const status = resolveSlotStatus(session);
    return status === 'exact' || status === 'at_least';
  }

  function isTrustedThresholdSession(session) {
    if (!session || !sessionThresholdScanVerified(session) || !hasTrustedSlotStatus(session)) return false;
    const scanTs = parseThresholdTimestamp(
      thresholdFieldOnSession(session, 'threshold_scanned_at')
        ?? thresholdFieldOnSession(session, 'thresholdScanAt')
        ?? thresholdFieldOnSession(session, 'threshold_scan_at'),
    );
    const suspendedTs = parseThresholdTimestamp(thresholdFieldOnSession(session, 'thresholdTrustedSuspendedAt'));
    if (suspendedTs != null && (scanTs == null || scanTs <= suspendedTs)) return false;
    const slotSource = thresholdFieldOnSession(session, 'slot_source');
    if (slotSource && slotSource !== 'entries_left_threshold_scan') return false;
    return true;
  }

  function parseThresholdTimestamp(value) {
    if (!value) return null;
    const ts = new Date(value).getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  function getSessionFilters() {
    if (sessionFilters) return sessionFilters;
    return typeof window !== 'undefined' ? window.LineupBrowse?.sessionFilters : null;
  }

  function getCapacityConfig() {
    if (capacityConfig) return capacityConfig;
    if (typeof window !== 'undefined') {
      return window.LineupCapacity || null;
    }
    return null;
  }

  function isLessonSession(session) {
    const filters = getSessionFilters();
    if (filters?.isLessonSession) return filters.isLessonSession(session);
    return false;
  }

  function browserResolveCanonicalSlotCount(session) {
    if (!session || !sessionThresholdScanVerified(session) || !hasTrustedSlotStatus(session)) return null;
    const slotSource = thresholdFieldOnSession(session, 'slot_source');
    if (slotSource && slotSource !== 'entries_left_threshold_scan') return null;
    const scanTs = parseThresholdTimestamp(
      thresholdFieldOnSession(session, 'threshold_scanned_at')
        ?? thresholdFieldOnSession(session, 'thresholdScanAt')
        ?? thresholdFieldOnSession(session, 'threshold_scan_at'),
    );
    const suspendedTs = parseThresholdTimestamp(thresholdFieldOnSession(session, 'thresholdTrustedSuspendedAt'));
    if (suspendedTs != null && (scanTs == null || scanTs <= suspendedTs)) return null;

    const directCandidates = [
      'slots',
      'available_entries',
      'thresholdInferredSlots',
      'threshold_inferred_slots',
    ];
    for (const field of directCandidates) {
      const num = readNumericField(session, field);
      if (num != null) return num;
    }

    if (session.available === false) return 0;
    return null;
  }

  if (!resolveCanonicalSlotCount) {
    resolveCanonicalSlotCount = browserResolveCanonicalSlotCount;
  }
  if (!getThresholdScannedAt) {
    getThresholdScannedAt = (session) => {
      const raw = thresholdFieldOnSession(session, 'threshold_scanned_at')
        ?? thresholdFieldOnSession(session, 'thresholdScanAt')
        ?? thresholdFieldOnSession(session, 'threshold_scan_at');
      if (!raw) return null;
      const ts = new Date(raw);
      return Number.isFinite(ts.getTime()) ? ts.toISOString() : null;
    };
  }

  function resolveCapacity(session) {
    const cfg = getCapacityConfig();
    if (!cfg?.resolveConfiguredCapacity) return null;
    return cfg.resolveConfiguredCapacity(session, { isLessonSession });
  }

  function resolveTrustedSpotsLeft(session) {
    if (!isTrustedThresholdSession(session)) {
      return { spotsLeft: null, hasTrustedSlots: false, source: null };
    }

    const directCandidates = [
      'slots',
      'available_entries',
      'thresholdInferredSlots',
      'threshold_inferred_slots',
    ];
    for (const field of directCandidates) {
      const num = readNumericField(session, field);
      if (num != null) {
        return {
          spotsLeft: num,
          hasTrustedSlots: true,
          source: thresholdFieldOnSession(session, 'slot_source') || 'entries_left_threshold_scan',
        };
      }
    }

    const canonical = resolveCanonicalSlotCount(session);
    if (canonical != null) {
      return {
        spotsLeft: canonical,
        hasTrustedSlots: true,
        source: thresholdFieldOnSession(session, 'slot_source') || 'entries_left_threshold_scan',
      };
    }

    return { spotsLeft: null, hasTrustedSlots: false, source: null };
  }

  function formatSpotsLabel(spotsLeft, session) {
    if (spotsLeft == null) return null;
    if (spotsLeft === 0) return 'Full';
    const atLeast = session?.thresholdConfidence === 'at_least'
      || session?.slot_status === 'at_least'
      || session?.slotsAtLeast === true
      || session?.available_entries_at_least === true;
    if (atLeast) {
      const max = session?.thresholdScanMaxTested || spotsLeft;
      return `${max}+ spots left`;
    }
    if (spotsLeft === 1) return '1 spot left';
    return `${spotsLeft} spots left`;
  }

  function inventoryKindFor(spotsLeft) {
    if (spotsLeft == null || spotsLeft === 0) return 'full';
    if (spotsLeft >= 10) return 'healthy';
    if (spotsLeft === 1) return 'packed';
    if (spotsLeft <= 3) return 'scarce';
    return 'healthy';
  }

  function getSessionOccupancyViewModel(session) {
    const empty = {
      capacity: null,
      spotsLeft: null,
      bookedCount: null,
      isOpen: false,
      isFull: false,
      hasTrustedSlots: false,
      verifiedAt: null,
      source: null,
      confidence: null,
      diagnosticWarnings: [],
      spotsLabel: null,
      statusLabel: 'Unknown',
      inventoryKind: 'unverified',
      hasTrustedCount: false,
    };
    if (!session) return empty;

    const trusted = resolveTrustedSpotsLeft(session);
    const spotsLeft = trusted.spotsLeft;
    const hasTrustedSlots = trusted.hasTrustedSlots;
    const verifiedAt = getThresholdScannedAt(session);
    const capacity = resolveCapacity(session);
    const source = trusted.source;
    const confidence = resolveConfidence(session);
    const diagnosticWarnings = [];

    let isOpen;
    let isFull;
    if (hasTrustedSlots) {
      isFull = spotsLeft === 0;
      isOpen = spotsLeft > 0;
    } else {
      isFull = session.available === false;
      isOpen = session.available === true;
    }

    let displaySpotsLeft = spotsLeft;
    if (capacity != null && spotsLeft != null && spotsLeft > capacity) {
      diagnosticWarnings.push({
        code: 'spots_exceed_capacity',
        spotsLeft,
        capacity,
      });
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[occupancy] spotsLeft exceeds configured capacity', {
          key: session.key,
          level: session.level,
          spotsLeft,
          capacity,
        });
      }
    }

    let bookedCount = null;
    if (capacity != null && displaySpotsLeft != null) {
      const clampedSpots = Math.max(0, Math.min(displaySpotsLeft, capacity));
      bookedCount = capacity - clampedSpots;
    }

    const spotsLabel = hasTrustedSlots ? formatSpotsLabel(displaySpotsLeft, session) : null;
    let statusLabel;
    let inventoryKind;
    if (isFull) {
      statusLabel = 'Full';
      inventoryKind = 'full';
    } else if (hasTrustedSlots) {
      statusLabel = spotsLabel;
      inventoryKind = inventoryKindFor(displaySpotsLeft);
    } else {
      statusLabel = isOpen ? 'Open' : 'Full';
      inventoryKind = isOpen ? 'unverified' : 'full';
    }

    return {
      capacity,
      spotsLeft: displaySpotsLeft,
      bookedCount,
      isOpen,
      isFull,
      hasTrustedSlots,
      verifiedAt,
      source,
      confidence,
      diagnosticWarnings,
      spotsLabel,
      statusLabel,
      inventoryKind,
      hasTrustedCount: hasTrustedSlots,
    };
  }

  function getSessionAvailabilityViewModel(session) {
    return getSessionOccupancyViewModel(session);
  }

  function countOpenSessions(sessions) {
    return (sessions || []).filter((session) => getSessionOccupancyViewModel(session).isOpen).length;
  }

  function safeDotCount(value, max = 200) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return 0;
    return Math.min(Math.floor(n), max);
  }

  function buildAvailabilityDotBarHtml(viewModel) {
    const vm = viewModel || {};
    const capacity = safeDotCount(vm.capacity);
    const spotsLeft = vm.spotsLeft == null ? null : safeDotCount(vm.spotsLeft, capacity || 200);

    if (!vm.hasTrustedSlots || capacity <= 0 || spotsLeft == null) {
      return '';
    }

    const open = Math.min(spotsLeft, capacity);
    const booked = capacity - open;
    const openClass = (vm.inventoryKind === 'scarce' || vm.inventoryKind === 'packed') ? 'scarce' : 'open';
    let dots = '';
    for (let i = 0; i < booked; i += 1) dots += '<span class="slot-dot"></span>';
    for (let i = 0; i < open; i += 1) dots += `<span class="slot-dot ${openClass}"></span>`;
    return `<div class="slot-bar">${dots}</div>`;
  }

  const availabilityView = {
    getSessionOccupancyViewModel,
    getSessionAvailabilityViewModel,
    countOpenSessions,
    buildAvailabilityDotBarHtml,
    resolveCapacity,
    resolveTrustedSpotsLeft,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = availabilityView;
  }

  if (typeof window !== 'undefined') {
    window.LineupBrowse = window.LineupBrowse || {};
    window.LineupBrowse.availabilityView = availabilityView;
  }
}());
