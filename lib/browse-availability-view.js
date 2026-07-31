'use strict';

(function initBrowseAvailabilityView() {
  let resolveCanonicalSlotCount;
  let getThresholdScannedAt;

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

    if (session.available === false) return 0;
    return null;
  }

  if (typeof require !== 'undefined') {
    const trustedSessionState = require('./trusted-session-state');
    resolveCanonicalSlotCount = trustedSessionState.resolveCanonicalSlotCount;
    getThresholdScannedAt = trustedSessionState.getThresholdScannedAt;
  } else {
    resolveCanonicalSlotCount = browserResolveCanonicalSlotCount;
    getThresholdScannedAt = (session) => {
      const raw = thresholdFieldOnSession(session, 'threshold_scanned_at')
        ?? thresholdFieldOnSession(session, 'thresholdScanAt')
        ?? thresholdFieldOnSession(session, 'threshold_scan_at');
      if (!raw) return null;
      const ts = new Date(raw);
      return Number.isFinite(ts.getTime()) ? ts.toISOString() : null;
    };
  }

  const LEVEL_DEFAULT_CAPACITY = {
    Progressive: 18,
    'Pro Turns': 10,
  };

  function resolveCapacity(session) {
    const candidates = [
      session?.capacity,
      session?.expectedCapacity,
      session?.parsed_capacity,
      session?.parsedCapacity,
      session?.raw?.capacity,
      session?.raw?.expectedCapacity,
    ];
    for (const value of candidates) {
      const num = Number(value);
      if (Number.isFinite(num) && num > 0) return num;
    }
    const level = session?.level || session?.session_type;
    if (level && LEVEL_DEFAULT_CAPACITY[level] != null) return LEVEL_DEFAULT_CAPACITY[level];
    return null;
  }

  function resolveTrustedSpotsLeft(session) {
    if (!isTrustedThresholdSession(session)) {
      return { spotsLeft: null, hasTrustedCount: false, source: null };
    }

    const directCandidates = [
      'available_entries',
      'thresholdInferredSlots',
      'threshold_inferred_slots',
    ];
    for (const field of directCandidates) {
      const num = readNumericField(session, field);
      if (num != null) {
        return {
          spotsLeft: num,
          hasTrustedCount: true,
          source: thresholdFieldOnSession(session, 'slot_source') || 'entries_left_threshold_scan',
        };
      }
    }

    const canonical = resolveCanonicalSlotCount(session);
    if (canonical != null) {
      return {
        spotsLeft: canonical,
        hasTrustedCount: true,
        source: thresholdFieldOnSession(session, 'slot_source') || 'entries_left_threshold_scan',
      };
    }

    return { spotsLeft: null, hasTrustedCount: false, source: null };
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

  function getSessionAvailabilityViewModel(session) {
    if (!session) {
      return {
        spotsLeft: null,
        capacity: null,
        isOpen: false,
        isFull: false,
        hasTrustedCount: false,
        verifiedAt: null,
        source: null,
        spotsLabel: null,
        statusLabel: 'Unknown',
        inventoryKind: 'unverified',
      };
    }

    const trusted = resolveTrustedSpotsLeft(session);
    const spotsLeft = trusted.spotsLeft;
    const hasTrustedCount = trusted.hasTrustedCount;
    const verifiedAt = getThresholdScannedAt(session);
    const capacity = resolveCapacity(session);
    const source = trusted.source;

    let isOpen;
    let isFull;
    if (hasTrustedCount) {
      isFull = spotsLeft === 0;
      isOpen = spotsLeft > 0;
    } else {
      isFull = session.available === false;
      isOpen = session.available === true;
    }

    const spotsLabel = hasTrustedCount ? formatSpotsLabel(spotsLeft, session) : null;
    let statusLabel;
    let inventoryKind;
    if (isFull) {
      statusLabel = 'Full';
      inventoryKind = 'full';
    } else if (hasTrustedCount) {
      statusLabel = spotsLabel;
      inventoryKind = inventoryKindFor(spotsLeft);
    } else {
      statusLabel = 'Open';
      inventoryKind = 'unverified';
    }

    return {
      spotsLeft,
      capacity,
      isOpen,
      isFull,
      hasTrustedCount,
      verifiedAt,
      source,
      spotsLabel,
      statusLabel,
      inventoryKind,
    };
  }

  function countOpenSessions(sessions) {
    return (sessions || []).filter((session) => getSessionAvailabilityViewModel(session).isOpen).length;
  }

  function buildAvailabilityDotBarHtml(viewModel) {
    const vm = viewModel || {};
    if (vm.isFull && vm.capacity != null) {
      let dots = '';
      for (let i = 0; i < vm.capacity; i += 1) dots += '<span class="slot-dot"></span>';
      return `<div class="slot-bar">${dots}</div>`;
    }
    if (vm.hasTrustedCount && vm.capacity != null && vm.spotsLeft != null) {
      const total = vm.capacity;
      const open = Math.min(Math.max(0, vm.spotsLeft), total);
      const taken = total - open;
      const openClass = (vm.inventoryKind === 'scarce' || vm.inventoryKind === 'packed') ? 'scarce' : 'open';
      let dots = '';
      for (let i = 0; i < taken; i += 1) dots += '<span class="slot-dot"></span>';
      for (let i = 0; i < open; i += 1) dots += `<span class="slot-dot ${openClass}"></span>`;
      return `<div class="slot-bar">${dots}</div>`;
    }
    if (vm.hasTrustedCount && vm.spotsLeft != null && vm.spotsLeft > 0) {
      const openClass = (vm.inventoryKind === 'scarce' || vm.inventoryKind === 'packed') ? 'scarce' : 'open';
      let dots = '';
      for (let i = 0; i < vm.spotsLeft; i += 1) dots += `<span class="slot-dot ${openClass}"></span>`;
      return `<div class="slot-bar">${dots}</div>`;
    }
    return '';
  }

  const availabilityView = {
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
