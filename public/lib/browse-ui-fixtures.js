'use strict';

/**
 * Fixture sessions for Browse UI prototype when Supabase/API is unavailable.
 * Covers live now, open, scarce, full, packed, stale, unverified, watched states.
 *
 * Scenario query param: both-live (default) | one-live | none-live
 */
(function initBrowseUiFixtures(global) {
  const TODAY = '2026-07-29';
  const TOMORROW = '2026-07-30';

  const SCENARIOS = {
    'both-live': { nowIso: `${TODAY}T19:30:00-04:00` },
    'one-live': { nowIso: `${TODAY}T19:30:00-04:00` },
    'none-live': { nowIso: `${TODAY}T18:30:00-04:00` },
  };

  function readScenario() {
    if (typeof location !== 'undefined') {
      const fromUrl = new URLSearchParams(location.search).get('scenario');
      if (fromUrl && SCENARIOS[fromUrl]) return fromUrl;
    }
    if (global.__BROWSE_FIXTURE_SCENARIO && SCENARIOS[global.__BROWSE_FIXTURE_SCENARIO]) {
      return global.__BROWSE_FIXTURE_SCENARIO;
    }
    return 'both-live';
  }

  const scenario = readScenario();
  const scenarioCfg = SCENARIOS[scenario];
  const nowMs = Date.parse(scenarioCfg.nowIso);

  const LEVELS = {
    Progressive: { s: 'PRG', c: '#48cae4' },
    Intermediate: { s: 'INT', c: '#00b4d8' },
    'Advanced Turns': { s: 'AT', c: '#7b2fff' },
    'Advanced Barrels': { s: 'AB', c: '#c77dff' },
    'Expert Turns': { s: 'ET', c: '#ff6b35' },
    'Expert Barrels': { s: 'EB', c: '#ff4444' },
    'Pro Turns': { s: 'PT', c: '#ffd166' },
  };

  function tsFor(isoDate, hour, minute = 0) {
    return Math.floor(new Date(`${isoDate}T${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00-04:00`).getTime() / 1000);
  }

  function relMins(minutesAgo) {
    return new Date(nowMs - minutesAgo * 60 * 1000).toISOString();
  }

  const liveLeft = {
    key: `${tsFor(TODAY, 19)}_1`,
    ts: tsFor(TODAY, 19),
    isoDate: TODAY,
    dateKey: TODAY,
    time: '7:00 pm',
    level: 'Advanced Turns',
    session_type: 'Advanced Turns',
    wave: 1,
    waveSide: 'Left Wave',
    durationMinutes: 60,
    available: true,
    capacity: 12,
    slots: 5,
    available_entries: 5,
    thresholdInferredSlots: 5,
    threshold_scan_verified: true,
    thresholdScanVerified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    thresholdConfidence: 'exact',
    threshold_scanned_at: relMins(8),
    lastBasicCheckAt: relMins(4),
  };

  const liveRightBoth = {
    key: `${tsFor(TODAY, 19)}_2`,
    ts: tsFor(TODAY, 19),
    isoDate: TODAY,
    dateKey: TODAY,
    time: '7:00 pm',
    level: 'Advanced Turns',
    session_type: 'Advanced Turns',
    wave: 2,
    waveSide: 'Right Wave',
    durationMinutes: 60,
    available: false,
    capacity: 10,
    slots: 0,
    available_entries: 0,
    thresholdInferredSlots: 0,
    threshold_scan_verified: true,
    thresholdScanVerified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    thresholdConfidence: 'exact',
    threshold_scanned_at: relMins(52),
    lastBasicCheckAt: relMins(4),
  };

  const liveRightOneOnly = {
    ...liveRightBoth,
    key: `${tsFor(TODAY, 20)}_2`,
    ts: tsFor(TODAY, 20),
    time: '8:00 pm',
    durationMinutes: 60,
    threshold_scanned_at: relMins(8),
  };

  const sessions = [
    liveLeft,
    scenario === 'one-live' ? liveRightOneOnly : liveRightBoth,
    {
      key: `${tsFor(TODAY, 20)}_1`,
      ts: tsFor(TODAY, 20),
      isoDate: TODAY,
      dateKey: TODAY,
      time: '8:00 pm',
      level: 'Expert Barrels',
      session_type: 'Expert Barrels',
      wave: 1,
      waveSide: 'Left Wave',
      durationMinutes: 60,
      available: false,
      capacity: 12,
      slots: 0,
      available_entries: 0,
      thresholdInferredSlots: 0,
      threshold_scan_verified: true,
      thresholdScanVerified: true,
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
      thresholdConfidence: 'exact',
      threshold_scanned_at: relMins(10),
      lastBasicCheckAt: relMins(6),
    },
    {
      key: `${tsFor(TODAY, 20)}_2u`,
      ts: tsFor(TODAY, 20),
      isoDate: TODAY,
      dateKey: TODAY,
      time: '8:00 pm',
      level: 'Progressive',
      session_type: 'Progressive',
      wave: 2,
      waveSide: 'Right Wave',
      durationMinutes: 60,
      available: true,
      capacity: null,
      slots: null,
      lastBasicCheckAt: relMins(20),
    },
    {
      key: `${tsFor(TODAY, 21)}_1`,
      ts: tsFor(TODAY, 21),
      isoDate: TODAY,
      dateKey: TODAY,
      time: '9:00 pm',
      level: 'Pro Turns',
      session_type: 'Pro Turns',
      wave: 1,
      waveSide: 'Left Wave',
      durationMinutes: 60,
      available: true,
      capacity: 10,
      slots: 2,
      available_entries: 2,
      thresholdInferredSlots: 2,
      threshold_scan_verified: true,
      thresholdScanVerified: true,
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
      thresholdConfidence: 'exact',
      threshold_scanned_at: relMins(180),
      lastBasicCheckAt: relMins(30),
    },
    {
      key: `${tsFor(TODAY, 21, 30)}_2`,
      ts: tsFor(TODAY, 21, 30),
      isoDate: TODAY,
      dateKey: TODAY,
      time: '9:30 pm',
      level: 'Intermediate',
      session_type: 'Intermediate',
      wave: 2,
      waveSide: 'Right Wave',
      durationMinutes: 60,
      available: true,
      capacity: 12,
      slots: 1,
      available_entries: 1,
      thresholdInferredSlots: 1,
      threshold_scan_verified: true,
      thresholdScanVerified: true,
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
      thresholdConfidence: 'exact',
      threshold_scanned_at: relMins(6),
      lastBasicCheckAt: relMins(5),
    },
    {
      key: `${tsFor(TOMORROW, 9)}_1`,
      ts: tsFor(TOMORROW, 9),
      isoDate: TOMORROW,
      dateKey: TOMORROW,
      time: '9:00 am',
      level: 'Expert Turns',
      session_type: 'Expert Turns',
      wave: 1,
      waveSide: 'Left Wave',
      durationMinutes: 90,
      available: true,
      capacity: 12,
      slots: 5,
      available_entries: 5,
      thresholdInferredSlots: 5,
      threshold_scan_verified: true,
      thresholdScanVerified: true,
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
      thresholdConfidence: 'exact',
      threshold_scanned_at: relMins(45),
      lastBasicCheckAt: relMins(40),
    },
  ];

  const dayRail = [
    { isoDate: '2026-07-28', label: 'Mon', dayNum: 28, watched: false },
    { isoDate: TODAY, label: 'Tue', dayNum: 29, watched: true },
    { isoDate: TOMORROW, label: 'Wed', dayNum: 30, watched: false },
    { isoDate: '2026-07-31', label: 'Thu', dayNum: 31, watched: false },
    { isoDate: '2026-08-01', label: 'Fri', dayNum: 1, watched: true },
    { isoDate: '2026-08-02', label: 'Sat', dayNum: 2, watched: false },
    { isoDate: '2026-08-03', label: 'Sun', dayNum: 3, watched: false },
  ];

  const watchedKeys = new Set([
    `${tsFor(TODAY, 20)}_1`,
  ]);

  global.BrowseUiFixtures = {
    TODAY,
    TOMORROW,
    LEVELS,
    sessions,
    dayRail,
    watchedKeys,
    alertsEnabled: true,
    alertsOff: false,
    scenario,
    nowMs,
    nowIso: scenarioCfg.nowIso,
  };
}(typeof window !== 'undefined' ? window : globalThis));
