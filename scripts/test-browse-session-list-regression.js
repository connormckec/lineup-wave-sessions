'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const AV = require('../lib/browse-availability-view');
const filters = require('../lib/browse-session-filters');

console.log('browse session list regression');

const DAY = '2026-07-31';
const FUTURE_TS = Math.floor(Date.now() / 1000) + 7200;

function trustedSession(overrides = {}) {
  return {
    key: overrides.key || `session-${Math.random().toString(36).slice(2, 8)}`,
    isoDate: overrides.isoDate ?? DAY,
    dateKey: overrides.dateKey ?? DAY,
    ts: overrides.ts ?? FUTURE_TS,
    time: overrides.time ?? '9:00 am',
    level: overrides.level ?? 'Progressive',
    wave: overrides.wave ?? 1,
    waveSide: overrides.waveSide ?? 'Left Wave',
    available: overrides.available ?? true,
    threshold_scan_verified: true,
    thresholdScanVerified: true,
    slot_status: 'exact',
    slot_source: 'entries_left_threshold_scan',
    thresholdConfidence: 'exact',
    threshold_scanned_at: '2026-07-31T12:00:00.000Z',
    ...overrides,
  };
}

function applyBrowseFilters(sessions, {
  showLessons = false,
  activeWave = null,
  activeLevels = new Set(),
  activeDayKey = DAY,
  excludePastLive = false,
} = {}) {
  let filtered = sessions.filter((s) => {
    const level = s.level;
    return level && !['Cabanas', 'Beach Pass'].includes(level) && ![5, 6].includes(Number(s.wave));
  });
  filtered = filtered.filter((s) => filters.matchesWaveFilter(s, { showLessons, activeWave }));
  if (activeLevels.size) filtered = filtered.filter((s) => activeLevels.has(s.level));
  return filtered;
}

{
  const openSessions = Array.from({ length: 18 }, (_, i) => trustedSession({
    key: `open-${i}`,
    available_entries: 5 + (i % 4),
    capacity: 18,
    ts: FUTURE_TS + i * 3600,
  }));
  const fullSessions = Array.from({ length: 5 }, (_, i) => trustedSession({
    key: `full-${i}`,
    available: false,
    available_entries: 0,
    thresholdInferredSlots: 0,
    capacity: 18,
    level: 'Advanced Turns',
    wave: 2,
    waveSide: 'Right Wave',
    ts: FUTURE_TS + (20 + i) * 3600,
  }));
  const all = [...openSessions, ...fullSessions];
  const visible = applyBrowseFilters(all, { showLessons: false });
  const openCount = AV.countOpenSessions(visible);
  const listCount = visible.length;

  assert.strictEqual(openCount, 18, 'summary counts open sessions only');
  assert.strictEqual(listCount, 23, 'list includes open and full sessions');
  assert.ok(listCount > openCount);
}

{
  const unknownCap = trustedSession({
    available_entries: 9,
    capacity: null,
    level: 'Unknown Level XYZ',
  });
  const vm = AV.getSessionAvailabilityViewModel(unknownCap);
  assert.strictEqual(vm.capacity, null);
  assert.strictEqual(vm.spotsLabel, '9 spots left');
  assert.strictEqual(AV.buildAvailabilityDotBarHtml(vm), '');
}

{
  const nullCapTrusted = trustedSession({
    available_entries: 4,
    capacity: undefined,
    level: 'Mystery',
  });
  const vm = AV.getSessionAvailabilityViewModel(nullCapTrusted);
  assert.strictEqual(vm.spotsLeft, 4);
  assert.strictEqual(AV.buildAvailabilityDotBarHtml(vm), '');
}

{
  const malformed = trustedSession({
    key: 'bad-capacity',
    available_entries: 3,
    capacity: 'not-a-number',
  });
  assert.doesNotThrow(() => AV.buildAvailabilityDotBarHtml(AV.getSessionAvailabilityViewModel(malformed)));
  const weird = trustedSession({
    key: 'zero-capacity',
    available_entries: 2,
    capacity: 0,
    level: 'Mystery Wave',
  });
  assert.strictEqual(AV.buildAvailabilityDotBarHtml(AV.getSessionAvailabilityViewModel(weird)), '');
}

{
  const pool = [
    trustedSession({ key: 'good', available_entries: 8, capacity: 18 }),
    trustedSession({
      key: 'bad',
      available_entries: 5,
      capacity: NaN,
      level: 'Advanced Turns',
      wave: 2,
      waveSide: 'Right Wave',
    }),
  ];
  const rendered = [];
  for (const session of applyBrowseFilters(pool)) {
    try {
      const vm = AV.getSessionAvailabilityViewModel(session);
      rendered.push({ key: session.key, vm, dots: AV.buildAvailabilityDotBarHtml(vm) });
    } catch (err) {
      rendered.push({ key: session.key, error: err.message });
    }
  }
  assert.strictEqual(rendered.length, 2);
  assert.ok(rendered.every((entry) => !entry.error));
  assert.ok(rendered.some((entry) => entry.key === 'good'));
  assert.ok(rendered.some((entry) => entry.key === 'bad'));
}

{
  const hiddenLesson = trustedSession({
    key: 'lesson-hidden',
    wave: 3,
    waveSide: 'Left Lesson',
    level: 'Lesson Only',
    available_entries: 6,
  });
  const visible = applyBrowseFilters([hiddenLesson, trustedSession({ key: 'turns' })], { showLessons: false });
  assert.strictEqual(visible.length, 1);
  assert.strictEqual(visible[0].key, 'turns');
}

{
  const leftOnly = applyBrowseFilters([
    trustedSession({ key: 'left', wave: 1 }),
    trustedSession({ key: 'right', wave: 2, waveSide: 'Right Wave' }),
  ], { activeWave: 1 });
  assert.strictEqual(leftOnly.length, 1);
  assert.strictEqual(leftOnly[0].key, 'left');
}

{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.match(html, /const browseState = \{/);
  assert.match(html, /function getBrowseSessionsForDay\(/);
  assert.match(html, /const browseSessionPool = Array\.isArray\(browseState\.selectedDateSessions\)/);
  assert.doesNotMatch(html, /function browseSessionPool\(/);
  assert.doesNotMatch(html, /function browseSessionPoolForDay\(/);
  assert.doesNotMatch(html, /browseSessionPool\(\)/);
  assert.ok(!html.includes('filtered.map(sessionCardHtml)'));
  assert.ok(html.includes('No sessions match these filters.'));
  assert.ok(html.includes('for (const session of filtered)'));
}

{
  function deriveBrowseSessionPool(state) {
    return Array.isArray(state.selectedDateSessions)
      ? state.selectedDateSessions
      : [];
  }

  const beforeLoad = { selectedDateSessions: [], meta: { isLoading: true } };
  assert.doesNotThrow(() => deriveBrowseSessionPool(beforeLoad).filter(() => true));
  assert.strictEqual(deriveBrowseSessionPool(beforeLoad).length, 0);
  assert.strictEqual(deriveBrowseSessionPool({ selectedDateSessions: null }).length, 0);
  assert.strictEqual(deriveBrowseSessionPool({}).length, 0);

  const loaded = {
    selectedDateSessions: Array.from({ length: 56 }, (_, i) => trustedSession({
      key: `loaded-${i}`,
      available_entries: i % 5,
      available: i % 7 !== 0,
      capacity: 18,
      ts: FUTURE_TS + i * 60,
    })),
    meta: { statusReason: 'saved_sessions_found', sessionsCount: 56 },
  };
  assert.strictEqual(deriveBrowseSessionPool(loaded).length, 56);
  const visible = applyBrowseFilters(deriveBrowseSessionPool(loaded), { showLessons: false });
  assert.ok(visible.length > 0);
  assert.ok(visible.some((s) => AV.getSessionAvailabilityViewModel(s).isOpen));
  assert.ok(visible.some((s) => AV.getSessionAvailabilityViewModel(s).isFull));
  assert.ok(visible.length >= AV.countOpenSessions(visible));
}

console.log('browse session list regression: ok');
