'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const AV = require('../lib/browse-availability-view');
const filters = require('../lib/browse-session-filters');

console.log('browse availability view regression');

const DAY = '2026-07-31';
const FUTURE_TS = Math.floor(Date.now() / 1000) + 7200;

function trustedSession(overrides = {}) {
  return {
    key: overrides.key || 'session-1',
    isoDate: overrides.isoDate ?? DAY,
    dateKey: overrides.dateKey ?? DAY,
    ts: overrides.ts ?? FUTURE_TS,
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

{
  const vm18 = AV.getSessionAvailabilityViewModel(trustedSession({
    available_entries: 9,
    thresholdInferredSlots: 9,
    capacity: 18,
  }));
  assert.strictEqual(vm18.spotsLeft, 9);
  assert.strictEqual(vm18.capacity, 18);
  assert.strictEqual(vm18.isOpen, true);
  assert.strictEqual(vm18.isFull, false);
  assert.strictEqual(vm18.hasTrustedCount, true);
  assert.strictEqual(vm18.spotsLabel, '9 spots left');
  const html = AV.buildAvailabilityDotBarHtml(vm18);
  assert.strictEqual((html.match(/class="slot-dot open/g) || []).length, 9);
  assert.strictEqual((html.match(/class="slot-dot/g) || []).length, 18);
}

{
  const vm17 = AV.getSessionAvailabilityViewModel(trustedSession({
    available_entries: 17,
    thresholdInferredSlots: 17,
    capacity: 18,
  }));
  assert.strictEqual(vm17.spotsLeft, 17);
  assert.strictEqual(vm17.capacity, 18);
  const html = AV.buildAvailabilityDotBarHtml(vm17);
  assert.strictEqual((html.match(/class="slot-dot open/g) || []).length, 17);
  assert.strictEqual((html.match(/class="slot-dot/g) || []).length, 18);
}

{
  const unknownCap = AV.getSessionAvailabilityViewModel(trustedSession({
    available_entries: 4,
    thresholdInferredSlots: 4,
    capacity: null,
    level: 'Unknown Level XYZ',
  }));
  assert.strictEqual(unknownCap.capacity, null);
  assert.strictEqual(unknownCap.spotsLeft, 4);
  const html = AV.buildAvailabilityDotBarHtml(unknownCap);
  assert.strictEqual((html.match(/slot-dot/g) || []).length, 4);
  assert.ok(!html.includes('slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"></span><span class="slot-dot"'));
}

{
  const full = AV.getSessionAvailabilityViewModel(trustedSession({
    available: false,
    available_entries: 0,
    thresholdInferredSlots: 0,
    capacity: 18,
  }));
  assert.strictEqual(full.spotsLeft, 0);
  assert.strictEqual(full.isFull, true);
  assert.strictEqual(full.isOpen, false);
  const html = AV.buildAvailabilityDotBarHtml(full);
  assert.strictEqual((html.match(/slot-dot/g) || []).length, 18);
  assert.ok(!html.includes('slot-dot open'));
}

{
  const conflict = AV.getSessionAvailabilityViewModel(trustedSession({
    available: false,
    available_entries: 4,
    thresholdInferredSlots: 4,
    capacity: 18,
  }));
  assert.strictEqual(conflict.spotsLeft, 4);
  assert.strictEqual(conflict.isOpen, true);
  assert.strictEqual(conflict.isFull, false);
}

function visibleBrowseSessions(sessions, {
  showLessons = false,
  activeWave = null,
  activeLevels = new Set(),
  activeDayKey = DAY,
} = {}) {
  let filtered = sessions.filter((s) => (s.dateKey || s.isoDate) === activeDayKey);
  filtered = filtered.filter((s) => filters.matchesWaveFilter(s, { showLessons, activeWave }));
  if (activeLevels.size) filtered = filtered.filter((s) => activeLevels.has(s.level));
  return filtered;
}

{
  const sessions = [
    trustedSession({ key: 'open-a', available_entries: 9, capacity: 18, wave: 1 }),
    trustedSession({ key: 'open-b', available_entries: 17, capacity: 18, wave: 2, level: 'Advanced Turns' }),
    trustedSession({
      key: 'lesson',
      available_entries: 6,
      capacity: 12,
      wave: 3,
      waveSide: 'Left Lesson',
      level: 'Lesson Only',
    }),
    trustedSession({ key: 'full-c', available: false, available_entries: 0, thresholdInferredSlots: 0, capacity: 18, wave: 1, level: 'Cruiser' }),
  ];

  const visible = visibleBrowseSessions(sessions, { showLessons: false });
  const summaryOpen = AV.countOpenSessions(visible);
  const cardOpen = visible.filter((s) => AV.getSessionAvailabilityViewModel(s).isOpen).length;
  assert.strictEqual(summaryOpen, 2);
  assert.strictEqual(cardOpen, summaryOpen);

  const withLessons = visibleBrowseSessions(sessions, { showLessons: true });
  assert.strictEqual(AV.countOpenSessions(withLessons), 3);

  const leftOnly = visibleBrowseSessions(sessions, { showLessons: false, activeWave: 1 });
  assert.strictEqual(AV.countOpenSessions(leftOnly), 1);
}

{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.ok(html.includes('/browse-availability-view.js?v=14'));
  assert.ok(html.includes('sessionAvailabilityViewModel'));
  assert.ok(html.includes('countOpenSessionsForDay'));
  assert.ok(html.includes('assertOpenCountConsistency'));
  assert.ok(html.includes('availabilityDotBarHtml'));
  assert.ok(!html.includes('dotBarHtmlFromState(state)'));
  assert.ok(!html.includes('sem?.DOT_COUNT || 10'));
}

{
  const sandbox = {
    window: {},
    console,
    Intl,
    Date,
    Object,
    Set,
    Number,
    String,
    Math,
    Array,
    RegExp,
    module: undefined,
    exports: undefined,
    require: undefined,
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  for (const rel of [
    'lib/lineup-config.js',
    'lib/browse-session-filters.js',
    'lib/browse-live-schedule.js',
    'lib/browse-availability-view.js',
    'lib/browse-ui-semantics.js',
  ]) {
    vm.runInContext(fs.readFileSync(path.join(__dirname, '..', rel), 'utf8'), sandbox, { filename: rel });
  }
  assert.strictEqual(typeof sandbox.LineupBrowse?.availabilityView?.getSessionAvailabilityViewModel, 'function');
}

console.log('browse availability view regression: ok');
