'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const capacity = require('../lib/session-capacity-config');
const AV = require('../lib/browse-availability-view');
const live = require('../lib/browse-live-schedule');
const filters = require('../lib/browse-session-filters');
const SEM = require('../lib/browse-ui-semantics');

console.log('session occupancy regression');

const DAY = '2026-07-31';
const FUTURE_TS = Math.floor(Date.now() / 1000) + 7200;

function trustedSession(overrides = {}) {
  return {
    key: overrides.key || `session-${Math.random().toString(36).slice(2, 8)}`,
    isoDate: overrides.isoDate ?? DAY,
    dateKey: overrides.dateKey ?? DAY,
    ts: overrides.ts ?? FUTURE_TS,
    time: overrides.time ?? '10:00 am',
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

function countDots(html, className) {
  if (!html) return 0;
  if (className === 'gray') return (html.match(/class="slot-dot"/g) || []).length;
  return (html.match(new RegExp(`class="slot-dot ${className}"`, 'g')) || []).length;
}

function atLocal(isoDate, clock) {
  const ms = live.wallClockToUtcMs(isoDate, clock);
  assert.ok(ms != null, `wall clock ${isoDate} ${clock}`);
  return ms;
}

{
  assert.strictEqual(capacity.resolveCapacityForLevel('Progressive'), 18);
  assert.strictEqual(capacity.resolveCapacityForLevel('intermediate'), 12);
  assert.strictEqual(capacity.resolveCapacityForLevel('Advanced Turns'), 12);
  assert.strictEqual(capacity.resolveCapacityForLevel('Advanced Barrels'), 12);
  assert.strictEqual(capacity.resolveCapacityForLevel('Expert Turns'), 12);
  assert.strictEqual(capacity.resolveCapacityForLevel('Expert Barrels'), 12);
  assert.strictEqual(capacity.resolveCapacityForLevel('Pro Turns'), 10);
  assert.strictEqual(capacity.resolveCapacityForLevel('Pro Barrels'), 10);
  assert.strictEqual(capacity.normalizeLevelName('  Advanced   Turns '), 'advanced turns');
}

function assertDotBar(session, { gray, teal }) {
  const vmModel = AV.getSessionOccupancyViewModel(session);
  const html = AV.buildAvailabilityDotBarHtml(vmModel);
  assert.strictEqual(countDots(html, 'gray'), gray, `gray dots for ${session.key || session.level}`);
  assert.strictEqual(countDots(html, 'open') + countDots(html, 'scarce'), teal, `teal dots for ${session.key || session.level}`);
}

{
  assertDotBar(trustedSession({ level: 'Progressive', available_entries: 17 }), { gray: 1, teal: 17 });
  assertDotBar(trustedSession({ level: 'Intermediate', available_entries: 1 }), { gray: 11, teal: 1 });
  assertDotBar(trustedSession({ level: 'Expert Turns', available_entries: 2 }), { gray: 10, teal: 2 });
  assertDotBar(trustedSession({ level: 'Pro Turns', available_entries: 8 }), { gray: 2, teal: 8 });
  assertDotBar(trustedSession({ level: 'Progressive', available_entries: 0, available: false }), { gray: 18, teal: 0 });
  assertDotBar(trustedSession({ level: 'Progressive', available_entries: 18 }), { gray: 0, teal: 18 });
}

{
  const over = AV.getSessionOccupancyViewModel(trustedSession({
    level: 'Progressive',
    available_entries: 25,
  }));
  assert.ok(over.diagnosticWarnings.some((w) => w.code === 'spots_exceed_capacity'));
  assert.strictEqual(over.capacity, 18);
  assert.strictEqual(over.spotsLeft, 25);
}

{
  const lesson = trustedSession({
    level: 'Lesson Only',
    wave: 3,
    waveSide: 'Left Lesson',
    available_entries: 6,
    capacity: 18,
  });
  const lessonVm = AV.getSessionOccupancyViewModel(lesson);
  assert.strictEqual(lessonVm.capacity, null);
  assert.strictEqual(AV.buildAvailabilityDotBarHtml(lessonVm), '');
}

{
  const session = trustedSession({ level: 'Intermediate', available_entries: 4 });
  const browseVm = AV.getSessionOccupancyViewModel(session);
  const lineupVm = AV.getSessionOccupancyViewModel({ ...session, capacity: 99, thresholdMaxVisible: 50 });
  assert.deepStrictEqual(
    AV.buildAvailabilityDotBarHtml(browseVm),
    AV.buildAvailabilityDotBarHtml(lineupVm),
  );
  assert.strictEqual(browseVm.spotsLabel, lineupVm.spotsLabel);
}

{
  const lesson930 = {
    key: 'lesson-930',
    isoDate: DAY,
    time: '9:30 am',
    tileText: 'From : 9:30 am - To : 11:00 am',
    wave: 3,
    waveSide: 'Left Lesson',
    level: 'Beginner (lesson only)',
  };
  const wave1000 = {
    key: 'wave-1000',
    isoDate: DAY,
    time: '10:00 am',
    tileText: 'From : 9:30 am - To : 11:00 am',
    wave: 1,
    waveSide: 'Left Wave',
    level: 'Intermediate',
  };
  const nowMs = atLocal(DAY, '10:22 am');
  assert.strictEqual(live.isStandardWaveSession(lesson930), false);
  assert.strictEqual(live.isStandardWaveSession({ wave: 4, waveSide: 'Right Lesson', level: 'Lesson Only' }), false);
  assert.strictEqual(live.isStandardWaveSession(wave1000), true);
  const picked = live.pickLiveSessionForSide([lesson930, wave1000], 'left', nowMs);
  assert.strictEqual(picked.key, 'wave-1000');
  assert.strictEqual(live.formatClockRange(wave1000), '10:00–11:00 am');
  const summary = SEM.computeLiveNowSummary([lesson930, wave1000], nowMs);
  assert.strictEqual(summary.mode, 'live');
  assert.strictEqual(summary.left.session.key, 'wave-1000');
  assert.match(summary.windowLabel || live.formatClockRange(wave1000), /10:00.*11:00/i);
}

{
  const nowMs = atLocal(DAY, '6:00 am');
  const summary = SEM.computeLiveNowSummary([
    trustedSession({ time: '2:00 pm', tileText: 'From : 2:00 pm - To : 3:00 pm' }),
  ], nowMs);
  assert.strictEqual(summary.mode, 'idle');
  assert.match(JSON.stringify(summary), /NO SESSION LIVE/i);
}

{
  assert.strictEqual(filters.isLessonSession({ level: 'Lesson Only' }), true);
  assert.strictEqual(filters.isLessonSession({ level: 'Beginner (lesson only)' }), true);
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Left Lesson', wave: 3 }), true);
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Right Lesson', wave: 4 }), true);
  assert.strictEqual(filters.isLessonSession(trustedSession()), false);
  assert.strictEqual(filters.matchesWaveFilter(
    trustedSession({ wave: 3, waveSide: 'Left Lesson', level: 'Lesson Only' }),
    { showLessons: false },
  ), false);
}

{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.ok(html.includes('/session-capacity-config.js?v=17'));
  assert.ok(html.includes('sessionOccupancyViewModel'));
  assert.ok(html.includes('resolveWatchedSessionRecord'));
  assert.ok(!html.includes('live-watch-btn'));
  assert.ok(!html.includes('live-book-btn'));
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
    'lib/session-capacity-config.js',
    'lib/browse-session-filters.js',
    'lib/browse-live-schedule.js',
    'lib/browse-availability-view.js',
    'lib/browse-ui-semantics.js',
  ]) {
    vm.runInContext(fs.readFileSync(path.join(__dirname, '..', rel), 'utf8'), sandbox, { filename: rel });
  }
  assert.strictEqual(typeof sandbox.LineupBrowse?.availabilityView?.getSessionOccupancyViewModel, 'function');
}

console.log('session occupancy regression: ok');
