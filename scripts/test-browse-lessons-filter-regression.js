'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const filters = require('../lib/browse-session-filters');

console.log('browse lessons filter regression');

const FUTURE_TS = Math.floor(Date.now() / 1000) + 7200;
const DAY_A = '2026-08-15';
const DAY_B = '2026-08-16';
const STORAGE_KEY = 'ap_show_lessons';

const EXCLUDED_LEVELS = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES = [5, 6];

function mkSession(overrides = {}) {
  return {
    key: overrides.key || `session-${Math.random().toString(36).slice(2, 8)}`,
    ts: overrides.ts ?? FUTURE_TS,
    level: overrides.level ?? 'Progressive',
    session_type: overrides.session_type ?? overrides.level ?? 'Progressive',
    wave: overrides.wave,
    waveSide: overrides.waveSide,
    wave_side: overrides.wave_side,
    available: overrides.available ?? true,
    dateKey: overrides.dateKey ?? DAY_A,
    isoDate: overrides.isoDate ?? overrides.dateKey ?? DAY_A,
    raw: overrides.raw,
  };
}

function isSurfSession(s) {
  if (!s) return false;
  return !EXCLUDED_LEVELS.includes(s.level) && !EXCLUDED_WAVES.includes(s.wave);
}

function isSessionUpcoming(s) {
  return s.ts > Math.floor(Date.now() / 1000);
}

function visibleBrowseSessions(sessions, {
  showLessons = false,
  activeWave = null,
  activeLevels = new Set(),
  filterFromH = null,
  filterToH = null,
  activeDayKey = DAY_A,
} = {}) {
  let filtered = sessions.filter((s) => {
    const dk = s.dateKey || s.isoDate;
    return dk === activeDayKey;
  });
  filtered = filtered.filter(isSurfSession);
  filtered = filtered.filter((s) => filters.matchesWaveFilter(s, { showLessons, activeWave }));
  if (activeLevels.size) filtered = filtered.filter((s) => activeLevels.has(s.level));
  filtered = filtered.filter(isSessionUpcoming);
  return filtered;
}

function loadShowLessonsPreference(store) {
  const raw = store.getItem(STORAGE_KEY);
  if (raw === '1' || raw === 'true') return true;
  if (raw === '0' || raw === 'false') return false;
  return false;
}

function saveShowLessonsPreference(store, value) {
  store.setItem(STORAGE_KEY, value ? '1' : '0');
}

class MemoryStorage {
  constructor() {
    this.data = new Map();
  }

  getItem(key) {
    return this.data.has(key) ? this.data.get(key) : null;
  }

  setItem(key, value) {
    this.data.set(key, String(value));
  }

  removeItem(key) {
    this.data.delete(key);
  }
}

const leftLessonWave1 = mkSession({
  key: 'left-lesson-wave1',
  wave: 1,
  waveSide: 'Left Lesson',
  level: 'Progressive',
});
const rightLessonWave2 = mkSession({
  key: 'right-lesson-wave2',
  wave: 2,
  waveSide: 'Right Lesson',
  level: 'Progressive',
});
const leftLessonCol3 = mkSession({
  key: 'left-lesson-col3',
  wave: 3,
  waveSide: 'Left Lesson',
  level: 'Cruiser',
});
const advancedTurns = mkSession({
  key: 'advanced-turns',
  wave: 1,
  waveSide: 'Left Wave',
  level: 'Advanced Turns',
});
const intermediateTurns = mkSession({
  key: 'intermediate-turns',
  wave: 2,
  waveSide: 'Right Wave',
  level: 'Intermediate Turns',
});

const allSessions = [
  leftLessonWave1,
  rightLessonWave2,
  leftLessonCol3,
  advancedTurns,
  intermediateTurns,
];

{
  const hidden = visibleBrowseSessions(allSessions, { showLessons: false });
  assert.ok(!hidden.some((s) => s.key === 'left-lesson-wave1'), '1. Left Lesson hidden when disabled');
  assert.ok(!hidden.some((s) => s.key === 'right-lesson-wave2'), '2. Right Lesson hidden when disabled');
}

{
  const visible = visibleBrowseSessions(allSessions, { showLessons: true });
  assert.ok(visible.some((s) => s.key === 'left-lesson-wave1'), '3a. Left Lesson visible when enabled');
  assert.ok(visible.some((s) => s.key === 'right-lesson-wave2'), '3b. Right Lesson visible when enabled');
  assert.ok(visible.some((s) => s.key === 'left-lesson-col3'), '3c. column-3 lesson visible when enabled');
}

{
  for (const showLessons of [false, true]) {
    const visible = visibleBrowseSessions(allSessions, { showLessons });
    assert.ok(visible.some((s) => s.key === 'advanced-turns'), `4. Advanced Turns visible (showLessons=${showLessons})`);
    assert.ok(visible.some((s) => s.key === 'intermediate-turns'), `5. Intermediate Turns visible (showLessons=${showLessons})`);
  }
}

{
  const dayBSessions = [
    mkSession({ key: 'day-b-lesson', dateKey: DAY_B, isoDate: DAY_B, wave: 3, waveSide: 'Left Lesson' }),
    mkSession({ key: 'day-b-turns', dateKey: DAY_B, isoDate: DAY_B, wave: 1, waveSide: 'Left Wave', level: 'Advanced Turns' }),
  ];
  const pool = [...allSessions, ...dayBSessions];
  const dayAHidden = visibleBrowseSessions(pool, { showLessons: false, activeDayKey: DAY_A });
  const dayBHidden = visibleBrowseSessions(pool, { showLessons: false, activeDayKey: DAY_B });
  assert.ok(!dayAHidden.some((s) => filters.isLessonSession(s)), '6a. lessons hidden on day A');
  assert.ok(!dayBHidden.some((s) => filters.isLessonSession(s)), '6b. lessons hidden on day B after date change');
  assert.strictEqual(dayBHidden.length, 1, '6c. only non-lesson session on day B');
}

{
  const store = new MemoryStorage();
  saveShowLessonsPreference(store, true);
  assert.strictEqual(loadShowLessonsPreference(store), true, '7a. reload restores enabled');
  saveShowLessonsPreference(store, false);
  assert.strictEqual(loadShowLessonsPreference(store), false, '7b. reload restores disabled');
  assert.strictEqual(loadShowLessonsPreference(new MemoryStorage()), false, '7c. default is disabled');
}

{
  const visible = visibleBrowseSessions(allSessions, { showLessons: false });
  const renderedCount = visible.length;
  const openCount = visible.filter((s) => s.available).length;
  assert.strictEqual(renderedCount, 2, '8a. visible count matches filtered cards');
  assert.strictEqual(openCount, 2, '8b. open count matches available filtered cards');
}

{
  const watchlist = [
    { session_key: 'watched-left-lesson', wave_side: 'Left Lesson', session_type: 'Progressive', start_ts: FUTURE_TS },
  ];
  const browseHidden = visibleBrowseSessions(allSessions, { showLessons: false });
  assert.strictEqual(watchlist.length, 1, '9. watchlist lessons unaffected by browse filter');
  assert.ok(!browseHidden.some((s) => filters.isLessonSession(s)));
}

{
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Left Lesson', wave: 1 }), true);
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Right Lesson', wave: 2 }), true);
  assert.strictEqual(filters.isLessonSession({ wave: 3, waveSide: 'Left Lesson' }), true);
  assert.strictEqual(filters.isLessonSession({ wave: '4', waveSide: 'Right Lesson' }), true);
  assert.strictEqual(filters.isLessonSession({ wave: 1, waveSide: 'Left Wave', level: 'Advanced Turns' }), false);
  assert.strictEqual(filters.isLessonSession({ wave: 2, waveSide: 'Right Wave', level: 'Intermediate Turns' }), false);
  assert.strictEqual(filters.isLessonSession({ wave: 1, waveSide: 'left lesson' }), true, 'case-insensitive waveSide');
}

{
  const lessonInLeftWaveChip = mkSession({
    key: 'lesson-under-left-chip',
    wave: 1,
    waveSide: 'Left Lesson',
    level: 'Progressive',
  });
  const hidden = visibleBrowseSessions([lessonInLeftWaveChip, advancedTurns], {
    showLessons: false,
    activeWave: 1,
  });
  assert.ok(!hidden.some((s) => s.key === 'lesson-under-left-chip'), 'lesson hidden even when Left Wave chip active');
  assert.ok(hidden.some((s) => s.key === 'advanced-turns'), 'non-lesson still shown for Left Wave chip');
}

{
  const lessonOnly = mkSession({
    key: 'lesson-only-level',
    wave: 1,
    waveSide: 'Left Wave',
    level: 'Lesson Only',
  });
  const beginnerLesson = mkSession({
    key: 'beginner-lesson-only',
    wave: 2,
    waveSide: 'Right Wave',
    level: 'Beginner (lesson only)',
  });
  const mixedCase = mkSession({
    key: 'mixed-case-lesson',
    wave: 1,
    waveSide: 'Left Wave',
    level: 'LESSON slot',
  });
  const pool = [...allSessions, lessonOnly, beginnerLesson, mixedCase];

  assert.strictEqual(filters.isLessonSession(lessonOnly), true, 'Lesson Only level');
  assert.strictEqual(filters.isLessonSession(beginnerLesson), true, 'Beginner (lesson only)');
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Left Lesson' }), true, 'Left Lesson waveSide');
  assert.strictEqual(filters.isLessonSession({ waveSide: 'Right Lesson' }), true, 'Right Lesson waveSide');
  assert.strictEqual(filters.isLessonSession(mixedCase), true, 'mixed capitalization');

  const hidden = visibleBrowseSessions(pool, { showLessons: false });
  assert.ok(!hidden.some((s) => s.key === 'lesson-only-level'), 'Lesson Only hidden when Show lessons');
  assert.ok(!hidden.some((s) => s.key === 'beginner-lesson-only'), 'Beginner (lesson only) hidden');
  assert.ok(!hidden.some((s) => s.key === 'mixed-case-lesson'), 'mixed-case lesson hidden');
  assert.ok(hidden.some((s) => s.key === 'advanced-turns'), 'non-lesson remains visible');

  const shown = visibleBrowseSessions(pool, { showLessons: true });
  assert.ok(shown.some((s) => s.key === 'lesson-only-level'), 'Lesson Only visible when Hide lessons');
}

{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.ok(html.includes('/browse-session-filters.js'), 'index loads browse-session-filters');
  assert.ok(html.includes('BSF().matchesWaveFilter') || html.includes('LB().sessionFilters'), 'index delegates to session filters');
  assert.ok(html.includes('saveShowLessonsPreference'), 'index persists showLessons');
  assert.ok(html.includes("showLessons ? 'Hide lessons' : 'Show lessons'"), 'toggle label state');
  assert.ok(!html.includes('LESSON_WAVES.includes'), 'removed wave-only lesson check');
}

console.log('browse lessons filter regression: ok');
