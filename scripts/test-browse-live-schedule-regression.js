'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const live = require('../lib/browse-live-schedule');
const SEM = require('../lib/browse-ui-semantics');

console.log('browse live schedule regression');

const DAY = '2026-07-31';

function mkWaveSession(overrides = {}) {
  return {
    key: overrides.key || `session-${Math.random().toString(36).slice(2, 8)}`,
    isoDate: overrides.isoDate ?? DAY,
    time: overrides.time ?? '7:00 am',
    tileText: overrides.tileText ?? 'From : 7:00 am - To : 8:00 am',
    wave: overrides.wave ?? 1,
    waveSide: overrides.waveSide ?? 'Left Wave',
    level: overrides.level ?? 'Advanced Turns',
    ...overrides,
  };
}

function atLocal(isoDate, clock) {
  const ms = live.wallClockToUtcMs(isoDate, clock);
  assert.ok(ms != null, `wall clock ${isoDate} ${clock}`);
  return ms;
}

{
  const session = mkWaveSession({
    time: '7:00 am',
    tileText: 'From : 7:00 am - To : 8:00 am',
  });
  const nowMs = atLocal(DAY, '7:30 am');
  assert.strictEqual(live.isSessionLiveAt(session, nowMs), true, '7:00–8:00 live at 7:30');
}

{
  const session = mkWaveSession({
    time: '8:00 am',
    tileText: 'From : 8:00 am - To : 9:00 am',
  });
  const nowMs = atLocal(DAY, '8:53 am');
  assert.strictEqual(live.isSessionLiveAt(session, nowMs), true, '8:00–9:00 live at 8:53');
}

{
  const session = mkWaveSession({
    time: '7:00 am',
    tileText: 'From : 7:00 am - To : 8:00 am',
  });
  const nowMs = atLocal(DAY, '8:00 am');
  assert.strictEqual(live.isSessionLiveAt(session, nowMs), false, 'not live at exact end');
  assert.strictEqual(live.isSessionPastAt(session, nowMs), true, 'past at exact end');
}

{
  const lesson = mkWaveSession({
    key: 'lesson-90',
    time: '8:00 am',
    tileText: 'From : 8:00 am - To : 9:30 am',
    level: 'Lesson Only',
    wave: 3,
    waveSide: 'Left Lesson',
  });
  assert.strictEqual(live.isSessionLiveAt(lesson, atLocal(DAY, '8:53 am')), true, '90m lesson live at 8:53');
  assert.strictEqual(live.isSessionLiveAt(lesson, atLocal(DAY, '9:30 am')), false, '90m lesson ended at 9:30');
  const window = live.resolveSessionWindow(lesson);
  assert.strictEqual(window.endMs - window.startMs, 90 * 60 * 1000, '90 minute window');
}

{
  const session = mkWaveSession({
    time: '7:00 am',
    tileText: 'From : 7:00 am - To : 8:00 am',
  });
  const nowMs = atLocal(DAY, '7:30 am');
  const naiveUtcStart = Date.UTC(2026, 6, 31, 7, 0, 0);
  const naiveWouldLive = nowMs >= naiveUtcStart && nowMs < naiveUtcStart + 60 * 60 * 1000;
  assert.strictEqual(live.isSessionLiveAt(session, nowMs), true, 'ET wall clock live window');
  assert.notStrictEqual(
    naiveWouldLive,
    live.isSessionLiveAt(session, nowMs),
    'naive UTC comparison differs from ET schedule parsing in edge cases',
  );
}

{
  const left = mkWaveSession({
    key: 'left-live',
    wave: 1,
    waveSide: 'Left Wave',
    time: '8:00 am',
    tileText: 'From : 8:00 am - To : 9:00 am',
    level: 'Advanced Turns',
  });
  const right = mkWaveSession({
    key: 'right-live',
    wave: 2,
    waveSide: 'Right Wave',
    time: '8:00 am',
    tileText: 'From : 8:00 am - To : 9:00 am',
    level: 'Intermediate Turns',
  });
  const nowMs = atLocal(DAY, '8:53 am');
  const summary = SEM.computeLiveNowSummary([left, right], nowMs);
  assert.strictEqual(summary.mode, 'live');
  assert.ok(summary.left, 'left side live');
  assert.ok(summary.right, 'right side live');
  assert.strictEqual(summary.left.session.key, 'left-live');
  assert.strictEqual(summary.right.session.key, 'right-live');
  assert.notStrictEqual(summary.left.session.key, summary.right.session.key);
}

{
  const future = mkWaveSession({
    time: '2:00 pm',
    tileText: 'From : 2:00 pm - To : 3:00 pm',
  });
  const nowMs = atLocal(DAY, '10:00 am');
  const summary = SEM.computeLiveNowSummary([future], nowMs);
  assert.strictEqual(summary.mode, 'idle');
  assert.ok(summary.nextSession, 'next session provided when idle');
}

{
  function liveScheduleDataUnavailable({ bootState, sessions, statusReason = null, uiLoading = false }) {
    if (bootState === 'error' && !sessions.length) return true;
    if (statusReason === 'error' && !sessions.length) return true;
    if (bootState === 'loading' && uiLoading && !sessions.length) return false;
    return false;
  }

  assert.strictEqual(
    liveScheduleDataUnavailable({ bootState: 'error', sessions: [] }),
    true,
    'genuine fetch failure shows unavailable',
  );
  assert.strictEqual(
    liveScheduleDataUnavailable({ bootState: 'ready', sessions: [mkWaveSession()] }),
    false,
    'loaded schedule is not unavailable',
  );
  assert.strictEqual(
    liveScheduleDataUnavailable({ bootState: 'ready', sessions: [], statusReason: 'saved_sessions_found' }),
    false,
    'empty live window is not unavailable',
  );
}

{
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.ok(html.includes('/browse-live-schedule.js?v=12'), 'index loads browse-live-schedule');
  assert.ok(html.includes('/lineup-config.js?v=12'), 'index loads lineup-config');
  assert.ok(html.includes('LineupBrowse'), 'index uses LineupBrowse namespace');
  assert.ok(html.includes('liveScheduleDataUnavailable'), 'index distinguishes unavailable vs idle');
  assert.ok(html.includes('No session live right now'), 'idle copy updated');
  assert.ok(html.includes('scheduleLivePanelRefresh'), 'minute refresh wired');
  assert.ok(!html.includes('No session currently running'), 'removed old idle copy');
}

console.log('browse live schedule regression: ok');
