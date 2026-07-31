'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const SEM = require('../lib/browse-ui-semantics');

console.log('browse ui production regression');

const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');

{
  assert.match(html, /\/browse-ui-semantics\.js\?v=9/);
  assert.match(html, /id="live-panel"/);
  assert.match(html, /id="day-rail"/);
  assert.match(html, /id="level-tabs"/);
  assert.match(html, /id="sidebar"/);
  assert.match(html, /function renderLivePanel\(/);
  assert.match(html, /function renderDayRail\(/);
  assert.match(html, /function renderSidebar\(/);
  assert.match(html, /class="watch-btn/);
  assert.match(html, /computeLiveNowSummary/);
  assert.match(html, /allKnownSessions\(\)/);
  assert.doesNotMatch(html, /trusted open/i);
  assert.doesNotMatch(html, /Live waves/i);
  assert.doesNotMatch(html, /id="level-chips"/);
  assert.doesNotMatch(html, /class="bell-btn/);
  assert.match(html, /Atlantic Park<\/p>/);
  assert.doesNotMatch(html, /Atlantic Park sessions/);
  assert.match(html, /id="date-prev"/);
  assert.match(html, /id="date-next"/);
  assert.match(html, /session-card\.is-full/);
  assert.match(html, /sessionsForLiveNow/);
  assert.match(html, /watchButtonHtml/);
  assert.match(html, /aria-label="Previous day"/);
  assert.match(html, /id="selected-date-label"/);
  assert.match(html, /formatSelectedDateBrowseLabel/);
  assert.match(html, /renderSelectedDateLabel/);
  assert.match(html, /Book now/);
  assert.doesNotMatch(html, /Book at Atlantic Park/);
  assert.match(html, /aria-label="Next day"/);
}

{
  const full = {
    available: false,
    slots: 0,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    available_entries: 0,
  };
  assert.strictEqual(SEM.watchLabelForSession(full, false, true).label, 'Watch');
  assert.strictEqual(SEM.watchLabelForSession(full, true, true).label, 'Watching');
}

{
  const nowMs = Date.parse('2026-07-29T19:30:00-04:00');
  const live = SEM.computeLiveNowSummary([
    {
      ts: Math.floor(Date.parse('2026-07-29T19:00:00-04:00') / 1000),
      wave: 1,
      waveSide: 'Left Wave',
      level: 'Advanced Turns',
      durationMinutes: 60,
      capacity: 12,
      available: true,
      available_entries: 5,
      threshold_scan_verified: true,
      slot_source: 'entries_left_threshold_scan',
      threshold_scanned_at: new Date(nowMs - 8 * 60 * 1000).toISOString(),
    },
    {
      ts: Math.floor(Date.parse('2026-07-29T19:00:00-04:00') / 1000),
      wave: 2,
      waveSide: 'Right Wave',
      level: 'Progressive',
      durationMinutes: 60,
      capacity: 10,
      available: false,
      available_entries: 0,
      threshold_scan_verified: true,
      slot_source: 'entries_left_threshold_scan',
      threshold_scanned_at: new Date(nowMs - 5 * 60 * 1000).toISOString(),
    },
  ], nowMs);
  assert.strictEqual(live.mode, 'live');
  assert.ok(live.left);
  assert.ok(live.right);
  assert.match(live.left.occupancyLine, /Estimated 7 surfers/);
  assert.strictEqual(live.right.stateLabel, 'Full');
}

console.log('browse ui production regression: ok');
