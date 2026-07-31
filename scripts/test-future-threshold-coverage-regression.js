'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const adaptiveSchedule = require('../lib/adaptive-threshold-schedule');
const maintenanceQueries = require('../lib/maintenance-queries');
const thresholdDatePipeline = require('../lib/threshold-date-pipeline');
const thresholdWorkerClaim = require('../lib/threshold-worker-claim');
const SEM = require('../lib/browse-ui-semantics');
const tm = require('../lib/threshold-maintenance');

console.log('future threshold coverage regression');

function session(overrides = {}) {
  return {
    key: overrides.key || 'sess-1',
    ts: overrides.ts ?? Math.floor(Date.now() / 1000) + 6 * 3600,
    isoDate: overrides.isoDate || '2026-07-31',
    dateKey: overrides.dateKey || overrides.isoDate || '2026-07-31',
    available: overrides.available ?? true,
    threshold_scanned_at: overrides.threshold_scanned_at ?? null,
    ...overrides,
  };
}

function daysFromTodayFn(isoDate, todayIso = '2026-07-27') {
  const a = new Date(`${isoDate}T12:00:00.000Z`);
  const b = new Date(`${todayIso}T12:00:00.000Z`);
  return Math.round((a - b) / (24 * 3600 * 1000));
}

// 1. General horizon extends to 168 hours (not 72).
{
  assert.strictEqual(adaptiveSchedule.NEAR_TERM_DATE_SCAN_MAX_HOURS_GENERAL, 168);
}

// 2. Ordinary session 94 hours away uses 120-minute cadence and is near-term eligible.
{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const ts = Math.floor(new Date('2026-07-31T10:00:00.000Z').getTime() / 1000);
  const s = session({
    key: 'future-94h',
    isoDate: '2026-07-31',
    ts,
    threshold_scanned_at: '2026-07-25T12:00:00.000Z',
  });
  const evalResult = adaptiveSchedule.evaluateInventorySchedule(s, { watched: false, now });
  assert.ok((evalResult.hoursUntilStart ?? 0) > 72 && (evalResult.hoursUntilStart ?? 0) <= 168, 'session is in 72–168h band');
  assert.strictEqual(evalResult.targetFreshnessMinutes, 120);
  assert.strictEqual(evalResult.due, true);

  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([s], {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
  });
  assert.strictEqual(dueScan.candidates.length, 1);
  assert.strictEqual(dueScan.candidates[0].isoDate, '2026-07-31');
}

// 3. Ordinary session 170 hours away is not near-term eligible.
{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const ts = Math.floor(new Date('2026-08-03T14:00:00.000Z').getTime() / 1000);
  const s = session({
    key: 'future-170h',
    isoDate: '2026-08-03',
    ts,
    threshold_scanned_at: null,
  });
  assert.ok((adaptiveSchedule.hoursUntilSessionStart(s, now) ?? 0) > 168);
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([s], {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
  });
  assert.strictEqual(dueScan.candidates.length, 0);
  assert.strictEqual(
    adaptiveSchedule.isDateWithinNearTermDateScan({
      isoDate: '2026-08-03',
      watched: false,
      todayIso: '2026-07-27',
      daysFromTodayFn,
      hoursUntilStart: adaptiveSchedule.hoursUntilSessionStart(s, now),
    }),
    false,
  );
}

// 4. Watched session keeps higher priority over general future date.
{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const watchedTs = Math.floor(new Date('2026-08-02T10:00:00.000Z').getTime() / 1000);
  const generalTs = Math.floor(new Date('2026-07-30T10:00:00.000Z').getTime() / 1000);
  const watched = session({
    key: 'watched-future',
    isoDate: '2026-08-02',
    ts: watchedTs,
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  const general = session({
    key: 'general-near',
    isoDate: '2026-07-30',
    ts: generalTs,
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([general, watched], {
    watchKeys: new Set(['watched-future']),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
  });
  assert.ok(dueScan.topCandidate);
  assert.strictEqual(dueScan.topCandidate.isoDate, '2026-08-02');
  assert.ok(dueScan.topCandidate.watchedDueCount >= 1);
}

// 5. <=72-hour general cadence remains unchanged (10m same-day, 30m inside 72h).
{
  const now = new Date('2026-07-27T18:00:00.000Z');
  const sameDay = session({
    ts: Math.floor(new Date('2026-07-27T20:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-27T17:50:00.000Z',
  });
  const within72 = session({
    ts: Math.floor(new Date('2026-07-29T12:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-27T17:00:00.000Z',
  });
  assert.strictEqual(
    adaptiveSchedule.evaluateInventorySchedule(sameDay, { watched: false, now }).targetFreshnessMinutes,
    10,
  );
  assert.strictEqual(
    adaptiveSchedule.evaluateInventorySchedule(within72, { watched: false, now }).targetFreshnessMinutes,
    30,
  );
}

// 6. Compact diagnostics include selected date freshness metadata.
{
  const candidate = {
    isoDate: '2026-07-31',
    watchedDueCount: 0,
    generalDueCount: 2,
    mostOverdueMinutes: 180,
    earliestHoursUntilStart: 94,
    sessions: [{
      sessionKey: 'future-94h',
      watched: false,
      due: true,
      hoursUntilStart: 94,
      targetFreshnessMinutes: 120,
      actualFreshnessMinutes: 3000,
      cadenceSource: 'general_proximity',
    }],
  };
  const diag = maintenanceQueries.buildSelectedDateDiagnostics(candidate, {
    remainingDueDateCount: 3,
    selectionReason: 'general_most_overdue',
  });
  assert.strictEqual(diag.isoDate, '2026-07-31');
  assert.strictEqual(diag.hoursUntilEarliestSession, 94);
  assert.strictEqual(diag.targetFreshnessMinutes, 120);
  assert.strictEqual(diag.actualFreshnessMinutes, 3000);
  assert.strictEqual(diag.selectionReason, 'general_most_overdue');
  assert.strictEqual(diag.remainingDueDateCount, 3);
}

// 7. Near-term tick enqueues at most one date and skips duplicate active dates.
{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const tickBody = serverJs.slice(
    serverJs.indexOf('async function runNearTermMaintenanceTick'),
    serverJs.indexOf('async function runBroadMaintenanceTick'),
  );
  assert.match(tickBody, /hasQueuedOrRunningThresholdScanJob/);
  assert.match(tickBody, /activeDateScans\.has\(top\.isoDate\)/);
  assert.match(tickBody, /enqueueNearTermDateThresholdScan\(top\.isoDate/);
  assert.match(tickBody, /selectedDateDiagnostics/);
  assert.doesNotMatch(tickBody, /for \(const candidate of dueScan\.candidates\)/);
}

// 8. Worker claim prevents concurrent Playwright scans.
{
  const eligible = thresholdWorkerClaim.evaluateQueuedJobClaimSkipReason(
    { id: 'job-1', mode: thresholdWorkerClaim.THRESHOLD_SCAN_JOB_MODE_DATE },
    { runningCount: 1, scrapeLockAvailable: true },
  );
  assert.strictEqual(eligible.eligible, false);
  assert.strictEqual(eligible.skipReason, 'job_already_running');
}

// 9. Stale exact count remains labeled stale in Live now presentation.
{
  const nowMs = Date.parse('2026-07-27T18:00:00.000Z');
  const s = {
    key: 'stale-trusted',
    level: 'Progressive',
    wave: 1,
    waveSide: 'Left Wave',
    ts: Math.floor(Date.parse('2026-07-27T20:00:00.000Z') / 1000),
    time: '4:00 pm',
    available: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 4,
    threshold_scanned_at: '2026-07-27T16:00:00.000Z',
  };
  const side = SEM.buildLiveSidePresentation(s, nowMs);
  assert.ok(side.stale, 'old trusted count is stale');
  assert.match(side.detailLine, /Verified 2h ago|Last verified 4 spots remaining · 2h ago/);
}

// 10. Open session without trusted count uses honest unavailable wording in Browse.
{
  const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
  assert.match(html, /Count not yet verified/);
  assert.match(html, /if \(s\?\.available !== false\)/);
}

// 11. Basic scrape cannot refresh threshold verification timestamp on merge.
{
  const existing = {
    key: 'sess-merge',
    available: true,
    thresholdScanVerified: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 5,
    thresholdScanAt: '2026-07-24T18:25:36.000Z',
    threshold_scanned_at: '2026-07-24T18:25:36.000Z',
  };
  const merged = tm.mergeSessionThresholdFields({
    key: 'sess-merge',
    available: true,
    lastBasicCheckAt: '2026-07-31T15:00:00.000Z',
    thresholdScanVerified: false,
    threshold_scan_verified: false,
  }, existing, {
    scrapeKind: 'basic',
    nowIso: '2026-07-31T15:00:00.000Z',
  });
  assert.strictEqual(merged.thresholdScanAt, '2026-07-24T18:25:36.000Z');
  assert.strictEqual(merged.available_entries, 5);
}

// 12. Gradual catch-up: many due dates still surface one top candidate only.
{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const sessions = [];
  const dueDates = ['2026-07-28', '2026-07-29', '2026-07-30', '2026-07-31', '2026-08-01'];
  for (let day = 0; day < dueDates.length; day += 1) {
    const isoDate = dueDates[day];
    sessions.push(session({
      key: `due-${day}`,
      isoDate,
      ts: Math.floor(new Date(`${isoDate}T14:00:00.000Z`).getTime() / 1000),
      threshold_scanned_at: '2026-07-20T12:00:00.000Z',
    }));
  }
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates(sessions, {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
  });
  assert.ok(dueScan.candidates.length >= 3);
  assert.ok(dueScan.topCandidate);
  const summary = maintenanceQueries.compactDueScanSummary(dueScan);
  assert.ok(summary.remainingDueDateCount >= 2);
  assert.ok(summary.topDueDateCandidate.targetFreshnessMinutes != null);
}

console.log('future threshold coverage regression: ok');
