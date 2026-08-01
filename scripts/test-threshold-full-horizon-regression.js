'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const supportedHorizon = require('../lib/supported-horizon-config');
const calendarNavigation = require('../lib/calendar-navigation');
const thresholdScanContract = require('../lib/threshold-scan-contract');
const adaptiveSchedule = require('../lib/adaptive-threshold-schedule');
const maintenanceQueries = require('../lib/maintenance-queries');
const thresholdDatePipeline = require('../lib/threshold-date-pipeline');
const thresholdWorkerClaim = require('../lib/threshold-worker-claim');

console.log('threshold full horizon regression');

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

// ── HORIZON CONFIG ──
{
  const horizon = supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-27' });
  assert.strictEqual(horizon.earliestSupportedDate, '2026-07-27');
  assert.strictEqual(horizon.latestSupportedDate, '2026-08-23');
  assert.strictEqual(horizon.supportedHorizonDays, 28);
  assert.strictEqual(horizon.thresholdScanMaxHours, 28 * 24);
  assert.ok(horizon.maxNavigationSteps >= 6);
}

{
  const capped = supportedHorizon.resolveSupportedHorizon({
    todayIso: '2026-07-27',
    scrapeWeeksAhead: 4,
    effectiveWeeksAhead: 2,
  });
  assert.strictEqual(capped.supportedHorizonDays, 28);
  assert.strictEqual(capped.effectiveWeeksAhead, 2);
  assert.strictEqual(capped.derivedDynamically, true);
}

{
  const bounds = supportedHorizon.computeThresholdSchedulingBounds('2026-07-27', {
    latestSupportedDate: '2026-08-23',
    supportedHorizonDays: 28,
  });
  assert.strictEqual(bounds.maxDate, '2026-08-23');
  assert.strictEqual(bounds.minDate, '2026-07-20');
}

// ── NAVIGATION ──
{
  const visible = ['2026-07-28', '2026-07-29', '2026-07-30'];
  assert.strictEqual(
    calendarNavigation.navigationDirectionForVisibleHeaders(visible, '2026-07-30'),
    null,
  );
  assert.strictEqual(
    calendarNavigation.estimateNavigationClicksRequired(visible, '2026-07-30'),
    0,
  );
}

{
  const visible = ['2026-07-28', '2026-07-29', '2026-07-30'];
  assert.strictEqual(
    calendarNavigation.navigationDirectionForVisibleHeaders(visible, '2026-08-05'),
    'next',
  );
}

{
  const visible = ['2026-08-20', '2026-08-21', '2026-08-22', '2026-08-23'];
  const horizon = supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-27' });
  assert.strictEqual(
    calendarNavigation.navigationDirectionForVisibleHeaders(visible, horizon.latestSupportedDate),
    null,
    'max horizon date already visible',
  );
}

{
  const partialWeek = ['2026-07-30', '2026-07-31', '2026-08-01', '2026-08-02'];
  assert.strictEqual(
    calendarNavigation.navigationDirectionForVisibleHeaders(partialWeek, '2026-08-05'),
    'next',
  );
}

{
  const futureStart = ['2026-08-10', '2026-08-11', '2026-08-12'];
  assert.strictEqual(
    calendarNavigation.navigationDirectionForVisibleHeaders(futureStart, '2026-07-29'),
    'prev',
  );
}

{
  const before = ['2026-07-28', '2026-07-29'];
  const after = ['2026-08-04', '2026-08-05'];
  assert.strictEqual(calendarNavigation.calendarStateChanged(before, after), true);
  const attempt = calendarNavigation.createNavigationAttempt({
    attemptNumber: 1,
    direction: 'next',
    controlClicked: '.glyphicon-chevron-right',
    beforeHeaders: before,
    afterHeaders: after,
    clickSucceeded: true,
  });
  assert.strictEqual(attempt.observableStateChanged, true);
}

{
  const headers = ['2026-07-28', '2026-07-29'];
  assert.strictEqual(
    calendarNavigation.mapNavigationFailureReason({
      targetVisible: false,
      direction: 'next',
      clickSucceeded: false,
      step: 2,
      maxSteps: 10,
    }),
    'calendar_navigation_stalled',
  );
  assert.strictEqual(
    calendarNavigation.mapNavigationFailureReason({
      targetVisible: false,
      step: 10,
      maxSteps: 10,
    }),
    'calendar_navigation_limit_reached',
  );
}

// ── COMPLETION CONTRACT ──
{
  const bad = thresholdScanContract.validateTargetDateScanEvidence({
    targetIsoDate: '2026-07-31',
    visibleHeaders: ['2026-07-28', '2026-07-29'],
  });
  assert.strictEqual(bad.ok, false);
  assert.strictEqual(bad.failureReason, 'target_date_not_visible');
}

{
  const tiles = [
    { isoDate: '2026-07-31', identityKey: '2026-07-31|10:00 am|left|PRG' },
    { isoDate: '2026-08-01', identityKey: '2026-08-01|10:00 am|left|PRG' },
  ];
  const wrongWeek = thresholdScanContract.validateTargetDateScanEvidence({
    targetIsoDate: '2026-07-31',
    visibleHeaders: ['2026-07-31'],
    tiles,
    inferences: [{ identityKey: '2026-08-01|10:00 am|left|PRG', inference: { thresholdConfidence: 'exact', thresholdScanVerified: true } }],
    sessionsOnTargetDate: 1,
  });
  assert.strictEqual(wrongWeek.ok, false);
  assert.strictEqual(wrongWeek.failureReason, 'target_date_identity_mismatch');
}

{
  const prep = thresholdScanContract.validatePreparedUpdatesForTargetDate([
    { isoDate: '2026-07-31', sessionKey: 'a' },
    { isoDate: '2026-08-01', sessionKey: 'b' },
  ], '2026-07-31');
  assert.strictEqual(prep.ok, false);
  assert.strictEqual(prep.failureReason, 'target_date_identity_mismatch');
}

// ── SCHEDULING / CADENCE ──
{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const horizon = supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-27', effectiveWeeksAhead: 4 });
  const within24 = session({
    ts: Math.floor(new Date('2026-07-27T20:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-27T17:50:00.000Z',
  });
  assert.strictEqual(adaptiveSchedule.evaluateInventorySchedule(within24, { watched: false, now }).targetFreshnessMinutes, 30);

  const within72 = session({
    isoDate: '2026-07-29',
    ts: Math.floor(new Date('2026-07-29T12:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-27T17:00:00.000Z',
  });
  assert.strictEqual(adaptiveSchedule.evaluateInventorySchedule(within72, { watched: false, now }).targetFreshnessMinutes, 120);

  const within7d = session({
    isoDate: '2026-07-31',
    ts: Math.floor(new Date('2026-07-31T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-25T12:00:00.000Z',
  });
  assert.strictEqual(adaptiveSchedule.evaluateInventorySchedule(within7d, { watched: false, now }).targetFreshnessMinutes, 360);

  const within14d = session({
    isoDate: '2026-08-05',
    ts: Math.floor(new Date('2026-08-05T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  assert.strictEqual(adaptiveSchedule.evaluateInventorySchedule(within14d, { watched: false, now }).targetFreshnessMinutes, 1440);

  const beyond14d = session({
    isoDate: '2026-08-15',
    ts: Math.floor(new Date('2026-08-15T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  assert.strictEqual(adaptiveSchedule.evaluateInventorySchedule(beyond14d, { watched: false, now }).targetFreshnessMinutes, 4320);

  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([beyond14d], {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
    horizon,
  });
  assert.strictEqual(dueScan.candidates.length, 1);
}

{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const beyondHorizon = session({
    isoDate: '2026-09-01',
    ts: Math.floor(new Date('2026-09-01T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: null,
  });
  const horizon = supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-27' });
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([beyondHorizon], {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
    horizon,
  });
  assert.strictEqual(dueScan.candidates.length, 0);
}

{
  const now = new Date('2026-07-27T12:00:00.000Z');
  const watched = session({
    key: 'watched-far',
    isoDate: '2026-08-20',
    ts: Math.floor(new Date('2026-08-20T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  const generalNear = session({
    key: 'general-near',
    isoDate: '2026-07-28',
    ts: Math.floor(new Date('2026-07-28T10:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-20T12:00:00.000Z',
  });
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([generalNear, watched], {
    watchKeys: new Set(['watched-far']),
    now,
    todayIso: '2026-07-27',
    daysFromTodayFn,
  });
  assert.strictEqual(dueScan.topCandidate.isoDate, '2026-08-20');
}

{
  const near = {
    isoDate: '2026-07-28',
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: 30,
    mostOverdueMinutes: 60,
    sessions: [{
      targetFreshnessMinutes: 120,
      actualFreshnessMinutes: 45,
    }],
  };
  const far = {
    isoDate: '2026-08-15',
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: 450,
    mostOverdueMinutes: 5000,
    sessions: [{
      targetFreshnessMinutes: 4320,
      actualFreshnessMinutes: 9000,
    }],
  };
  const sorted = thresholdDatePipeline.sortDateScanCandidates([far, near], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
  });
  assert.strictEqual(sorted[0].isoDate, '2026-07-28', 'near-term band outranks distant normalized overdue');
}

{
  const budget = thresholdDatePipeline.selectDueDateScanCandidate([
    {
      isoDate: '2026-08-15',
      watchedDueCount: 0,
      generalDueCount: 1,
      earliestHoursUntilStart: 450,
      sessions: [{ targetFreshnessMinutes: 4320, actualFreshnessMinutes: 9000 }],
    },
    {
      isoDate: '2026-07-28',
      watchedDueCount: 0,
      generalDueCount: 1,
      earliestHoursUntilStart: 30,
      sessions: [{ targetFreshnessMinutes: 30, actualFreshnessMinutes: 45 }],
    },
  ], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentFarGeneralScanCount: 1,
  });
  assert.strictEqual(budget.selected?.isoDate, '2026-07-28');
  assert.strictEqual(budget.deferredReason, null);
}

{
  const onlyFar = thresholdDatePipeline.selectDueDateScanCandidate([
    {
      isoDate: '2026-08-15',
      watchedDueCount: 0,
      generalDueCount: 1,
      earliestHoursUntilStart: 450,
      sessions: [{ targetFreshnessMinutes: 4320, actualFreshnessMinutes: 9000 }],
    },
  ], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentFarGeneralScanCount: 1,
  });
  assert.strictEqual(onlyFar.selected, null);
  assert.strictEqual(onlyFar.deferredReason, 'far_general_hourly_budget_exhausted');
}

{
  assert.strictEqual(thresholdDatePipeline.GENERAL_SCAN_ADMISSION_WINDOW_MINUTES, 15);
  assert.strictEqual(thresholdDatePipeline.GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW, 1);
  assert.strictEqual(
    thresholdDatePipeline.GENERAL_SCAN_ADMISSION_WINDOW_MS,
    15 * 60 * 1000,
  );
  const admission = thresholdDatePipeline.evaluateGeneralScanAdmission({
    recentGeneralEnqueueCount: 1,
    lastGeneralEnqueueAt: '2026-07-27T12:00:00.000Z',
  });
  assert.strictEqual(admission.windowMinutes, 15);
  assert.strictEqual(admission.maxPerWindow, 1);
}

{
  const admissionBlocked = thresholdDatePipeline.selectDueDateScanCandidate([
    {
      isoDate: '2026-07-28',
      watchedDueCount: 0,
      generalDueCount: 1,
      earliestHoursUntilStart: 30,
      sessions: [{ targetFreshnessMinutes: 120, actualFreshnessMinutes: 45 }],
    },
  ], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    generalScanAdmission: thresholdDatePipeline.evaluateGeneralScanAdmission({
      recentGeneralEnqueueCount: 1,
      lastGeneralEnqueueAt: '2026-07-27T12:00:00.000Z',
    }),
  });
  assert.strictEqual(admissionBlocked.selected, null);
  assert.strictEqual(admissionBlocked.deferredReason, 'general_scan_budget_exhausted');
  assert.ok(admissionBlocked.deferredGeneralAdmissionCandidate);
}

{
  const watchedWins = thresholdDatePipeline.selectDueDateScanCandidate([
    {
      isoDate: '2026-07-28',
      watchedDueCount: 1,
      generalDueCount: 0,
      earliestHoursUntilStart: 30,
      sessions: [{ targetFreshnessMinutes: 5, actualFreshnessMinutes: 10 }],
    },
    {
      isoDate: '2026-08-15',
      watchedDueCount: 0,
      generalDueCount: 1,
      earliestHoursUntilStart: 450,
      sessions: [{ targetFreshnessMinutes: 4320, actualFreshnessMinutes: 9000 }],
    },
  ], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    generalScanAdmission: thresholdDatePipeline.evaluateGeneralScanAdmission({
      recentGeneralEnqueueCount: 1,
      lastGeneralEnqueueAt: '2026-07-27T12:00:00.000Z',
    }),
  });
  assert.strictEqual(watchedWins.selected?.isoDate, '2026-07-28');
}

{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /fetchGeneralScanAdmissionState/);
  assert.match(serverJs, /deferredGeneralScanAdmission/);
  const tickBody = serverJs.slice(
    serverJs.indexOf('async function runNearTermMaintenanceTick'),
    serverJs.indexOf('async function runBroadMaintenanceTick'),
  );
  assert.match(tickBody, /hasQueuedOrRunningThresholdScanJob/);
  assert.match(tickBody, /enqueueNearTermDateThresholdScan\(top\.isoDate/);
  assert.doesNotMatch(tickBody, /for \(const candidate of dueScan\.candidates\)/);
  assert.match(serverJs, /getRuntimeSupportedHorizon/);
  assert.match(serverJs, /openGate8ThresholdBrowserSession/);
}

{
  const eligible = thresholdWorkerClaim.evaluateQueuedJobClaimSkipReason(
    { id: 'job-1', mode: thresholdWorkerClaim.THRESHOLD_SCAN_JOB_MODE_DATE },
    { runningCount: 1, scrapeLockAvailable: true },
  );
  assert.strictEqual(eligible.skipReason, 'job_already_running');
}

{
  const diag = maintenanceQueries.buildSelectedDateDiagnostics({
    isoDate: '2026-08-10',
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: 336,
    sessions: [{
      targetFreshnessMinutes: 720,
      actualFreshnessMinutes: 3000,
      hoursUntilStart: 336,
    }],
  }, {
    horizon: supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-27' }),
    remainingOverdueByCadenceTier: { '<=14d': 2, beyond_14d: 1 },
  });
  assert.strictEqual(diag.targetCadenceMinutes, 720);
  assert.ok(diag.normalizedOverdueRatio > 3);
  assert.strictEqual(diag.configuredLatestSupportedDate, '2026-08-23');
}

function dueBucket(isoDate, hours, targetMin, actualMin, lastCheck = null) {
  return {
    isoDate,
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: hours,
    mostOverdueMinutes: actualMin == null
      ? targetMin
      : Math.max(0, actualMin - targetMin),
    sessions: [{
      targetFreshnessMinutes: targetMin,
      actualFreshnessMinutes: actualMin,
      lastSuccessfulCheck: lastCheck,
      due: true,
    }],
  };
}

{
  const near = dueBucket('2026-07-28', 30, 120, 500, '2026-07-25T12:00:00.000Z');
  const week1 = dueBucket('2026-07-31', 94, 360, 2000, '2026-07-20T12:00:00.000Z');
  const week2 = dueBucket('2026-08-08', 280, 1440, 10000, '2026-07-20T12:00:00.000Z');
  const week3 = dueBucket('2026-08-15', 450, 4320, 20000, '2026-07-20T12:00:00.000Z');
  const candidates = [week3, week2, week1, near];

  const normalOnly = thresholdDatePipeline.trySelectFirstEligible(
    thresholdDatePipeline.sortDateScanCandidates(candidates, { daysFromTodayFn, todayIso: '2026-07-27' }),
    { daysFromTodayFn, todayIso: '2026-07-27' },
  );
  assert.strictEqual(normalOnly.selected?.isoDate, '2026-07-28', 'strict band ordering starves distant dates');

  const fairnessPick = thresholdDatePipeline.selectDueDateScanCandidate(candidates, {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 0,
  });
  assert.strictEqual(fairnessPick.selectionPath, 'future_horizon_fairness');
  assert.ok(fairnessPick.selected.earliestHoursUntilStart > 72);
  assert.strictEqual(fairnessPick.selected.isoDate, '2026-08-08', 'highest normalized overdue wins future slot');
}

{
  const near = dueBucket('2026-07-28', 30, 120, 200, '2026-07-27T08:00:00.000Z');
  const week1 = dueBucket('2026-07-31', 94, 360, 5000, '2026-07-20T12:00:00.000Z');
  const week2 = dueBucket('2026-08-08', 280, 1440, 10000, '2026-07-20T12:00:00.000Z');
  const candidates = [week2, week1, near];
  const tickStartMs = Date.parse('2026-07-27T12:00:00.000Z');
  const selections = [];
  let recentBeyond72GeneralScanCount = 0;
  let lastBeyond72GeneralScanAt = null;
  let recentGeneralEnqueueCount = 0;
  let lastGeneralEnqueueAt = null;

  for (let tick = 0; tick < 13; tick += 1) {
    const nowMs = tickStartMs + tick * thresholdDatePipeline.NEAR_TERM_TICK_INTERVAL_MS;
    if (lastBeyond72GeneralScanAt
      && nowMs - new Date(lastBeyond72GeneralScanAt).getTime() >= thresholdDatePipeline.FUTURE_HORIZON_FAIRNESS_WINDOW_MS) {
      recentBeyond72GeneralScanCount = 0;
      lastBeyond72GeneralScanAt = null;
    }
    if (lastGeneralEnqueueAt
      && nowMs - new Date(lastGeneralEnqueueAt).getTime() >= thresholdDatePipeline.GENERAL_SCAN_ADMISSION_WINDOW_MS) {
      recentGeneralEnqueueCount = 0;
      lastGeneralEnqueueAt = null;
    }
    const generalScanAdmission = thresholdDatePipeline.evaluateGeneralScanAdmission({
      recentGeneralEnqueueCount,
      lastGeneralEnqueueAt,
      nowMs,
    });
    const result = thresholdDatePipeline.selectDueDateScanCandidate(candidates, {
      daysFromTodayFn,
      todayIso: '2026-07-27',
      recentBeyond72GeneralScanCount,
      lastBeyond72GeneralScanAt,
      generalScanAdmission,
      nowMs,
    });
    if (!result.selected || !generalScanAdmission.allowed) continue;
    selections.push({
      isoDate: result.selected.isoDate,
      selectionPath: result.selectionPath,
      tick,
    });
    recentGeneralEnqueueCount += 1;
    lastGeneralEnqueueAt = new Date(nowMs).toISOString();
    if (result.selected.earliestHoursUntilStart > 72) {
      recentBeyond72GeneralScanCount = 1;
      lastBeyond72GeneralScanAt = new Date(nowMs).toISOString();
    }
  }

  const futurePicks = selections.filter((row) => row.selectionPath === 'future_horizon_fairness');
  assert.ok(futurePicks.length >= 2, 'future fairness fires at least twice across a 60-minute window');
  assert.ok(selections.some((row) => row.isoDate === '2026-07-28'), 'near-term still receives scans');
}

{
  const neverVerified = dueBucket('2026-08-10', 336, 1440, null, null);
  const stale = dueBucket('2026-08-15', 450, 4320, 9000, '2026-07-01T12:00:00.000Z');
  const sorted = thresholdDatePipeline.sortFutureFairnessCandidates([stale, neverVerified]);
  assert.strictEqual(sorted[0].isoDate, '2026-08-10', 'never-verified dates outrank merely stale dates');
  const neverFreshness = thresholdDatePipeline.evaluateOverdueFreshness({
    targetFreshnessMinutes: 1440,
    actualFreshnessMinutes: null,
  });
  assert.strictEqual(neverFreshness.neverVerified, true);
  assert.strictEqual(neverFreshness.normalizedOverdueRatio, null);
}

{
  const at72 = {
    isoDate: '2026-07-30',
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: 72,
    sessions: [{ targetFreshnessMinutes: 120, actualFreshnessMinutes: 500, due: true }],
  };
  const beyond72 = {
    isoDate: '2026-07-31',
    watchedDueCount: 0,
    generalDueCount: 1,
    earliestHoursUntilStart: 72.01,
    sessions: [{ targetFreshnessMinutes: 360, actualFreshnessMinutes: 5000, due: true }],
  };
  assert.strictEqual(
    thresholdDatePipeline.isBeyond72HourGeneralBucket(at72, { daysFromTodayFn, todayIso: '2026-07-27' }),
    false,
    '<=72 hours stays on near-term priority',
  );
  assert.strictEqual(
    thresholdDatePipeline.isBeyond72HourGeneralBucket(beyond72, { daysFromTodayFn, todayIso: '2026-07-27' }),
    true,
    '>72 hours is eligible for future-horizon fairness',
  );
  const fairnessOverride = thresholdDatePipeline.selectDueDateScanCandidate([
    beyond72,
    dueBucket('2026-07-28', 30, 120, 500, '2026-07-25T12:00:00.000Z'),
  ], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 0,
  });
  assert.strictEqual(fairnessOverride.selectionPath, 'future_horizon_fairness');
  assert.strictEqual(fairnessOverride.selected?.isoDate, '2026-07-31');
  const nearOnly = thresholdDatePipeline.selectDueDateScanCandidate([at72], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 0,
  });
  assert.strictEqual(nearOnly.selectionPath, 'near_term_priority');
  assert.strictEqual(nearOnly.selected?.isoDate, '2026-07-30');
  assert.strictEqual(nearOnly.futureFairness?.applied, false);
}

{
  const near = dueBucket('2026-07-28', 30, 120, 200, '2026-07-27T08:00:00.000Z');
  const week1 = dueBucket('2026-07-31', 94, 360, 5000, '2026-07-20T12:00:00.000Z');
  const result = thresholdDatePipeline.selectDueDateScanCandidate([week1, near], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 1,
    lastBeyond72GeneralScanAt: '2026-07-27T12:00:00.000Z',
  });
  assert.strictEqual(result.selectionPath, 'near_term_priority');
  assert.strictEqual(result.selected?.isoDate, '2026-07-28');
}

{
  const watched = {
    isoDate: '2026-08-15',
    watchedDueCount: 1,
    generalDueCount: 0,
    earliestHoursUntilStart: 450,
    sessions: [{ targetFreshnessMinutes: 5, actualFreshnessMinutes: 10, due: true, watched: true }],
  };
  const near = dueBucket('2026-07-28', 30, 120, 500, '2026-07-25T12:00:00.000Z');
  const far = dueBucket('2026-08-15', 450, 4320, 20000, '2026-07-01T12:00:00.000Z');
  const result = thresholdDatePipeline.selectDueDateScanCandidate([far, near, watched], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 0,
  });
  assert.strictEqual(result.selected?.isoDate, '2026-08-15');
  assert.strictEqual(result.selectionPath, 'watched_priority');
}

{
  const week2 = dueBucket('2026-08-08', 280, 1440, 10000, '2026-07-01T12:00:00.000Z');
  const week3 = dueBucket('2026-08-15', 450, 4320, 12000, '2026-07-01T12:00:00.000Z');
  const futurePick = thresholdDatePipeline.selectDueDateScanCandidate([week3, week2], {
    daysFromTodayFn,
    todayIso: '2026-07-27',
    recentBeyond72GeneralScanCount: 0,
  });
  assert.strictEqual(futurePick.selected?.isoDate, '2026-08-08', '8–14-day dates can win over >14-day when more overdue');
}

{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /fetchRecentBeyond72GeneralDateScanState/);
  assert.match(serverJs, /futureHorizonFairness/);
  assert.match(serverJs, /nearTermMaintenanceSelection/);
}

console.log('threshold full horizon regression: ok');
