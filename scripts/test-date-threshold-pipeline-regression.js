'use strict';

const assert = require('assert');
const adaptiveSchedule = require('../lib/adaptive-threshold-schedule');
const thresholdDatePipeline = require('../lib/threshold-date-pipeline');
const thresholdNearTermScheduler = require('../lib/threshold-near-term-scheduler');
const thresholdMaintenance = require('../lib/threshold-maintenance');
const sessionChangeEvents = require('../lib/session-change-events');

console.log('date threshold pipeline regression');

function makePreparedUpdate(sessionKey, isoDate, slots, scannedAt) {
  return {
    sessionKey,
    isoDate,
    available_entries: slots,
    thresholdInferredSlots: slots,
    threshold_confidence: 'exact',
    slot_status: 'exact',
    slot_source: 'entries_left_threshold_scan',
    threshold_scan_verified: true,
    threshold_scanned_at: scannedAt,
    thresholdScanAt: scannedAt,
  };
}

function makeDateScanResults(overrides = {}) {
  return {
    mode: 'date_threshold_write',
    targetIsoDate: '2026-07-27',
    targetDates: ['2026-07-27'],
    scanRunId: 'scan-1',
    stage: 'completed',
    fullScanContractOk: true,
    exactCount: 2,
    preparedUpdatesCount: 2,
    preparedUpdatesByDate: {
      '2026-07-27': [
        makePreparedUpdate('s1', '2026-07-27', 4, '2026-07-27T18:00:00.000Z'),
        makePreparedUpdate('s2', '2026-07-27', 2, '2026-07-27T18:00:00.000Z'),
      ],
    },
    preparedScanCompletedAt: '2026-07-27T18:00:00.000Z',
    preparedZeroReason: null,
    ...overrides,
  };
}

// 1. Contract rejects null preparedUpdatesCount.
{
  const invalid = thresholdDatePipeline.validateDateScanResultContract(makeDateScanResults({
    preparedUpdatesCount: null,
  }));
  assert.strictEqual(invalid.ok, false);
  assert.ok(invalid.errors.includes('preparedUpdatesCount_null'));
}

// 2. Valid contract accepts numeric zero with reason.
{
  const zero = makeDateScanResults({
    preparedUpdatesCount: 0,
    preparedUpdatesByDate: {},
    preparedZeroReason: 'no_exact_inferences',
    exactCount: 0,
  });
  const validation = thresholdDatePipeline.validateDateScanResultContract(zero);
  assert.strictEqual(validation.ok, true);
  assert.strictEqual(thresholdDatePipeline.isDryScanReadyToApply(zero), false);
}

// 3. Unchanged scan still produces prepared verification updates.
{
  const unchanged = makePreparedUpdate('s1', '2026-07-27', 4, '2026-07-27T18:00:00.000Z');
  const existing = {
    key: 's1',
    available: true,
    available_entries: 4,
    threshold_scan_verified: true,
    threshold_scanned_at: '2026-07-27T15:00:00.000Z',
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
  };
  const applied = thresholdMaintenance.applyPreparedThresholdUpdate(existing, unchanged);
  assert.strictEqual(applied.threshold_scanned_at, '2026-07-27T18:00:00.000Z');
  assert.strictEqual(applied.available_entries, 4);
}

// 4. Changed scan prepared payload differs by timestamp and slots.
{
  const prev = makePreparedUpdate('s1', '2026-07-27', 2, '2026-07-27T15:00:00.000Z');
  const next = makePreparedUpdate('s1', '2026-07-27', 5, '2026-07-27T18:00:00.000Z');
  assert.notStrictEqual(prev.available_entries, next.available_entries);
  assert.notStrictEqual(prev.threshold_scanned_at, next.threshold_scanned_at);
}

// 5–6. Apply idempotency by sourceJobId via maintenance helper.
{
  const sourceJobId = 'source-date-1';
  const scanAt = '2026-07-27T18:00:00.000Z';
  const applyJobs = [{
    status: 'completed',
    completed_at: '2026-07-27T18:05:00.000Z',
    results_json: { sourceJobId, partialApply: false, rowsUnresolved: 0 },
  }];
  assert.strictEqual(
    thresholdMaintenance.isDryScanSourceFullyApplied(sourceJobId, scanAt, applyJobs),
    true,
  );
  assert.strictEqual(
    thresholdMaintenance.isDryScanSourceFullyApplied(sourceJobId, scanAt, []),
    false,
  );
}

// 7–8. Trusted timestamps advance on apply; unchanged counts still advance.
{
  const existing = {
    key: 's1',
    available: true,
    available_entries: 3,
    threshold_scan_verified: true,
    threshold_scanned_at: '2026-07-27T12:00:00.000Z',
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
  };
  const prep = makePreparedUpdate('s1', '2026-07-27', 3, '2026-07-27T18:00:00.000Z');
  assert.strictEqual(thresholdMaintenance.shouldApplyPreparedThresholdUpdate(existing, prep), true);
  const applied = thresholdMaintenance.applyPreparedThresholdUpdate(existing, prep);
  assert.strictEqual(applied.threshold_scanned_at, '2026-07-27T18:00:00.000Z');
}

// 9. Ambiguous contract without exact inferences is not ready to apply.
{
  const ambiguous = makeDateScanResults({
    preparedUpdatesCount: 0,
    preparedUpdatesByDate: {},
    preparedZeroReason: 'ambiguous_only',
    exactCount: 0,
    atLeastCount: 2,
    fullScanContractOk: false,
    stage: 'failed',
  });
  assert.strictEqual(thresholdDatePipeline.isDryScanReadyToApply(ambiguous), false);
}

// 10. Failed scan contract remains eligible for retry (not operationally complete).
{
  const failed = {
    mode: 'date_threshold_write',
    stage: 'failed',
    fullScanContractOk: false,
    preparedUpdatesCount: null,
  };
  assert.strictEqual(thresholdDatePipeline.isDateScanOperationallyComplete(failed), false);
}

// 11–12. Completed-but-unapplied with valid prepared rows is ready; null contract is not.
{
  const ready = makeDateScanResults();
  assert.strictEqual(thresholdDatePipeline.isDryScanReadyToApply(ready), true);
  const hollow = makeDateScanResults({ preparedUpdatesCount: null, stage: null });
  assert.strictEqual(thresholdDatePipeline.isDateScanOperationallyComplete(hollow), false);
}

// 13–15. Active date tracking is DB-backed; restart has no in-memory phantom dates.
{
  const fs = require('fs');
  const path = require('path');
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /fetchActiveDateThresholdScanIsoDates/);
  assert.doesNotMatch(serverJs, /activeDateScans\s*=\s*new Map/);
  assert.match(serverJs, /\.in\('status', \['queued', 'running'\]\)/);
}

// 16. Ended sessions are excluded from inventory scheduling.
{
  const now = new Date('2026-07-27T20:00:00.000Z');
  const ended = {
    key: 'ended-1',
    ts: Math.floor(new Date('2026-07-27T10:00:00.000Z').getTime() / 1000),
    available: true,
    threshold_scanned_at: '2026-07-27T08:00:00.000Z',
  };
  assert.strictEqual(adaptiveSchedule.isSessionEnded(ended, now), true);
  assert.strictEqual(
    adaptiveSchedule.evaluateInventorySchedule(ended, { watched: false, now }).eligible,
    false,
  );
}

// 17. Very old far session cannot outrank a near watched date.
{
  const todayIso = '2026-07-27';
  const daysFromTodayFn = (isoDate) => {
    const a = new Date(`${isoDate}T12:00:00.000Z`);
    const b = new Date(`${todayIso}T12:00:00.000Z`);
    return Math.round((a - b) / (24 * 3600 * 1000));
  };
  const far = {
    isoDate: '2026-06-01',
    watchedDueCount: 0,
    generalDueCount: 3,
    mostOverdueMinutes: 50000,
  };
  const nearWatched = {
    isoDate: '2026-07-27',
    watchedDueCount: 1,
    generalDueCount: 0,
    mostOverdueMinutes: 30,
  };
  const sorted = thresholdDatePipeline.sortDateScanCandidates([far, nearWatched], {
    daysFromTodayFn,
    todayIso,
  });
  assert.strictEqual(sorted[0].isoDate, '2026-07-27');
}

// 18. Due backlog with idle pipeline is unhealthy.
{
  const backlog = thresholdDatePipeline.buildBacklogHealthDiagnostics({
    dueScan: { watchedDueCount: 0, generalDueCount: 158, candidates: [{ isoDate: '2026-07-27' }] },
    queuedDateScans: 0,
    runningDateScans: 0,
    readyDateApplies: 0,
    queuedDateApplies: 0,
    runningDateApplies: 0,
  });
  assert.strictEqual(backlog.unhealthy, true);
  assert.strictEqual(backlog.unhealthyReason, 'due_backlog_with_idle_pipeline');
}

// 19. Scheduler ownership surfaces dual-run risk.
{
  thresholdNearTermScheduler.resetNearTermSchedulerStateForTests();
  const external = thresholdNearTermScheduler.buildNearTermSchedulerDiagnostics({
    inProcessMaintenanceSchedulerEnabled: false,
    inlineThresholdWorkerEnabled: false,
  });
  assert.strictEqual(external.ownership, 'external_cron');
  const dual = thresholdNearTermScheduler.buildNearTermSchedulerDiagnostics({
    inProcessMaintenanceSchedulerEnabled: true,
    inlineThresholdWorkerEnabled: false,
  });
  assert.strictEqual(dual.dualSchedulerRisk, true);
}

// 20–21. Weekly path strings remain; notifications stay post-apply.
{
  const fs = require('fs');
  const path = require('path');
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /executeThresholdWeekScanJob/);
  assert.match(serverJs, /threshold_week_apply_prepared/);
  const prev = {
    key: 'n1',
    available: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 2,
    thresholdInferredSlots: 2,
    threshold_scanned_at: '2026-07-27T12:00:00.000Z',
  };
  const next = {
    ...prev,
    available_entries: 5,
    thresholdInferredSlots: 5,
    threshold_scanned_at: '2026-07-27T18:00:00.000Z',
  };
  assert.strictEqual(
    sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: prev,
      nextSession: next,
      writeSucceeded: false,
    }),
    null,
  );
  assert.ok(sessionChangeEvents.deriveSessionChangeEvent({
    previousSession: prev,
    nextSession: next,
    writeSucceeded: true,
  }));
}

// 22. Market observations remain disabled by default.
{
  const marketObservations = require('../lib/market-observations');
  const prev = process.env.MARKET_OBSERVATIONS_ENABLED;
  delete process.env.MARKET_OBSERVATIONS_ENABLED;
  assert.strictEqual(marketObservations.isMarketObservationsEnabled(), false);
  if (prev != null) process.env.MARKET_OBSERVATIONS_ENABLED = prev;
}

// Near-term scheduler records tick metadata.
{
  thresholdNearTermScheduler.resetNearTermSchedulerStateForTests();
  thresholdNearTermScheduler.recordNearTermMaintenanceTick({
    ok: true,
    action: 'enqueued_date_scan',
    selectedIsoDate: '2026-07-27',
  }, 'external');
  const diag = thresholdNearTermScheduler.buildNearTermSchedulerDiagnostics({
    inProcessMaintenanceSchedulerEnabled: false,
    inlineThresholdWorkerEnabled: false,
  });
  assert.ok(diag.lastNearTermTickAt);
  assert.strictEqual(diag.lastNearTermTickSource, 'external');
  assert.strictEqual(diag.lastNearTermTickResult.action, 'enqueued_date_scan');
}

console.log('date threshold pipeline regression: all tests passed');
