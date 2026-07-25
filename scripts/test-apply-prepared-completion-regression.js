'use strict';

const assert = require('assert');
const tm = require('../lib/threshold-maintenance');

const SOURCE_JOB_ID = '73399f59-fa9b-4030-82f1-da20f95ddd66';
const SOURCE_SCAN_AT = '2026-07-25T14:46:47.000Z';
const WEEK_START = '2026-08-10';

function productionWeekDateResults() {
  return [
    { isoDate: '2026-08-10', rowsPrepared: 28, rowsMatched: 28, rowsWritten: 28, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-11', rowsPrepared: 36, rowsMatched: 36, rowsWritten: 36, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-12', rowsPrepared: 36, rowsMatched: 36, rowsWritten: 36, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-13', rowsPrepared: 32, rowsMatched: 32, rowsWritten: 32, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-14', rowsPrepared: 43, rowsMatched: 43, rowsWritten: 43, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-15', rowsPrepared: 44, rowsMatched: 44, rowsWritten: 44, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-16', rowsPrepared: 41, rowsMatched: 41, rowsWritten: 41, rowsUnresolved: 0, partialApply: false },
  ];
}

function simulatePreFixCompletionPath(dateResults, workerError = null) {
  try {
    const preparedUpdatesCount = 260;
    void preparedUpdatesCount;
  } catch (err) {
    workerError = err.message || String(err);
  }
  return tm.resolveApplyJobCompletion({
    dateResults,
    workerError,
    preparedUpdatesCount,
  });
}

function simulateFixedCompletionPath(dateResults, workerError, sourcePreparedUpdatesCount, sourceWeekStart) {
  try {
    void sourcePreparedUpdatesCount;
  } catch (err) {
    workerError = err.message || String(err);
  }
  return tm.buildApplyPreparedFinalResults({
    dateResults,
    workerError,
    sourcePreparedUpdatesCount,
    sourceWeekStart,
  });
}

function assertNumericCompletionFields(completion, expectedPreparedCount) {
  assert.strictEqual(typeof completion.preparedUpdatesCount, 'number');
  assert.strictEqual(typeof completion.rowsPrepared, 'number');
  assert.strictEqual(typeof completion.rowsMatched, 'number');
  assert.strictEqual(typeof completion.rowsWritten, 'number');
  assert.strictEqual(typeof completion.rowsUnresolved, 'number');
  assert.strictEqual(completion.preparedUpdatesCount, expectedPreparedCount);
  assert.strictEqual(completion.rowsPrepared, expectedPreparedCount);
  assert.strictEqual(completion.rowsMatched, expectedPreparedCount);
  assert.strictEqual(completion.rowsWritten, expectedPreparedCount);
  assert.strictEqual(completion.rowsUnresolved, 0);
}

console.log('apply-prepared completion regression');

{
  const dateResults = productionWeekDateResults();
  assert.throws(
    () => simulatePreFixCompletionPath(dateResults),
    (err) => err instanceof ReferenceError && /preparedUpdatesCount is not defined/.test(err.message),
    'pre-fix path must throw ReferenceError: preparedUpdatesCount is not defined',
  );
}

{
  const dateResults = productionWeekDateResults();
  const sourcePreparedUpdatesCount = 260;
  const applyJob = { results_json: { sourceJobId: SOURCE_JOB_ID, preparedUpdatesCount: null } };
  void applyJob;

  const completion = simulateFixedCompletionPath(
    dateResults,
    null,
    sourcePreparedUpdatesCount,
    WEEK_START,
  );

  assert.strictEqual(completion.ok, true);
  assert.strictEqual(completion.status, 'completed');
  assert.strictEqual(completion.stage, 'completed');
  assert.strictEqual(completion.weekStart, WEEK_START);
  assertNumericCompletionFields(completion, 260);
}

{
  const dateResults = productionWeekDateResults();
  const completion = simulateFixedCompletionPath(dateResults, null, 260, WEEK_START);
  assert.strictEqual(completion.ok, true);

  const prep = {
    sessionKey: 'sess-1',
    available_entries: 7,
    thresholdScanAt: SOURCE_SCAN_AT,
    threshold_scanned_at: SOURCE_SCAN_AT,
  };
  const alreadyApplied = {
    key: 'sess-1',
    thresholdScanAt: SOURCE_SCAN_AT,
    threshold_scanned_at: SOURCE_SCAN_AT,
    available_entries: 7,
  };
  const rewritten = tm.applyPreparedThresholdUpdate(alreadyApplied, prep);
  assert.strictEqual(rewritten.thresholdScanAt, SOURCE_SCAN_AT);
  assert.strictEqual(rewritten.available_entries, 7);

  const newerExisting = {
    ...alreadyApplied,
    thresholdScanAt: '2026-07-26T00:00:00.000Z',
    threshold_scanned_at: '2026-07-26T00:00:00.000Z',
  };
  assert.strictEqual(tm.shouldApplyPreparedThresholdUpdate(newerExisting, prep), false);
}

{
  const partialDateResults = [
    ...productionWeekDateResults().slice(0, 6),
    {
      isoDate: '2026-08-16',
      rowsPrepared: 41,
      rowsMatched: 30,
      rowsWritten: 30,
      rowsUnresolved: 11,
      partialApply: true,
    },
  ];
  const completion = simulateFixedCompletionPath(partialDateResults, null, 260, WEEK_START);
  assert.strictEqual(completion.ok, true);
  assert.strictEqual(completion.partialApply, true);
  assert.strictEqual(completion.status, 'completed');
  assert.strictEqual(completion.stage, 'completed_partial');
  assert.strictEqual(completion.rowsUnresolved, 11);
  assert.strictEqual(completion.preparedUpdatesCount, 260);
}

{
  const unresolvedDateResults = [{
    isoDate: '2026-08-10',
    rowsPrepared: 4,
    rowsMatched: 0,
    rowsWritten: 0,
    rowsUnresolved: 4,
    partialApply: false,
    error: 'missing_sessions:4',
  }];
  const completion = simulateFixedCompletionPath(unresolvedDateResults, null, 4, WEEK_START);
  assert.strictEqual(completion.ok, false);
  assert.strictEqual(completion.status, 'failed');
  assert.strictEqual(completion.rowsUnresolved, 4);
}

{
  const failedApply = {
    status: 'failed',
    completed_at: '2026-07-25T15:05:00.000Z',
    error: 'preparedUpdatesCount is not defined',
    results_json: {
      sourceJobId: SOURCE_JOB_ID,
      partialApply: false,
      rowsUnresolved: 0,
    },
  };
  assert.strictEqual(
    tm.isDryScanSourceFullyApplied(SOURCE_JOB_ID, SOURCE_SCAN_AT, [failedApply]),
    false,
  );

  const retryCompletion = simulateFixedCompletionPath(productionWeekDateResults(), null, 260, WEEK_START);
  assert.strictEqual(retryCompletion.ok, true);

  const successfulRetryApply = {
    status: 'completed',
    completed_at: '2026-07-25T15:10:00.000Z',
    results_json: {
      sourceJobId: SOURCE_JOB_ID,
      weekStart: WEEK_START,
      preparedUpdatesCount: retryCompletion.preparedUpdatesCount,
      partialApply: false,
      rowsUnresolved: 0,
      rowsWritten: retryCompletion.rowsWritten,
    },
  };
  assert.strictEqual(
    tm.isDryScanSourceFullyApplied(SOURCE_JOB_ID, SOURCE_SCAN_AT, [failedApply, successfulRetryApply]),
    true,
  );
}

console.log('apply-prepared completion regression: all tests passed');
