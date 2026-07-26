'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
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
  return tm.buildApplyPreparedFinalResults({
    dateResults,
    workerError,
    sourcePreparedUpdatesCount,
    sourceWeekStart,
  });
}

function compactMaintenanceJob(job) {
  const resultsJson = job.results_json || {};
  return {
    jobId: job.id,
    status: job.status,
    mode: job.mode,
    weekStart: resultsJson.weekStart ?? null,
    sourceJobId: resultsJson.sourceJobId ?? null,
    error: job.error ?? null,
    stage: resultsJson.stage ?? null,
    preparedUpdatesCount: resultsJson.preparedUpdatesCount ?? null,
    rowsWrittenSuccessfully: resultsJson.rowsWrittenSuccessfully ?? resultsJson.rowsWritten ?? null,
    completedAt: job.completed_at ?? null,
  };
}

function assertNumericCompletionFields(completion, expectedPreparedCount, expectedWrittenCount) {
  assert.strictEqual(typeof completion.preparedUpdatesCount, 'number');
  assert.strictEqual(typeof completion.rowsPrepared, 'number');
  assert.strictEqual(typeof completion.rowsMatched, 'number');
  assert.strictEqual(typeof completion.rowsWritten, 'number');
  assert.strictEqual(typeof completion.rowsWrittenSuccessfully, 'number');
  assert.strictEqual(typeof completion.rowsUnresolved, 'number');
  assert.strictEqual(completion.preparedUpdatesCount, expectedPreparedCount);
  assert.strictEqual(completion.rowsPrepared, expectedPreparedCount);
  assert.strictEqual(completion.rowsMatched, expectedPreparedCount);
  assert.strictEqual(completion.rowsWritten, expectedWrittenCount);
  assert.strictEqual(completion.rowsWrittenSuccessfully, expectedWrittenCount);
  assert.strictEqual(completion.rowsUnresolved, 0);
}

console.log('apply-prepared completion regression');

// 1. Apply with prepared rows completes without ReferenceError.
{
  const completion = simulateFixedCompletionPath(productionWeekDateResults(), null, 260, WEEK_START);
  assert.strictEqual(completion.ok, true);
  assert.strictEqual(completion.status, 'completed');
}

// Pre-fix path throws ReferenceError (root cause regression guard).
{
  assert.throws(
    () => simulatePreFixCompletionPath(productionWeekDateResults()),
    (err) => err instanceof ReferenceError && /preparedUpdatesCount is not defined/.test(err.message),
  );
}

// 2. preparedUpdatesCount equals validated prepared row count.
{
  const completion = simulateFixedCompletionPath(productionWeekDateResults(), null, 260, WEEK_START);
  assert.strictEqual(completion.preparedUpdatesCount, 260);
}

// 3. Successfully written count is reported separately.
{
  const partialWritten = [
    { isoDate: '2026-08-10', rowsPrepared: 10, rowsMatched: 10, rowsWritten: 10, rowsUnresolved: 0, partialApply: false },
    { isoDate: '2026-08-11', rowsPrepared: 5, rowsMatched: 5, rowsWritten: 3, rowsUnresolved: 0, partialApply: false },
  ];
  const completion = simulateFixedCompletionPath(partialWritten, null, 15, WEEK_START);
  assert.strictEqual(completion.preparedUpdatesCount, 15);
  assert.strictEqual(completion.rowsWritten, 13);
  assert.strictEqual(completion.rowsWrittenSuccessfully, 13);
  assert.notStrictEqual(completion.preparedUpdatesCount, completion.rowsWrittenSuccessfully);
}

// 4. Zero prepared rows handled deliberately.
{
  const completion = simulateFixedCompletionPath([], 'source_prepared_updates_empty', 0, WEEK_START);
  assert.strictEqual(completion.ok, false);
  assert.strictEqual(completion.status, 'failed');
  assert.strictEqual(completion.preparedUpdatesCount, 0);
  assert.strictEqual(completion.rowsWrittenSuccessfully, 0);
}

// 5. Partial rejection does not produce undefined variable.
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
  assert.strictEqual(typeof completion.preparedUpdatesCount, 'number');
  assert.strictEqual(completion.partialApply, true);
  assert.strictEqual(completion.rowsUnresolved, 11);
  assert.strictEqual(completion.rowsWrittenSuccessfully, 249);
}

// 6. current_sessions threshold fields update only through apply helper (idempotent guard).
{
  const prep = {
    sessionKey: 'sess-1',
    available_entries: 7,
    thresholdScanAt: SOURCE_SCAN_AT,
    threshold_scanned_at: SOURCE_SCAN_AT,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
  };
  const session = { key: 'sess-1', available: true, slots: 4 };
  const applied = tm.applyPreparedThresholdUpdate(session, prep);
  assert.strictEqual(applied.threshold_scanned_at, SOURCE_SCAN_AT);
  assert.strictEqual(applied.available_entries, 7);
  assert.strictEqual(tm.isThresholdSlotsTrusted(applied), true);

  const newerExisting = {
    ...applied,
    thresholdScanAt: '2026-07-26T00:00:00.000Z',
    threshold_scanned_at: '2026-07-26T00:00:00.000Z',
  };
  assert.strictEqual(tm.shouldApplyPreparedThresholdUpdate(newerExisting, prep), false);
}

// 7. Failed database write does not mark job completed.
{
  const completion = simulateFixedCompletionPath(
    [{
      isoDate: '2026-08-10',
      rowsPrepared: 4,
      rowsMatched: 4,
      rowsWritten: 0,
      rowsUnresolved: 0,
      partialApply: false,
      error: 'upsert_failed',
    }],
    'upsert_failed',
    4,
    WEEK_START,
  );
  assert.strictEqual(completion.ok, false);
  assert.strictEqual(completion.status, 'failed');
  assert.strictEqual(completion.rowsWrittenSuccessfully, 0);
}

// 8. Retrying same prepared source job is idempotent.
{
  const failedApply = {
    status: 'failed',
    completed_at: '2026-07-25T15:05:00.000Z',
    error: 'preparedUpdatesCount is not defined',
    results_json: { sourceJobId: SOURCE_JOB_ID, partialApply: false, rowsUnresolved: 0 },
  };
  assert.strictEqual(tm.isDryScanSourceFullyApplied(SOURCE_JOB_ID, SOURCE_SCAN_AT, [failedApply]), false);

  const retryCompletion = simulateFixedCompletionPath(productionWeekDateResults(), null, 260, WEEK_START);
  const successfulRetryApply = {
    status: 'completed',
    completed_at: '2026-07-25T15:10:00.000Z',
    results_json: {
      sourceJobId: SOURCE_JOB_ID,
      weekStart: WEEK_START,
      preparedUpdatesCount: retryCompletion.preparedUpdatesCount,
      rowsWrittenSuccessfully: retryCompletion.rowsWrittenSuccessfully,
      partialApply: false,
      rowsUnresolved: 0,
    },
  };
  assert.strictEqual(
    tm.isDryScanSourceFullyApplied(SOURCE_JOB_ID, SOURCE_SCAN_AT, [failedApply, successfulRetryApply]),
    true,
  );

  const alreadyApplied = {
    key: 'sess-1',
    thresholdScanAt: SOURCE_SCAN_AT,
    threshold_scanned_at: SOURCE_SCAN_AT,
    available_entries: 7,
  };
  const rewritten = tm.applyPreparedThresholdUpdate(alreadyApplied, {
    sessionKey: 'sess-1',
    available_entries: 7,
    thresholdScanAt: SOURCE_SCAN_AT,
    threshold_scanned_at: SOURCE_SCAN_AT,
  });
  assert.strictEqual(rewritten.thresholdScanAt, SOURCE_SCAN_AT);
}

// 9. Market-observation insertion failure must not fail threshold apply (isolated in server.js).
{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /recordMarketObservationsAfterWrite\(/);
  assert.match(serverJs, /\[market-observations\] threshold apply failed:/);
  const applyBlock = serverJs.slice(
    serverJs.indexOf('async function applyGate8ThresholdWriteRows'),
    serverJs.indexOf('function serializeGate8PreparedUpdate'),
  );
  assert.match(applyBlock, /try \{[\s\S]*recordMarketObservationsAfterWrite[\s\S]*\} catch \(err\)/);
}

// 10. MARKET_OBSERVATIONS_ENABLED=false leaves threshold apply unchanged.
{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const recordBlock = serverJs.slice(
    serverJs.indexOf('async function recordMarketObservationsAfterWrite'),
    serverJs.indexOf('async function recordMarketObservationsAfterWrite') + 1200,
  );
  assert.match(recordBlock, /if \(!MARKET_OBSERVATIONS_ENABLED\)/);
  assert.match(recordBlock, /skippedDisabled: true/);
  const marketObservations = require('../lib/market-observations');
  const prev = process.env.MARKET_OBSERVATIONS_ENABLED;
  delete process.env.MARKET_OBSERVATIONS_ENABLED;
  assert.strictEqual(marketObservations.isMarketObservationsEnabled(), false);
  process.env.MARKET_OBSERVATIONS_ENABLED = prev;
}

// 11. Maintenance status reports queued, running, completed, and failed jobs.
{
  const jobs = [
    { id: 'q1', status: 'queued', mode: 'threshold_week_write_contract', results_json: { weekStart: WEEK_START }, completed_at: null, error: null },
    { id: 'r1', status: 'running', mode: 'threshold_week_apply_prepared', results_json: { sourceJobId: SOURCE_JOB_ID, stage: 'applying_prepared_writes' }, completed_at: null, error: null },
    { id: 'c1', status: 'completed', mode: 'threshold_week_apply_prepared', completed_at: '2026-07-25T15:10:00.000Z', error: null, results_json: { sourceJobId: SOURCE_JOB_ID, preparedUpdatesCount: 260, rowsWrittenSuccessfully: 260, weekStart: WEEK_START } },
    { id: 'f1', status: 'failed', mode: 'threshold_week_apply_prepared', completed_at: '2026-07-25T15:05:00.000Z', error: 'preparedUpdatesCount is not defined', results_json: { sourceJobId: SOURCE_JOB_ID, stage: 'failed' } },
  ];
  const compact = jobs.map(compactMaintenanceJob);
  assert.strictEqual(compact[0].status, 'queued');
  assert.strictEqual(compact[1].status, 'running');
  assert.strictEqual(compact[2].status, 'completed');
  assert.strictEqual(compact[2].preparedUpdatesCount, 260);
  assert.strictEqual(compact[2].rowsWrittenSuccessfully, 260);
  assert.strictEqual(compact[3].status, 'failed');
  assert.strictEqual(compact[3].error, 'preparedUpdatesCount is not defined');
  assert.ok(compact[3].completedAt);
}

// 12. Scoped apply completion variables in server.js (regression guard).
{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const fn = serverJs.slice(
    serverJs.indexOf('async function executeThresholdWeekApplyPreparedJob'),
    serverJs.indexOf('async function executeThresholdWeekScanJob'),
  );
  assert.match(fn, /let sourcePreparedUpdatesCount = 0;/);
  assert.match(fn, /buildApplyPreparedFinalResults\(/);
  assert.doesNotMatch(fn, /resolveApplyJobCompletion\(\{[\s\S]*preparedUpdatesCount,\s*\n\s*sourceWeekStart/s);
}

// Production-shaped success case.
{
  const completion = simulateFixedCompletionPath(productionWeekDateResults(), null, 260, WEEK_START);
  assert.strictEqual(completion.ok, true);
  assert.strictEqual(completion.stage, 'completed');
  assertNumericCompletionFields(completion, 260, 260);
}

// Hard failure when all rows missing.
{
  const completion = simulateFixedCompletionPath([{
    isoDate: '2026-08-10',
    rowsPrepared: 4,
    rowsMatched: 0,
    rowsWritten: 0,
    rowsUnresolved: 4,
    partialApply: false,
    error: 'missing_sessions:4',
  }], null, 4, WEEK_START);
  assert.strictEqual(completion.ok, false);
  assert.strictEqual(completion.status, 'failed');
}

console.log('apply-prepared completion regression: all tests passed');
