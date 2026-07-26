'use strict';

const assert = require('assert');
const twc = require('../lib/threshold-worker-claim');

const TODAY = '2026-07-26';

function weekDryRunJob(overrides = {}) {
  return {
    id: overrides.id || '195846ab-c222-4c35-8d4f-8564aff216fa',
    status: 'queued',
    mode: twc.THRESHOLD_SCAN_JOB_MODE_WEEK,
    dry_run: true,
    write_enabled: false,
    created_at: overrides.created_at || '2026-07-26T17:04:22.756098+00:00',
    results_json: {
      weekStart: overrides.weekStart || '2026-07-20',
      targetDates: overrides.targetDates || [
        '2026-07-20', '2026-07-21', '2026-07-22', '2026-07-23',
        '2026-07-24', '2026-07-25', '2026-07-26',
      ],
    },
    ...overrides,
  };
}

console.log('threshold worker claim regression');

// 1. Queued threshold_week_write_contract dry-run job is eligible by mode.
{
  const job = weekDryRunJob();
  const mode = twc.evaluateQueuedJobModeEligibility(job);
  assert.strictEqual(mode.modeEligible, true);
  const claim = twc.evaluateQueuedJobClaimSkipReason(job, { runningCount: 0, scrapeLockAvailable: true });
  assert.strictEqual(claim.eligible, true);
  assert.strictEqual(claim.modeEligible, true);
}

// 2. write_enabled=false does not exclude a dry-run scan.
{
  const job = weekDryRunJob({ write_enabled: false, dry_run: true });
  const claim = twc.evaluateQueuedJobClaimSkipReason(job, { runningCount: 0, scrapeLockAvailable: true });
  assert.strictEqual(claim.eligible, true);
}

// 3. Worker claims oldest eligible job by priority order.
{
  const older = weekDryRunJob({
    id: 'older-week',
    created_at: '2026-07-26T16:00:00.000Z',
    results_json: { weekStart: '2026-07-20', targetDates: ['2026-07-20'] },
  });
  const newerApply = {
    id: 'apply-job',
    mode: twc.THRESHOLD_SCAN_JOB_MODE_APPLY,
    dry_run: false,
    write_enabled: true,
    created_at: '2026-07-26T17:30:00.000Z',
    results_json: { sourceJobId: 'source-1' },
  };
  const sorted = [older, newerApply].sort((a, b) => twc.compareThresholdScanJobPriority(a, b, TODAY));
  assert.strictEqual(sorted[0].id, 'apply-job');
  assert.strictEqual(sorted[1].id, 'older-week');
}

// 4. Ineligible jobs return documented skip reasons.
{
  const unsupported = weekDryRunJob({ mode: 'legacy_unknown_mode' });
  const unsupportedEval = twc.evaluateQueuedJobClaimSkipReason(unsupported, {
    runningCount: 0,
    scrapeLockAvailable: true,
  });
  assert.strictEqual(unsupportedEval.eligible, false);
  assert.strictEqual(unsupportedEval.skipReason, 'unsupported_mode');

  const blockedByRunning = weekDryRunJob();
  const runningEval = twc.evaluateQueuedJobClaimSkipReason(blockedByRunning, {
    runningCount: 1,
    scrapeLockAvailable: true,
  });
  assert.strictEqual(runningEval.skipReason, 'job_already_running');

  const blockedByLock = weekDryRunJob();
  const lockEval = twc.evaluateQueuedJobClaimSkipReason(blockedByLock, {
    runningCount: 0,
    scrapeLockAvailable: false,
  });
  assert.strictEqual(lockEval.skipReason, 'scrape_lock_unavailable');
}

// 5. Concurrency lock behavior is visible safely.
{
  const analysis = twc.analyzeThresholdWorkerQueue({
    queuedJobs: [weekDryRunJob()],
    runningCount: 0,
    scrapeLockHeld: true,
    scrapeLockAgeMs: 120000,
    todayIso: TODAY,
  });
  assert.strictEqual(analysis.scrapeLockHeld, true);
  assert.strictEqual(analysis.scrapeLockAgeMs, 120000);
  assert.strictEqual(analysis.primaryBlocker, 'scrape_lock_unavailable');
  assert.strictEqual(analysis.queuedModeEligibleCount, 1);
  assert.strictEqual(analysis.queuedEligibleCount, 0);
}

// 6. Worker cannot report normal idle when mode-eligible queued jobs exist but are blocked.
{
  const analysis = twc.analyzeThresholdWorkerQueue({
    queuedJobs: [weekDryRunJob()],
    runningCount: 1,
    scrapeLockHeld: false,
    todayIso: TODAY,
  });
  assert.strictEqual(analysis.primaryBlocker, 'job_already_running');
  assert.ok(analysis.adminWarnings.some((w) => w.code === 'eligible_queued_jobs_unclaimed'));
  const result = twc.buildThresholdWorkerClaimResult({
    claimed: null,
    reason: 'job_already_running',
    queueAnalysis: analysis,
  });
  assert.strictEqual(result.diagnostics.adminWarnings.length > 0, true);
}

// Production-shaped job remains mode-eligible.
{
  const analysis = twc.analyzeThresholdWorkerQueue({
    queuedJobs: [weekDryRunJob()],
    runningCount: 0,
    scrapeLockHeld: false,
    todayIso: TODAY,
  });
  assert.strictEqual(analysis.queuedModeEligibleCount, 1);
  assert.strictEqual(analysis.queuedEligibleCount, 1);
  assert.strictEqual(analysis.nextClaimCandidate.jobId, '195846ab-c222-4c35-8d4f-8564aff216fa');
  assert.strictEqual(analysis.nextClaimCandidate.needsScrapeLock, true);
  assert.strictEqual(analysis.adminWarnings.length, 0);
}

// Worker endpoint path documented in server.js.
{
  const fs = require('fs');
  const path = require('path');
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /app\.post\('\/api\/admin\/run-threshold-scan-job'/);
  assert.match(serverJs, /app\.get\('\/api\/admin\/threshold-worker\/diagnostics'/);
  assert.match(serverJs, /warning: eligibleQueuedRemain \? 'eligible_queued_jobs_unclaimed'/);
  assert.doesNotMatch(serverJs, /\.eq\('dry_run', false\)/);
  assert.doesNotMatch(serverJs, /\.eq\('write_enabled', true\).*threshold_scan_jobs/s);
}

console.log('threshold worker claim regression: all tests passed');
