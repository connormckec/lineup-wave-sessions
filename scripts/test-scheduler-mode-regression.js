'use strict';

const assert = require('assert');
const publicSessionEnrich = require('../lib/public-session-enrich');

console.log('scheduler mode regression');

{
  const mode = publicSessionEnrich.resolveSchedulingMode({
    inProcessMaintenanceSchedulerEnabled: false,
    inlineThresholdWorkerEnabled: false,
  });
  assert.strictEqual(mode.mode, 'railway_crons');
  assert.strictEqual(mode.maintenanceScheduler, 'railway_cron');
  assert.strictEqual(mode.thresholdWorker, 'railway_cron');
  assert.strictEqual(mode.warnings.length, 0);
}

{
  const mode = publicSessionEnrich.resolveSchedulingMode({
    inProcessMaintenanceSchedulerEnabled: true,
    inlineThresholdWorkerEnabled: false,
  });
  assert.strictEqual(mode.mode, 'in_process');
  assert.strictEqual(mode.maintenanceScheduler, 'in_process');
  assert.strictEqual(mode.thresholdWorker, 'railway_cron');
  assert.ok(mode.warnings.some((w) => w.includes('maintenance-tick-cron')));
}

{
  const mode = publicSessionEnrich.resolveSchedulingMode({
    inProcessMaintenanceSchedulerEnabled: false,
    inlineThresholdWorkerEnabled: true,
  });
  assert.strictEqual(mode.mode, 'in_process');
  assert.strictEqual(mode.maintenanceScheduler, 'railway_cron');
  assert.strictEqual(mode.thresholdWorker, 'in_process');
  assert.ok(mode.warnings.some((w) => w.includes('threshold-worker-cron')));
}

{
  const mode = publicSessionEnrich.resolveSchedulingMode({
    inProcessMaintenanceSchedulerEnabled: true,
    inlineThresholdWorkerEnabled: true,
  });
  assert.strictEqual(mode.mode, 'in_process');
  assert.strictEqual(mode.warnings.length, 2);
}

console.log('scheduler mode regression: all tests passed');
