'use strict';

const datePipeline = require('./threshold-date-pipeline');

const schedulerState = datePipeline.createNearTermSchedulerState();

function recordNearTermMaintenanceTick(result, source = 'unknown') {
  datePipeline.recordNearTermTick(schedulerState, result, source);
}

function getNearTermSchedulerState() {
  return { ...schedulerState };
}

function resetNearTermSchedulerStateForTests() {
  const fresh = datePipeline.createNearTermSchedulerState();
  Object.assign(schedulerState, fresh);
}

function buildNearTermSchedulerDiagnostics(options = {}) {
  return datePipeline.buildNearTermSchedulerDiagnostics(schedulerState, options);
}

module.exports = {
  recordNearTermMaintenanceTick,
  getNearTermSchedulerState,
  resetNearTermSchedulerStateForTests,
  buildNearTermSchedulerDiagnostics,
};
