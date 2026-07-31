'use strict';

function visibleRangeFromHeaders(headers) {
  const dates = [...new Set((headers || []).filter(Boolean))].sort();
  if (!dates.length) {
    return { min: null, max: null, dates: [] };
  }
  return { min: dates[0], max: dates[dates.length - 1], dates };
}

function navigationDirectionForVisibleHeaders(visibleHeaders, validationTarget) {
  if (!visibleHeaders?.length || !validationTarget) return null;
  if (visibleHeaders.includes(validationTarget)) return null;
  const min = visibleHeaders[0];
  const max = visibleHeaders[visibleHeaders.length - 1];
  if (validationTarget < min) return 'prev';
  if (validationTarget > max) return 'next';
  return null;
}

function calendarStateSignature(headers) {
  const range = visibleRangeFromHeaders(headers);
  return {
    visibleMin: range.min,
    visibleMax: range.max,
    visibleDates: range.dates,
    signature: range.dates.join(','),
  };
}

function calendarStateChanged(beforeHeaders, afterHeaders) {
  const before = calendarStateSignature(beforeHeaders);
  const after = calendarStateSignature(afterHeaders);
  if (!before.signature && !after.signature) return false;
  return before.signature !== after.signature;
}

function mapNavigationFailureReason({
  targetVisible,
  direction,
  clickSucceeded,
  stateChanged,
  step,
  maxSteps,
  headerParseFailed,
}) {
  if (targetVisible) return null;
  if (headerParseFailed) return 'calendar_headers_not_ready';
  if (direction && clickSucceeded === false) return 'calendar_navigation_stalled';
  if (step >= maxSteps) return 'calendar_navigation_limit_reached';
  return 'target_date_not_visible';
}

function createNavigationAttempt({
  attemptNumber,
  direction,
  controlClicked,
  beforeHeaders,
  afterHeaders,
  clickSucceeded,
}) {
  const before = calendarStateSignature(beforeHeaders);
  const after = calendarStateSignature(afterHeaders);
  const stateChanged = calendarStateChanged(beforeHeaders, afterHeaders);
  return {
    attemptNumber,
    navigationDirection: direction,
    controlClicked: controlClicked || null,
    domStateBefore: before,
    domStateAfter: after,
    clickSucceeded: clickSucceeded === true,
    observableStateChanged: stateChanged,
  };
}

function buildThresholdDateScanNavigationLog({
  jobId = null,
  targetIsoDate,
  horizon = null,
  initialHeaders = [],
  navigationAttempts = [],
  targetDateVisible = false,
  targetDateTileCount = null,
  parsedTargetDateSessionCount = null,
  preparedUpdateCount = null,
  completionReason = null,
  failureReason = null,
} = {}) {
  const initial = calendarStateSignature(initialHeaders);
  return {
    jobId,
    targetIsoDate,
    configuredLatestSupportedDate: horizon?.latestSupportedDate ?? null,
    configuredMaximumHorizonDays: horizon?.supportedHorizonDays ?? null,
    initialVisibleDates: initial.dates,
    initialVisibleMin: initial.min,
    initialVisibleMax: initial.max,
    navigationAttempts,
    targetDateVisible: targetDateVisible === true,
    targetDateTileCount,
    parsedTargetDateSessionCount,
    preparedUpdateCount,
    completionReason,
    failureReason,
  };
}

function estimateNavigationClicksRequired(visibleHeaders, targetIsoDate) {
  if (!targetIsoDate) return null;
  if ((visibleHeaders || []).includes(targetIsoDate)) return 0;
  const range = visibleRangeFromHeaders(visibleHeaders);
  if (!range.min || !range.max) return null;
  if (targetIsoDate > range.max) return 'forward_unknown';
  if (targetIsoDate < range.min) return 'backward_unknown';
  return null;
}

module.exports = {
  visibleRangeFromHeaders,
  navigationDirectionForVisibleHeaders,
  calendarStateSignature,
  calendarStateChanged,
  mapNavigationFailureReason,
  createNavigationAttempt,
  buildThresholdDateScanNavigationLog,
  estimateNavigationClicksRequired,
};
