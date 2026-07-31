'use strict';

const MS_PER_DAY = 24 * 60 * 60 * 1000;

function parsePositiveInt(value, fallback) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function addDaysToIso(isoDate, days) {
  const [y, m, d] = isoDate.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return dt.toISOString().slice(0, 10);
}

function daysBetweenIso(startIso, endIso) {
  const a = new Date(`${startIso}T12:00:00.000Z`);
  const b = new Date(`${endIso}T12:00:00.000Z`);
  return Math.round((b - a) / MS_PER_DAY);
}

/**
 * Canonical supported future horizon for schedule collection and threshold scanning.
 * Schedule and threshold horizons share the same latest bookable date; threshold
 * cadence slows with distance but eligibility extends through this date.
 */
function resolveSupportedHorizon({
  todayIso,
  scrapeWeeksAhead = parsePositiveInt(process.env.SCRAPE_WEEKS_AHEAD, 4),
  effectiveWeeksAhead = null,
  maxBookingHorizonDays = parsePositiveInt(process.env.MAX_BOOKING_HORIZON_DAYS, 120),
} = {}) {
  if (!todayIso) {
    throw new Error('resolveSupportedHorizon requires todayIso');
  }
  const configuredWeeks = Math.max(1, scrapeWeeksAhead);
  const effectiveWeeks = Math.max(1, effectiveWeeksAhead ?? configuredWeeks);
  const scheduleHorizonDays = Math.max(configuredWeeks, effectiveWeeks) * 7;
  const supportedHorizonDays = Math.min(scheduleHorizonDays, maxBookingHorizonDays);
  const earliestSupportedDate = todayIso;
  const latestSupportedDate = addDaysToIso(todayIso, supportedHorizonDays - 1);
  const thresholdScanMaxHours = supportedHorizonDays * 24;
  // Partial-week pages may require multiple clicks; bound from full horizon, not week count.
  const maxNavigationSteps = Math.max(6, Math.ceil(supportedHorizonDays / 4) + 4);

  return {
    todayIso,
    earliestSupportedDate,
    latestSupportedDate,
    supportedHorizonDays,
    supportedHorizonWeeks: supportedHorizonDays / 7,
    scheduleHorizonDays,
    maxBookingHorizonDays,
    thresholdScanMaxHours,
    maxNavigationSteps,
    scrapeWeeksAhead: configuredWeeks,
    effectiveWeeksAhead: effectiveWeeks,
    scheduleHorizonDiffersFromBookingCap: scheduleHorizonDays > maxBookingHorizonDays,
    derivedDynamically: effectiveWeeksAhead != null && effectiveWeeksAhead !== configuredWeeks,
  };
}

function isIsoDateWithinSupportedHorizon(isoDate, horizon, { allowPast = false } = {}) {
  if (!isoDate || !horizon?.todayIso) return false;
  const daysAhead = daysBetweenIso(horizon.todayIso, isoDate);
  if (daysAhead < 0) return allowPast;
  if (daysAhead >= horizon.supportedHorizonDays) return false;
  return isoDate <= horizon.latestSupportedDate;
}

function isHoursWithinThresholdScanHorizon(hoursUntilStart, horizon, { watched = false } = {}) {
  if (watched) {
    return hoursUntilStart == null || hoursUntilStart <= horizon.thresholdScanMaxHours;
  }
  if (hoursUntilStart == null) return true;
  return hoursUntilStart >= 0 && hoursUntilStart <= horizon.thresholdScanMaxHours;
}

function computeThresholdSchedulingBounds(todayIso, horizon, { pastDaysForWatched = 7 } = {}) {
  return {
    minDate: addDaysToIso(todayIso, -pastDaysForWatched),
    maxDate: horizon.latestSupportedDate,
  };
}

function remainingOverdueCountByCadenceTier(candidates, { resolveTargetMinutesFn, now = new Date() } = {}) {
  const tiers = [
    { label: '<=24h', maxHours: 24 },
    { label: '<=72h', maxHours: 72 },
    { label: '<=168h', maxHours: 168 },
    { label: '<=14d', maxHours: 14 * 24 },
    { label: 'beyond_14d', maxHours: Infinity },
  ];
  const counts = Object.fromEntries(tiers.map((t) => [t.label, 0]));
  for (const bucket of candidates || []) {
    const hours = bucket.earliestHoursUntilStart;
    if (hours == null) continue;
    const tier = tiers.find((t) => hours <= t.maxHours);
    if (tier) counts[tier.label] += 1;
    if (resolveTargetMinutesFn) {
      void resolveTargetMinutesFn(hours);
    }
    void now;
  }
  return counts;
}

module.exports = {
  parsePositiveInt,
  addDaysToIso,
  daysBetweenIso,
  resolveSupportedHorizon,
  isIsoDateWithinSupportedHorizon,
  isHoursWithinThresholdScanHorizon,
  computeThresholdSchedulingBounds,
  remainingOverdueCountByCadenceTier,
};
