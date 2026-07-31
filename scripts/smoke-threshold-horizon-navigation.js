#!/usr/bin/env node
'use strict';

/**
 * Live Atlantic Park calendar navigation smoke (dry-run only).
 * Does not write trusted counts or advance verification timestamps.
 *
 * Usage:
 *   node scripts/smoke-threshold-horizon-navigation.js
 *   node scripts/smoke-threshold-horizon-navigation.js --headed
 */

const path = require('path');

process.chdir(path.join(__dirname, '..'));

const headed = process.argv.includes('--headed');
const maxThreshold = parseInt(
  process.argv.find((a) => a.startsWith('--maxThreshold='))?.split('=')[1] || '3',
  10,
);

async function smokeOneTarget(server, targetIsoDate, label) {
  const started = Date.now();
  let writeRun = null;
  let launched = null;
  try {
    const recovery = await server.runGate8DateWriteContractWithRecovery({
      isoDate: targetIsoDate,
      weekMode: true,
      minThreshold: 1,
      maxThreshold,
      dryRun: true,
      write: false,
      launched: null,
    });
    launched = recovery.launched;
    writeRun = recovery.writeRun || {};
    const nav = writeRun.navigation || recovery.navigation || null;
    const navigationLog = nav?.navigationLog || null;
    const targetDateEvidence = writeRun.targetDateEvidence || null;

    return {
      label,
      targetIsoDate,
      initialVisibleMin: navigationLog?.initialVisibleMin ?? nav?.visibleWeekStart ?? null,
      initialVisibleMax: navigationLog?.initialVisibleMax ?? nav?.visibleWeekEnd ?? null,
      initialVisibleDates: navigationLog?.initialVisibleDates ?? nav?.visibleIsoDatesFromHeaders ?? [],
      navigationClicks: nav?.clickedNextWeekCount ?? 0,
      navigationAttempts: navigationLog?.navigationAttempts ?? nav?.navigationAttempts ?? [],
      targetDateVisible: nav?.targetDateVisibleFromHeaders === true,
      targetDateTileCount: targetDateEvidence?.targetDateTileCount ?? null,
      parsedTargetDateSessionCount: writeRun.inferredForIsoDateCount ?? targetDateEvidence?.parsedTargetDateSessionCount ?? null,
      fullScanContractOk: writeRun.fullScanContractOk === true,
      contractError: writeRun.error || null,
      failureReason: writeRun.error || nav?.navigationError || null,
      durationMs: Date.now() - started,
    };
  } catch (err) {
    return {
      label,
      targetIsoDate,
      error: err.message || String(err),
      failureReason: err.message || String(err),
      durationMs: Date.now() - started,
    };
  } finally {
    await server.safeCloseBrowser(launched);
  }
}

async function main() {
  const server = require('../server.js');
  const todayIso = server.getParkTodayIso();
  const horizon = server.getRuntimeSupportedHorizon(todayIso);

  const targets = [
    { label: 'already_visible_or_near', isoDate: server.addDaysToParkIso(todayIso, 1) },
    { label: 'about_8_days', isoDate: server.addDaysToParkIso(todayIso, 8) },
    { label: 'about_15_days', isoDate: server.addDaysToParkIso(todayIso, 15) },
    { label: 'latest_supported', isoDate: horizon.latestSupportedDate },
  ];

  if (headed) process.env.PLAYWRIGHT_HEADED = '1';

  console.log(JSON.stringify({
    smoke: 'threshold_horizon_navigation',
    dryRun: true,
    writeEnabled: false,
    todayIso,
    horizon: {
      latestSupportedDate: horizon.latestSupportedDate,
      supportedHorizonDays: horizon.supportedHorizonDays,
      maxNavigationSteps: horizon.maxNavigationSteps,
    },
    targets: targets.map((t) => ({ label: t.label, isoDate: t.isoDate })),
  }, null, 2));

  const results = [];
  for (const target of targets) {
    console.error(`smoke: ${target.label} → ${target.isoDate}`);
    const row = await smokeOneTarget(server, target.isoDate, target.label);
    results.push(row);
    console.log(JSON.stringify({ smokeResult: row }, null, 2));
  }

  const summary = {
    smoke: 'threshold_horizon_navigation_complete',
    allTargetDatesVisible: results.every((r) => r.targetDateVisible === true),
    results,
  };
  console.log(JSON.stringify(summary, null, 2));
  process.exit(results.some((r) => r.failureReason && !r.fullScanContractOk) ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
