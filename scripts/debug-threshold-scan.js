#!/usr/bin/env node
'use strict';

/**
 * Local threshold scanner debug runner.
 *
 * Usage:
 *   node scripts/debug-threshold-scan.js
 *   node scripts/debug-threshold-scan.js --isoDate=2026-06-30 --headed --trace --screenshot
 *
 * Requires env vars used by server.js (Supabase optional for dry-run navigation only).
 */

const path = require('path');

process.chdir(path.join(__dirname, '..'));

const isoDate = process.argv.find(a => a.startsWith('--isoDate='))?.split('=')[1] || '2026-06-30';
const headed = process.argv.includes('--headed');
const trace = process.argv.includes('--trace');
const screenshot = process.argv.includes('--screenshot');
const maxThreshold = parseInt(process.argv.find(a => a.startsWith('--maxThreshold='))?.split('=')[1] || '3', 10);

async function main() {
  const server = require('../server.js');
  const runThresholdScansChunked = server.runThresholdScansChunked || global.runThresholdScansChunked;

  if (typeof runThresholdScansChunked !== 'function') {
    console.error('runThresholdScansChunked is not exported from server.js');
    console.error('Run via admin route instead:');
    console.error('  curl -s -X POST http://localhost:3000/api/admin/scan-entries-left-thresholds \\');
    console.error('    -H "Content-Type: application/json" \\');
    console.error(`    -d '{"isoDate":"${isoDate}","weekMode":true,"minThreshold":1,"maxThreshold":${maxThreshold},"wait":true,"dryRun":true,"debug":true,"trace":${trace},"screenshot":${screenshot},"headed":${headed}}'`);
    process.exit(1);
  }

  const computedWeekStart = server.getMondayWeekStartIso?.(isoDate)
    || require('../server.js').getMondayWeekStartIso?.(isoDate);

  const result = await runThresholdScansChunked([isoDate], {
    dryRun: true,
    minThreshold: 1,
    maxThreshold,
    maxWeeksPerRun: 1,
    requestedIsoDate: isoDate,
    navigationIsoDate: computedWeekStart || isoDate,
    computedWeekStart,
    weekMode: true,
    debug: true,
    trace,
    screenshot,
    headed,
  });

  console.log(JSON.stringify({
    statusReason: result.dateResults?.[0]?.statusReason,
    requestedIsoDate: isoDate,
    computedWeekStart,
    navigationIsoDate: computedWeekStart,
    currentUrl: result.dateResults?.[0]?.currentUrl,
    pageTitle: result.dateResults?.[0]?.pageTitle,
    bodyTextLength: result.dateResults?.[0]?.bodyTextLength,
    bodyTextSample: result.dateResults?.[0]?.bodyTextSample,
    calendarReadySignals: result.dateResults?.[0]?.calendarReadySignals,
    auditTrail: result.auditTrail,
    headerScrapeAttempts: result.dateResults?.[0]?.headerScrapeAttempts,
    earlyExitStage: result.dateResults?.[0]?.earlyExitStage,
    earlyExitReason: result.dateResults?.[0]?.earlyExitReason,
    error: result.dateResults?.[0]?.error,
    crashed: result.crashed,
    tracePath: result.tracePath,
  }, null, 2));

  process.exit(result.crashed ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
