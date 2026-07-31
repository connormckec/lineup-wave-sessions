#!/usr/bin/env node
'use strict';

/**
 * Read-only production full-contract smoke for far-horizon threshold dates.
 * Uses production session inventory via Supabase (preferred) or PRODUCTION_API_URL.
 * Does not write rows, advance timestamps, enqueue jobs, or send notifications.
 *
 * Usage:
 *   PLAYWRIGHT_BROWSERS_PATH=$HOME/Library/Caches/ms-playwright \
 *     node scripts/smoke-threshold-horizon-contract-readonly.js
 *
 * Optional:
 *   PRODUCTION_API_URL=https://lineup-wave-sessions-production.up.railway.app
 *   SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (preferred for fingerprints)
 */

const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const thresholdDatePipeline = require('../lib/threshold-date-pipeline');

process.chdir(path.join(__dirname, '..'));

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const PRODUCTION_API_URL = (process.env.PRODUCTION_API_URL
  || 'https://lineup-wave-sessions-production.up.railway.app').replace(/\/$/, '');
const PARK = 'atlantic_park';
const MAX_THRESHOLD = Math.max(18, parseInt(process.env.THRESHOLD_JOB_MAX_THRESHOLD || '20', 10));

function daysBetween(fromIso, toIso) {
  const a = new Date(`${fromIso}T12:00:00.000Z`);
  const b = new Date(`${toIso}T12:00:00.000Z`);
  return Math.round((b - a) / (24 * 3600 * 1000));
}

function sessionFingerprint(rows) {
  return [...(rows || [])]
    .map((row) => ({
      session_key: row.session_key || row.key,
      threshold_scanned_at: row.raw?.threshold_scanned_at
        ?? row.raw?.thresholdScanAt
        ?? row.threshold_scanned_at
        ?? row.thresholdScanAt
        ?? null,
      available_entries: row.raw?.available_entries ?? row.available_entries ?? null,
      slot_source: row.raw?.slot_source ?? row.slot_source ?? null,
    }))
    .sort((a, b) => String(a.session_key).localeCompare(String(b.session_key)));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} → HTTP ${res.status}`);
  return res.json();
}

async function fetchSessionInventoryFingerprintSupabase(supabase, isoDate) {
  const { data, error } = await supabase
    .from('current_sessions')
    .select('session_key, iso_date, raw')
    .eq('park', PARK)
    .eq('iso_date', isoDate)
    .order('session_key', { ascending: true });
  if (error) throw error;
  return sessionFingerprint(data);
}

async function fetchSessionInventoryFingerprintProduction(isoDate) {
  const payload = await fetchJson(`${PRODUCTION_API_URL}/api/debug/date/${isoDate}`);
  return sessionFingerprint(payload.currentSessionsForDate || []);
}

async function findCandidateDatesFromSupabase(supabase, todayIso, latestIso) {
  const { data, error } = await supabase
    .from('current_sessions')
    .select('iso_date')
    .eq('park', PARK)
    .gte('iso_date', todayIso)
    .lte('iso_date', latestIso);
  if (error) throw error;
  const counts = new Map();
  for (const row of data || []) {
    const iso = String(row.iso_date).slice(0, 10);
    counts.set(iso, (counts.get(iso) || 0) + 1);
  }
  return pickBandCandidates(counts, todayIso);
}

async function findCandidateDatesFromProduction(todayIso, latestIso) {
  const payload = await fetchJson(
    `${PRODUCTION_API_URL}/api/session-date-coverage?startDate=${todayIso}&endDate=${latestIso}`,
  );
  const counts = new Map();
  for (const row of payload.dates || []) {
    if (!row?.isoDate || (row.sessionCount || 0) <= 0) continue;
    counts.set(String(row.isoDate).slice(0, 10), row.sessionCount);
  }
  return pickBandCandidates(counts, todayIso);
}

function pickBandCandidates(counts, todayIso) {
  const week2 = [];
  const beyond14 = [];
  for (const [isoDate, count] of counts.entries()) {
    const days = daysBetween(todayIso, isoDate);
    if (count <= 0) continue;
    if (days >= 8 && days <= 14) week2.push({ isoDate, sessionCount: count, daysAhead: days });
    if (days > 14) beyond14.push({ isoDate, sessionCount: count, daysAhead: days });
  }
  week2.sort((a, b) => b.sessionCount - a.sessionCount);
  beyond14.sort((a, b) => b.sessionCount - a.sessionCount);
  return { week2: week2[0] || null, beyond14: beyond14[0] || null };
}

async function seedProductionSessionsForDate(server, isoDate) {
  const payload = await fetchJson(`${PRODUCTION_API_URL}/api/sessions?date=${isoDate}`);
  const sessions = payload.sessions || [];
  if (!sessions.length) throw new Error(`production API returned no sessions for ${isoDate}`);
  server.seedSessionsForGate8DryRun(sessions);
  return sessions.length;
}

async function countRecentThresholdJobsSupabase(supabase) {
  const since = new Date(Date.now() - 5 * 60 * 1000).toISOString();
  const { count, error } = await supabase
    .from('threshold_scan_jobs')
    .select('*', { count: 'exact', head: true })
    .gte('created_at', since);
  if (error) throw error;
  return count || 0;
}

async function runContractForDate({
  server,
  supabase,
  target,
  label,
  fingerprintFn,
  trackJobs,
}) {
  const isoDate = target.isoDate;
  const beforeFingerprint = await fingerprintFn(isoDate);
  const jobCountBefore = trackJobs ? await countRecentThresholdJobsSupabase(supabase) : 0;

  const seededCount = await seedProductionSessionsForDate(server, isoDate);

  let launched = null;
  let writeRun = null;
  try {
    const recovery = await server.runGate8DateWriteContractWithRecovery({
      isoDate,
      weekMode: true,
      minThreshold: 1,
      maxThreshold: MAX_THRESHOLD,
      dryRun: true,
      write: false,
      launched: null,
    });
    launched = recovery.launched;
    writeRun = recovery.writeRun || {};
  } finally {
    await server.safeCloseBrowser(launched);
  }

  const afterFingerprint = await fingerprintFn(isoDate);
  const jobCountAfter = trackJobs ? await countRecentThresholdJobsSupabase(supabase) : jobCountBefore;
  const nav = writeRun.navigation || null;
  const preparedRows = writeRun.preparedRows || [];
  const preparedIsoDates = [...new Set(preparedRows.map((row) => {
    const key = row.thresholdDiagnostics?.identityKey || row.identityKey || '';
    return key.split('|')[0] || isoDate;
  }))];
  const resultsJson = {
    mode: thresholdDatePipeline.THRESHOLD_SCAN_JOB_MODE_DATE,
    stage: writeRun.fullScanContractOk ? 'completed' : 'failed',
    fullScanContractOk: writeRun.fullScanContractOk === true,
    preparedUpdatesCount: writeRun.rowsPrepared ?? preparedRows.length,
    preparedScanCompletedAt: writeRun.fullScanContractOk ? new Date().toISOString() : null,
    targetIsoDate: isoDate,
  };

  return {
    label,
    targetIsoDate: isoDate,
    daysAhead: target.daysAhead,
    sessionCount: target.sessionCount,
    seededSessionCount: seededCount,
    initialVisibleMin: nav?.visibleWeekStart ?? nav?.navigationLog?.initialVisibleMin ?? null,
    initialVisibleMax: nav?.visibleWeekEnd ?? nav?.navigationLog?.initialVisibleMax ?? null,
    navigationClicks: nav?.clickedNextWeekCount ?? 0,
    navigationAttempts: nav?.navigationLog?.navigationAttempts ?? nav?.navigationAttempts ?? [],
    targetDateVisible: nav?.targetDateVisibleFromHeaders === true,
    targetDateTileCount: writeRun.targetDateEvidence?.targetDateTileCount ?? null,
    parsedTargetDateSessionCount: writeRun.inferredForIsoDateCount
      ?? writeRun.targetDateEvidence?.parsedTargetDateSessionCount
      ?? null,
    exactCount: writeRun.exactCount ?? 0,
    preparedUpdateCount: preparedRows.length,
    preparedIsoDates,
    preparedRowsAllMatchTarget: preparedIsoDates.every((d) => d === isoDate),
    fullScanContractOk: writeRun.fullScanContractOk === true,
    operationallyComplete: thresholdDatePipeline.isDateScanOperationallyComplete({
      ...resultsJson,
      preparedUpdatesCount: writeRun.rowsPrepared ?? preparedRows.length,
    }),
    failureReason: writeRun.error || null,
    productionWritesDetected: JSON.stringify(beforeFingerprint) !== JSON.stringify(afterFingerprint),
    thresholdJobsCreated: jobCountAfter > jobCountBefore,
    beforeFingerprint,
    afterFingerprint,
    durationMs: null,
  };
}

async function tryAlternateDates({
  server,
  supabase,
  band,
  candidates,
  todayIso,
  fingerprintFn,
  trackJobs,
}) {
  const sorted = [...candidates].sort((a, b) => b.sessionCount - a.sessionCount);
  for (const target of sorted.slice(0, 5)) {
    console.error(`contract smoke: trying ${band} → ${target.isoDate} (${target.sessionCount} sessions)`);
    const started = Date.now();
    const row = await runContractForDate({
      server,
      supabase,
      target,
      label: band,
      fingerprintFn,
      trackJobs,
    });
    row.durationMs = Date.now() - started;
    if (row.fullScanContractOk && row.operationallyComplete) return row;
    console.error(`  failed: ${row.failureReason || 'contract_incomplete'}`);
  }
  return null;
}

async function main() {
  const server = require('../server.js');
  const todayIso = server.getParkTodayIso();
  const horizon = server.getRuntimeSupportedHorizon(todayIso);
  const latestIso = horizon.latestSupportedDate;

  const useSupabase = !!(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
  const supabase = useSupabase ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY) : null;
  const fingerprintFn = useSupabase
    ? (isoDate) => fetchSessionInventoryFingerprintSupabase(supabase, isoDate)
    : (isoDate) => fetchSessionInventoryFingerprintProduction(isoDate);

  const candidates = useSupabase
    ? await findCandidateDatesFromSupabase(supabase, todayIso, latestIso)
    : await findCandidateDatesFromProduction(todayIso, latestIso);

  console.log(JSON.stringify({
    smoke: 'threshold_horizon_contract_readonly',
    dryRun: true,
    writeEnabled: false,
    todayIso,
    latestIso,
    maxThreshold: MAX_THRESHOLD,
    inventorySource: useSupabase ? 'supabase' : 'production_api',
    productionApiUrl: PRODUCTION_API_URL,
    candidates,
  }, null, 2));

  const results = [];
  for (const [band, initial] of [
    ['week_2_band', candidates.week2],
    ['beyond_14_days', candidates.beyond14],
  ]) {
    if (!initial) {
      console.error(`contract smoke: no candidate for ${band}`);
      continue;
    }
    const bandCandidates = useSupabase
      ? (await findCandidateDatesFromSupabase(supabase, todayIso, latestIso))[band === 'week_2_band' ? 'week2' : 'beyond14']
        ? [initial]
        : []
      : [initial];

    const allInBand = [];
    if (band === 'week_2_band') {
      const payload = useSupabase
        ? await findCandidateDatesFromSupabase(supabase, todayIso, latestIso)
        : await findCandidateDatesFromProduction(todayIso, latestIso);
      const counts = new Map();
      if (useSupabase) {
        const { data } = await supabase
          .from('current_sessions')
          .select('iso_date')
          .eq('park', PARK)
          .gte('iso_date', todayIso)
          .lte('iso_date', latestIso);
        for (const row of data || []) {
          const iso = String(row.iso_date).slice(0, 10);
          counts.set(iso, (counts.get(iso) || 0) + 1);
        }
      } else {
        const payload2 = await fetchJson(
          `${PRODUCTION_API_URL}/api/session-date-coverage?startDate=${todayIso}&endDate=${latestIso}`,
        );
        for (const row of payload2.dates || []) {
          if ((row.sessionCount || 0) > 0) counts.set(row.isoDate, row.sessionCount);
        }
      }
      for (const [isoDate, count] of counts.entries()) {
        const days = daysBetween(todayIso, isoDate);
        if (days >= 8 && days <= 14) allInBand.push({ isoDate, sessionCount: count, daysAhead: days });
      }
    } else {
      const payload = useSupabase
        ? null
        : await fetchJson(
          `${PRODUCTION_API_URL}/api/session-date-coverage?startDate=${todayIso}&endDate=${latestIso}`,
        );
      if (useSupabase) {
        const { data } = await supabase
          .from('current_sessions')
          .select('iso_date')
          .eq('park', PARK)
          .gte('iso_date', todayIso)
          .lte('iso_date', latestIso);
        const counts = new Map();
        for (const row of data || []) {
          const iso = String(row.iso_date).slice(0, 10);
          counts.set(iso, (counts.get(iso) || 0) + 1);
        }
        for (const [isoDate, count] of counts.entries()) {
          const days = daysBetween(todayIso, isoDate);
          if (days > 14) allInBand.push({ isoDate, sessionCount: count, daysAhead: days });
        }
      } else {
        for (const row of payload.dates || []) {
          const days = daysBetween(todayIso, row.isoDate);
          if ((row.sessionCount || 0) > 0 && days > 14) {
            allInBand.push({ isoDate: row.isoDate, sessionCount: row.sessionCount, daysAhead: days });
          }
        }
      }
    }

    const row = await tryAlternateDates({
      server,
      supabase,
      band,
      candidates: allInBand.length ? allInBand : [initial],
      todayIso,
      fingerprintFn,
      trackJobs: useSupabase,
    });
    if (!row) {
      results.push({
        label: band,
        targetIsoDate: initial.isoDate,
        fullScanContractOk: false,
        operationallyComplete: false,
        failureReason: 'no_passing_production_date_in_band',
      });
    } else {
      results.push(row);
      console.log(JSON.stringify({ contractSmokeResult: row }, null, 2));
    }
  }

  const allPassed = results.length >= 2 && results.every((row) => row.fullScanContractOk
    && row.operationallyComplete
    && row.targetDateVisible !== false
    && row.preparedRowsAllMatchTarget !== false
    && !row.productionWritesDetected
    && !row.thresholdJobsCreated
    && row.exactCount > 0
    && row.preparedUpdateCount > 0);

  console.log(JSON.stringify({
    smoke: 'threshold_horizon_contract_readonly_complete',
    allPassed,
    results,
  }, null, 2));

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
