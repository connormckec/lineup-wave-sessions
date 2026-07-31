'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const maintenanceQueries = require('../lib/maintenance-queries');

console.log('maintenance query regression');

function makeDueSession(index, {
  isoDate = '2026-07-30',
  staleMinutes = 120,
  now = new Date('2026-07-30T18:00:00.000Z'),
} = {}) {
  const scannedAt = new Date(now.getTime() - staleMinutes * 60 * 1000).toISOString();
  const ts = Math.floor(new Date(`${isoDate}T22:00:00.000Z`).getTime() / 1000);
  return {
    key: `session-${index}`,
    isoDate,
    dateKey: isoDate,
    ts,
    start_ts: ts,
    available: true,
    threshold_scanned_at: scannedAt,
    threshold_scan_verified: true,
    thresholdScanVerified: true,
    thresholdConfidence: 'exact',
    slot_source: 'entries_left_threshold_scan',
    available_entries: 2,
  };
}

function daysFromTodayFn(isoDate, todayIso = '2026-07-30') {
  const a = new Date(`${isoDate}T12:00:00.000Z`);
  const b = new Date(`${todayIso}T12:00:00.000Z`);
  return Math.round((a - b) / (24 * 3600 * 1000));
}

async function runTests() {
  // 1. 143+ overdue sessions still yield a single top candidate date.
  {
    const now = new Date('2026-07-30T18:00:00.000Z');
    const sessions = [];
    for (let i = 0; i < 143; i += 1) {
      sessions.push(makeDueSession(i, {
        isoDate: i < 100 ? '2026-07-30' : '2026-07-31',
        staleMinutes: 200 + i,
        now,
      }));
    }
    const dueScan = maintenanceQueries.collectDueScanFromSchedulingSessions(sessions, {
      watchKeys: new Set(),
      todayIso: '2026-07-30',
      daysFromTodayFn,
      now,
    });
    assert.strictEqual(dueScan.watchedDueCount + dueScan.generalDueCount, 143);
    assert.ok(dueScan.topCandidate);
    assert.strictEqual(dueScan.topCandidate.isoDate, '2026-07-30');
    const summary = maintenanceQueries.compactDueScanSummary(dueScan);
    assert.strictEqual(summary.dueDateCount, 2);
    assert.ok(summary.topDueDateCandidate);
  }

  // 2. Instrumentation logs query metadata without row bodies.
  {
    const logs = [];
    const originalLog = console.log;
    console.log = (...args) => logs.push(args.join(' '));
    try {
      const instrument = maintenanceQueries.createQueryInstrumenter('test-route');
      const payload = await instrument.runInstrumentedQuery('sample_query', async () => ([{ id: 1 }, { id: 2 }]));
      assert.strictEqual(payload.ok, true);
      assert.strictEqual(payload.rowCount, 2);
      const parsed = JSON.parse(logs[0]);
      assert.strictEqual(parsed.maintenanceRoute, 'test-route');
      assert.strictEqual(parsed.queryName, 'sample_query');
      assert.strictEqual(parsed.rowCount, 2);
      assert.ok(!JSON.stringify(parsed).includes('session-'));
    } finally {
      console.log = originalLog;
    }
  }

  // 3. Optional query timeout returns warning, not throw.
  {
    const instrument = maintenanceQueries.createQueryInstrumenter('status-test');
    const optional = await instrument.runOptionalQuery('slow_optional', () => new Promise(() => {}), {
      timeoutMs: 20,
    });
    assert.strictEqual(optional.skipped, true);
    assert.strictEqual(optional.warning.code, 'maintenance_query_timeout');
    assert.strictEqual(optional.warning.queryName, 'slow_optional');
  }

  // 4. Near-term tick route no longer hydrates scheduleDiagnostics or full session arrays.
  {
    const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    const tickBody = serverJs.slice(
      serverJs.indexOf('async function runNearTermMaintenanceTick'),
      serverJs.indexOf('async function runBroadMaintenanceTick'),
    );
    assert.doesNotMatch(tickBody, /scheduleDiagnostics/);
    assert.doesNotMatch(tickBody, /buildAdaptiveScheduleDiagnostics/);
    assert.doesNotMatch(tickBody, /ensureSessionsForStatus/);
    assert.match(tickBody, /fetchNearTermDueSummary/);
    assert.match(tickBody, /buildNearTermTickCompactResult/);
    assert.match(serverJs, /createQueryInstrumenter\('near-term-tick'\)/);
    assert.match(serverJs, /createQueryInstrumenter\('maintenance-status'\)/);
  }

  // 5. Status defaults to compact mode and surfaces diagnosticsWarnings.
  {
    const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.match(serverJs, /compact = req\.query\?\.compact !== '0'/);
    assert.match(serverJs, /diagnosticsWarnings/);
    assert.match(serverJs, /coverage_current_sessions_range/);
    assert.doesNotMatch(
      serverJs.slice(
        serverJs.indexOf('async function buildThresholdCoverageForRange'),
        serverJs.indexOf('function getThresholdStaleMaxAgeMs'),
      ),
      /fetchGate8CurrentSessionsForIsoDate/,
    );
  }

  // 6. Existing queued/running job skip reasons preserved in near-term tick compact result.
  {
    const skipped = maintenanceQueries.buildNearTermTickCompactResult({
      ok: true,
      action: 'skipped',
      reason: 'job_already_running_or_queued',
      startDate: '2026-07-30',
      endDate: '2026-08-29',
    });
    assert.strictEqual(skipped.reason, 'job_already_running_or_queued');
    const none = maintenanceQueries.buildNearTermTickCompactResult({
      ok: true,
      action: 'none_needed',
      reason: 'due_date_already_queued',
      dueSummary: { watchedDueCount: 0, generalDueCount: 143 },
    });
    assert.strictEqual(none.reason, 'due_date_already_queued');
  }

  // 7. Near-term tick enqueues at most one job (static guard).
  {
    const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    const tickBody = serverJs.slice(
      serverJs.indexOf('async function runNearTermMaintenanceTick'),
      serverJs.indexOf('async function runBroadMaintenanceTick'),
    );
    assert.match(tickBody, /enqueueNearTermDateThresholdScan\(top\.isoDate/);
    assert.doesNotMatch(tickBody, /for \(const job of/);
  }

  // 8. Backlog health still marks idle pipeline unhealthy with 143 due sessions.
  {
    const backlog = maintenanceQueries.buildBacklogFromDueSummary(
      { watchedDueCount: 0, generalDueCount: 143, topDueDateCandidate: { isoDate: '2026-07-30' } },
      {
        queuedDateScans: 0,
        runningDateScans: 0,
        readyDateApplies: 0,
        queuedDateApplies: 0,
        runningDateApplies: 0,
      },
    );
    assert.strictEqual(backlog.dueSessionCount, 143);
    assert.strictEqual(backlog.unhealthyReason, 'due_backlog_with_idle_pipeline');
  }

  // 9. Bounded near-term session query uses current_sessions, not availability_snapshots.
  {
    const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    const fn = serverJs.slice(
      serverJs.indexOf('async function fetchNearTermSchedulingSessions'),
      serverJs.indexOf('async function fetchNearTermDueSummary'),
    );
    assert.match(fn, /\.from\('current_sessions'\)/);
    assert.match(fn, /\.limit\(2000\)/);
    assert.doesNotMatch(fn, /availability_snapshots/);
  }

  // 10. Migration file exists and is non-destructive.
  {
    const migrationPath = path.join(__dirname, '../supabase/migrations/202607302100_maintenance_query_indexes.sql');
    const sql = fs.readFileSync(migrationPath, 'utf8');
    assert.match(sql, /CREATE INDEX IF NOT EXISTS/);
    assert.doesNotMatch(sql, /DROP /i);
    assert.doesNotMatch(sql, /TRUNCATE /i);
  }

  console.log('maintenance query regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
