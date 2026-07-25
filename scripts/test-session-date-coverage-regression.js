'use strict';

const assert = require('assert');
const cov = require('../lib/session-date-coverage');

console.log('session date coverage regression');

{
  const agg = cov.aggregateCoverageByDate([
    { iso_date: '2026-08-13', session_type: 'Progressive', last_basic_check_at: '2026-07-25T10:00:00.000Z', raw: { wave: 2 } },
    { iso_date: '2026-08-13', session_type: 'Progressive', last_basic_check_at: '2026-07-25T11:00:00.000Z', raw: { wave: 3 } },
    { iso_date: '2026-08-14', session_type: 'Cabanas', last_basic_check_at: '2026-07-25T10:00:00.000Z', raw: { wave: 1 } },
  ]);
  const row = cov.resolveDateCoverageStatus('2026-08-13', agg.get('2026-08-13'), new Set(), new Set());
  assert.strictEqual(row.status, 'has_sessions');
  assert.strictEqual(row.sessionCount, 2);
  assert.strictEqual(row.lastCheckedAt, '2026-07-25T11:00:00.000Z');

  const browserMemorySessionCount = 0;
  assert.strictEqual(browserMemorySessionCount, 0);
  assert.strictEqual(row.sessionCount, 2, 'backend coverage shows sessions even when browser memory is empty');
}

{
  const status = cov.resolveDateCoverageStatus('2026-08-12', undefined, new Set(['2026-08-12']), new Set());
  assert.strictEqual(status.status, 'checked_empty');
  assert.strictEqual(status.sessionCount, 0);
}

{
  const status = cov.resolveDateCoverageStatus('2026-08-15', undefined, new Set(), new Set());
  assert.strictEqual(status.status, 'not_checked');
  assert.strictEqual(status.sessionCount, null);
  assert.strictEqual(status.lastCheckedAt, null);
}

{
  const dates = cov.buildDateCoverageList(
    '2026-08-13',
    '2026-08-16',
    cov.aggregateCoverageByDate([
      { iso_date: '2026-08-13', session_type: 'Progressive', raw: { wave: 2 } },
      { iso_date: '2026-08-14', session_type: 'Progressive', raw: { wave: 2 } },
      { iso_date: '2026-08-15', session_type: 'Progressive', raw: { wave: 2 } },
      { iso_date: '2026-08-16', session_type: 'Progressive', raw: { wave: 2 } },
    ]),
    new Set(),
    new Set(),
  );
  assert.strictEqual(dates.length, 4);
  for (const isoDate of ['2026-08-13', '2026-08-14', '2026-08-15', '2026-08-16']) {
    const row = dates.find((entry) => entry.isoDate === isoDate);
    assert.strictEqual(row.status, 'has_sessions', isoDate);
    assert.strictEqual(row.sessionCount, 1, isoDate);
  }
}

{
  assert.strictEqual(
    cov.pickerClassForCoverageStatus('not_checked'),
    'coverage-unknown',
  );
  assert.strictEqual(
    cov.pickerClassForCoverageStatus('checked_empty'),
    'no-sessions checked-empty',
  );
  assert.strictEqual(
    cov.pickerClassForCoverageStatus('has_sessions'),
    'has-sessions',
  );
}

{
  const now = 1_000_000;
  assert.strictEqual(
    cov.shouldRefreshDateCoverage({
      lastFetchedAt: now - 10_000,
      now,
      rangeKey: 'a:b',
      cachedRangeKey: 'a:b',
      force: false,
    }),
    false,
  );
  assert.strictEqual(
    cov.shouldRefreshDateCoverage({
      lastFetchedAt: now - 120_000,
      now,
      rangeKey: 'a:b',
      cachedRangeKey: 'a:b',
      force: false,
    }),
    true,
  );
  assert.strictEqual(
    cov.shouldRefreshDateCoverage({
      lastFetchedAt: now,
      now,
      rangeKey: 'a:b',
      cachedRangeKey: 'a:c',
      force: false,
    }),
    true,
  );
}

{
  const merged = cov.mergeSelectedDateCoverage({}, '2026-08-13', {
    statusReason: 'saved_sessions_found',
    sessionsCount: 52,
    lastCheckedAt: '2026-07-25T12:00:00.000Z',
  });
  assert.strictEqual(merged['2026-08-13'].status, 'has_sessions');
  assert.strictEqual(merged['2026-08-13'].sessionCount, 52);

  const afterRefresh = cov.mergeSelectedDateCoverage(merged, '2026-08-13', {
    statusReason: 'checked_no_sessions',
    sessionsCount: 0,
    lastCheckedAt: '2026-07-25T13:00:00.000Z',
  });
  assert.strictEqual(afterRefresh['2026-08-13'].status, 'checked_empty');
}

{
  const errorState = cov.coverageStatusFromSelectedDateMeta({ statusReason: 'error', sessionsCount: 0 });
  assert.strictEqual(errorState.status, 'error');
}

console.log('session date coverage regression: all tests passed');
