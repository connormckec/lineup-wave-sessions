'use strict';

const assert = require('assert');
const cov = require('../lib/session-date-coverage');

console.log('session date coverage truncation regression');

function makeCoverageRow(isoDate, sessionKey, opts = {}) {
  return {
    iso_date: isoDate,
    session_key: sessionKey,
    session_type: opts.session_type || 'Progressive',
    last_basic_check_at: opts.last_basic_check_at || '2026-07-25T10:00:00.000Z',
    raw: { wave: opts.wave ?? 2 },
  };
}

function listIsoDates(startDate, endDate) {
  const dates = [];
  let cur = startDate;
  while (cur <= endDate) {
    dates.push(cur);
    cur = cov.addDaysToIso(cur, 1);
  }
  return dates;
}

function buildTruncationFixture() {
  const rows = [];
  let keyIndex = 0;
  const nextKey = () => `sess-${String(keyIndex++).padStart(6, '0')}`;

  const preAug13Dates = listIsoDates('2026-07-25', '2026-08-12');
  for (let i = 0; i < 1000; i += 1) {
    rows.push(makeCoverageRow(preAug13Dates[i % preAug13Dates.length], nextKey()));
  }

  const aug13PlusCounts = {
    '2026-08-13': 50,
    '2026-08-14': 56,
    '2026-08-15': 60,
    '2026-08-16': 56,
    '2026-08-17': 52,
    '2026-08-18': 52,
    '2026-08-19': 48,
    '2026-08-20': 52,
    '2026-08-21': 56,
  };
  for (const [isoDate, count] of Object.entries(aug13PlusCounts)) {
    for (let i = 0; i < count; i += 1) {
      rows.push(makeCoverageRow(isoDate, nextKey()));
    }
  }

  rows.sort((a, b) => (
    a.iso_date.localeCompare(b.iso_date) || a.session_key.localeCompare(b.session_key)
  ));
  return { rows, aug13PlusCounts };
}

async function paginateFixtureRows(allRows, pageSize = cov.COVERAGE_QUERY_PAGE_SIZE) {
  const pages = cov.paginateRowsForTest(allRows, pageSize);
  return cov.fetchAllCoverageRowsFromPages(pages, pageSize);
}

function buildPayloadFromRows(allRows, startDate, endDate, {
  queryComplete = true,
  rowCountScanned = allRows.length,
  checkedEmptySet = new Set(),
  persistedCheckedSet = new Set(),
} = {}) {
  return cov.buildDateCoveragePayload({
    startDate,
    endDate,
    rows: allRows,
    queryComplete,
    rowCountScanned,
    checkedEmptySet,
    persistedCheckedSet,
  });
}

function dateRow(payload, isoDate) {
  return payload.dates.find((entry) => entry.isoDate === isoDate);
}

async function runTests() {
  {
    const { rows, aug13PlusCounts } = buildTruncationFixture();
    assert.strictEqual(rows.length, 1482);

    const pageResult = await paginateFixtureRows(rows);
    assert.strictEqual(pageResult.complete, true);
    assert.strictEqual(pageResult.rowCountScanned, 1482);
    assert.strictEqual(pageResult.rows.length, 1482);

    const payload = buildPayloadFromRows(pageResult.rows, '2026-07-25', '2026-08-21', {
      queryComplete: pageResult.complete,
      rowCountScanned: pageResult.rowCountScanned,
    });
    assert.strictEqual(payload.complete, true);
    assert.ok(payload.rowCountScanned > 1000);
    assert.strictEqual(payload.dates.length, listIsoDates('2026-07-25', '2026-08-21').length);

    for (const [isoDate, count] of Object.entries(aug13PlusCounts)) {
      const row = dateRow(payload, isoDate);
      assert.ok(row, isoDate);
      assert.strictEqual(row.status, 'has_sessions', isoDate);
      assert.strictEqual(row.sessionCount, count, isoDate);
    }
  }

  {
    const { rows, aug13PlusCounts } = buildTruncationFixture();

    const narrowFirst = buildPayloadFromRows(rows, '2026-08-13', '2026-08-13');
    assert.strictEqual(dateRow(narrowFirst, '2026-08-13').status, 'has_sessions');
    assert.strictEqual(dateRow(narrowFirst, '2026-08-13').sessionCount, 50);

    const interior = buildPayloadFromRows(rows, '2026-08-10', '2026-08-16');
    assert.strictEqual(dateRow(interior, '2026-08-13').status, 'has_sessions');
    assert.strictEqual(dateRow(interior, '2026-08-13').sessionCount, 50);
    assert.strictEqual(dateRow(interior, '2026-08-16').sessionCount, aug13PlusCounts['2026-08-16']);

    const widePaginated = buildPayloadFromRows(
      (await paginateFixtureRows(rows)).rows,
      '2026-07-25',
      '2026-08-21',
      { queryComplete: true, rowCountScanned: 1482 },
    );
    assert.strictEqual(dateRow(widePaginated, '2026-08-13').status, 'has_sessions');
    assert.strictEqual(dateRow(widePaginated, '2026-08-13').sessionCount, 50);
  }

  {
    const makeRows = (count) => {
      const out = [];
      for (let i = 0; i < count; i += 1) {
        out.push(makeCoverageRow('2026-08-01', `sess-${String(i).padStart(6, '0')}`));
      }
      return out;
    };

    const onePage = await paginateFixtureRows(makeRows(500), 500);
    assert.strictEqual(onePage.complete, true);
    assert.strictEqual(onePage.rowCountScanned, 500);

    const fullPlusOne = await paginateFixtureRows(makeRows(501), 500);
    assert.strictEqual(fullPlusOne.complete, true);
    assert.strictEqual(fullPlusOne.rowCountScanned, 501);

    const threePages = await paginateFixtureRows(makeRows(1500), 500);
    assert.strictEqual(threePages.complete, true);
    assert.strictEqual(threePages.rowCountScanned, 1500);

    const partialFinal = await paginateFixtureRows(makeRows(750), 500);
    assert.strictEqual(partialFinal.complete, true);
    assert.strictEqual(partialFinal.rowCountScanned, 750);

    const zeroRows = await paginateFixtureRows([], 500);
    assert.strictEqual(zeroRows.complete, true);
    assert.strictEqual(zeroRows.rowCountScanned, 0);
    assert.strictEqual(zeroRows.rows.length, 0);
  }

  {
    const rows = [
      makeCoverageRow('2026-08-13', 'dup-key'),
      makeCoverageRow('2026-08-13', 'dup-key'),
      makeCoverageRow('2026-08-14', 'other-key'),
    ];
    const shuffled = [rows[2], rows[0], rows[1]];
    const pageResult = await paginateFixtureRows(shuffled, 2);
    assert.strictEqual(pageResult.rows.length, 2);
    const payload = buildPayloadFromRows(pageResult.rows, '2026-08-13', '2026-08-14');
    assert.strictEqual(dateRow(payload, '2026-08-13').sessionCount, 1);
    assert.strictEqual(dateRow(payload, '2026-08-14').sessionCount, 1);

    const ordered = await paginateFixtureRows(rows, 500);
    const orderedPayload = buildPayloadFromRows(ordered.rows, '2026-08-13', '2026-08-14');
    assert.deepStrictEqual(
      orderedPayload.dates.map((entry) => [entry.isoDate, entry.sessionCount]),
      payload.dates.map((entry) => [entry.isoDate, entry.sessionCount]),
    );
  }

  {
    const { rows } = buildTruncationFixture();
    const truncated = rows.slice(0, 1000);
    const persisted = new Set([
      '2026-08-13', '2026-08-14', '2026-08-15', '2026-08-16', '2026-08-17',
      '2026-08-18', '2026-08-19', '2026-08-20', '2026-08-21',
    ]);

    const incompletePayload = buildPayloadFromRows(
      truncated,
      '2026-07-25',
      '2026-08-21',
      { queryComplete: false, rowCountScanned: 1000, persistedCheckedSet: persisted },
    );
    assert.strictEqual(incompletePayload.complete, false);
    for (const isoDate of persisted) {
      const row = dateRow(incompletePayload, isoDate);
      assert.ok(row, isoDate);
      assert.notStrictEqual(row.status, 'checked_empty', isoDate);
      assert.strictEqual(row.status, 'not_checked', isoDate);
    }

    const buggyCompletePayload = buildPayloadFromRows(
      truncated,
      '2026-07-25',
      '2026-08-21',
      { queryComplete: true, rowCountScanned: 1000, persistedCheckedSet: persisted },
    );
    assert.strictEqual(dateRow(buggyCompletePayload, '2026-08-13').status, 'checked_empty');
  }

  {
    const { rows, aug13PlusCounts } = buildTruncationFixture();
    const payload = buildPayloadFromRows(
      (await paginateFixtureRows(rows)).rows,
      '2026-07-25',
      '2026-08-21',
    );
    const expectedDates = listIsoDates('2026-07-25', '2026-08-21');
    assert.strictEqual(payload.dates.length, expectedDates.length);
    for (const isoDate of expectedDates) {
      assert.ok(dateRow(payload, isoDate), isoDate);
    }
    for (const isoDate of Object.keys(aug13PlusCounts)) {
      assert.strictEqual(dateRow(payload, isoDate).status, 'has_sessions', isoDate);
    }
  }

  {
    let byDate = {};
    byDate = cov.mergeSelectedDateCoverage(byDate, '2026-08-13', {
      statusReason: 'saved_sessions_found',
      sessionsCount: 50,
      lastCheckedAt: '2026-07-25T12:00:00.000Z',
    });
    assert.strictEqual(byDate['2026-08-13'].status, 'has_sessions');
    assert.strictEqual(byDate['2026-08-13'].sessionCount, 50);

    const incompleteByDate = {
      '2026-08-13': { isoDate: '2026-08-13', status: 'has_sessions', sessionCount: 50 },
    };
    const downgraded = {
      '2026-08-13': { isoDate: '2026-08-13', status: 'not_checked', sessionCount: null },
    };
    const preserved = cov.mergeCoverageByDatePreservingConfirmed(incompleteByDate, downgraded);
    assert.strictEqual(preserved['2026-08-13'].status, 'has_sessions');
  }

  {
    const incompleteResult = await cov.fetchAllCoverageRowsPaginated(async ({ offset, limit }) => {
      const { rows } = buildTruncationFixture();
      return rows.slice(offset, offset + limit);
    }, { pageSize: 500, maxRows: 1000 });
    assert.strictEqual(incompleteResult.complete, false);
    assert.strictEqual(incompleteResult.rowCountScanned, 1000);
  }

  console.log('session date coverage truncation regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
