'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const marketObservations = require('../lib/market-observations');

function verifiedSession(overrides = {}) {
  return {
    key: '1785758400_1',
    ts: 1785758400,
    isoDate: '2026-08-03',
    dateKey: '2026-08-03',
    time: '8:00 am',
    weekday: 'Monday',
    waveSide: 'Left Wave',
    wave: 1,
    level: 'Expert Turns',
    available: true,
    slots: 8,
    capacity: 12,
    detailVerified: true,
    detailConfidence: 'exact_match',
    detailStatus: 'checked_with_slots',
    modalValidation: {
      match: true,
      confidence: 'exact_match',
      parsedFromModal: {
        isoDate: '2026-08-03',
        startTime: '8:00 am',
        sessionType: 'Expert Turns',
        waveSide: 'Left Wave',
      },
      mismatches: [],
    },
    detailRawText: 'New booking: Aug. 3, 2026 at 8:00 am Left Wave Sessions Expert Turns EXPERT TURNS SURF SESSION $ 145.00',
    priceText: '$145.00',
    priceMin: 145,
    priceMax: 145,
    currency: 'USD',
    lastDetailedCheckAt: '2026-07-26T17:00:00.000Z',
    ...overrides,
  };
}

function trustedInventorySession(overrides = {}) {
  return verifiedSession({
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 6,
    threshold_scanned_at: '2026-07-26T17:00:00.000Z',
    lastBasicCheckAt: '2026-07-26T17:00:00.000Z',
    ...overrides,
  });
}

function mockSupabase() {
  const observations = [];
  const products = [];
  return {
    observations,
    products,
    client: {
      from(table) {
        if (table === 'session_market_observations') {
          return {
            insert(row) {
              return {
                select() {
                  return {
                    maybeSingle: async () => {
                      const existing = observations.find((o) => o.dedupe_key === row.dedupe_key);
                      if (existing) {
                        return { data: null, error: { code: '23505', message: 'duplicate' } };
                      }
                      const record = { id: `obs-${observations.length + 1}`, ...row };
                      observations.push(record);
                      return { data: record, error: null };
                    },
                  };
                },
              };
            },
          };
        }
        if (table === 'session_product_price_observations') {
          return {
            insert: async (rows) => {
              for (const row of rows) products.push(row);
              return { error: null };
            },
          };
        }
        throw new Error(`unexpected table ${table}`);
      },
    },
  };
}

async function runTests() {
  marketObservations.resetObservationMemoryForTests();
  const migrationSql = fs.readFileSync(
    path.join(__dirname, '../supabase/migrations/202607261730_session_market_observations.sql'),
    'utf8',
  );

  const observedAt = new Date('2026-07-26T17:00:00.000Z');

  // 1. Inventory 30 minutes old is fresh.
  {
    const inv = marketObservations.extractTrustedInventoryFromSession(trustedInventorySession({
      threshold_scanned_at: '2026-07-26T16:30:00.000Z',
    }));
    assert.strictEqual(
      marketObservations.classifyInventoryFreshness(inv, new Date('2026-07-26T17:00:00.000Z')),
      'fresh',
    );
  }

  // 2. Inventory 61 minutes old is stale.
  {
    const inv = marketObservations.extractTrustedInventoryFromSession(trustedInventorySession({
      threshold_scanned_at: '2026-07-25T15:59:00.000Z',
    }));
    assert.strictEqual(
      marketObservations.classifyInventoryFreshness(inv, new Date('2026-07-26T17:00:00.000Z')),
      'stale',
    );
  }

  // 3. Missing threshold timestamp is missing.
  {
    const inv = marketObservations.extractTrustedInventoryFromSession(verifiedSession({
      threshold_scan_verified: false,
      available_entries: 6,
      threshold_scanned_at: null,
    }));
    assert.strictEqual(
      marketObservations.classifyInventoryFreshness(inv, observedAt),
      'missing',
    );
  }

  // 4. Delta is calculated between two fresh trusted observations.
  {
    const prior = trustedInventorySession({
      available_entries: 8,
      threshold_scanned_at: '2026-07-26T16:45:00.000Z',
    });
    const next = trustedInventorySession({
      available_entries: 5,
      threshold_scanned_at: '2026-07-26T16:55:00.000Z',
    });
    const built = marketObservations.buildSessionMarketObservationRow({
      session: next,
      previousSession: prior,
      observedAt: new Date('2026-07-26T17:00:00.000Z'),
      freshPriceFromCurrentRun: false,
    });
    assert.strictEqual(built.row.observed_net_booking_delta, 3);
    assert.strictEqual(built.row.inventory_freshness, 'fresh');
  }

  // 5. Delta is not calculated when either observation is stale.
  {
    const prior = trustedInventorySession({
      available_entries: 8,
      threshold_scanned_at: '2026-07-25T10:00:00.000Z',
    });
    const next = trustedInventorySession({
      available_entries: 5,
      threshold_scanned_at: '2026-07-26T16:55:00.000Z',
    });
    const built = marketObservations.buildSessionMarketObservationRow({
      session: next,
      previousSession: prior,
      observedAt: new Date('2026-07-26T17:00:00.000Z'),
    });
    assert.strictEqual(built.row.observed_net_booking_delta, null);
  }

  // 6. Old verified price remains last-known but is marked stale.
  {
    marketObservations.resetObservationMemoryForTests();
    const mock = mockSupabase();
    const s = verifiedSession({ lastDetailedCheckAt: '2026-07-20T10:00:00.000Z' });
    const freshBuilt = marketObservations.buildSessionMarketObservationRow({
      session: s,
      observedAt: new Date('2026-07-20T10:00:00.000Z'),
      freshPriceFromCurrentRun: true,
    });
    await marketObservations.insertMarketObservation(mock.client, freshBuilt, s, { freshPriceFromCurrentRun: true });

    const thresholdOnly = trustedInventorySession({ lastDetailedCheckAt: '2026-07-20T10:00:00.000Z' });
    const staleBuilt = marketObservations.buildSessionMarketObservationRow({
      session: thresholdOnly,
      observedAt: new Date('2026-07-26T17:00:00.000Z'),
      freshPriceFromCurrentRun: false,
    });
    assert.strictEqual(staleBuilt.row.price_freshness, 'stale');
    assert.strictEqual(staleBuilt.row.price_verified_at, '2026-07-20T10:00:00.000Z');
    assert.strictEqual(staleBuilt.row.price_exact_cents, 14500);
  }

  // 7. Old price is not described as newly observed or unchanged.
  {
    marketObservations.resetObservationMemoryForTests();
    const mock = mockSupabase();
    const prior = verifiedSession({
      key: '1785758400_7',
      lastDetailedCheckAt: '2026-07-20T10:00:00.000Z',
    });
    await marketObservations.insertMarketObservation(
      mock.client,
      marketObservations.buildSessionMarketObservationRow({
        session: prior,
        observedAt: new Date('2026-07-20T10:00:00.000Z'),
        freshPriceFromCurrentRun: true,
      }),
      prior,
      { freshPriceFromCurrentRun: true },
    );
    const built = marketObservations.buildSessionMarketObservationRow({
      session: trustedInventorySession({
        key: '1785758400_7',
        lastDetailedCheckAt: '2026-07-20T10:00:00.000Z',
      }),
      observedAt: new Date('2026-07-26T17:00:00.000Z'),
      freshPriceFromCurrentRun: false,
    });
    assert.strictEqual(built.row.price_freshness, 'stale');
    assert.strictEqual(built.row.price_changed_during_interval, false);
    assert.strictEqual(built.row.raw_evidence.priceReobserved, false);
    assert.ok(!built.row.observation_reason.includes('price_change'));
  }

  // 8. Failed current modal does not refresh price_verified_at.
  {
    marketObservations.resetObservationMemoryForTests();
    const mock = mockSupabase();
    const prior = verifiedSession({ lastDetailedCheckAt: '2026-07-20T10:00:00.000Z' });
    await marketObservations.insertMarketObservation(
      mock.client,
      marketObservations.buildSessionMarketObservationRow({
        session: prior,
        observedAt: new Date('2026-07-20T10:00:00.000Z'),
        freshPriceFromCurrentRun: true,
      }),
      prior,
      { freshPriceFromCurrentRun: true },
    );
    const failed = verifiedSession({
      detailVerified: false,
      detailConfidence: 'mismatch',
      detailStatus: 'failed_modal',
      modalValidation: { match: false, confidence: 'mismatch', mismatches: [{ field: 'waveSide' }] },
      lastDetailedCheckAt: '2026-07-26T17:00:00.000Z',
    });
    const resolved = marketObservations.resolvePriceEvidenceForObservation(failed, { freshPriceFromCurrentRun: true });
    assert.notStrictEqual(resolved.evidence.priceVerifiedAt, '2026-07-26T17:00:00.000Z');
    assert.strictEqual(resolved.freshness, 'stale');
    assert.strictEqual(resolved.evidence.priceVerifiedAt, '2026-07-20T10:00:00.000Z');
  }

  // 9. Failed current modal creates no product-price rows.
  {
    const failed = verifiedSession({
      key: '1785758400_9',
      detailVerified: false,
      detailConfidence: 'mismatch',
      detailStatus: 'failed_modal',
      detailRawText: 'NOVICE GROUP LESSON $ 125.00 NOVICE SESSION WITH PRIVATE COACH $ 209.00',
      modalValidation: { match: false, confidence: 'mismatch', mismatches: [{ field: 'waveSide' }] },
    });
    const built = marketObservations.buildSessionMarketObservationRow({
      session: failed,
      freshPriceFromCurrentRun: true,
    });
    assert.ok(built.record);
    assert.strictEqual(built.productRows.length, 0);
  }

  // 10. Newly exact-matched modal creates fresh price evidence.
  {
    const built = marketObservations.buildSessionMarketObservationRow({
      session: verifiedSession({ key: '1785758400_10' }),
      freshPriceFromCurrentRun: true,
    });
    assert.ok(built.record);
    assert.strictEqual(built.row.price_freshness, 'fresh');
    assert.strictEqual(built.row.price_exact_cents, 14500);
    assert.strictEqual(built.row.raw_evidence.priceReobserved, true);
  }

  // 11. Only fresh modal evidence can set price_changed.
  {
    marketObservations.resetObservationMemoryForTests();
    const mock = mockSupabase();
    const s1 = verifiedSession();
    await marketObservations.insertMarketObservation(
      mock.client,
      marketObservations.buildSessionMarketObservationRow({ session: s1, freshPriceFromCurrentRun: true }),
      s1,
      { freshPriceFromCurrentRun: true },
    );
    const staleOnly = trustedInventorySession({ lastDetailedCheckAt: '2026-07-26T17:00:00.000Z' });
    const staleBuilt = marketObservations.buildSessionMarketObservationRow({
      session: staleOnly,
      observedAt: new Date('2026-07-26T17:30:00.000Z'),
      freshPriceFromCurrentRun: false,
    });
    assert.strictEqual(staleBuilt.row.price_changed_during_interval, false);
    assert.ok(!staleBuilt.row.observation_reason.includes('price_change'));

    const s2 = verifiedSession({
      detailRawText: 'Left Wave Sessions Expert Turns $ 155.00',
      priceText: '$155.00',
      priceMin: 155,
      priceMax: 155,
      lastDetailedCheckAt: '2026-07-26T18:00:00.000Z',
    });
    const freshChange = marketObservations.buildSessionMarketObservationRow({
      session: s2,
      previousSession: s1,
      observedAt: new Date('2026-07-26T18:00:00.000Z'),
      freshPriceFromCurrentRun: true,
    });
    assert.strictEqual(freshChange.row.price_freshness, 'fresh');
    assert.strictEqual(freshChange.row.price_changed_during_interval, true);
    assert.ok(freshChange.row.observation_reason.includes('price_change'));
  }

  // 12. Same non-null run ID pairs Left and Right.
  {
    const diffs = marketObservations.pairLeftRightObservations([
      {
        session_key: '1785758400_1', observed_at: '2026-07-26T17:00:00.000Z', wave_side: 'Left Wave',
        price_exact_cents: 14500, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-1',
      },
      {
        session_key: '1785758400_2', observed_at: '2026-07-26T17:02:00.000Z', wave_side: 'Right Wave',
        price_exact_cents: 15000, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-1',
      },
    ]);
    assert.strictEqual(diffs.length, 1);
    assert.strictEqual(diffs[0].pairConfidence, 'exact_run_match');
  }

  // 13. Different non-null run IDs do not pair.
  {
    const diffs = marketObservations.pairLeftRightObservations([
      {
        session_key: '1785758400_1', observed_at: '2026-07-26T17:00:00.000Z', wave_side: 'Left Wave',
        price_exact_cents: 14500, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-a',
      },
      {
        session_key: '1785758400_2', observed_at: '2026-07-26T17:02:00.000Z', wave_side: 'Right Wave',
        price_exact_cents: 15000, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-b',
      },
    ]);
    assert.strictEqual(diffs.length, 0);
  }

  // 14. Both-null run IDs may use the bounded fallback.
  {
    const diffs = marketObservations.pairLeftRightObservations([
      {
        session_key: '1785758400_1', observed_at: '2026-07-26T17:00:00.000Z', wave_side: 'Left Wave',
        price_exact_cents: 14500, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: null,
      },
      {
        session_key: '1785758400_2', observed_at: '2026-07-26T17:20:00.000Z', wave_side: 'Right Wave',
        price_exact_cents: 15000, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: null,
      },
    ], { toleranceMs: 30 * 60 * 1000 });
    assert.strictEqual(diffs.length, 1);
    assert.strictEqual(diffs[0].pairConfidence, 'fallback_time_tolerance');
  }

  // 15. Fallback pairing reports lower confidence.
  {
    const diffs = marketObservations.pairLeftRightObservations([
      {
        session_key: '1785758400_1', observed_at: '2026-07-26T17:00:00.000Z', wave_side: 'Left Wave',
        price_exact_cents: 14500, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-exact',
      },
      {
        session_key: '1785758400_2', observed_at: '2026-07-26T17:02:00.000Z', wave_side: 'Right Wave',
        price_exact_cents: 15000, iso_date: '2026-08-03', session_start_at: '2026-08-03T12:00:00.000Z',
        session_type: 'Expert Turns', observation_run_id: 'run-exact',
      },
    ]);
    assert.strictEqual(diffs[0].pairConfidence, 'exact_run_match');
    assert.notStrictEqual(diffs[0].pairConfidence, 'fallback_time_tolerance');
  }

  // Modal verification hardening (retained).
  {
    const s = verifiedSession({
      waveSide: 'Right Wave',
      wave: 2,
      key: '1785758400_2',
      modalValidation: {
        match: false,
        confidence: 'mismatch',
        parsedFromModal: { waveSide: 'Left Wave Lessons', isoDate: '2026-08-03', startTime: '8:00 am', sessionType: 'Expert Turns' },
        mismatches: [{ field: 'waveSide', expected: 'Right Wave', found: 'Left Wave Lessons' }],
      },
      detailConfidence: 'mismatch',
      detailVerified: false,
    });
    assert.strictEqual(marketObservations.isPriceVerificationTrusted(s), false);
    const built = marketObservations.buildSessionMarketObservationRow({ session: s, freshPriceFromCurrentRun: true });
    assert.strictEqual(built.row.price_display_text, null);
    assert.strictEqual(built.productRows.length, 0);
  }

  // Stale threshold inventory row metadata.
  {
    const s = trustedInventorySession({ threshold_scanned_at: '2026-07-25T10:00:00.000Z' });
    const built = marketObservations.buildSessionMarketObservationRow({
      session: s,
      observedAt: new Date('2026-07-26T17:00:00.000Z'),
    });
    assert.strictEqual(built.row.inventory_freshness, 'stale');
    assert.strictEqual(built.row.raw_evidence.inventoryFreshnessCutoffMinutes, 60);
  }

  // Env override bounded for inventory freshness.
  {
    const prev = process.env.MARKET_INVENTORY_FRESHNESS_MINUTES;
    process.env.MARKET_INVENTORY_FRESHNESS_MINUTES = '9999';
    assert.strictEqual(marketObservations.resolveInventoryFreshnessMinutes(), 60);
    process.env.MARKET_INVENTORY_FRESHNESS_MINUTES = '90';
    assert.strictEqual(marketObservations.resolveInventoryFreshnessMinutes(), 90);
    process.env.MARKET_INVENTORY_FRESHNESS_MINUTES = prev;
  }

  // State dedupe: unchanged retry suppressed across minute buckets.
  {
    marketObservations.resetObservationMemoryForTests();
    const s = trustedInventorySession();
    const mock = mockSupabase();
    const first = marketObservations.buildSessionMarketObservationRow({
      session: s,
      observedAt: new Date('2026-07-26T18:00:00.000Z'),
    });
    await marketObservations.insertMarketObservation(mock.client, first, s);
    const retry = marketObservations.buildSessionMarketObservationRow({
      session: s,
      observedAt: new Date('2026-07-26T18:05:00.000Z'),
    });
    assert.strictEqual(retry.record, false);
  }

  // Six-hour heartbeat remains bounded.
  {
    marketObservations.resetObservationMemoryForTests();
    const s = verifiedSession({ ts: 1785153600, isoDate: '2026-07-27', key: '1785153600_1' });
    await marketObservations.insertMarketObservation(
      mockSupabase().client,
      marketObservations.buildSessionMarketObservationRow({ session: s, observedAt: new Date('2026-07-26T10:00:00.000Z') }),
      s,
    );
    assert.strictEqual(
      marketObservations.buildSessionMarketObservationRow({ session: s, observedAt: new Date('2026-07-26T12:00:00.000Z') }).record,
      false,
    );
    const heartbeat = marketObservations.buildSessionMarketObservationRow({ session: s, observedAt: new Date('2026-07-26T16:30:00.000Z') });
    assert.strictEqual(heartbeat.record, true);
    assert.ok(heartbeat.policy.reasons.includes('heartbeat'));
  }

  // 16. RLS, dedupe, modal, notification-related invariants retained.
  assert.match(migrationSql, /session_market_observations enable row level security/i);
  assert.match(migrationSql, /session_product_price_observations enable row level security/i);
  assert.match(migrationSql, /revoke all on table session_market_observations from public, anon, authenticated/i);
  assert.match(migrationSql, /grant all on table session_market_observations to service_role/i);
  assert.match(migrationSql, /price_freshness text/i);

  {
    const sanitized = marketObservations.sanitizeDiagnosticsObservation({
      session_key: '1785758400_1',
      raw_evidence: { rawModalExcerpt: 'secret modal text' },
    });
    assert.strictEqual(sanitized.raw_evidence, undefined);
  }

  {
    const prev = process.env.MARKET_OBSERVATIONS_ENABLED;
    process.env.MARKET_OBSERVATIONS_ENABLED = 'false';
    assert.strictEqual(marketObservations.isMarketObservationsEnabled(), false);
    const summary = await marketObservations.recordMarketObservationsForWrites(mockSupabase().client, {
      updatedSessions: [verifiedSession()],
      writeSucceeded: true,
      enabled: false,
    });
    assert.strictEqual(summary.skippedDisabled, true);
    process.env.MARKET_OBSERVATIONS_ENABLED = prev;
  }

  {
    const failingClient = {
      from() {
        return {
          insert() {
            return { select() { return { maybeSingle: async () => ({ data: null, error: { message: 'insert failed' } }) }; } };
          },
        };
      },
    };
    const built = marketObservations.buildSessionMarketObservationRow({ session: verifiedSession(), freshPriceFromCurrentRun: true });
    const result = await marketObservations.insertMarketObservation(failingClient, built, verifiedSession());
    assert.strictEqual(result.inserted, false);
    assert.ok(result.error);
  }

  // Parser coverage retained.
  {
    const parsed = marketObservations.parseExactPriceFromText('$145.00');
    assert.strictEqual(parsed.priceExactCents, 14500);
    const products = marketObservations.parseProductsFromModalText('NOVICE GROUP LESSON $ 125.00 NOVICE SESSION WITH PRIVATE COACH $ 209.00');
    assert.strictEqual(products.products.length, 2);
    const multiBuilt = marketObservations.buildSessionMarketObservationRow({
      session: verifiedSession({
        detailRawText: 'NOVICE GROUP LESSON $ 125.00 NOVICE SESSION WITH PRIVATE COACH $ 209.00',
      }),
      freshPriceFromCurrentRun: true,
    });
    assert.ok(multiBuilt.productRows.length >= 2);
  }

  console.log('market-observations regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
