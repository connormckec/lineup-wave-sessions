'use strict';

const crypto = require('crypto');
const trustedState = require('./trusted-session-state');
const thresholdMaintenance = require('./threshold-maintenance');

const HEARTBEAT_MS = 6 * 60 * 60 * 1000;
const NEAR_TERM_HEARTBEAT_HOURS = 7 * 24;
const adaptiveSchedule = require('./adaptive-threshold-schedule');
const DEFAULT_INVENTORY_FRESHNESS_MINUTES = 60;
const MIN_INVENTORY_FRESHNESS_MINUTES = 5;
const MAX_INVENTORY_FRESHNESS_MINUTES = 240;
const LEFT_RIGHT_PAIR_TOLERANCE_MS = 30 * 60 * 1000;
const BOOKING_TZ = 'America/New_York';

const lastObservationAtByKey = new Map();
const lastObservationSnapshotByKey = new Map();
const lastValidPriceSnapshotByKey = new Map();

function resolveInventoryFreshnessMinutes() {
  const raw = process.env.MARKET_INVENTORY_FRESHNESS_MINUTES;
  if (raw == null || String(raw).trim() === '') return DEFAULT_INVENTORY_FRESHNESS_MINUTES;
  const n = parseInt(String(raw).trim(), 10);
  if (!Number.isFinite(n) || n < MIN_INVENTORY_FRESHNESS_MINUTES || n > MAX_INVENTORY_FRESHNESS_MINUTES) {
    return DEFAULT_INVENTORY_FRESHNESS_MINUTES;
  }
  return n;
}

function getInventoryFreshnessCutoffMs() {
  return resolveInventoryFreshnessMinutes() * 60 * 1000;
}

function isMarketObservationsEnabled() {
  return process.env.MARKET_OBSERVATIONS_ENABLED === 'true';
}

function resetObservationMemoryForTests() {
  lastObservationAtByKey.clear();
  lastObservationSnapshotByKey.clear();
  lastValidPriceSnapshotByKey.clear();
}

function dollarsToCents(value) {
  if (value == null || !Number.isFinite(Number(value))) return null;
  return Math.round(Number(value) * 100);
}

function slugifyProductKey(label, index = 0) {
  const base = String(label || `product_${index}`)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);
  return base || `product_${index}`;
}

function parseExactPriceFromText(text) {
  if (!text) return {};
  const currency = 'USD';
  const rangeMatch = text.match(/\$\s*([\d,]+(?:\.\d{2})?)\s*[–\-]\s*\$\s*([\d,]+(?:\.\d{2})?)/);
  if (rangeMatch) {
    const min = parseFloat(rangeMatch[1].replace(/,/g, ''));
    const max = parseFloat(rangeMatch[2].replace(/,/g, ''));
    return {
      priceDisplayText: `$${rangeMatch[1].replace(/,/g, '')}–$${rangeMatch[2].replace(/,/g, '')}`,
      priceMinCents: dollarsToCents(min),
      priceMaxCents: dollarsToCents(max),
      priceExactCents: null,
      currency,
      confidence: 'medium_range',
      kind: 'range',
    };
  }
  const allMatches = [...text.matchAll(/\$\s*([\d,]+(?:\.\d{2})?)/g)];
  if (allMatches.length > 1) {
    const cents = allMatches.map((m) => dollarsToCents(parseFloat(m[1].replace(/,/g, ''))));
    const min = Math.min(...cents.filter((v) => v != null));
    const max = Math.max(...cents.filter((v) => v != null));
    return {
      priceDisplayText: allMatches.map((m) => `$${m[1].replace(/,/g, '')}`).join(' / '),
      priceMinCents: min,
      priceMaxCents: max,
      priceExactCents: null,
      currency,
      confidence: 'high_multi_product',
      kind: 'multi',
    };
  }
  const singleMatch = text.match(/\$\s*([\d,]+(?:\.\d{2})?)/);
  if (singleMatch) {
    const v = parseFloat(singleMatch[1].replace(/,/g, ''));
    const cents = dollarsToCents(v);
    return {
      priceDisplayText: `$${singleMatch[1].replace(/,/g, '')}`,
      priceExactCents: cents,
      priceMinCents: cents,
      priceMaxCents: cents,
      currency,
      confidence: 'high_exact',
      kind: 'exact',
    };
  }
  return {
    priceDisplayText: null,
    priceExactCents: null,
    priceMinCents: null,
    priceMaxCents: null,
    currency,
    confidence: 'none',
    kind: 'none',
  };
}

function parseProductsFromModalText(text) {
  if (!text || /no products are available/i.test(text)) {
    return { available: false, products: [], confidence: 'high_unavailable' };
  }
  const products = [];
  const priceRe = /\$\s*([\d,]+(?:\.\d{2})?)/g;
  let match;
  let index = 0;
  while ((match = priceRe.exec(text)) !== null) {
    const before = text.slice(Math.max(0, match.index - 220), match.index);
    const labelMatch = before.match(/([A-Z][A-Z0-9\s\-\/\(\)]{3,90})\s*$/);
    const productLabel = labelMatch ? labelMatch[1].trim() : null;
    const priceCents = dollarsToCents(parseFloat(match[1].replace(/,/g, '')));
    const rawText = text.slice(Math.max(0, match.index - 90), match.index + match[0].length).trim();
    products.push({
      productKey: slugifyProductKey(productLabel, index),
      productLabel,
      priceCents,
      originalPriceCents: null,
      available: true,
      rawText,
      confidence: productLabel ? 'high_labeled' : 'medium_unlabeled',
    });
    index += 1;
  }
  const deduped = [];
  const seen = new Set();
  for (const p of products) {
    const key = `${p.productKey}|${p.priceCents}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(p);
  }
  return {
    available: deduped.length > 0,
    products: deduped,
    confidence: deduped.length ? 'high_modal' : 'none',
  };
}

function modalValidationOnSession(session) {
  return session?.modalValidation || session?.raw?.modalValidation || null;
}

function isPriceExactMatchNow(session) {
  if (!session) return false;
  if (session.detailVerified !== true && session?.raw?.detailVerified !== true) return false;
  const confidence = session.detailConfidence || session?.raw?.detailConfidence || null;
  const validation = modalValidationOnSession(session);
  const exactMatch = confidence === 'exact_match' || validation?.confidence === 'exact_match';
  if (!exactMatch) return false;
  if (validation?.match === false) return false;
  const detailStatus = session.detailStatus || session.detail_status || session?.raw?.detailStatus || null;
  if (/failed_modal|failed_tile|mismatch|stale|weak_match/i.test(String(detailStatus || ''))) return false;
  const verifiedAt = session.lastDetailedCheckAt || session.last_detailed_check_at || null;
  return !!verifiedAt;
}

function isPriceVerificationTrusted(session) {
  return isPriceExactMatchNow(session);
}

function buildLastKnownPriceEvidence(snapshot) {
  if (!snapshot?.priceParse?.priceDisplayText) return null;
  return {
    priceSource: snapshot.priceSource || 'last_known_verified',
    priceVerifiedAt: snapshot.priceVerifiedAt || null,
    priceParse: { ...snapshot.priceParse },
    products: Array.isArray(snapshot.products) ? snapshot.products.map((p) => ({ ...p })) : [],
    productsAvailable: snapshot.productsAvailable ?? null,
    confidence: 'last_known_stale',
    trusted: false,
    reobserved: false,
  };
}

function extractVerificationDiagnostics(session) {
  const validation = modalValidationOnSession(session);
  const rawModal = session?.detailRawText || session?.raw?.detailRawText || null;
  return {
    intendedSessionKey: session?.key || null,
    intendedStartTs: session?.ts ?? session?.start_ts ?? null,
    intendedWaveColumn: session?.wave ?? null,
    intendedWaveSide: session?.waveSide || session?.wave_side || null,
    intendedSessionType: session?.level || session?.session_type || null,
    modalHeadingSide: validation?.parsedFromModal?.waveSide || null,
    modalDate: validation?.parsedFromModal?.isoDate || null,
    modalTime: validation?.parsedFromModal?.startTime || null,
    modalSessionType: validation?.parsedFromModal?.sessionType || null,
    verificationResult: validation?.confidence || (session?.detailVerified ? 'verified_without_validation' : 'unverified'),
    verificationMismatches: validation?.mismatches || [],
    detailStatus: session?.detailStatus || session?.detail_status || null,
    rawModalExcerpt: rawModal ? rawModal.slice(0, 500) : null,
  };
}

function extractFreshPriceEvidenceFromSession(session) {
  const rawModal = session?.detailRawText || session?.raw?.detailRawText || null;
  const rawTile = session?.detailRawTileText || session?.raw?.detailRawTileText || session?.tileText || session?.raw?.tileText || null;
  const verification = extractVerificationDiagnostics(session);
  if (!isPriceExactMatchNow(session) || !rawModal) {
    return null;
  }
  const productParse = parseProductsFromModalText(rawModal);
  const priceParse = productParse.products.length > 1
    ? {
      priceDisplayText: productParse.products.map((p) => `$${(p.priceCents / 100).toFixed(2)}`).join(' / '),
      priceExactCents: null,
      priceMinCents: Math.min(...productParse.products.map((p) => p.priceCents)),
      priceMaxCents: Math.max(...productParse.products.map((p) => p.priceCents)),
      currency: 'USD',
      confidence: 'high_multi_product',
      kind: 'multi_product',
    }
    : (productParse.products.length === 1
      ? {
        priceDisplayText: `$${(productParse.products[0].priceCents / 100).toFixed(2)}`,
        priceExactCents: productParse.products[0].priceCents,
        priceMinCents: productParse.products[0].priceCents,
        priceMaxCents: productParse.products[0].priceCents,
        currency: 'USD',
        confidence: 'high_exact',
        kind: 'exact',
      }
      : parseExactPriceFromText(rawModal));

  return {
    priceSource: 'modal.innerText',
    priceVerifiedAt: session.lastDetailedCheckAt || session.last_detailed_check_at || null,
    priceParse,
    products: productParse.products,
    productsAvailable: productParse.available,
    rawModal: rawModal.slice(0, 1500),
    rawTile: rawTile ? rawTile.slice(0, 800) : null,
    confidence: productParse.confidence,
    verification,
    trusted: true,
    reobserved: true,
  };
}

function resolvePriceEvidenceForObservation(session, {
  freshPriceFromCurrentRun = false,
  lastKnownSnapshot = null,
} = {}) {
  if (freshPriceFromCurrentRun) {
    const fresh = extractFreshPriceEvidenceFromSession(session);
    if (fresh) {
      return { evidence: fresh, freshness: 'fresh' };
    }
  }
  const known = lastKnownSnapshot || lastValidPriceSnapshotByKey.get(session?.key) || null;
  if (known) {
    const staleEvidence = buildLastKnownPriceEvidence(known);
    if (staleEvidence) {
      return { evidence: staleEvidence, freshness: 'stale' };
    }
  }
  return {
    evidence: {
      priceSource: null,
      priceVerifiedAt: null,
      priceParse: parseExactPriceFromText(''),
      products: [],
      productsAvailable: null,
      rawModal: null,
      rawTile: null,
      confidence: 'none',
      verification: extractVerificationDiagnostics(session),
      trusted: false,
      reobserved: false,
    },
    freshness: 'missing',
  };
}

function extractPriceEvidenceFromSession(session, { forObservation = false, freshPriceFromCurrentRun = false } = {}) {
  const resolved = resolvePriceEvidenceForObservation(session, { freshPriceFromCurrentRun });
  if (forObservation) return { ...resolved.evidence, priceFreshness: resolved.freshness };
  return resolved.evidence;
}

function classifyInventoryFreshness(inventory, observedAt = new Date()) {
  if (!inventory?.trusted || inventory.trustedSpotsRemaining == null) return 'missing';
  if (!inventory.thresholdScannedAt) return 'missing';
  const ageMs = observedAt.getTime() - new Date(inventory.thresholdScannedAt).getTime();
  if (!Number.isFinite(ageMs) || ageMs < 0) return 'missing';
  return ageMs <= getInventoryFreshnessCutoffMs() ? 'fresh' : 'stale';
}

function extractTrustedInventoryFromSession(session) {
  const trusted = trustedState.extractTrustedAvailabilityState(session);
  if (trusted.trusted && thresholdMaintenance.isThresholdSlotsTrusted(session)) {
    return {
      trustedSpotsRemaining: trusted.slots,
      availabilitySource: 'entries_left_threshold_scan',
      thresholdScannedAt: trusted.thresholdScannedAt,
      available: trusted.available,
      trusted: true,
    };
  }
  return {
    trustedSpotsRemaining: null,
    availabilitySource: null,
    thresholdScannedAt: trusted.thresholdScannedAt || null,
    available: session?.available ?? null,
    trusted: false,
  };
}

function buildProductFingerprint(products = []) {
  return products
    .map((p) => `${p.productKey}:${p.priceCents}:${p.available === false ? 0 : 1}`)
    .sort()
    .join('|');
}

function buildPriceFingerprint(priceParse = {}) {
  return [
    priceParse.priceDisplayText || '',
    priceParse.priceExactCents == null ? 'null' : String(priceParse.priceExactCents),
    priceParse.priceMinCents == null ? 'null' : String(priceParse.priceMinCents),
    priceParse.priceMaxCents == null ? 'null' : String(priceParse.priceMaxCents),
  ].join('|');
}

function buildAvailabilityFingerprint(inventory = {}, inventoryFreshness = 'missing') {
  return [
    inventory.available === true ? '1' : inventory.available === false ? '0' : 'null',
    inventory.trustedSpotsRemaining == null ? 'null' : String(inventory.trustedSpotsRemaining),
    inventory.availabilitySource || '',
    inventory.thresholdScannedAt || '',
    inventoryFreshness,
  ].join('|');
}

function buildMetadataFingerprint(session = {}) {
  return [
    session.isoDate || session.dateKey || '',
    session.time || '',
    session.waveSide || session.wave_side || '',
    session.level || session.session_type || '',
  ].join('|');
}

function buildObservationSnapshot(session, {
  observedAt = new Date(),
  freshPriceFromCurrentRun = false,
} = {}) {
  const scheduleScannedAt = session?.lastBasicCheckAt || session?.last_basic_check_at || session?.lastScraped || null;
  const resolvedPrice = resolvePriceEvidenceForObservation(session, { freshPriceFromCurrentRun });
  const priceEvidence = resolvedPrice.evidence;
  const priceFreshness = resolvedPrice.freshness;
  const inventory = extractTrustedInventoryFromSession(session);
  const inventoryFreshness = classifyInventoryFreshness(inventory, observedAt);
  return {
    sessionKey: session.key,
    isoDate: session.isoDate || session.dateKey || null,
    waveSide: session.waveSide || session.wave_side || null,
    sessionType: session.level || session.session_type || null,
    startTs: session.ts ?? session.start_ts ?? null,
    scheduleScannedAt,
    priceEvidence,
    priceFreshness,
    inventory,
    inventoryFreshness,
    priceFingerprint: priceFreshness === 'fresh'
      ? buildPriceFingerprint(priceEvidence.priceParse)
      : 'not_fresh',
    productFingerprint: priceFreshness === 'fresh'
      ? buildProductFingerprint(priceEvidence.products)
      : 'not_fresh',
    availabilityFingerprint: buildAvailabilityFingerprint(inventory, inventoryFreshness),
    metadataFingerprint: buildMetadataFingerprint(session),
  };
}

function computeHoursUntilSession(session, observedAt = new Date()) {
  const ts = session?.ts ?? session?.start_ts;
  if (ts == null) return null;
  return Number(((Number(ts) * 1000 - observedAt.getTime()) / 3600000).toFixed(2));
}

function computeObservedNetBookingDelta(previousTrusted, currentTrusted, {
  previousFreshness = 'missing',
  currentFreshness = 'missing',
} = {}) {
  if (previousTrusted == null || currentTrusted == null) return null;
  if (previousFreshness !== 'fresh' || currentFreshness !== 'fresh') return null;
  return previousTrusted - currentTrusted;
}

function observationTimeBucket(observedAt, { heartbeat = false, heartbeatIntervalMs = HEARTBEAT_MS } = {}) {
  const ms = observedAt instanceof Date ? observedAt.getTime() : new Date(observedAt).getTime();
  if (heartbeat) return Math.floor(ms / heartbeatIntervalMs);
  return 'state';
}

function buildObservationDedupeKey({
  park = 'atlantic_park',
  sessionKey,
  snapshot,
  observedAt,
  heartbeat = false,
  heartbeatIntervalMs = HEARTBEAT_MS,
}) {
  const canonical = [
    park,
    sessionKey,
    snapshot.priceFingerprint,
    snapshot.productFingerprint,
    snapshot.availabilityFingerprint,
    observationTimeBucket(observedAt, { heartbeat, heartbeatIntervalMs }),
  ].join('|');
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function shouldRecordObservation({
  previousSnapshot,
  currentSnapshot,
  lastObservationAt,
  observedAt = new Date(),
  hoursUntilSession,
}) {
  const reasons = [];
  if (!previousSnapshot) reasons.push('session_discovered');
  if (
    previousSnapshot
    && currentSnapshot.priceFreshness === 'fresh'
    && previousSnapshot.priceFreshness === 'fresh'
    && previousSnapshot.priceFingerprint !== currentSnapshot.priceFingerprint
  ) {
    reasons.push('price_change');
  }
  if (
    previousSnapshot
    && currentSnapshot.priceFreshness === 'fresh'
    && previousSnapshot.priceFreshness === 'fresh'
    && previousSnapshot.productFingerprint !== currentSnapshot.productFingerprint
  ) {
    reasons.push('product_set_change');
  }
  if (previousSnapshot && previousSnapshot.availabilityFingerprint !== currentSnapshot.availabilityFingerprint) {
    if (previousSnapshot.inventory.available !== currentSnapshot.inventory.available) {
      reasons.push('availability_change');
    }
    if (previousSnapshot.inventory.trustedSpotsRemaining !== currentSnapshot.inventory.trustedSpotsRemaining) {
      reasons.push('inventory_change');
    }
  }
  if (previousSnapshot && previousSnapshot.metadataFingerprint !== currentSnapshot.metadataFingerprint) {
    reasons.push('metadata_change');
  }

  if (reasons.length) {
    return { record: true, reasons, heartbeat: false };
  }

  if (hoursUntilSession != null && hoursUntilSession >= 0) {
    const heartbeatIntervalMs = adaptiveSchedule.resolveHeartbeatIntervalMs({ hoursUntilStart: hoursUntilSession });
    const lastAtMs = lastObservationAt ? new Date(lastObservationAt).getTime() : 0;
    if (observedAt.getTime() - lastAtMs >= heartbeatIntervalMs) {
      return {
        record: true,
        reasons: ['heartbeat'],
        heartbeat: true,
        heartbeatIntervalMs,
        heartbeatTargetMinutes: adaptiveSchedule.resolveHeartbeatIntervalMinutes({ hoursUntilStart: hoursUntilSession }),
      };
    }
  }

  return { record: false, reasons: [], heartbeat: false };
}

function buildPriceRowFields(priceEvidence, priceFreshness) {
  if (priceFreshness === 'missing') {
    return {
      price_display_text: null,
      price_exact_cents: null,
      price_min_cents: null,
      price_max_cents: null,
      currency: null,
      price_source: null,
      price_verified_at: null,
    };
  }
  return {
    price_display_text: priceEvidence.priceParse.priceDisplayText,
    price_exact_cents: priceFreshness === 'fresh' && priceEvidence.products.length <= 1
      ? priceEvidence.priceParse.priceExactCents
      : (priceFreshness === 'stale' ? priceEvidence.priceParse.priceExactCents : null),
    price_min_cents: priceEvidence.products.length > 1
      ? priceEvidence.priceParse.priceMinCents
      : (priceEvidence.priceParse.kind === 'range'
        ? priceEvidence.priceParse.priceMinCents
        : priceEvidence.priceParse.priceExactCents),
    price_max_cents: priceEvidence.products.length > 1
      ? priceEvidence.priceParse.priceMaxCents
      : (priceEvidence.priceParse.kind === 'range'
        ? priceEvidence.priceParse.priceMaxCents
        : priceEvidence.priceParse.priceExactCents),
    currency: priceEvidence.priceParse.currency || 'USD',
    price_source: priceEvidence.priceSource,
    price_verified_at: priceEvidence.priceVerifiedAt,
  };
}

function buildSessionMarketObservationRow({
  park = 'atlantic_park',
  session,
  previousSession = null,
  observedAt = new Date(),
  sourceJobId = null,
  observationRunId = null,
  freshPriceFromCurrentRun = false,
} = {}) {
  const currentSnapshot = buildObservationSnapshot(session, { observedAt, freshPriceFromCurrentRun });
  const previousSnapshot = lastObservationSnapshotByKey.get(session.key)
    || (previousSession
      ? buildObservationSnapshot(previousSession, { observedAt, freshPriceFromCurrentRun: false })
      : null);
  const lastObsAt = lastObservationAtByKey.get(session.key) || null;
  const hoursUntilSession = computeHoursUntilSession(session, observedAt);
  const policy = shouldRecordObservation({
    previousSnapshot,
    currentSnapshot,
    lastObservationAt: lastObsAt,
    observedAt,
    hoursUntilSession,
  });

  if (!policy.record) {
    return { record: false, reason: 'dedupe_unchanged', policy };
  }

  const dedupeKey = buildObservationDedupeKey({
    park,
    sessionKey: session.key,
    snapshot: currentSnapshot,
    observedAt,
    heartbeat: policy.heartbeat,
    heartbeatIntervalMs: policy.heartbeatIntervalMs || HEARTBEAT_MS,
  });

  const { priceEvidence, inventory, inventoryFreshness, scheduleScannedAt, priceFreshness } = currentSnapshot;
  const prevInventory = previousSnapshot?.inventory || null;
  const prevPriceText = previousSnapshot?.priceFreshness === 'fresh'
    ? previousSnapshot.priceEvidence?.priceParse?.priceDisplayText || null
    : (lastValidPriceSnapshotByKey.get(session.key)?.priceParse?.priceDisplayText || null);
  const intervalSeconds = lastObsAt
    ? Number(((observedAt.getTime() - new Date(lastObsAt).getTime()) / 1000).toFixed(1))
    : null;
  const netDelta = computeObservedNetBookingDelta(
    prevInventory?.trustedSpotsRemaining ?? null,
    inventory.trustedSpotsRemaining,
    {
      previousFreshness: previousSnapshot?.inventoryFreshness || 'missing',
      currentFreshness: inventoryFreshness,
    },
  );
  const priceChanged = (
    currentSnapshot.priceFreshness === 'fresh'
    && previousSnapshot?.priceFreshness === 'fresh'
    && previousSnapshot.priceFingerprint !== currentSnapshot.priceFingerprint
  );

  const startTs = session.ts ?? session.start_ts;
  const priceFields = buildPriceRowFields(priceEvidence, priceFreshness);
  const row = {
    park,
    session_key: session.key,
    observed_at: observedAt.toISOString(),
    session_start_at: startTs != null ? new Date(Number(startTs) * 1000).toISOString() : null,
    iso_date: currentSnapshot.isoDate,
    wave_side: currentSnapshot.waveSide,
    session_type: currentSnapshot.sessionType,
    available: inventory.available,
    trusted_spots_remaining: inventory.trustedSpotsRemaining,
    availability_source: inventory.availabilitySource,
    threshold_scanned_at: inventory.thresholdScannedAt,
    schedule_scanned_at: scheduleScannedAt,
    inventory_freshness: inventoryFreshness,
    price_freshness: priceFreshness,
    ...priceFields,
    hours_until_session: hoursUntilSession,
    source_job_id: sourceJobId,
    observation_run_id: observationRunId || sourceJobId || null,
    observation_reason: policy.reasons.join(','),
    raw_evidence: {
      priceConfidence: priceEvidence.confidence,
      priceKind: priceEvidence.priceParse.kind,
      productCount: priceEvidence.products.length,
      priceIncluded: priceFreshness !== 'missing',
      priceReobserved: priceFreshness === 'fresh',
      priceFreshness,
      verification: priceEvidence.verification,
      metadataFingerprint: currentSnapshot.metadataFingerprint,
      availabilityFingerprint: currentSnapshot.availabilityFingerprint,
      priceFingerprint: currentSnapshot.priceFingerprint,
      productFingerprint: currentSnapshot.productFingerprint,
      inventoryFreshness,
      inventoryFreshnessCutoffMinutes: resolveInventoryFreshnessMinutes(),
      scheduleScannedAt,
      previousThresholdScannedAt: prevInventory?.thresholdScannedAt ?? null,
    },
    dedupe_key: dedupeKey,
    observed_net_booking_delta: netDelta,
    observation_interval_seconds: intervalSeconds,
    price_changed_during_interval: priceChanged,
    previous_price_display_text: prevPriceText,
    previous_trusted_spots_remaining: prevInventory?.trustedSpotsRemaining ?? null,
  };

  const productRows = priceFreshness === 'fresh' && priceEvidence.products.length > 1
    ? priceEvidence.products.map((product, index) => ({
      session_key: session.key,
      product_key: product.productKey || slugifyProductKey(product.productLabel, index),
      product_label: product.productLabel,
      price_cents: product.priceCents,
      original_price_cents: product.originalPriceCents,
      available: product.available !== false,
      raw_text: product.rawText,
      observed_at: observedAt.toISOString(),
      source: priceEvidence.priceSource,
      confidence: product.confidence,
      dedupe_key: crypto.createHash('sha256').update(`${dedupeKey}|${product.productKey}|${product.priceCents}`).digest('hex'),
    }))
    : [];

  return {
    record: true,
    row,
    productRows,
    policy,
    dedupeKey,
  };
}

function rememberObservationMemory(session, built, { freshPriceFromCurrentRun = false } = {}) {
  if (!built?.record || !built.row?.session_key) return;
  lastObservationAtByKey.set(built.row.session_key, built.row.observed_at);
  if (!session) return;
  const snap = buildObservationSnapshot(session, {
    observedAt: new Date(built.row.observed_at),
    freshPriceFromCurrentRun,
  });
  lastObservationSnapshotByKey.set(built.row.session_key, snap);
  if (snap.priceFreshness === 'fresh') {
    lastValidPriceSnapshotByKey.set(built.row.session_key, {
      ...snap.priceEvidence,
      priceVerifiedAt: snap.priceEvidence.priceVerifiedAt,
    });
  }
}

async function insertMarketObservation(supabase, built, sessionForMemory = null, { freshPriceFromCurrentRun = false } = {}) {
  if (!built?.record || !supabase) {
    return { inserted: false, suppressed: false, skipped: true };
  }
  try {
    const { data, error } = await supabase
      .from('session_market_observations')
      .insert(built.row)
      .select('id')
      .maybeSingle();
    if (error) {
      if (error.code === '23505') {
        return { inserted: false, suppressed: true, dedupeKey: built.dedupeKey };
      }
      throw error;
    }
    if (built.productRows.length > 0 && data?.id) {
      const products = built.productRows.map((p) => ({ ...p, observation_id: data.id }));
      const { error: productError } = await supabase
        .from('session_product_price_observations')
        .insert(products);
      if (productError && productError.code !== '23505') {
        console.error('[market-observations] product insert failed:', productError.message);
      }
    }
    rememberObservationMemory(sessionForMemory, built, { freshPriceFromCurrentRun });
    return { inserted: true, observationId: data?.id, dedupeKey: built.dedupeKey };
  } catch (err) {
    console.error('[market-observations] insert failed:', built.row.session_key, err.message);
    return { inserted: false, error: err.message, dedupeKey: built.dedupeKey };
  }
}

async function recordMarketObservationsForWrites(supabase, {
  previousByKey = new Map(),
  updatedSessions = [],
  writeSucceeded = false,
  sourceJobId = null,
  observationRunId = null,
  freshPriceFromCurrentRun = false,
  park = 'atlantic_park',
  enabled = isMarketObservationsEnabled(),
} = {}) {
  const summary = {
    attempted: 0,
    inserted: 0,
    suppressed: 0,
    skippedUnchanged: 0,
    skippedDisabled: !enabled,
    errors: [],
  };
  if (!enabled || !writeSucceeded || !supabase || !updatedSessions.length) return summary;

  for (const session of updatedSessions) {
    if (!session?.key) continue;
    summary.attempted += 1;
    const built = buildSessionMarketObservationRow({
      park,
      session,
      previousSession: previousByKey.get(session.key) || null,
      sourceJobId,
      observationRunId: observationRunId || sourceJobId || null,
      freshPriceFromCurrentRun,
    });
    if (!built.record) {
      summary.skippedUnchanged += 1;
      continue;
    }
    const result = await insertMarketObservation(supabase, built, session, { freshPriceFromCurrentRun });
    if (result.inserted) summary.inserted += 1;
    else if (result.suppressed) summary.suppressed += 1;
    else if (result.error) summary.errors.push({ sessionKey: session.key, error: result.error });
  }
  return summary;
}

function sanitizeDiagnosticsObservation(row) {
  if (!row) return row;
  const copy = { ...row };
  delete copy.raw_evidence;
  return copy;
}

function isLeftWaveSide(waveSide) {
  return /left/i.test(waveSide || '') && !/lesson/i.test(waveSide || '');
}

function isRightWaveSide(waveSide) {
  return /right/i.test(waveSide || '') && !/lesson/i.test(waveSide || '');
}

function pairLeftRightObservations(rows, { toleranceMs = LEFT_RIGHT_PAIR_TOLERANCE_MS, limit = 25 } = {}) {
  const byStart = new Map();
  for (const row of rows || []) {
    const startKey = `${row.iso_date}|${row.session_start_at}|${row.session_type}`;
    if (!byStart.has(startKey)) byStart.set(startKey, []);
    byStart.get(startKey).push(row);
  }

  const diffs = [];
  for (const group of byStart.values()) {
    const leftCandidates = group.filter((r) => isLeftWaveSide(r.wave_side));
    const rightCandidates = group.filter((r) => isRightWaveSide(r.wave_side));
    for (const left of leftCandidates) {
      const leftRunId = left.observation_run_id || left.source_job_id || null;
      let right = null;
      let pairConfidence = null;

      if (leftRunId) {
        right = rightCandidates.find((r) => (r.observation_run_id || r.source_job_id) === leftRunId) || null;
        if (right) pairConfidence = 'exact_run_match';
      } else {
        const leftAt = new Date(left.observed_at).getTime();
        const candidates = rightCandidates
          .filter((r) => !(r.observation_run_id || r.source_job_id))
          .map((r) => ({ r, deltaMs: Math.abs(new Date(r.observed_at).getTime() - leftAt) }))
          .filter((x) => x.deltaMs <= toleranceMs)
          .sort((a, b) => a.deltaMs - b.deltaMs);
        if (candidates.length) {
          right = candidates[0].r;
          pairConfidence = 'fallback_time_tolerance';
        }
      }

      if (!right) continue;
      const rightRunId = right.observation_run_id || right.source_job_id || null;
      if (leftRunId && rightRunId && leftRunId !== rightRunId) continue;

      const leftPrice = left.price_exact_cents ?? left.price_min_cents;
      const rightPrice = right.price_exact_cents ?? right.price_min_cents;
      if (leftPrice == null || rightPrice == null) continue;

      diffs.push({
        isoDate: left.iso_date,
        sessionStartAt: left.session_start_at,
        sessionType: left.session_type,
        leftSessionKey: left.session_key,
        rightSessionKey: right.session_key,
        leftPriceCents: leftPrice,
        rightPriceCents: rightPrice,
        deltaCents: leftPrice - rightPrice,
        leftObservedAt: left.observed_at,
        rightObservedAt: right.observed_at,
        observationRunId: leftRunId || rightRunId || null,
        pairConfidence,
        pairToleranceMs: toleranceMs,
      });
      if (diffs.length >= limit) return diffs;
    }
  }
  return diffs;
}

async function fetchMarketObservationDiagnostics(supabase, { limit = 50 } = {}) {
  const out = {
    latestBySession: [],
    recentPriceChanges: [],
    sessionsWithPriceRanges: [],
    missingTrustedInventory: [],
    parserFailures: [],
    verificationFailures: [],
    duplicateSuppressionCounts: { suppressed: 0 },
    inventoryFreshnessCutoffMinutes: resolveInventoryFreshnessMinutes(),
  };
  if (!supabase) return out;

  const { data: latestRows, error: latestError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, price_display_text, price_exact_cents, price_min_cents, price_max_cents, trusted_spots_remaining, observation_reason, wave_side, session_type, iso_date, session_start_at, inventory_freshness, price_freshness, schedule_scanned_at, price_verified_at, threshold_scanned_at, observation_run_id, source_job_id')
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (latestError) throw latestError;
  out.latestBySession = (latestRows || []).map(sanitizeDiagnosticsObservation);

  const { data: priceChanges, error: priceChangeError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, price_display_text, previous_price_display_text, observation_reason, hours_until_session, price_freshness')
    .eq('price_changed_during_interval', true)
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (priceChangeError) throw priceChangeError;
  out.recentPriceChanges = priceChanges || [];

  const { data: rangeRows, error: rangeError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, price_display_text, price_min_cents, price_max_cents, price_freshness')
    .not('price_min_cents', 'is', null)
    .not('price_max_cents', 'is', null)
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (rangeError) throw rangeError;
  out.sessionsWithPriceRanges = (rangeRows || []).filter((r) => r.price_min_cents !== r.price_max_cents);

  const { data: missingInv, error: missingInvError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, available, trusted_spots_remaining, availability_source, inventory_freshness')
    .is('trusted_spots_remaining', null)
    .eq('available', true)
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (missingInvError) throw missingInvError;
  out.missingTrustedInventory = missingInv || [];

  const { data: lowConf, error: lowConfError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, price_display_text, price_source, observation_reason, price_freshness')
    .or('price_source.is.null,and(price_display_text.is.null,observation_reason.ilike.%price_change%)')
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (lowConfError) throw lowConfError;
  out.parserFailures = lowConf || [];

  const { data: verifyFails, error: verifyError } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, observation_reason, price_freshness')
    .eq('price_freshness', 'missing')
    .order('observed_at', { ascending: false })
    .limit(limit);
  if (!verifyError) {
    out.verificationFailures = verifyFails || [];
  }

  return out;
}

async function fetchLeftRightPairedPriceDiffs(supabase, { limit = 25 } = {}) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from('session_market_observations')
    .select('session_key, observed_at, wave_side, price_exact_cents, price_min_cents, price_max_cents, iso_date, session_start_at, session_type, observation_run_id, source_job_id')
    .order('observed_at', { ascending: false })
    .limit(1000);
  if (error) throw error;
  return pairLeftRightObservations(data, { limit });
}

function buildOperationalStatus({
  schemaAvailable = false,
  sessions = [],
  enrichmentMetrics = {},
} = {}) {
  const enabled = isMarketObservationsEnabled();
  let latestVerifiedPriceAt = null;
  let latestTrustedInventoryAt = null;
  let sessionsMissingFreshInventory = 0;
  let verificationFailures = 0;

  for (const s of sessions) {
    const verifiedAt = s.lastDetailedCheckAt || s.last_detailed_check_at;
    if (isPriceExactMatchNow(s) && verifiedAt) {
      if (!latestVerifiedPriceAt || verifiedAt > latestVerifiedPriceAt) latestVerifiedPriceAt = verifiedAt;
    } else if (/failed_modal|failed_tile|mismatch|stale/i.test(String(s.detailStatus || s.detail_status || ''))) {
      verificationFailures += 1;
    }
    const thresholdAt = s.threshold_scanned_at || s.thresholdScanAt || s.threshold_scan_at;
    if (thresholdAt && (!latestTrustedInventoryAt || thresholdAt > latestTrustedInventoryAt)) {
      latestTrustedInventoryAt = thresholdAt;
    }
    const inv = extractTrustedInventoryFromSession(s);
    if (classifyInventoryFreshness(inv) !== 'fresh') sessionsMissingFreshInventory += 1;
  }

  return {
    schemaAvailable,
    observationWritesEnabled: enabled,
    detailEnrichmentHealthy: !enrichmentMetrics.lastDetailEnrichmentError,
    latestVerifiedPriceAt,
    latestTrustedInventoryAt,
    sessionsMissingFreshInventory,
    tileModalVerificationFailures: verificationFailures,
    inventoryFreshnessCutoffMinutes: resolveInventoryFreshnessMinutes(),
    leftRightPairToleranceMinutes: LEFT_RIGHT_PAIR_TOLERANCE_MS / 60000,
  };
}

module.exports = {
  HEARTBEAT_MS,
  NEAR_TERM_HEARTBEAT_HOURS,
  DEFAULT_INVENTORY_FRESHNESS_MINUTES,
  MIN_INVENTORY_FRESHNESS_MINUTES,
  MAX_INVENTORY_FRESHNESS_MINUTES,
  LEFT_RIGHT_PAIR_TOLERANCE_MS,
  BOOKING_TZ,
  resolveInventoryFreshnessMinutes,
  getInventoryFreshnessCutoffMs,
  isMarketObservationsEnabled,
  resetObservationMemoryForTests,
  rememberObservationMemoryForTests: rememberObservationMemory,
  dollarsToCents,
  parseExactPriceFromText,
  parseProductsFromModalText,
  isPriceExactMatchNow,
  isPriceVerificationTrusted,
  extractVerificationDiagnostics,
  extractPriceEvidenceFromSession,
  extractTrustedInventoryFromSession,
  classifyInventoryFreshness,
  resolvePriceEvidenceForObservation,
  buildObservationSnapshot,
  buildObservationDedupeKey,
  shouldRecordObservation,
  buildSessionMarketObservationRow,
  insertMarketObservation,
  recordMarketObservationsForWrites,
  computeObservedNetBookingDelta,
  computeHoursUntilSession,
  sanitizeDiagnosticsObservation,
  fetchMarketObservationDiagnostics,
  fetchLeftRightPairedPriceDiffs,
  pairLeftRightObservations,
  buildOperationalStatus,
};
