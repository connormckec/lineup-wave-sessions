'use strict';

const assert = require('assert');
const adminAuth = require('../lib/admin-auth');
const publicSessionEnrich = require('../lib/public-session-enrich');

function mockRes() {
  const res = {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
  return res;
}

function requireAdminTokenMiddleware(configuredToken) {
  return (req, res, next) => {
    const result = adminAuth.validateAdminToken(
      adminAuth.extractAdminTokenFromRequest(req),
      configuredToken,
    );
    if (!result.ok) {
      return res.status(result.status).json({ ok: false, error: result.error });
    }
    return next();
  };
}

function runAdminAuthTests() {
  console.log('admin auth regression');

  {
    const result = adminAuth.validateAdminToken('', 'secret-token');
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 401);
    assert.strictEqual(result.error, 'unauthorized');
  }

  {
    const result = adminAuth.validateAdminToken('wrong', 'secret-token');
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 401);
  }

  {
    const result = adminAuth.validateAdminToken('secret-token', 'secret-token');
    assert.strictEqual(result.ok, true);
  }

  {
    const result = adminAuth.validateAdminToken('secret-token', '');
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 503);
    assert.strictEqual(result.error, 'admin_token_not_configured');
  }

  {
    const middleware = requireAdminTokenMiddleware('secret-token');
    const req = { headers: {} };
    const res = mockRes();
    let called = false;
    middleware(req, res, () => { called = true; });
    assert.strictEqual(called, false);
    assert.strictEqual(res.statusCode, 401);
    assert.strictEqual(res.body.error, 'unauthorized');
  }

  {
    const middleware = requireAdminTokenMiddleware('secret-token');
    const req = { headers: { 'x-admin-token': 'secret-token' } };
    const res = mockRes();
    let called = false;
    middleware(req, res, () => { called = true; });
    assert.strictEqual(called, true);
  }

  {
    const middleware = requireAdminTokenMiddleware('secret-token');
    const req = { headers: { authorization: 'Bearer secret-token' } };
    const res = mockRes();
    let called = false;
    middleware(req, res, () => { called = true; });
    assert.strictEqual(called, true);
  }
}

function runPublicEnrichTests() {
  console.log('public session enrich regression');

  {
    const bad = publicSessionEnrich.validatePublicEnrichDateBody({ isoDate: '2026-13-40' });
    assert.strictEqual(bad.ok, false);
    assert.strictEqual(bad.error, 'invalid_isoDate');
  }

  {
    const bad = publicSessionEnrich.validatePublicEnrichDateBody({ isoDate: '2026-07-25', mode: 'failed_first' });
    assert.strictEqual(bad.ok, false);
    assert.strictEqual(bad.error, 'only_isoDate_allowed');
  }

  {
    const bad = publicSessionEnrich.validatePublicEnrichDateBody({ url: 'https://evil.example' });
    assert.strictEqual(bad.ok, false);
    assert.strictEqual(bad.error, 'only_isoDate_allowed');
  }

  {
    const ok = publicSessionEnrich.validatePublicEnrichDateBody({ isoDate: '2026-07-25' });
    assert.strictEqual(ok.ok, true);
    assert.strictEqual(ok.isoDate, '2026-07-25');
  }

  {
    const inHorizon = publicSessionEnrich.isIsoDateWithinHorizon('2026-07-25', {
      todayIso: '2026-07-01',
      maxIso: '2026-08-01',
    });
    assert.strictEqual(inHorizon, true);
    const outHorizon = publicSessionEnrich.isIsoDateWithinHorizon('2025-01-01', {
      todayIso: '2026-07-01',
      maxIso: '2026-08-01',
    });
    assert.strictEqual(outHorizon, false);
  }

  {
    const state = publicSessionEnrich.createPublicEnrichRateLimiter();
    const ip = '203.0.113.10';
    let limited = false;
    for (let i = 0; i < publicSessionEnrich.PUBLIC_ENRICH_IP_MAX; i += 1) {
      limited = publicSessionEnrich.recordPublicEnrichIpHit(state, ip, { now: 1_000_000 });
    }
    assert.strictEqual(limited, false);
    limited = publicSessionEnrich.recordPublicEnrichIpHit(state, ip, { now: 1_000_001 });
    assert.strictEqual(limited, true);
  }

  {
    const state = publicSessionEnrich.createPublicEnrichRateLimiter();
    const now = 2_000_000;
    publicSessionEnrich.markPublicEnrichDateCooldown(state, '2026-07-25', { now });
    const cooling = publicSessionEnrich.resolvePublicEnrichStatus({
      isoDate: '2026-07-25',
      state,
      openSessionCount: 3,
      now,
    });
    assert.strictEqual(cooling.status, 'already_running');
  }

  {
    const recentSessions = [
      { available: true, lastDetailedCheckAt: new Date(Date.now() - 60_000).toISOString() },
    ];
    assert.strictEqual(publicSessionEnrich.sessionsRecentlyDetailed(recentSessions), true);
    const outcome = publicSessionEnrich.resolvePublicEnrichStatus({
      isoDate: '2026-07-25',
      state: publicSessionEnrich.createPublicEnrichRateLimiter(),
      openSessionCount: 1,
      allRecentlyDetailed: true,
    });
    assert.strictEqual(outcome.status, 'recently_checked');
  }

  {
    const outcome = publicSessionEnrich.resolvePublicEnrichStatus({
      isoDate: '2026-07-25',
      state: publicSessionEnrich.createPublicEnrichRateLimiter(),
      openSessionCount: 2,
      allRecentlyDetailed: false,
    });
    assert.strictEqual(outcome.status, 'accepted');
    assert.strictEqual(outcome.httpStatus, 202);
  }

  const adminOnlyFields = ['mode', 'write', 'dryRun', 'jobId', 'adminOverride', 'selector', 'url'];
  for (const field of adminOnlyFields) {
    const rejected = publicSessionEnrich.validatePublicEnrichDateBody({ isoDate: '2026-07-25', [field]: 'x' });
    assert.strictEqual(rejected.ok, false, `expected ${field} to be rejected`);
    assert.strictEqual(rejected.error, 'only_isoDate_allowed');
  }
}

runAdminAuthTests();
runPublicEnrichTests();
console.log('route/auth regression: all tests passed');
