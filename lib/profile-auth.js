'use strict';

const crypto = require('crypto');

function normalizeProfileCode(code) {
  return String(code || '').trim().toLowerCase().replace(/\s+/g, '-');
}

function deriveUserKeyFromProfileCodeSync(code) {
  const normalized = normalizeProfileCode(code);
  if (!normalized) return null;
  const hex = crypto.createHash('sha256')
    .update(`lineup-profile-v1:${normalized}`)
    .digest('hex')
    .slice(0, 32);
  return `profile:${hex}`;
}

async function deriveUserKeyFromProfileCode(code) {
  return deriveUserKeyFromProfileCodeSync(code);
}

function extractProfileSecretFromRequest(req = {}) {
  const headers = req.headers || {};
  const authHeader = String(headers.authorization || '').trim();
  if (/^Bearer\s+/i.test(authHeader)) {
    return authHeader.replace(/^Bearer\s+/i, '').trim();
  }
  return '';
}

async function resolveProfileFromRequest(req) {
  const secret = extractProfileSecretFromRequest(req);
  if (!secret) {
    return { ok: false, status: 401, error: 'profile_auth_required' };
  }
  const userKey = await deriveUserKeyFromProfileCode(secret);
  if (!userKey) {
    return { ok: false, status: 401, error: 'invalid_profile_secret' };
  }
  return {
    ok: true,
    userKey,
    normalizedCode: normalizeProfileCode(secret),
  };
}

function createRequireProfileAuth() {
  return async function requireProfileAuth(req, res, next) {
    try {
      const result = await resolveProfileFromRequest(req);
      if (!result.ok) {
        return res.status(result.status).json({ ok: false, error: result.error });
      }
      req.profileAuth = {
        userKey: result.userKey,
        normalizedCode: result.normalizedCode,
      };
      return next();
    } catch (err) {
      return res.status(500).json({ ok: false, error: 'profile_auth_failed' });
    }
  };
}

module.exports = {
  normalizeProfileCode,
  deriveUserKeyFromProfileCode,
  deriveUserKeyFromProfileCodeSync,
  extractProfileSecretFromRequest,
  resolveProfileFromRequest,
  createRequireProfileAuth,
};
