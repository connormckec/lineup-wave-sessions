'use strict';

function extractAdminTokenFromRequest(req = {}) {
  const headers = req.headers || {};
  const headerToken = String(headers['x-admin-token'] || '').trim();
  const authHeader = String(headers.authorization || '').trim();
  const bearer = authHeader.replace(/^Bearer\s+/i, '').trim();
  return headerToken || bearer || '';
}

function validateAdminToken(provided, configuredToken) {
  const expected = String(configuredToken || '').trim();
  if (!expected) {
    return { ok: false, status: 503, error: 'admin_token_not_configured' };
  }
  const token = String(provided || '').trim();
  if (!token || token !== expected) {
    return { ok: false, status: 401, error: 'unauthorized' };
  }
  return { ok: true };
}

module.exports = {
  extractAdminTokenFromRequest,
  validateAdminToken,
};
