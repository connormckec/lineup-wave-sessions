'use strict';

const profileAuth = require('./profile-auth');
const notificationTopic = require('./notification-topic');

function buildProfileAuthHeaders(profileCode, extraHeaders = {}) {
  const normalized = profileAuth.normalizeProfileCode(profileCode);
  if (!normalized) {
    return {
      ok: false,
      error: 'profile_code_missing',
      headers: { ...extraHeaders },
    };
  }
  return {
    ok: true,
    headers: {
      ...extraHeaders,
      Authorization: `Bearer ${normalized}`,
    },
  };
}

function formatProfileAuthError(code) {
  switch (code) {
    case 'profile_code_missing':
      return 'Save a profile sync code in Settings first.';
    case 'profile_auth_required':
      return 'Profile authentication required — save your sync code in Settings and try again.';
    case 'invalid_profile_secret':
      return 'Profile sync code is invalid — check Settings and save again.';
    default:
      return 'Could not authenticate your profile — save your sync code in Settings.';
  }
}

function buildProfileAuthDiagnostic(profileCode) {
  const normalized = profileAuth.normalizeProfileCode(profileCode);
  const userKey = normalized
    ? profileAuth.deriveUserKeyFromProfileCodeSync(normalized)
    : null;
  const auth = buildProfileAuthHeaders(normalized);
  return {
    profileCodePresent: !!normalized,
    profileCodeLength: normalized ? normalized.length : 0,
    derivedUserKeyPrefix: userKey ? `${userKey.slice(0, 16)}…` : null,
    authHeaderPrepared: auth.ok,
  };
}

function maskNtfyTopicForDisplay(topic) {
  const trimmed = String(topic || '').trim();
  if (!trimmed) return null;
  return notificationTopic.maskDestination(trimmed);
}

module.exports = {
  buildProfileAuthHeaders,
  formatProfileAuthError,
  buildProfileAuthDiagnostic,
  maskNtfyTopicForDisplay,
  normalizeProfileCode: profileAuth.normalizeProfileCode,
  deriveUserKeyFromProfileCodeSync: profileAuth.deriveUserKeyFromProfileCodeSync,
};
