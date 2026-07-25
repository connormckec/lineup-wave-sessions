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

function mergeProfileAuthFetchOptions(profileCode, options = {}) {
  const auth = buildProfileAuthHeaders(profileCode, options.headers || {});
  if (!auth.ok) {
    return { ok: false, error: auth.error, fetchOptions: null };
  }
  const { headers: _ignoredHeaders, ...rest } = options;
  return {
    ok: true,
    fetchOptions: {
      ...rest,
      headers: {
        ...(options.headers || {}),
        ...auth.headers,
      },
    },
  };
}

function formatNotifyTestError(status, errorCode, { network = false } = {}) {
  if (network) {
    return 'Could not reach the server — check your connection and try again.';
  }
  switch (errorCode) {
    case 'profile_code_missing':
    case 'profile_auth_required':
    case 'invalid_profile_secret':
      return formatProfileAuthError(errorCode);
    case 'notification_topic_not_configured':
      return 'Save your ntfy topic in Settings first.';
    case 'test_rate_limited':
      return 'Test notification cooldown — wait a minute and try again.';
    case 'notify_failed':
    case 'provider_transport_failed':
    case 'provider_timeout':
    case 'provider_dns_failed':
    case 'provider_tls_failed':
    case 'provider_http_error':
      return 'Notification provider could not deliver the test — try again shortly.';
    default:
      if (status === 401) return formatProfileAuthError('profile_auth_required');
      if (status === 429) return 'Test notification cooldown — wait a minute and try again.';
      if (status === 502) return 'Notification provider could not deliver the test — try again shortly.';
      if (status === 400) return 'Save your ntfy topic in Settings first.';
      return 'Test notification failed — try again.';
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
  mergeProfileAuthFetchOptions,
  formatProfileAuthError,
  formatNotifyTestError,
  buildProfileAuthDiagnostic,
  maskNtfyTopicForDisplay,
  normalizeProfileCode: profileAuth.normalizeProfileCode,
  deriveUserKeyFromProfileCodeSync: profileAuth.deriveUserKeyFromProfileCodeSync,
};
