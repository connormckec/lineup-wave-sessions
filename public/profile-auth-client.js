/* Browser profile auth helpers — logic must match lib/profile-auth-client.js */
(function (global) {
  'use strict';

  function normalizeProfileCode(code) {
    return String(code || '').trim().toLowerCase().replace(/\s+/g, '-');
  }

  function buildProfileAuthHeaders(profileCode, extraHeaders) {
    const extra = extraHeaders || {};
    const normalized = normalizeProfileCode(profileCode);
    if (!normalized) {
      return { ok: false, error: 'profile_code_missing', headers: { ...extra } };
    }
    return {
      ok: true,
      headers: {
        ...extra,
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

  function maskNtfyTopicForDisplay(topic) {
    const trimmed = String(topic || '').trim();
    if (!trimmed) return null;
    if (trimmed.length <= 4) return '••••';
    return `${trimmed.slice(0, 2)}••••${trimmed.slice(-2)}`;
  }

  async function deriveUserKeyFromProfileCodeAsync(code) {
    const normalized = normalizeProfileCode(code);
    if (!normalized) return null;
    try {
      const bytes = new TextEncoder().encode(`lineup-profile-v1:${normalized}`);
      const buf = await crypto.subtle.digest('SHA-256', bytes);
      const hex = [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
      return `profile:${hex.slice(0, 32)}`;
    } catch {
      return null;
    }
  }

  function mergeProfileAuthFetchOptions(profileCode, options) {
    const opts = options || {};
    const auth = buildProfileAuthHeaders(profileCode, opts.headers || {});
    if (!auth.ok) {
      return { ok: false, error: auth.error, fetchOptions: null };
    }
    const rest = { ...opts };
    delete rest.headers;
    return {
      ok: true,
      fetchOptions: {
        ...rest,
        headers: {
          ...(opts.headers || {}),
          ...auth.headers,
        },
      },
    };
  }

  function formatNotifyTestError(status, errorCode, opts) {
    const network = opts && opts.network;
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

  function buildProfileAuthDiagnostic(profileCode, derivedUserKey) {
    const normalized = normalizeProfileCode(profileCode);
    const auth = buildProfileAuthHeaders(normalized);
    const prefix = derivedUserKey ? `${String(derivedUserKey).slice(0, 16)}…` : null;
    return {
      profileCodePresent: !!normalized,
      profileCodeLength: normalized ? normalized.length : 0,
      derivedUserKeyPrefix: prefix,
      authHeaderPrepared: auth.ok,
    };
  }

  global.ProfileAuthClient = {
    normalizeProfileCode,
    buildProfileAuthHeaders,
    mergeProfileAuthFetchOptions,
    formatProfileAuthError,
    formatNotifyTestError,
    maskNtfyTopicForDisplay,
    deriveUserKeyFromProfileCodeAsync,
    buildProfileAuthDiagnostic,
  };
})(typeof window !== 'undefined' ? window : globalThis);
