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
    formatProfileAuthError,
    maskNtfyTopicForDisplay,
    deriveUserKeyFromProfileCodeAsync,
    buildProfileAuthDiagnostic,
  };
})(typeof window !== 'undefined' ? window : globalThis);
