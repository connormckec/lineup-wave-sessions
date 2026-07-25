'use strict';

const { URL } = require('url');

const MIN_RELAY_SECRET_LENGTH = 32;
const BLOCKED_RELAY_HOSTS = new Set([
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
  '::1',
]);

function resolveTransportMode(env = process.env) {
  const mode = String(env.NTFY_TRANSPORT_MODE || 'direct').trim().toLowerCase();
  return mode === 'relay' ? 'relay' : 'direct';
}

function validateRelayUrl(rawUrl) {
  const text = String(rawUrl || '').trim();
  if (!text) {
    return { ok: false, error: 'relay_url_required' };
  }
  let parsed;
  try {
    parsed = new URL(text);
  } catch {
    return { ok: false, error: 'relay_url_invalid' };
  }
  if (parsed.protocol !== 'https:') {
    return { ok: false, error: 'relay_url_must_be_https' };
  }
  if (parsed.username || parsed.password) {
    return { ok: false, error: 'relay_url_credentials_forbidden' };
  }
  const host = parsed.hostname.toLowerCase();
  if (BLOCKED_RELAY_HOSTS.has(host) || host.endsWith('.localhost')) {
    return { ok: false, error: 'relay_url_host_forbidden' };
  }
  if (!parsed.pathname || parsed.pathname === '/') {
    return { ok: false, error: 'relay_url_path_required' };
  }
  return {
    ok: true,
    relayUrl: parsed.toString(),
    relayHost: parsed.hostname,
    relayPath: parsed.pathname + parsed.search,
  };
}

function validateRelaySecret(rawSecret) {
  const secret = String(rawSecret || '').trim();
  if (!secret) {
    return { ok: false, error: 'relay_secret_required' };
  }
  if (secret.length < MIN_RELAY_SECRET_LENGTH) {
    return { ok: false, error: 'relay_secret_too_short' };
  }
  return { ok: true, relaySecret: secret };
}

function resolveRelaySettings(env = process.env) {
  const url = validateRelayUrl(env.NTFY_RELAY_URL);
  const secret = validateRelaySecret(env.NTFY_RELAY_SECRET);
  if (!url.ok) return { ok: false, error: url.error };
  if (!secret.ok) return { ok: false, error: secret.error };
  return {
    ok: true,
    relayUrl: url.relayUrl,
    relayHost: url.relayHost,
    relayPath: url.relayPath,
    relaySecret: secret.relaySecret,
  };
}

function deriveRelayHealthUrl(relayUrl) {
  const parsed = new URL(relayUrl);
  parsed.pathname = '/health';
  parsed.search = '';
  parsed.hash = '';
  return parsed.toString();
}

function resolveProviderSettings(env = process.env) {
  const mode = resolveTransportMode(env);
  if (mode === 'relay') {
    const relay = resolveRelaySettings(env);
    if (!relay.ok) {
      return { ok: false, mode, error: relay.error };
    }
    return {
      ok: true,
      mode,
      relayUrl: relay.relayUrl,
      relayHost: relay.relayHost,
      relayPath: relay.relayPath,
      relaySecret: relay.relaySecret,
      relayHealthUrl: deriveRelayHealthUrl(relay.relayUrl),
      relaySecretConfigured: true,
    };
  }
  return {
    ok: true,
    mode: 'direct',
    relaySecretConfigured: !!String(env.NTFY_RELAY_SECRET || '').trim(),
  };
}

function buildStartupLogLines(settings) {
  if (settings.mode === 'relay') {
    return [
      'ntfy provider transport: relay',
      `relay host: ${settings.relayHost}`,
      `relay secret configured: ${settings.relaySecretConfigured ? 'yes' : 'no'}`,
    ];
  }
  return [
    'ntfy provider transport: direct',
    `relay secret configured: ${settings.relaySecretConfigured ? 'yes' : 'no'}`,
  ];
}

module.exports = {
  MIN_RELAY_SECRET_LENGTH,
  resolveTransportMode,
  validateRelayUrl,
  validateRelaySecret,
  resolveRelaySettings,
  deriveRelayHealthUrl,
  resolveProviderSettings,
  buildStartupLogLines,
};
