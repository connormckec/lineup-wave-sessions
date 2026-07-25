'use strict';

const dns = require('dns').promises;
const https = require('https');
const { URL } = require('url');
const { maskIpAddress } = require('./notification-provider-errors');
const {
  resolveProviderSettings,
  deriveRelayHealthUrl,
} = require('./notification-provider-config');

const NTFY_HOST = 'ntfy.sh';
const PROBE_TIMEOUT_MS = 8000;

function resolveProbeFamily(value) {
  if (value === 4 || value === '4' || value === 'ipv4') return 4;
  if (value === 6 || value === '6' || value === 'ipv6') return 6;
  return undefined;
}

function maskLookupResults(results) {
  return (results || []).map((row) => ({
    family: row.family,
    address: maskIpAddress(row.address),
  }));
}

async function probeDnsLookup() {
  const started = Date.now();
  try {
    const results = await dns.lookup(NTFY_HOST, { all: true });
    return {
      ok: true,
      transport: 'dns.lookup',
      durationMs: Date.now() - started,
      addresses: maskLookupResults(results),
      errorCode: null,
      causeCode: null,
      family: null,
      statusCode: null,
    };
  } catch (err) {
    return {
      ok: false,
      transport: 'dns.lookup',
      durationMs: Date.now() - started,
      addresses: [],
      errorCode: err.code || null,
      causeCode: err.cause?.code || null,
      family: null,
      statusCode: null,
    };
  }
}

function probeHttpsRequest({ hostname, path = '/', family, label, method = 'HEAD' }) {
  return new Promise((resolve) => {
    const started = Date.now();
    let settled = false;
    const finish = (payload) => {
      if (settled) return;
      settled = true;
      resolve(payload);
    };

    const req = https.request({
      hostname,
      port: 443,
      path,
      method,
      family: resolveProbeFamily(family),
      timeout: PROBE_TIMEOUT_MS,
    }, (res) => {
      res.resume();
      finish({
        ok: res.statusCode != null && res.statusCode < 500,
        transport: label,
        durationMs: Date.now() - started,
        statusCode: res.statusCode ?? null,
        errorCode: null,
        causeCode: null,
        family: resolveProbeFamily(family) ?? 'default',
      });
    });

    req.on('timeout', () => {
      if (typeof req.destroy === 'function') {
        req.destroy(Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' }));
      }
    });

    req.on('error', (err) => {
      finish({
        ok: false,
        transport: label,
        durationMs: Date.now() - started,
        statusCode: null,
        errorCode: err.code || null,
        causeCode: err.cause?.code || null,
        family: resolveProbeFamily(family) ?? 'default',
      });
    });

    req.end();
  });
}

async function probeNodeFetch() {
  const started = Date.now();
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
    const res = await fetch(`https://${NTFY_HOST}/`, {
      method: 'HEAD',
      signal: controller.signal,
    });
    clearTimeout(timer);
    return {
      ok: res.status < 500,
      transport: 'fetch',
      durationMs: Date.now() - started,
      statusCode: res.status,
      errorCode: null,
      causeCode: null,
      family: 'default',
    };
  } catch (err) {
    return {
      ok: false,
      transport: 'fetch',
      durationMs: Date.now() - started,
      statusCode: null,
      errorCode: err.code || err.name || null,
      causeCode: err.cause?.code || null,
      family: 'default',
    };
  }
}

async function probeRelayHealth(relayHealthUrl) {
  const started = Date.now();
  try {
    const parsed = new URL(relayHealthUrl);
    const result = await probeHttpsRequest({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      label: 'relay.health',
      method: 'GET',
    });
    return {
      ...result,
      host: parsed.hostname,
    };
  } catch (err) {
    return {
      ok: false,
      transport: 'relay.health',
      durationMs: Date.now() - started,
      statusCode: null,
      errorCode: err.code || err.message || null,
      causeCode: null,
      family: 'default',
      host: null,
    };
  }
}

async function runProviderProbe(env = process.env) {
  const settings = resolveProviderSettings(env);
  const [dnsResult, fetchResult, httpsDefault, httpsIpv4, httpsIpv6] = await Promise.all([
    probeDnsLookup(),
    probeNodeFetch(),
    probeHttpsRequest({ hostname: NTFY_HOST, label: 'https.request' }),
    probeHttpsRequest({ hostname: NTFY_HOST, family: 4, label: 'https.request.ipv4' }),
    probeHttpsRequest({ hostname: NTFY_HOST, family: 6, label: 'https.request.ipv6' }),
  ]);

  let relayHealth = null;
  if (settings.ok && settings.mode === 'relay' && settings.relayHealthUrl) {
    relayHealth = await probeRelayHealth(settings.relayHealthUrl);
  } else if (settings.mode === 'relay' && settings.relayUrl) {
    relayHealth = await probeRelayHealth(deriveRelayHealthUrl(settings.relayUrl));
  }

  return {
    ok: true,
    host: NTFY_HOST,
    nodeVersion: process.version,
    configuredMode: settings.mode || 'direct',
    configuredFamily: env.NTFY_HTTP_FAMILY || null,
    relayHost: settings.relayHost || null,
    relayConfigured: settings.mode === 'relay' && settings.ok,
    relayConfigError: settings.ok ? null : settings.error || null,
    relaySecretConfigured: settings.relaySecretConfigured ?? false,
    probes: [dnsResult, fetchResult, httpsDefault, httpsIpv4, httpsIpv6],
    relayHealth,
  };
}

module.exports = {
  NTFY_HOST,
  PROBE_TIMEOUT_MS,
  runProviderProbe,
  probeDnsLookup,
  probeHttpsRequest,
  probeNodeFetch,
  probeRelayHealth,
  maskLookupResults,
};
