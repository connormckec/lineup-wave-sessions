'use strict';

const { maskDestination } = require('./notification-topic');

function maskIpAddress(address) {
  const text = String(address || '').trim();
  if (!text) return null;
  if (text.includes(':')) {
    const parts = text.split(':').filter(Boolean);
    if (parts.length <= 2) return '••••';
    return `${parts[0]}:••••:${parts[parts.length - 1]}`;
  }
  const octets = text.split('.');
  if (octets.length === 4) return `${octets[0]}.${octets[1]}.••••.••••`;
  return '••••';
}

function extractSafeTransportError(err, {
  timeoutTriggered = false,
  providerHost = 'ntfy.sh',
} = {}) {
  const cause = err?.cause || err;
  return {
    name: err?.name || null,
    message: err?.message || null,
    code: err?.code || null,
    causeName: cause?.name || null,
    causeMessage: cause?.message || null,
    causeCode: cause?.code || null,
    causeErrno: cause?.errno ?? null,
    causeSyscall: cause?.syscall || null,
    causeHostname: cause?.hostname || null,
    causeAddress: cause?.address ? maskIpAddress(cause.address) : null,
    causePort: cause?.port ?? null,
    causeReason: cause?.reason || null,
    timeoutTriggered: !!timeoutTriggered,
    providerHost,
    nodeVersion: process.version,
  };
}

function buildSafeRequestMetadata({
  destination,
  title,
  message,
  clickUrl,
  timeoutMs,
  transport,
  family,
}) {
  let clickHost = null;
  if (clickUrl) {
    try {
      clickHost = new URL(String(clickUrl)).hostname;
    } catch {
      clickHost = 'invalid';
    }
  }
  return {
    providerHost: 'ntfy.sh',
    destinationMasked: maskDestination(destination),
    topicLength: String(destination || '').trim().length,
    titleLength: String(title || '').length,
    messageLength: String(message || '').length,
    clickHost,
    timeoutMs,
    transport,
    family: family ?? null,
    nodeVersion: process.version,
  };
}

function classifyTransportFailure(err, { timeoutTriggered = false } = {}) {
  if (timeoutTriggered || err?.code === 'ETIMEDOUT' || err?.name === 'AbortError') {
    return { error: 'provider_timeout', transient: true };
  }
  const code = err?.code || err?.cause?.code;
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
    return { error: 'provider_dns_failed', transient: true };
  }
  if (code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE'
    || code === 'CERT_HAS_EXPIRED'
    || code === 'DEPTH_ZERO_SELF_SIGNED_CERT'
    || (err?.message || '').toLowerCase().includes('certificate')) {
    return { error: 'provider_tls_failed', transient: false };
  }
  return { error: 'provider_transport_failed', transient: true };
}

function logProviderFailure(label, diagnostic, requestMeta) {
  console.error(`[notification-provider] ${label}`, {
    ...diagnostic,
    ...requestMeta,
  });
}

module.exports = {
  maskIpAddress,
  extractSafeTransportError,
  buildSafeRequestMetadata,
  classifyTransportFailure,
  logProviderFailure,
};
