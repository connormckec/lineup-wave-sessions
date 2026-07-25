'use strict';

function classifyRelayHttpResponse(statusCode) {
  if (statusCode >= 200 && statusCode < 300) {
    return { ok: true, error: null, transient: false };
  }
  if (statusCode === 400) {
    return { ok: false, error: 'provider_http_error', transient: false };
  }
  if (statusCode === 401) {
    return { ok: false, error: 'relay_auth_failed', transient: false };
  }
  if (statusCode === 429) {
    return { ok: false, error: 'provider_http_error', transient: true };
  }
  if (statusCode === 504) {
    return { ok: false, error: 'provider_timeout', transient: true };
  }
  if (statusCode >= 500) {
    return { ok: false, error: 'provider_http_error', transient: true };
  }
  return {
    ok: false,
    error: 'provider_http_error',
    transient: statusCode >= 500,
  };
}

module.exports = {
  classifyRelayHttpResponse,
};
