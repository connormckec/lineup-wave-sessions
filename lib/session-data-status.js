'use strict';

function normalizeSource(value) {
  return String(value || '').trim().toLowerCase();
}

function isExplicitFallbackSource({
  dataSource = null,
  isFallback = false,
  statusReason = null,
  isUsingCachedData = false,
} = {}) {
  if (isFallback) return true;
  if (statusReason === 'fallback_sessions_found') return true;
  const src = normalizeSource(dataSource);
  if (!src) return Boolean(isUsingCachedData);
  if (src.includes('fallback')) return true;
  if (src.includes('scrape_snapshots') || src.includes('availability_snapshots')) return true;
  if (src.includes('memory-fallback') || src === 'memory') return true;
  if (src.includes('schema-missing')) return true;
  return false;
}

function isSuccessfulDatabaseSource({
  dataSource = null,
  isFallback = false,
  statusReason = null,
} = {}) {
  if (isExplicitFallbackSource({ dataSource, isFallback, statusReason })) return false;
  if (statusReason === 'saved_sessions_found') return true;
  const src = normalizeSource(dataSource);
  if (!src) return false;
  if (src.includes('supabase') || src.includes('current_sessions')) return true;
  return false;
}

function formatAgeLabel({ checkedAt = null, ageMinutes = null, now = Date.now() } = {}) {
  if (ageMinutes != null && Number.isFinite(Number(ageMinutes))) {
    const mins = Number(ageMinutes);
    if (mins < 1) return 'just now';
    return `${mins}m ago`;
  }
  if (checkedAt) {
    const mins = Math.round((now - new Date(checkedAt).getTime()) / 60_000);
    if (!Number.isFinite(mins) || mins < 0) return 'earlier';
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.round(hrs / 24)}d ago`;
  }
  return 'earlier';
}

function buildStatusModel({
  key,
  primaryPhrase,
  detailPhrase,
  severity = 'neutral',
  showSessionCount = false,
}) {
  const fullText = detailPhrase ? `${primaryPhrase} · ${detailPhrase}` : primaryPhrase;
  return {
    key,
    primaryPhrase,
    detailPhrase: detailPhrase || null,
    fullText,
    severity,
    showSessionCount,
  };
}

function deriveSessionDataStatus(input = {}) {
  const {
    dataSource = null,
    isFallback = false,
    statusReason = null,
    isRefreshing = false,
    isLoading = false,
    hasUsableSessions = false,
    lastFetchError = null,
    lastScrapeError = null,
    isUsingCachedData = false,
    checkedAt = null,
    ageMinutes = null,
    now = Date.now(),
  } = input;

  const ageLabel = formatAgeLabel({ checkedAt, ageMinutes, now });
  const fromDatabase = isSuccessfulDatabaseSource({ dataSource, isFallback, statusReason });
  const fromCache = isExplicitFallbackSource({
    dataSource,
    isFallback,
    statusReason,
    isUsingCachedData: isUsingCachedData && !fromDatabase,
  });
  const refreshFailed = Boolean(lastFetchError || lastScrapeError);
  const fetchFailed = Boolean(lastFetchError);

  if (isLoading && !hasUsableSessions) {
    return buildStatusModel({
      key: 'loading',
      primaryPhrase: 'Loading sessions…',
      severity: 'neutral',
      showSessionCount: false,
    });
  }

  if (!hasUsableSessions && (statusReason === 'error' || statusReason === 'schema_error' || fetchFailed)) {
    return buildStatusModel({
      key: 'error',
      primaryPhrase: 'Unable to load sessions',
      severity: 'error',
      showSessionCount: false,
    });
  }

  if (!hasUsableSessions && statusReason === 'not_checked' && !isLoading) {
    return buildStatusModel({
      key: 'waiting',
      primaryPhrase: 'Waiting for first session check',
      severity: 'neutral',
      showSessionCount: false,
    });
  }

  if (!hasUsableSessions && statusReason === 'checked_no_sessions') {
    return buildStatusModel({
      key: 'empty_checked',
      primaryPhrase: 'No sessions found',
      detailPhrase: `checked ${ageLabel}`,
      severity: 'neutral',
      showSessionCount: false,
    });
  }

  if (fromDatabase) {
    if (isRefreshing) {
      return buildStatusModel({
        key: 'database_refreshing',
        primaryPhrase: 'Loaded from database',
        detailPhrase: 'refreshing…',
        severity: 'ok',
        showSessionCount: true,
      });
    }
    if (refreshFailed) {
      return buildStatusModel({
        key: 'database_refresh_failed',
        primaryPhrase: 'Loaded from database',
        detailPhrase: 'refresh failed',
        severity: 'warn',
        showSessionCount: true,
      });
    }
    return buildStatusModel({
      key: 'database_loaded',
      primaryPhrase: 'Loaded from database',
      detailPhrase: `checked ${ageLabel}`,
      severity: 'ok',
      showSessionCount: true,
    });
  }

  if (fromCache || isUsingCachedData) {
    if (isRefreshing) {
      return buildStatusModel({
        key: 'cached_refreshing',
        primaryPhrase: 'Showing cached data',
        detailPhrase: 'refreshing…',
        severity: 'warn',
        showSessionCount: true,
      });
    }
    if (refreshFailed) {
      return buildStatusModel({
        key: 'cached_refresh_failed',
        primaryPhrase: 'Showing cached data',
        detailPhrase: 'refresh failed',
        severity: 'warn',
        showSessionCount: true,
      });
    }
    return buildStatusModel({
      key: 'cached_loaded',
      primaryPhrase: 'Showing cached data',
      detailPhrase: `checked ${ageLabel === 'just now' ? 'earlier' : ageLabel}`,
      severity: 'warn',
      showSessionCount: true,
    });
  }

  if (hasUsableSessions) {
    if (isRefreshing) {
      return buildStatusModel({
        key: 'unknown_refreshing',
        primaryPhrase: 'Sessions loaded',
        detailPhrase: 'refreshing…',
        severity: 'neutral',
        showSessionCount: true,
      });
    }
    if (refreshFailed) {
      return buildStatusModel({
        key: 'unknown_refresh_failed',
        primaryPhrase: 'Sessions loaded',
        detailPhrase: 'refresh failed',
        severity: 'warn',
        showSessionCount: true,
      });
    }
    return buildStatusModel({
      key: 'unknown_loaded',
      primaryPhrase: 'Sessions loaded',
      detailPhrase: `checked ${ageLabel}`,
      severity: 'neutral',
      showSessionCount: true,
    });
  }

  if (!hasUsableSessions && refreshFailed) {
    return buildStatusModel({
      key: 'error',
      primaryPhrase: 'Unable to load sessions',
      severity: 'error',
      showSessionCount: false,
    });
  }

  return buildStatusModel({
    key: 'waiting',
    primaryPhrase: 'Waiting for first session check',
    severity: 'neutral',
    showSessionCount: false,
  });
}

const exported = {
  normalizeSource,
  isExplicitFallbackSource,
  isSuccessfulDatabaseSource,
  formatAgeLabel,
  deriveSessionDataStatus,
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = exported;
}

if (typeof window !== 'undefined') {
  window.deriveSessionDataStatus = deriveSessionDataStatus;
  window.formatSessionDataAgeLabel = formatAgeLabel;
}
