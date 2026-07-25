'use strict';

const assert = require('assert');
const status = require('../lib/session-data-status');

const NOW = Date.parse('2026-07-25T15:00:00.000Z');
const CHECKED_AT = '2026-07-25T14:59:00.000Z';

function derive(overrides = {}) {
  return status.deriveSessionDataStatus({
    checkedAt: CHECKED_AT,
    ageMinutes: 1,
    now: NOW,
    ...overrides,
  });
}

console.log('session data status regression');

{
  const model = derive({
    dataSource: 'supabase/current_sessions',
    statusReason: 'saved_sessions_found',
    hasUsableSessions: true,
  });
  assert.strictEqual(model.key, 'database_loaded');
  assert.strictEqual(model.fullText, 'Loaded from database · checked 1m ago');
}

{
  const model = derive({
    dataSource: 'supabase/current_sessions',
    statusReason: 'checked_no_sessions',
    hasUsableSessions: false,
  });
  assert.strictEqual(model.key, 'empty_checked');
  assert.strictEqual(model.fullText, 'No sessions found · checked 1m ago');
}

{
  const model = derive({
    dataSource: 'supabase/current_sessions',
    statusReason: 'saved_sessions_found',
    hasUsableSessions: true,
    isRefreshing: true,
  });
  assert.strictEqual(model.key, 'database_refreshing');
  assert.strictEqual(model.fullText, 'Loaded from database · refreshing…');
}

{
  const model = derive({
    dataSource: 'supabase/scrape_snapshots_fallback',
    statusReason: 'fallback_sessions_found',
    isFallback: true,
    hasUsableSessions: true,
  });
  assert.strictEqual(model.key, 'cached_loaded');
  assert.strictEqual(model.primaryPhrase, 'Showing cached data');
}

{
  const model = derive({
    dataSource: 'supabase/scrape_snapshots_fallback',
    statusReason: 'fallback_sessions_found',
    isFallback: true,
    hasUsableSessions: true,
    lastFetchError: 'network error',
  });
  assert.strictEqual(model.key, 'cached_refresh_failed');
  assert.strictEqual(model.fullText, 'Showing cached data · refresh failed');
}

{
  const model = derive({
    dataSource: 'supabase/current_sessions',
    statusReason: 'saved_sessions_found',
    hasUsableSessions: true,
    lastScrapeError: 'playwright crashed',
  });
  assert.strictEqual(model.key, 'database_refresh_failed');
  assert.strictEqual(model.fullText, 'Loaded from database · refresh failed');
  assert.ok(!model.fullText.includes('cached'));
}

{
  const model = derive({
    statusReason: 'not_checked',
    hasUsableSessions: false,
  });
  assert.strictEqual(model.key, 'waiting');
  assert.strictEqual(model.fullText, 'Waiting for first session check');
}

{
  const model = derive({
    isLoading: true,
    hasUsableSessions: false,
  });
  assert.strictEqual(model.key, 'loading');
  assert.strictEqual(model.fullText, 'Loading sessions…');
}

{
  const model = derive({
    statusReason: 'error',
    hasUsableSessions: false,
    lastFetchError: '500 internal',
  });
  assert.strictEqual(model.key, 'error');
  assert.strictEqual(model.fullText, 'Unable to load sessions');
  assert.ok(!model.fullText.includes('supabase'));
}

{
  const model = derive({
    dataSource: 'unknown-provider',
    statusReason: 'not_checked',
    hasUsableSessions: true,
    isUsingCachedData: true,
  });
  assert.strictEqual(model.key, 'cached_loaded');
  assert.strictEqual(model.primaryPhrase, 'Showing cached data');
}

{
  const model = derive({
    dataSource: 'unknown-provider',
    statusReason: null,
    hasUsableSessions: true,
  });
  assert.strictEqual(model.key, 'unknown_loaded');
  assert.ok(!model.fullText.toLowerCase().includes('cached'));
  assert.ok(!model.fullText.toLowerCase().includes('database'));
}

{
  const dbModel = derive({
    dataSource: 'supabase/current_sessions',
    statusReason: 'saved_sessions_found',
    hasUsableSessions: true,
  });
  assert.ok(!dbModel.fullText.toLowerCase().includes('cached'));
  assert.ok(!dbModel.fullText.toLowerCase().includes('saved data'));
}

console.log('session data status regression: all tests passed');
