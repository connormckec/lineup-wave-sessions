'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { chromium } = require('playwright');
const SEM = require('../lib/browse-ui-semantics');

console.log('browse ui prototype regression');

const ROOT = path.join(__dirname, '..');
const TEST_PORT = 3097;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForServer(port, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/browse-prototype.html`);
      if (res.ok) return;
    } catch (_) { /* retry */ }
    await sleep(250);
  }
  throw new Error(`Test server did not start on port ${port}`);
}

async function withTestServer(fn) {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(TEST_PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  try {
    await waitForServer(TEST_PORT);
    await fn(`http://127.0.0.1:${TEST_PORT}`);
  } finally {
    child.kill('SIGTERM');
    await sleep(200);
  }
}

// 1. Canonical semantics exports computeLiveNowSummary.
{
  assert.strictEqual(typeof SEM.computeLiveNowSummary, 'function');
  assert.strictEqual(SEM.SEMANTICS_API_VERSION, '4');
}

// 2. Full sessions remain watchable with standard labels.
{
  const full = {
    available: false, slots: 0, threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan', available_entries: 0,
  };
  assert.strictEqual(SEM.watchLabelForSession(full, false, true).label, 'Watch');
  assert.strictEqual(SEM.watchLabelForSession(full, true, true).label, 'Watching');
}

// 3. Live now helper supports trusted inventory scenarios.
{
  const nowMs = Date.parse('2026-07-29T19:30:00-04:00');
  const live = SEM.computeLiveNowSummary([
    {
      ts: Math.floor(Date.parse('2026-07-29T19:00:00-04:00') / 1000),
      wave: 1,
      waveSide: 'Left Wave',
      level: 'Advanced Turns',
      durationMinutes: 60,
      capacity: 12,
      available: true,
      available_entries: 5,
      thresholdInferredSlots: 5,
      threshold_scan_verified: true,
      slot_status: 'exact',
      thresholdConfidence: 'exact',
      slot_source: 'entries_left_threshold_scan',
      threshold_scanned_at: new Date(nowMs - 8 * 60 * 1000).toISOString(),
    },
  ], nowMs);
  assert.strictEqual(live.mode, 'live');
  assert.match(live.left.occupancyLine, /Estimated 7 surfers/);
}

// 4. Prototype HTML uses versioned canonical script paths and single error root.
{
  const html = fs.readFileSync(path.join(__dirname, '../public/browse-prototype.html'), 'utf8');
  const semanticsSrc = fs.readFileSync(path.join(__dirname, '../lib/browse-ui-semantics.js'), 'utf8');
  assert.match(html, /\/browse-ui-semantics\.js\?v=12/);
  assert.match(html, /\/lineup-config\.js\?v=12/);
  assert.match(html, /\/browse-live-schedule\.js\?v=12/);
  assert.match(html, /\/lib\/browse-ui-fixtures\.js\?v=2/);
  assert.match(html, /id="proto-error-root"/);
  assert.match(html, /validateSemanticsContract/);
  assert.match(html, /REQUIRED_SEM_FUNCTIONS/);
  assert.doesNotMatch(html, /browse-ui-semantics\.js"><\/script>/);
  assert.match(semanticsSrc, /computeLiveNowSummary/);
  assert.match(semanticsSrc, /window\.LineupBrowse\.semantics = semantics/);
}

// 5. Production index.html unchanged.
{
  const prod = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
  assert.doesNotMatch(prod, /browse-prototype\.html/);
}

// 6. Playwright normal navigation (no manual render injection).
(async () => {
  await withTestServer(async (origin) => {
    const browser = await chromium.launch({ channel: 'chrome' }).catch(() => chromium.launch());

    // Success path with service-worker control on same origin.
    {
      const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
      const page = await context.newPage();
      await page.goto(`${origin}/browse-prototype.html`, { waitUntil: 'domcontentloaded' });
      await page.evaluate(async () => {
        if ('serviceWorker' in navigator) {
          await navigator.serviceWorker.register('/sw.js', { updateViaCache: 'none' });
          await navigator.serviceWorker.ready;
        }
      });
      await page.reload({ waitUntil: 'domcontentloaded' });
      await page.waitForFunction(() => {
        const cards = document.querySelectorAll('.sc').length;
        const err = document.querySelector('#proto-error-root .proto-error');
        return cards > 0 || !!err;
      }, { timeout: 15000 });

      const state = await page.evaluate(() => ({
        semType: typeof window.LineupBrowse?.semantics?.computeLiveNowSummary,
        semVersion: window.LineupBrowse?.semantics?.SEMANTICS_API_VERSION || null,
        error: document.querySelector('#proto-error-root .proto-error')?.textContent || null,
        errorCount: document.querySelectorAll('.proto-error').length,
        liveText: document.getElementById('live-panel')?.textContent?.trim() || '',
        dayRailCount: document.getElementById('day-rail')?.children.length || 0,
        filterCount: document.getElementById('level-tabs')?.children.length || 0,
        sessionCards: document.querySelectorAll('.sc').length,
        sidebarText: document.getElementById('sidebar')?.textContent || '',
        watchLabels: Array.from(document.querySelectorAll('.watch-btn')).map((b) => b.textContent.trim()),
      }));

      assert.strictEqual(state.error, null, `init error: ${state.error}`);
      assert.strictEqual(state.errorCount, 0);
      assert.strictEqual(state.semType, 'function');
      assert.strictEqual(state.semVersion, '4');
      assert.match(state.liveText, /LIVE NOW/i);
      assert.ok(state.dayRailCount >= 5);
      assert.ok(state.filterCount >= 5);
      assert.ok(state.sessionCards >= 1);
      assert.ok(state.sidebarText.length > 0);
      assert.ok(state.watchLabels.every((l) => l === 'Watch' || l === 'Watching' || l === 'Alerts off'));
      await context.close();
    }

    // Forced failure renders exactly one error panel.
    {
      const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
      const page = await context.newPage();
      await page.route('**/browse-ui-semantics.js*', (route) => route.abort());
      await page.goto(`${origin}/browse-prototype.html`, { waitUntil: 'domcontentloaded' });
      await page.waitForSelector('#proto-error-root .proto-error', { timeout: 10000 });
      const failure = await page.evaluate(() => ({
        errorCount: document.querySelectorAll('.proto-error').length,
        liveHtml: document.getElementById('live-panel')?.innerHTML || '',
        rootHidden: document.getElementById('proto-error-root')?.hidden,
      }));
      assert.strictEqual(failure.errorCount, 1);
      assert.strictEqual(failure.liveHtml, '');
      assert.strictEqual(failure.rootHidden, false);
      await context.close();
    }

    await browser.close();
  });
})().then(() => {
  console.log('browse ui prototype regression: all tests passed');
}).catch((err) => {
  console.error(err);
  process.exit(1);
});
