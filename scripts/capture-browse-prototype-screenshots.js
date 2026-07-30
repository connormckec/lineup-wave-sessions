'use strict';

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const OUT_DIR = path.join(__dirname, '../public/prototype-screenshots');
const BASE = process.env.PROTOTYPE_BASE_URL || 'http://127.0.0.1:3000/browse-prototype.html';

const VIEWPORTS = [
  { name: 'iphone-se-compact', width: 375, height: 667, viewportOnly: true },
  { name: 'iphone-12', width: 390, height: 844, viewportOnly: true },
  { name: 'iphone-14-plus', width: 430, height: 932, viewportOnly: true },
  { name: 'ipad-portrait', width: 768, height: 1024, viewportOnly: false },
  { name: 'ipad-landscape', width: 1024, height: 768, viewportOnly: false },
  { name: 'desktop', width: 1280, height: 800, viewportOnly: false },
  { name: 'desktop-wide', width: 1440, height: 900, viewportOnly: false },
];

async function assertRendered(page) {
  await page.waitForFunction(() => {
    const list = document.getElementById('session-list');
    return list && list.querySelector('.sc, .proto-error, p');
  }, { timeout: 10000 });
  const state = await page.evaluate(() => ({
    sessionCards: document.querySelectorAll('.sc').length,
    dayRail: document.getElementById('day-rail')?.children.length || 0,
    livePanel: document.getElementById('live-panel')?.textContent?.trim().length || 0,
    contentHeight: document.getElementById('content')?.offsetHeight || 0,
    error: document.querySelector('.proto-error')?.textContent || null,
    hasLiveWavesSidebar: (document.getElementById('sidebar')?.textContent || '').includes('Live waves'),
    hasTrustedOpen: (document.getElementById('day-rail')?.textContent || '').includes('trusted open'),
  }));
  if (state.error) throw new Error(`Prototype init error: ${state.error}`);
  if (state.sessionCards < 1) throw new Error('Expected at least one session card to render');
  if (state.contentHeight < 40) throw new Error(`Main content collapsed (${state.contentHeight}px)`);
  if (state.hasLiveWavesSidebar) throw new Error('Sidebar still contains duplicate Live waves panel');
  if (state.hasTrustedOpen) throw new Error('Day rail still contains trusted open wording');
  return state;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const browser = await chromium.launch({ channel: 'chrome' }).catch(() => chromium.launch());
  const results = [];

  for (const vp of VIEWPORTS) {
    const context = await browser.newContext({
      viewport: { width: vp.width, height: vp.height },
      deviceScaleFactor: vp.width < 768 ? 2 : 1,
      isMobile: vp.width < 768,
      hasTouch: vp.width < 768,
    });
    const page = await context.newPage();
    await page.goto(BASE, { waitUntil: 'networkidle', timeout: 30000 });
    await assertRendered(page);
    await page.waitForTimeout(300);
    const filePath = path.join(OUT_DIR, `browse-prototype-${vp.name}.png`);
    await page.screenshot({ path: filePath, fullPage: !vp.viewportOnly });
    results.push({ viewport: vp.name, path: filePath, size: `${vp.width}x${vp.height}` });
    await context.close();
  }

  await browser.close();
  console.log('Captured browse prototype screenshots:');
  for (const row of results) {
    console.log(`  ${row.viewport} (${row.size}): ${row.path}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
