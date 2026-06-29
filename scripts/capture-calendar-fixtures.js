#!/usr/bin/env node
'use strict';

/**
 * Gate 5A — Capture frozen calendar fixtures for local parser/network discovery.
 *
 * Usage:
 *   node scripts/capture-calendar-fixtures.js --isoDate=2026-06-30 --weekMode=true --thresholds=1,2,3
 *
 * Output:
 *   fixtures/atlantic/<isoDate>/threshold-N.{html,png,dom.json,network.json}
 *   fixtures/atlantic/<isoDate>/capture-summary.json
 */

const fs = require('fs');
const path = require('path');

process.chdir(path.join(__dirname, '..'));

const { scrapeCalendarFixtureDom } = require('../lib/calendar-fixture-dom-scraper');
const { createFixtureNetworkCapture, analyzeNetworkForSummary } = require('../lib/calendar-fixture-network');

function parseArg(name, fallback = null) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  if (!hit) return fallback;
  return hit.slice(prefix.length);
}

function parseBool(value, fallback = true) {
  if (value == null) return fallback;
  return !/^(false|0|no)$/i.test(String(value));
}

function parseThresholds(raw) {
  const text = raw || '1,2,3';
  return text
    .split(',')
    .map((part) => Number(part.trim()))
    .filter((n) => Number.isFinite(n) && n >= 1);
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function mapSelectionFields(setResult) {
  if (!setResult) return null;
  return {
    requestedThreshold: setResult.requestedThreshold,
    beforeLabel: setResult.beforeLabel ?? null,
    afterLabel: setResult.afterLabel ?? null,
    matchingOptionText: setResult.matchingOptionText ?? null,
    filterSetOk: setResult.filterSetOk === true,
    filterSetError: setResult.filterSetError ?? null,
  };
}

async function main() {
  const isoDate = parseArg('isoDate', '2026-06-30');
  const weekMode = parseBool(parseArg('weekMode', 'true'), true);
  const thresholds = parseThresholds(parseArg('thresholds', '1,2,3'));
  const headed = process.argv.includes('--headed');

  if (!/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    throw new Error('isoDate must be YYYY-MM-DD');
  }
  if (!thresholds.length) {
    throw new Error('thresholds must include at least one positive integer');
  }

  const helpers = require('../server.js');
  const {
    launchBrowser,
    openBookingPageForThreshold,
    waitForThresholdCalendarShell,
    navigateCalendarToShowDate,
    normalizeBookingFiltersOnPage,
    setEntriesLeftThreshold,
    dismissEntriesLeftPopup,
    getMondayWeekStartIso,
    safeCloseBrowser,
    THRESHOLD_FILTER_SETTLE_MS,
  } = helpers;

  const outDir = path.join('fixtures', 'atlantic', isoDate);
  ensureDir(outDir);

  const computedWeekStart = getMondayWeekStartIso(isoDate);
  const navigationIsoDate = weekMode ? computedWeekStart : isoDate;

  const captureSummary = {
    gate: '5A',
    isoDate,
    weekMode,
    thresholds,
    computedWeekStart,
    navigationIsoDate,
    outputDir: outDir,
    startedAt: new Date().toISOString(),
    thresholdCaptures: [],
    networkDataCandidateFound: false,
    candidateResponses: [],
  };

  let launched = null;
  const allNetworkEntries = [];

  try {
    launched = await launchBrowser({ headed });
    const network = createFixtureNetworkCapture();
    network.attach(launched.page);
    network.setPhase('page_load');

    await openBookingPageForThreshold(launched.page);
    await waitForThresholdCalendarShell(launched.page).catch(() => {});

    const nav = await navigateCalendarToShowDate(launched.page, navigationIsoDate, {
      headerOnly: true,
      validateIsoDate: isoDate,
      waitForShell: true,
    });

    await normalizeBookingFiltersOnPage(launched.page).catch(() => {});
    await launched.page.waitForTimeout(500);
    network.setPhase('calendar_ready');

    for (const threshold of thresholds) {
      const prefix = `threshold-${threshold}`;
      const phaseTag = `threshold_${threshold}`;
      const networkStartIndex = network.snapshot().length;
      network.setPhase(`${phaseTag}_before_select`);

      const selectionRaw = await setEntriesLeftThreshold(launched.page, threshold);
      const selection = mapSelectionFields(selectionRaw);

      await dismissEntriesLeftPopup(launched.page);
      await launched.page.waitForTimeout(Math.max(THRESHOLD_FILTER_SETTLE_MS, 1200));
      network.setPhase(`${phaseTag}_after_settle`);

      const html = await launched.page.content();
      const screenshotPath = path.join(outDir, `${prefix}.png`);
      await launched.page.screenshot({ path: screenshotPath, fullPage: true });

      const domPayload = await launched.page.evaluate(scrapeCalendarFixtureDom);
      domPayload.threshold = threshold;
      domPayload.isoDate = isoDate;
      domPayload.weekMode = weekMode;
      domPayload.navigation = {
        rawMonthLabel: nav.rawMonthLabel ?? null,
        rawDayHeaderTexts: nav.rawDayHeaderTexts ?? [],
        visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders ?? [],
        targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders ?? null,
        currentUrl: nav.currentUrl ?? null,
      };
      domPayload.selection = selection;

      const networkSnapshot = network.snapshot();
      const thresholdNetworkEntries = networkSnapshot.slice(networkStartIndex);
      const networkPayload = {
        capturedAt: new Date().toISOString(),
        threshold,
        isoDate,
        networkStartIndex,
        requestCount: thresholdNetworkEntries.length,
        phases: [...new Set(thresholdNetworkEntries.map((entry) => entry.phase))],
        entries: thresholdNetworkEntries,
      };

      fs.writeFileSync(path.join(outDir, `${prefix}.html`), html, 'utf8');
      writeJson(path.join(outDir, `${prefix}-dom.json`), domPayload);
      writeJson(path.join(outDir, `${prefix}-network.json`), networkPayload);

      allNetworkEntries.push(...thresholdNetworkEntries);

      captureSummary.thresholdCaptures.push({
        threshold,
        files: {
          html: `${prefix}.html`,
          png: `${prefix}.png`,
          dom: `${prefix}-dom.json`,
          network: `${prefix}-network.json`,
        },
        selection,
        navigation: domPayload.navigation,
        domSummary: domPayload.summary,
        networkEntryCount: thresholdNetworkEntries.length,
      });
    }

    const networkAnalysis = analyzeNetworkForSummary(allNetworkEntries);
    captureSummary.networkDataCandidateFound = networkAnalysis.networkDataCandidateFound;
    captureSummary.candidateResponses = networkAnalysis.candidateResponses;
    captureSummary.thresholdsSummary = captureSummary.thresholdCaptures;
    captureSummary.finishedAt = new Date().toISOString();
    captureSummary.ok = captureSummary.thresholdCaptures.every(
      (item) => item.selection?.filterSetOk && item.domSummary?.hasLeftWaveHeader && item.domSummary?.hasRightWaveHeader,
    );

    writeJson(path.join(outDir, 'capture-summary.json'), captureSummary);

    console.log(JSON.stringify({
      ok: captureSummary.ok,
      outputDir: outDir,
      thresholds,
      networkDataCandidateFound: captureSummary.networkDataCandidateFound,
      candidateResponseCount: captureSummary.candidateResponses.length,
      thresholdCaptures: captureSummary.thresholdCaptures.map((item) => ({
        threshold: item.threshold,
        filterSetOk: item.selection?.filterSetOk,
        sessionTileCount: item.domSummary?.sessionTileCount,
        sessionTileLeftCount: item.domSummary?.sessionTileLeftCount,
        sessionTileRightCount: item.domSummary?.sessionTileRightCount,
        hasLeftWaveHeader: item.domSummary?.hasLeftWaveHeader,
        hasRightWaveHeader: item.domSummary?.hasRightWaveHeader,
      })),
    }, null, 2));

    process.exit(captureSummary.ok ? 0 : 1);
  } finally {
    await safeCloseBrowser(launched);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
