#!/usr/bin/env node
'use strict';

/**
 * Gate 5B — Parse frozen calendar DOM fixtures locally (no Playwright).
 *
 * Usage:
 *   node scripts/test-calendar-tile-parser-fixture.js fixtures/atlantic/2026-06-30/threshold-1-dom.json
 *   node scripts/test-calendar-tile-parser-fixture.js fixtures/atlantic/2026-06-30/threshold-1-dom.json | jq
 */

const fs = require('fs');
const path = require('path');

const { parseCalendarFixtureDom } = require('../lib/calendar-fixture-tile-parser');

function main() {
  const inputArg = process.argv[2];
  if (!inputArg) {
    console.error('Usage: node scripts/test-calendar-tile-parser-fixture.js <fixture-dom.json>');
    process.exit(1);
  }

  const inputFile = path.resolve(process.cwd(), inputArg);
  if (!fs.existsSync(inputFile)) {
    console.error(`Fixture not found: ${inputFile}`);
    process.exit(1);
  }

  let fixture;
  try {
    fixture = JSON.parse(fs.readFileSync(inputFile, 'utf8'));
  } catch (err) {
    console.error(`Failed to read fixture JSON: ${err.message}`);
    process.exit(1);
  }

  const result = parseCalendarFixtureDom(fixture);
  const output = {
    ok: result.ok,
    inputFile,
    parsedCount: result.parsedCount,
    duplicateCount: result.duplicateCount,
    excludedCount: result.excludedCount,
    countsByDate: result.countsByDate,
    countsByWaveSide: result.countsByWaveSide,
    countsBySessionCode: result.countsBySessionCode,
    parsedIdentitiesSample: result.parsedIdentitiesSample,
    excludedSamples: result.excludedSamples,
    warnings: result.warnings,
    validation: {
      ...result.validation.checks,
    },
    validationErrors: result.validation.errors,
    meta: result.meta,
  };

  console.log(JSON.stringify(output, null, 2));
  process.exit(result.ok ? 0 : 1);
}

main();
