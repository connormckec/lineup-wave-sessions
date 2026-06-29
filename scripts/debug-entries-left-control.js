#!/usr/bin/env node
'use strict';

/**
 * Gate 2 — Entries-left control reconnaissance (local).
 *
 * Usage:
 *   node scripts/debug-entries-left-control.js
 *   node scripts/debug-entries-left-control.js --isoDate=2026-06-30
 *
 * Or via admin route on a running server:
 *   curl -s -X POST http://localhost:3000/api/admin/debug-entries-left-control \
 *     -H 'Content-Type: application/json' \
 *     -d '{"isoDate":"2026-06-30","weekMode":true,"thresholds":[1,2,3],"wait":true,"dryRun":true,"debug":true}' | jq
 */

const path = require('path');
process.chdir(path.join(__dirname, '..'));

const isoDate = process.argv.find((a) => a.startsWith('--isoDate='))?.split('=')[1] || '2026-06-30';
const port = process.env.PORT || 3000;
const baseUrl = process.env.BASE_URL || `http://127.0.0.1:${port}`;

async function main() {
  const res = await fetch(`${baseUrl}/api/admin/debug-entries-left-control`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      isoDate,
      weekMode: true,
      thresholds: [1, 2, 3],
      dryRun: true,
      debug: true,
      wait: true,
    }),
  });
  const body = await res.json();
  console.log(JSON.stringify(body, null, 2));
  process.exit(res.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
