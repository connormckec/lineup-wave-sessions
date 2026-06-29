# Threshold Scanner Stabilization Plan

Project: **Atlantic Park Surf Dashboard**  
Process: **Layer-specific contracts with hard gates.** Each gate gets its own commit. Do not combine dropdown reconnaissance, tile parsing, and inference in one patch.

Current baseline (2026-06-30):

- Gate 1 (calendar/navigation): **passing**
- Gate 2+ (entries-left control): **in progress**
- Threshold writes: **disabled** unless Gate 8 criteria met

---

## Gate 0 — Production safety

**Purpose:** Prevent bad data from reaching Supabase during stabilization.

**Rules:**

- Threshold writes disabled unless `thresholdWriteSafe === true`
- Dry-run is the default for threshold debugging
- No fake/default slot counts (10/12/2)
- No real backfill during stabilization
- Modal detail enrichment stays off (`BACKGROUND_DETAIL_ENRICHMENT_ENABLED` default false)
- Do not touch frontend during stabilization

**Acceptance:**

- `applyThresholdScanReport()` returns early when `thresholdWriteSafe === false`
- Admin debug routes default `dryRun: true`
- No `available_entries` written unless Gate 8 passes

---

## Gate 1 — Calendar / navigation contract

**Purpose:** Prove the scanner can reach the correct week without timeouts or evaluate errors.

**Acceptance for `isoDate: "2026-06-30"`:**

| Field | Expected |
|-------|----------|
| `currentUrl` | contains `booking.atlanticparksurf.com/activity-agenda` |
| `rawMonthLabel` | `"June 2026"` |
| `rawDayHeaderTexts` | `Mon. 29` … `Sun. 5` (7 headers) |
| `visibleIsoDatesFromHeaders` | includes `2026-06-30` |
| `targetDateVisibleFromHeaders` | `true` |
| Errors | no navigation timeout, no `page.evaluate is not a function` |

**Status:** Passing (as of commits through `645b51f`).

**Test command:**

```bash
curl -s -X POST https://lineup-wave-sessions-production.up.railway.app/api/admin/scan-entries-left-thresholds \
  -H 'Content-Type: application/json' \
  -d '{"isoDate":"2026-06-30","weekMode":true,"minThreshold":1,"maxThreshold":3,"wait":true,"dryRun":true,"debug":true}' \
  | jq '{
    statusReason,
    error,
    crashed,
    currentUrl,
    rawMonthLabel,
    rawDayHeaderTexts,
    visibleIsoDatesFromHeaders,
    targetDateVisibleFromHeaders,
    earlyExitStage,
    earlyExitReason
  }'
```

**Pass criteria:**

- `targetDateVisibleFromHeaders: true`
- `error: null` (or no evaluate/navigation timeout in `earlyExitReason`)
- `crashed: false`

---

## Gate 2 — Entries-left control reconnaissance

**Purpose:** Inspect the actual Entries left dropdown DOM before changing click logic.

**Deliverable:** `POST /api/admin/debug-entries-left-control` (Gate 2 commit only — no selection fix).

**Input:**

```json
{
  "isoDate": "2026-06-30",
  "weekMode": true,
  "thresholds": [1, 2, 3],
  "dryRun": true,
  "debug": true,
  "wait": true
}
```

**Output shape:**

```js
{
  gate: 2,
  isoDate,
  currentLabel,
  controlCandidates: [{ text, role, tagName, className, ariaLabel, id, selectorHint, boundingBox, visible, source }],
  afterOpen: {
    popupVisible,
    openMethod,
    optionCandidates: [{ text, normalizedText, role, tagName, className, ariaSelected, selectorHint, boundingBox, visible, source }],
    bodyTextSampleAroundEntriesLeft
  },
  thresholdAvailability: [{ threshold, optionFound, matchingOptionTexts }]
}
```

**Acceptance:**

- Exact control element(s) for Entries left are listed
- All exposed option texts are visible in `afterOpen.optionCandidates`
- We know whether option `2` exists (text, button, menu item, list item, hidden input, custom component)
- **No slot inference, no threshold writes, no selection contract yet**

**Test command:**

```bash
curl -s -X POST https://lineup-wave-sessions-production.up.railway.app/api/admin/debug-entries-left-control \
  -H 'Content-Type: application/json' \
  -d '{"isoDate":"2026-06-30","weekMode":true,"thresholds":[1,2,3],"dryRun":true,"debug":true,"wait":true}' \
  | jq
```

---

## Gate 3 — Entries-left selection contract

**Purpose:** Prove `setEntriesLeftThreshold(page, n)` can set the visible label.

**Only after Gate 2 identifies the DOM pattern.**

**Return shape:**

```js
{
  requestedThreshold: n,
  beforeLabel,
  optionTexts,
  matchingOptionText,
  clickSelectorUsed,
  afterLabel,
  filterSetOk,
  filterSetError
}
```

**Acceptance:**

| N | Expected |
|---|----------|
| 1 | `afterLabel = "Entries left : 1"`, `filterSetOk: true` |
| 2 | `afterLabel = "Entries left : 2"`, `filterSetOk: true` |
| 3 | `afterLabel = "Entries left : 3"`, `filterSetOk: true` |

If N=2 is not selectable: `filterSetError: "entries_left_option_not_found"`, `optionTexts` shows what was available.

**No tile parsing in this gate.**

---

## Gate 4 — Grid-change contract

**Purpose:** Prove the grid responds (or document why it does not) after label changes.

**Only after Gate 3 passes.**

For N=1,2,3 collect:

- `beforeGridHash`, `afterGridHash`
- `beforeTileCount`, `afterTileCount`
- `gridChanged`, `visibleTileCount`

**Acceptance:** Label changes correctly; grid changes or returns explicit reason. No slot inference.

---

## Gate 5 — Tile parser contract

**Purpose:** Parse visible bookable session identities at N=1 only.

**Only after Gate 4 passes.**

Each identity:

```js
{ isoDate, timeLabel, waveSide, sessionCode, identityKey, sourceText }
```

**Acceptance:** Excludes X cells, day headers, time labels, filter chips, hidden/stale DOM; dedupes parent/child; returns samples for review.

---

## Gate 6 — Threshold inference contract

**Purpose:** Prove inference logic on N=1..3 before scaling to 20.

**Only after Gate 5 passes.**

**Acceptance:**

- Selected labels correct for each N
- `visibleTileCountsByThreshold` exists
- `thresholdPresenceBySessionSample` exists
- Exact inference only when visible at N and gone at N+1
- Flat counts → `threshold_filter_not_effective`
- No writes

---

## Gate 7 — Full scan 1..20

**Purpose:** Scale to full threshold range.

**Only after Gate 6 passes.**

**Acceptance:**

- Labels correct through available thresholds
- Scanner stops cleanly when option unavailable
- `exactCount > 0` OR clear no-write reason
- `thresholdWriteSafe: true` only when exact inference is proven

---

## Gate 8 — Real writes

**Purpose:** Allow Supabase threshold slot writes.

**Only after Gate 7 passes.**

Writes allowed only if **all** of:

```js
targetDateVisibleFromHeaders === true
thresholdWriteSafe === true
exactCount > 0
thresholdWriteBlockReason == null
dryRun === false
```

---

## Development rules

1. One gate per commit
2. No integrated “fix everything” patches during stabilization
3. No backfill until Gate 8
4. No frontend changes during stabilization
5. Document pass/fail with the gate’s curl command before moving to the next gate

## Commit map (planned)

| Gate | Commit focus |
|------|----------------|
| 0 | (existing) write guardrails in `applyThresholdScanReport` |
| 1 | (existing) header/navigation + audit trail |
| 2 | `debug-entries-left-control` route + this plan |
| 3 | `setEntriesLeftThreshold` selection contract |
| 4 | grid-change contract |
| 5 | tile parser contract |
| 6 | inference contract N=1..3 |
| 7 | full scan 1..20 |
| 8 | enable writes behind Gate 8 flag |
