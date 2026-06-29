# Threshold Scanner End-to-End Audit

Date: 2026-06-27  
Scope: `POST /api/admin/scan-entries-left-thresholds` with  
`{"isoDate":"2026-06-30","weekMode":true,"minThreshold":1,"maxThreshold":3,"wait":true,"dryRun":true}`

## Executive summary

Production was returning `calendar_headers_not_ready` with **null diagnostics** because `withPlaywrightGuard()` timed out during `navigateCalendarToShowDate()` and the catch path never sampled the live Playwright page. The page was often loaded, but the 45s navigation budget was consumed by repeated header scrapes and week pagination before diagnostics were copied into the API response.

This audit adds `collectPageDiagnostics()`, an `auditTrail`, and timeout/catch enrichment so failures are explainable from the response alone.

---

## 1. Exact execution path

```
POST /api/admin/scan-entries-left-thresholds
  └─ runScan()
       ├─ tryAcquireScrapeLock('entries-left threshold scan')
       ├─ ensureSessionsForStatus()
       └─ runThresholdScansChunked([requestedIsoDate], { navigationIsoDate: computedWeekStart, ... })
            ├─ launchBrowser()
            ├─ openBookingPageForThreshold(page)
            │    ├─ openBookingPage() → page.goto(BOOKING), wait .dynamic-cal-booking-ts
            │    └─ waitForThresholdCalendarShell()
            └─ runThresholdScanForWeek(page)
                 └─ scanEntriesLeftThresholdsForWeek(page)
                      ├─ waitForThresholdCalendarShell()  [again]
                      └─ withPlaywrightGuard( navigateCalendarToShowDate, 45000ms )  ← TIMEOUT HERE
                           └─ navigateCalendarToShowDate(navigationIsoDate=2026-06-29, validateIsoDate=2026-06-30)
                                ├─ readVisibleDates()
                                │    ├─ waitForThresholdCalendarShell()
                                │    └─ scrapeCalendarHeadersWithRetry (up to 4 attempts × 1.5s)
                                ├─ if target not in headers → openBookingPage() again
                                ├─ loop advanceCalendarWeek (.glyphicon-chevron-right) × (effectiveWeeksAhead+3)
                                │    └─ readVisibleDates() after each click
                                └─ loop retreatCalendarWeek (.glyphicon-chevron-left) × (effectiveWeeksAhead+3)
```

**Date model (correct):**

| Field | Value for 2026-06-30 request |
|-------|------------------------------|
| `requestedIsoDate` | `2026-06-30` (never overwritten) |
| `computedWeekStart` | `2026-06-29` (Monday UTC) |
| `navigationIsoDate` | `2026-06-29` (weekMode navigates to week start) |
| `validateIsoDate` | `2026-06-30` (header week must contain requested date) |

---

## 2. Why null diagnostics on timeout

| Step | What happened | Diagnostics |
|------|---------------|-------------|
| `withPlaywrightGuard` | `withTimeout()` rejects with `threshold_week_navigation_timeout_after_45000ms` | None attached (before this patch) |
| `scanEntriesLeftThresholdsForWeek` catch | Sets `earlyExitReason` / `error` from `e.message` | Never called `page.url()` or `page.evaluate()` |
| `navigateCalendarToShowDate` | Aborted mid-flight; `diag` object never returned | `report.currentUrl` etc. stay null |
| `flattenThresholdScanApiResponse` | Reads `week.currentUrl ?? nav.currentUrl` | Both null → API nulls |

**Root cause:** diagnostics were only populated when `navigateCalendarToShowDate()` **returned successfully**. Timeout is an exception path with no page sampling.

---

## 3. Failure mode checklist

| Hypothesis | Assessment |
|------------|------------|
| Failing to open booking page | Unlikely primary cause — `openBookingPageForThreshold` runs **before** the guarded navigation and uses the same 45s budget for goto + shell wait |
| Stuck before calendar render | Possible if `.dynamic-cal-booking-ts` never appears; audit `booking_calendar_wait` step shows body text |
| Stuck in month/week navigation | **Likely** — navigation loops call `readVisibleDates()` each iteration (4 header attempts + shell waits each) |
| Waiting on wrong selector | Shell wait accepts `.dynamic-cal-booking-ts, table` OR month/weekday text in body |
| Clicking wrong next/prev | Uses `.glyphicon-chevron-right` / `.glyphicon-chevron-left`; audit `navigation_click` records before/after fingerprint labels |
| Entering month-picker state | Not explicitly handled; `bodyTextSample` will show if month dropdown opened |
| Applying filters too early | **No** — filters run only after headers + `targetDateVisibleFromHeaders` |
| Diagnostics dropped in flatten | Partially — flatten merges week/nav fields but had nothing to merge on timeout |
| Timeout inside uninstrumented race | **Yes** — `withTimeout()` in `withPlaywrightGuard` had no page snapshot |

---

## 4. Audit report answers

1. **Which function throws or times out?**  
   `withPlaywrightGuard()` wrapping `navigateCalendarToShowDate()` with stage `threshold_week_navigation` and `THRESHOLD_SCAN_PAGE_TIMEOUT_MS` (45000ms).

2. **Is the booking page actually loaded?**  
   Usually yes — `openBookingPageForThreshold` completes first. After patch, `auditTrail` step `page_opened` / `booking_calendar_wait` confirms URL and body text even when navigation times out.

3. **What is `page.url()` at timeout?**  
   Previously unknown (null in API). Now captured in `timeout_or_success` audit step and top-level `currentUrl`.

4. **What text is visible in `document.body.innerText` at timeout?**  
   Previously unknown. Now in `bodyTextSample` (1500 chars) and `calendarReadySignals`.

5. **Did we ever see `June 2026`?**  
   Check `calendarReadySignals.hasMonthYearText` and `rawMonthLabel` in `header_scrape_attempt` steps.

6. **Did we ever see `Left Wave Sessions`?**  
   Check `calendarReadySignals.hasLeftWaveSessionsText`.

7. **Did we ever see weekday headers?**  
   Check `calendarReadySignals.hasWeekdayText`, `rawDayHeaderTexts`, `header_scrape_attempt` steps.

8. **Did navigation click next/prev? Which selector?**  
   Check `auditTrail` entries with `step: "navigation_click"`, `selectorUsed: ".glyphicon-chevron-right"` or `".glyphicon-chevron-left"`.

9. **Are we validating requested date or Monday anchor?**  
   Navigate to Monday (`navigationIsoDate=2026-06-29`); validate week contains **requested** date (`validateIsoDate=2026-06-30`).

10. **Are diagnostics created but dropped by flatten?**  
    On timeout, they were **never created**. Flatten logic is fine when week report is populated. Patch adds diagnostics at catch/timeout before flatten.

---

## 5. Instrumentation added (this commit)

- `collectPageDiagnostics(page, label)` — safe URL/title/body/signals sampler
- `auditTrail` on dry-run API responses when `debug: true`
- `withPlaywrightGuard` enriches timeout errors with page diagnostics
- `scanEntriesLeftThresholdsForWeek` catch calls `enrichFailureWithPageDiagnostics`
- Local-only: Playwright trace, screenshots, HTML snapshots in `debug-threshold-scans/`

---

## 6. Recommended next functional fixes (not in this commit)

These are follow-ups after reviewing post-deploy `auditTrail`:

1. **Separate navigation timeout from header scrape budget** — 45s for entire `navigateCalendarToShowDate` is tight when each `readVisibleDates` can take ~20–35s.
2. **Skip redundant `openBookingPage` inside navigation** when threshold runner already opened booking page.
3. **Reduce header retry cost during pagination** — single scrape per navigation click, not 4× retry each time.
4. **Consider raising `THRESHOLD_SCAN_PAGE_TIMEOUT_MS`** or splitting stages into separate guarded calls with per-stage diagnostics.

---

## 7. Acceptance curl

```bash
curl -s -X POST https://lineup-wave-sessions-production.up.railway.app/api/admin/scan-entries-left-thresholds \
  -H 'Content-Type: application/json' \
  -d '{"isoDate":"2026-06-30","weekMode":true,"minThreshold":1,"maxThreshold":3,"wait":true,"dryRun":true,"debug":true}' \
  | jq '{statusReason,requestedIsoDate,computedWeekStart,navigationIsoDate,currentUrl,pageTitle,bodyTextLength,bodyTextSample,calendarReadySignals,auditTrail,headerScrapeAttempts,rawMonthLabel,rawDayHeaderTexts,visibleIsoDatesFromHeaders,targetDateVisibleFromHeaders,earlyExitStage,earlyExitReason,error,crashed}'
```

**Pass:** On timeout or failure, `currentUrl`, `bodyTextSample`, `calendarReadySignals`, and `auditTrail` are non-null (unless `pageAvailable: false` with explicit reason).
