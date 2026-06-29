'use strict';

const VISIBLE_CODE_RE = /^(AT|AB|ET|EB|PRG|INT|PT|PB|BGN)\*?$/i;
const DAY_HEADER_RE = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.\s*\d+\s*$/i;
const FILTER_TEXT_RE = /entries?\s*left|at least \d+ entries|show only activities|show only events/i;

const LEVEL_TO_CODE = {
  'advanced trick': 'AT',
  'advanced tricks': 'AT',
  advanced: 'AT',
  'advanced beginner': 'AB',
  'expert trick': 'ET',
  'expert tricks': 'ET',
  expert: 'ET',
  'expert beginner': 'EB',
  progressive: 'PRG',
  intermediate: 'INT',
  'pro turns': 'PT',
  'pro turn': 'PT',
  'progressive beginner': 'PB',
  beginner: 'BGN',
};

function normalizeText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function normalizeTimeLabel(time) {
  if (!time || time === '?') return '?';
  return normalizeText(time).toLowerCase();
}

function normalizeVisibleSessionCode(text) {
  const raw = normalizeText(text);
  if (!raw) return null;
  const compact = raw.replace(/\s+/g, '');
  const m = compact.match(VISIBLE_CODE_RE);
  if (!m) return null;
  return { sessionCode: m[1].toUpperCase(), sourceText: raw };
}

function levelToSessionCode(level) {
  const norm = normalizeText(level).toLowerCase();
  if (!norm) return null;
  if (LEVEL_TO_CODE[norm]) return LEVEL_TO_CODE[norm];
  for (const [key, code] of Object.entries(LEVEL_TO_CODE)) {
    if (norm.includes(key) || key.includes(norm)) return code;
  }
  return null;
}

function titleInferredCode(titleText) {
  if (!titleText) return null;
  const m = titleText.match(/Session level\s*:<\/b>\s*([^<]+)/i);
  return m ? levelToSessionCode(m[1]) : null;
}

function timeFromTitle(titleText) {
  if (!titleText) return null;
  const m = titleText.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
  return m ? normalizeText(m[1]) : null;
}

function isoFromTimestampSec(tsSec) {
  if (!Number.isFinite(tsSec)) return null;
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(tsSec * 1000));
}

function timestampFromClassName(className) {
  const m = String(className || '').match(/booking-agenda-clickable_(\d+)_(\d+)/);
  return m ? { ts: Number(m[1]), wave: Number(m[2]) } : null;
}

function buildIdentityKey(isoDate, timeLabel, waveSide, sessionCode) {
  return `${isoDate}|${normalizeTimeLabel(timeLabel)}|${waveSide || '?'}|${sessionCode || '?'}`;
}

function buildDayHeaderIsoMap(fixture) {
  const map = new Map();
  const texts = fixture?.navigation?.rawDayHeaderTexts || [];
  const isos = fixture?.navigation?.visibleIsoDatesFromHeaders || [];
  for (let i = 0; i < texts.length; i += 1) {
    const header = normalizeDayHeader(texts[i]);
    if (header && isos[i]) map.set(header, isos[i]);
  }
  return map;
}

function normalizeDayHeader(text) {
  return normalizeText(text);
}

function isoFromNearestDayHeader(nearestDayHeader, dayHeaderIsoMap) {
  const headerText = normalizeDayHeader(
    nearestDayHeader?.text || nearestDayHeader?.label || null,
  );
  if (!headerText) return null;
  return dayHeaderIsoMap.get(headerText) || null;
}

function shouldExcludeSessionTile(element) {
  const visible = normalizeText(
    element.tileDetails?.visibleText || element.innerText || element.text || '',
  );
  if (!visible) return 'empty_text';
  if (/^x$/i.test(visible)) return 'x_cell';
  if (DAY_HEADER_RE.test(visible)) return 'day_header';
  if (FILTER_TEXT_RE.test(visible)) return 'filter_label';
  if (/^at least \d+ entries?\s*left$/i.test(visible)) return 'dropdown_option';
  if ((element.className || '').includes('expired_timeslot')) return 'unavailable_tile';
  if ((element.className || '').includes('disabled') || (element.className || '').includes('unavailable')) {
    return 'unavailable_tile';
  }
  return null;
}

function visibleCodeMatchesSessionCode(sourceText, sessionCode) {
  const normalized = normalizeVisibleSessionCode(sourceText);
  if (!normalized) return true;
  return normalized.sessionCode === sessionCode;
}

function parseCalendarFixtureDom(fixture) {
  const warnings = [];
  const excluded = [];
  const parsed = [];
  const identitySeen = new Set();
  let duplicateCount = 0;

  const dayHeaderIsoMap = buildDayHeaderIsoMap(fixture);
  const sessionTiles = (fixture?.elements || []).filter((el) => el.category === 'session_tile');

  for (const element of sessionTiles) {
    const excludeReason = shouldExcludeSessionTile(element);
    if (excludeReason) {
      excluded.push({
        sourceText: normalizeText(element.tileDetails?.visibleText || element.innerText || element.text).slice(0, 120),
        reason: excludeReason,
        boundingBox: element.boundingBox || null,
      });
      continue;
    }

    const visibleText = normalizeText(
      element.tileDetails?.visibleText || element.innerText || element.text || '',
    );
    const visibleCode = normalizeVisibleSessionCode(visibleText);
    if (!visibleCode) {
      excluded.push({
        sourceText: visibleText.slice(0, 120),
        reason: 'unknown_session_code',
        boundingBox: element.boundingBox || null,
      });
      continue;
    }

    const titleText = element.dataOriginalTitle
      || element.tileDetails?.dataOriginalTitle
      || null;
    const titleCode = titleInferredCode(titleText);
    const tsInfo = timestampFromClassName(element.className);
    const isoFromTs = tsInfo ? isoFromTimestampSec(tsInfo.ts) : null;
    const isoFromHeader = isoFromNearestDayHeader(
      element.tileDetails?.nearestDayHeader,
      dayHeaderIsoMap,
    );
    const isoDate = isoFromTs || isoFromHeader;
    const timeFromNearest = normalizeText(
      element.tileDetails?.nearestTimeLabel?.text
      || element.tileDetails?.nearestTimeLabel?.label
      || '',
    ) || null;
    const timeLabel = timeFromTitle(titleText) || timeFromNearest || '?';
    const waveSide = element.tileDetails?.inferredWaveSide || null;

    if (!isoDate) {
      excluded.push({
        sourceText: visibleCode.sourceText,
        reason: 'missing_iso_date',
        boundingBox: element.boundingBox || null,
      });
      continue;
    }
    if (waveSide !== 'left' && waveSide !== 'right') {
      excluded.push({
        sourceText: visibleCode.sourceText,
        reason: 'wave_side_unmapped',
        boundingBox: element.boundingBox || null,
      });
      continue;
    }

    const identityKey = buildIdentityKey(isoDate, timeLabel, waveSide, visibleCode.sessionCode);
    if (identitySeen.has(identityKey)) {
      duplicateCount += 1;
      continue;
    }
    identitySeen.add(identityKey);

    const entry = {
      isoDate,
      timeLabel,
      waveSide,
      sessionCode: visibleCode.sessionCode,
      identityKey,
      sourceText: visibleCode.sourceText,
      titleText,
      boundingBox: element.boundingBox || null,
      confidence: isoFromTs && timeFromTitle(titleText) ? 'high' : 'medium',
      parseMethod: isoFromTs ? 'fixture_session_tile_title' : 'fixture_session_tile_spatial',
    };

    if (titleCode && titleCode !== visibleCode.sessionCode) {
      warnings.push({
        type: 'title_visible_code_mismatch',
        sourceText: visibleCode.sourceText,
        visibleNormalizedCode: visibleCode.sessionCode,
        titleInferredCode: titleCode,
        titleText,
        identityKey,
      });
    }

    parsed.push(entry);
  }

  const countsByDate = {};
  const countsByWaveSide = {};
  const countsBySessionCode = {};
  for (const item of parsed) {
    countsByDate[item.isoDate] = (countsByDate[item.isoDate] || 0) + 1;
    countsByWaveSide[item.waveSide] = (countsByWaveSide[item.waveSide] || 0) + 1;
    countsBySessionCode[item.sessionCode] = (countsBySessionCode[item.sessionCode] || 0) + 1;
  }

  const validation = buildFixtureParserValidation({
    parsed,
    excluded,
    warnings,
  });

  return {
    parsedCount: parsed.length,
    duplicateCount,
    excludedCount: excluded.length,
    countsByDate,
    countsByWaveSide,
    countsBySessionCode,
    parsedIdentitiesSample: parsed.slice(0, 30),
    identityKeys: parsed.map((item) => item.identityKey),
    excludedSamples: excluded.slice(0, 15),
    warnings,
    validation,
    ok: validation.ok,
    meta: {
      threshold: fixture?.threshold ?? null,
      isoDate: fixture?.isoDate ?? null,
      fixtureSummary: fixture?.summary ?? null,
      sessionTileInputCount: sessionTiles.length,
    },
  };
}

function buildFixtureParserValidation({ parsed, excluded, warnings }) {
  const parsedCount = parsed.length;
  const leftCount = parsed.filter((p) => p.waveSide === 'left').length;
  const rightCount = parsed.filter((p) => p.waveSide === 'right').length;
  const pbParsed = parsed.some((p) => p.sessionCode === 'PB')
    || parsed.some((p) => /PB\*?/i.test(p.sourceText));
  const xExcluded = excluded.some(
    (e) => e.reason === 'x_cell' || /^x$/i.test(String(e.sourceText || '').trim()),
  );
  const visibleCodeWins = parsed.every(
    (p) => visibleCodeMatchesSessionCode(p.sourceText, p.sessionCode),
  );

  const checks = {
    parsedCountPositive: parsedCount > 0,
    hasLeftWave: leftCount > 0,
    hasRightWave: rightCount > 0,
    pbParsed,
    xExcluded,
    visibleCodeWins,
  };

  const errors = [];
  if (!checks.parsedCountPositive) errors.push('parsed_count_zero');
  if (!checks.hasLeftWave) errors.push('missing_left_wave');
  if (!checks.hasRightWave) errors.push('missing_right_wave');
  if (!checks.visibleCodeWins) errors.push('visible_code_mismatch');

  return {
    ok: errors.length === 0,
    errors,
    warningCount: warnings.length,
    checks,
  };
}

module.exports = {
  parseCalendarFixtureDom,
  buildFixtureParserValidation,
  normalizeVisibleSessionCode,
};
