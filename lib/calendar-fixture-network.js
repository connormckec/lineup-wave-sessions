'use strict';

const PREVIEW_MAX = 10_000;

function looksLikeSchedulePayload(text, url) {
  const blob = `${url}\n${text || ''}`.toLowerCase();
  return /slot|avail|session|agenda|calendar|booking|activity|schedule|timeslot|capacity|remaining|entries/.test(blob);
}

function summarizeJsonPayload(parsed) {
  if (parsed == null) return { parsedTopLevelKeys: null, sampleRows: null, itemCount: null };
  if (Array.isArray(parsed)) {
    return {
      parsedTopLevelKeys: ['<array>'],
      sampleRows: parsed.slice(0, 3),
      itemCount: parsed.length,
    };
  }
  if (typeof parsed !== 'object') {
    return { parsedTopLevelKeys: [typeof parsed], sampleRows: [parsed], itemCount: 1 };
  }
  const keys = Object.keys(parsed);
  let sampleRows = null;
  let itemCount = null;
  for (const key of keys) {
    if (Array.isArray(parsed[key])) {
      sampleRows = parsed[key].slice(0, 3);
      itemCount = parsed[key].length;
      break;
    }
  }
  return { parsedTopLevelKeys: keys.slice(0, 40), sampleRows, itemCount };
}

function createFixtureNetworkCapture() {
  const entries = [];
  let phase = 'init';

  function attach(page) {
    page.on('response', async (response) => {
      try {
        const request = response.request();
        const resourceType = request.resourceType();
        if (!['xhr', 'fetch', 'document'].includes(resourceType)) return;

        const url = response.url();
        if (/\.(png|jpg|jpeg|gif|webp|svg|woff|woff2|css|ico)(\?|$)/i.test(url)) return;

        const method = request.method();
        const status = response.status();
        const contentType = response.headers()['content-type'] || '';
        const requestPostData = request.postData?.() ?? null;

        let responsePreview = '';
        try {
          responsePreview = (await response.text()).slice(0, PREVIEW_MAX);
        } catch {
          responsePreview = '';
        }

        const trimmed = responsePreview.trim();
        const looksLikeJson = trimmed.startsWith('{') || trimmed.startsWith('[');
        let parsedSummary = { parsedTopLevelKeys: null, sampleRows: null, itemCount: null };
        if (looksLikeJson) {
          try {
            parsedSummary = summarizeJsonPayload(JSON.parse(responsePreview));
          } catch {
            parsedSummary = { parsedTopLevelKeys: ['<invalid_json>'], sampleRows: null, itemCount: null };
          }
        }

        entries.push({
          phase,
          capturedAt: new Date().toISOString(),
          url,
          method,
          status,
          resourceType,
          contentType,
          requestPostData,
          responsePreview,
          responsePreviewLength: responsePreview.length,
          looksLikeJson,
          scheduleLike: looksLikeSchedulePayload(responsePreview, url),
          ...parsedSummary,
        });
      } catch {
        // ignore capture errors
      }
    });
  }

  return {
    entries,
    attach,
    setPhase(nextPhase) {
      phase = nextPhase;
    },
    snapshot() {
      return entries.slice();
    },
  };
}

function analyzeNetworkForSummary(allEntries) {
  const candidateResponses = allEntries.filter(
    (entry) => entry.scheduleLike && (entry.looksLikeJson || /json/i.test(entry.contentType || '')),
  );
  return {
    networkDataCandidateFound: candidateResponses.length > 0,
    candidateResponses: candidateResponses.map((entry) => ({
      phase: entry.phase,
      url: entry.url,
      method: entry.method,
      status: entry.status,
      contentType: entry.contentType,
      parsedTopLevelKeys: entry.parsedTopLevelKeys,
      itemCount: entry.itemCount,
      sampleRows: entry.sampleRows,
      responsePreviewLength: entry.responsePreviewLength,
    })),
  };
}

module.exports = {
  createFixtureNetworkCapture,
  analyzeNetworkForSummary,
  PREVIEW_MAX,
};
