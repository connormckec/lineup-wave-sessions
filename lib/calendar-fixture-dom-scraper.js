'use strict';

/**
 * Browser-side DOM fixture scraper. Passed to page.evaluate — must be self-contained.
 */
function scrapeCalendarFixtureDom() {
  const DAY_HEADER_RE = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.\s*\d+\s*$/i;
  const TIME_LABEL_RE = /^(\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{1,2}am|\d{1,2}pm)$/i;
  const VISIBLE_CODE_RE = /^(AT|AB|ET|EB|PRG|INT|PT|PB|BGN)\*?$/i;
  const FILTER_TEXT_RE = /entries?\s*left|at least \d+ entries|show only activities|show only events/i;

  function normalizeText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }
  function isVisible(el) {
    if (!el?.getBoundingClientRect) return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 1 || rect.height < 1) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
    let node = el;
    while (node && node !== document.body) {
      const st = window.getComputedStyle(node);
      if (st.display === 'none' || st.visibility === 'hidden') return false;
      node = node.parentElement;
    }
    return true;
  }
  function rectOf(el) {
    const r = el.getBoundingClientRect();
    return {
      x: Math.round(r.left),
      y: Math.round(r.top),
      width: Math.round(r.width),
      height: Math.round(r.height),
    };
  }
  function centerOf(rect) {
    return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
  }
  function distance(a, b) {
    return Math.hypot(a.x - b.x, a.y - b.y);
  }
  function findCalendarContainer() {
    let best = null;
    let bestLen = 0;
    for (const el of document.querySelectorAll('div, section, main, table, tbody, article')) {
      if (!isVisible(el)) continue;
      const text = el.innerText || '';
      if (!text.includes('Left Wave Sessions') || !text.includes('Right Wave Sessions')) continue;
      if (text.length > bestLen) {
        best = el;
        bestLen = text.length;
      }
    }
    return best;
  }
  function categorizeElement(el, text) {
    const t = normalizeText(text);
    if (!t && !el.matches?.('div.dynamic-cal-booking-ts[data-original-title]')) return null;
    if (el.matches?.('div.dynamic-cal-booking-ts[data-original-title]')) return 'session_tile';
    if (/^left wave sessions$/i.test(t) || /^right wave sessions$/i.test(t)) return 'wave_header';
    if (DAY_HEADER_RE.test(t)) return 'day_header';
    if (TIME_LABEL_RE.test(t)) return 'time_label';
    if (/^at least \d+ entries?\s*left$/i.test(t)) return 'dropdown_option';
    if (FILTER_TEXT_RE.test(t)) return 'filter_control';
    if (VISIBLE_CODE_RE.test(t.replace(/\s+/g, ''))) return 'session_code_leaf';
    if ((t.includes('Left Wave Sessions') || t.includes('Right Wave Sessions')) && t.length < 120) {
      return 'calendar_container_candidate';
    }
    if (t.length > 0 && t.length < 200) return 'other_visible_text';
    return null;
  }
  function nearestByCategory(centers, categoryItems) {
    if (!categoryItems.length) return { label: null, distance: null, text: null };
    let best = null;
    let bestDist = Number.POSITIVE_INFINITY;
    for (const item of categoryItems) {
      const d = distance(centers, item.center);
      if (d < bestDist) {
        best = item;
        bestDist = d;
      }
    }
    return best
      ? { label: best.text, distance: Math.round(bestDist), text: best.text, boundingBox: best.boundingBox }
      : { label: null, distance: null, text: null };
  }

  const container = findCalendarContainer();
  const roots = [];
  if (container) roots.push(container);
  for (const el of document.querySelectorAll('select, button, [role="combobox"], [role="listbox"], .dropdown-menu')) {
    if (isVisible(el)) roots.push(el.closest('form, div, section') || el);
  }

  const seen = new Set();
  const elements = [];
  const categorized = {
    wave_header: [],
    day_header: [],
    time_label: [],
    session_tile: [],
    session_code_leaf: [],
    filter_control: [],
    dropdown_option: [],
    calendar_container_candidate: [],
    other_visible_text: [],
  };

  for (const root of roots) {
    if (!root) continue;
    for (const el of root.querySelectorAll('th, td, div, span, button, label, select, option, a, li')) {
      if (seen.has(el)) continue;
      seen.add(el);
      if (!isVisible(el)) continue;

      const text = normalizeText(el.textContent || '');
      const innerText = normalizeText(el.innerText || '');
      const category = categorizeElement(el, innerText || text);
      if (!category) continue;

      const boundingBox = rectOf(el);
      const style = window.getComputedStyle(el);
      const record = {
        text: text.slice(0, 240),
        innerText: innerText.slice(0, 240),
        title: el.getAttribute?.('title') || null,
        dataOriginalTitle: el.dataset?.originalTitle || null,
        tagName: el.tagName,
        className: typeof el.className === 'string' ? el.className : '',
        role: el.getAttribute?.('role') || null,
        id: el.id || null,
        ariaLabel: el.getAttribute?.('aria-label') || null,
        boundingBox,
        computedStyle: {
          display: style.display,
          visibility: style.visibility,
          opacity: style.opacity,
        },
        visible: true,
        category,
      };

      if (category === 'session_tile') {
        const childTexts = [...el.children]
          .map((child) => normalizeText(child.innerText || child.textContent || ''))
          .filter(Boolean)
          .slice(0, 12);
        record.tileDetails = {
          visibleText: innerText.slice(0, 80),
          dataOriginalTitle: el.dataset?.originalTitle || null,
          classList: [...(el.classList || [])],
          parentText: normalizeText(el.parentElement?.innerText || '').slice(0, 160),
          childTexts,
        };
      }

      elements.push(record);
      categorized[category].push({
        text: innerText.slice(0, 120) || text.slice(0, 120),
        boundingBox,
        center: centerOf(boundingBox),
      });
    }
  }

  for (const record of elements) {
    if (record.category !== 'session_tile' || !record.tileDetails) continue;
    const c = centerOf(record.boundingBox);
    record.tileDetails.nearestWaveHeader = nearestByCategory(c, categorized.wave_header);
    record.tileDetails.nearestDayHeader = nearestByCategory(c, categorized.day_header);
    record.tileDetails.nearestTimeLabel = nearestByCategory(c, categorized.time_label);
    const waveText = (record.tileDetails.nearestWaveHeader.text || '').toLowerCase();
    record.tileDetails.inferredWaveSide = /right wave/.test(waveText)
      ? 'right'
      : (/left wave/.test(waveText) ? 'left' : null);
  }

  const sessionTiles = elements.filter((e) => e.category === 'session_tile');
  const leftTiles = sessionTiles.filter((e) => e.tileDetails?.inferredWaveSide === 'left');
  const rightTiles = sessionTiles.filter((e) => e.tileDetails?.inferredWaveSide === 'right');

  return {
    capturedAt: new Date().toISOString(),
    currentUrl: location.href,
    pageTitle: document.title,
    calendarContainerFound: Boolean(container),
    elementCount: elements.length,
    elements,
    summary: {
      waveHeaderCount: categorized.wave_header.length,
      dayHeaderCount: categorized.day_header.length,
      timeLabelCount: categorized.time_label.length,
      sessionTileCount: sessionTiles.length,
      sessionTileLeftCount: leftTiles.length,
      sessionTileRightCount: rightTiles.length,
      sessionCodeLeafCount: categorized.session_code_leaf.length,
      byCategory: Object.fromEntries(
        Object.entries(categorized).map(([key, items]) => [key, items.length]),
      ),
      waveHeaders: categorized.wave_header.map((item) => item.text),
      hasLeftWaveHeader: categorized.wave_header.some((item) => /left wave sessions/i.test(item.text)),
      hasRightWaveHeader: categorized.wave_header.some((item) => /right wave sessions/i.test(item.text)),
    },
  };
}

module.exports = {
  scrapeCalendarFixtureDom,
};
