import assert from 'node:assert/strict';
import test from 'node:test';

import { tocScrollDirection, visibleTocRange } from './toc-progress.ts';

test('highlights only heading elements that overlap the readable viewport', () => {
  const sections = [
    { top: 40, bottom: 80, depth: 2 as const },
    { top: 120, bottom: 160, depth: 3 as const },
    { top: 200, bottom: 240, depth: 3 as const },
  ];

  assert.deepEqual(visibleTocRange(sections, { top: 100, bottom: 180 }), { start: 1, end: 1 });
});

test('includes a heading partially visible below the sticky header', () => {
  const sections = [{ top: 70, bottom: 110, depth: 2 as const }];

  assert.deepEqual(visibleTocRange(sections, { top: 80, bottom: 500 }), { start: 0, end: 0 });
});

test('returns no range when headings are outside or exactly on viewport boundaries', () => {
  const sections = [
    { top: 20, bottom: 80, depth: 2 as const },
    { top: 500, bottom: 540, depth: 3 as const },
  ];

  assert.equal(visibleTocRange([], { top: 80, bottom: 500 }), null);
  assert.equal(visibleTocRange(sections, { top: 80, bottom: 500 }), null);
});

test('keeps a single visible heading limited to its own ToC row', () => {
  const sections = [
    { top: 20, bottom: 60, depth: 2 as const },
    { top: 100, bottom: 140, depth: 3 as const },
    { top: 200, bottom: 240, depth: 3 as const },
  ];

  assert.deepEqual(visibleTocRange(sections, { top: 80, bottom: 180 }), { start: 1, end: 1 });
});

test('changes direction on document scroll reversals and preserves it while stationary', () => {
  assert.equal(tocScrollDirection(100, 180, 'up'), 'down');
  assert.equal(tocScrollDirection(180, 100, 'down'), 'up');
  assert.equal(tocScrollDirection(100, 100, 'up'), 'up');
  assert.equal(tocScrollDirection(100, 100, 'down'), 'down');
});
