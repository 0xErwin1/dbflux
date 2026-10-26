import assert from 'node:assert/strict';
import test from 'node:test';

import { journeyChapterTarget, journeyEligibility, journeyProgress } from './product-journey.ts';

test('clamps progress and normalizes every non-finite input', () => {
  assert.deepEqual(journeyProgress(-0.5), {
    progress: 0,
    activeIndex: 0,
    activeScene: 'explore',
    fromIndex: 0,
    fromScene: 'explore',
    toIndex: 1,
    toScene: 'builder',
    transition: 0,
  });
  assert.deepEqual(journeyProgress(1.5), {
    progress: 1,
    activeIndex: 4,
    activeScene: 'governance',
    fromIndex: 4,
    fromScene: 'governance',
    toIndex: 4,
    toScene: 'governance',
    transition: 0,
  });
  assert.deepEqual(journeyProgress(Number.NaN), journeyProgress(0));
  assert.deepEqual(journeyProgress(Number.POSITIVE_INFINITY), journeyProgress(1));
  assert.deepEqual(journeyProgress(Number.NEGATIVE_INFINITY), journeyProgress(0));
});

test('resolves all five real-capture scenes at their direct scroll positions', () => {
  const expected = [
    {
      progress: 0,
      activeIndex: 0,
      activeScene: 'explore',
      fromIndex: 0,
      fromScene: 'explore',
      toIndex: 1,
      toScene: 'builder',
      transition: 0,
    },
    {
      progress: 0.25,
      activeIndex: 1,
      activeScene: 'builder',
      fromIndex: 1,
      fromScene: 'builder',
      toIndex: 2,
      toScene: 'edit',
      transition: 0,
    },
    {
      progress: 0.5,
      activeIndex: 2,
      activeScene: 'edit',
      fromIndex: 2,
      fromScene: 'edit',
      toIndex: 3,
      toScene: 'visualize',
      transition: 0,
    },
    {
      progress: 0.75,
      activeIndex: 3,
      activeScene: 'visualize',
      fromIndex: 3,
      fromScene: 'visualize',
      toIndex: 4,
      toScene: 'governance',
      transition: 0,
    },
    {
      progress: 1,
      activeIndex: 4,
      activeScene: 'governance',
      fromIndex: 4,
      fromScene: 'governance',
      toIndex: 4,
      toScene: 'governance',
      transition: 0,
    },
  ];

  for (const state of expected) assert.deepEqual(journeyProgress(state.progress), state);
});

test('produces identical forward and reverse transition states', () => {
  const positions = [0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1];
  const forward = positions.map(journeyProgress);
  const reverse = positions.toReversed().map(journeyProgress);

  assert.deepEqual(reverse, forward.toReversed());
  assert.deepEqual(journeyProgress(0.125), {
    progress: 0.125,
    activeIndex: 1,
    activeScene: 'builder',
    fromIndex: 0,
    fromScene: 'explore',
    toIndex: 1,
    toScene: 'builder',
    transition: 0.5,
  });
  assert.deepEqual(journeyProgress(0.875), {
    progress: 0.875,
    activeIndex: 4,
    activeScene: 'governance',
    fromIndex: 3,
    fromScene: 'visualize',
    toIndex: 4,
    toScene: 'governance',
    transition: 0.5,
  });
});

test('maps direct chapter hashes to complete deterministic targets', () => {
  assert.deepEqual(journeyChapterTarget('#journey-explore'), {
    id: 'journey-explore',
    index: 0,
    scene: 'explore',
    progress: 0,
  });
  assert.deepEqual(journeyChapterTarget('#journey-builder'), {
    id: 'journey-builder',
    index: 1,
    scene: 'builder',
    progress: 0.25,
  });
  assert.deepEqual(journeyChapterTarget('#journey-edit'), {
    id: 'journey-edit',
    index: 2,
    scene: 'edit',
    progress: 0.5,
  });
  assert.deepEqual(journeyChapterTarget('#journey-visualize'), {
    id: 'journey-visualize',
    index: 3,
    scene: 'visualize',
    progress: 0.75,
  });
  assert.deepEqual(journeyChapterTarget('#governance'), {
    id: 'governance',
    index: 4,
    scene: 'governance',
    progress: 1,
  });
  assert.equal(journeyChapterTarget('#keyboard'), null);
});

test('enhances only above 1180px and keeps reduced motion static', () => {
  assert.deepEqual(journeyEligibility(1180, false), {
    mode: 'static',
    reason: 'viewport',
  });
  assert.deepEqual(journeyEligibility(1181, false), {
    mode: 'enhanced',
    reason: 'eligible',
  });
  assert.deepEqual(journeyEligibility(1181, true), {
    mode: 'static',
    reason: 'reduced-motion',
  });
  assert.deepEqual(journeyEligibility(390, true), {
    mode: 'static',
    reason: 'reduced-motion',
  });
});
