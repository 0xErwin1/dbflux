import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_DEMO_ANCHORS,
  DEMO_APP,
  DEMO_DURATION,
  DEMO_STEPS,
  demoEligibility,
  demoFrame,
  demoSnapshot,
  demoStepAt,
  demoStepTarget,
  demoTime,
} from './live-demo.ts';
import { DEMO_ROWS } from './live-demo-data.ts';

const stage = { width: 1312, height: 770 };

function frameAt(time: number) {
  return demoFrame(time, DEFAULT_DEMO_ANCHORS, stage);
}

function editedRow(time: number) {
  const index = DEMO_ROWS.findIndex((row) => row.id === 2);
  return frameAt(time).rows[index];
}

test('maps scroll progress to demo time and normalizes non-finite input', () => {
  assert.equal(demoTime(0), 0);
  assert.equal(demoTime(0.5), DEMO_DURATION / 2);
  assert.equal(demoTime(1), DEMO_DURATION);
  assert.equal(demoTime(-1), 0);
  assert.equal(demoTime(2), DEMO_DURATION);
  assert.equal(demoTime(Number.NaN), 0);
  assert.equal(demoTime(Number.POSITIVE_INFINITY), DEMO_DURATION);
  assert.equal(demoTime(Number.NEGATIVE_INFINITY), 0);
});

test('declares six contiguous steps that cover the whole timeline', () => {
  assert.deepEqual(
    DEMO_STEPS.map((step) => step.id),
    ['explore', 'filter', 'builder', 'edit', 'chart', 'audit'],
  );
  assert.equal(DEMO_STEPS[0]?.start, 0);
  assert.equal(DEMO_STEPS.at(-1)?.end, DEMO_DURATION);
  for (let index = 1; index < DEMO_STEPS.length; index += 1) {
    assert.equal(DEMO_STEPS[index]?.start, DEMO_STEPS[index - 1]?.end);
  }
  for (const step of DEMO_STEPS) {
    assert.ok(step.hero > step.start && step.hero < step.end, step.id);
  }
});

test('names the active step and how far through it the timeline is', () => {
  assert.deepEqual(demoStepAt(0), { index: 0, id: 'explore', fill: 0 });
  assert.deepEqual(demoStepAt(150), { index: 1, id: 'filter', fill: 0.5 });
  assert.deepEqual(demoStepAt(420), { index: 4, id: 'chart', fill: 0 });
  assert.deepEqual(demoStepAt(DEMO_DURATION), { index: 5, id: 'audit', fill: 1 });
});

test('keeps the audit chapter on the #governance anchor', () => {
  assert.equal(DEMO_STEPS.at(-1)?.anchor, 'governance');
  assert.deepEqual(demoStepTarget('#governance'), {
    id: 'audit',
    anchor: 'governance',
    index: 5,
    progress: 512 / DEMO_DURATION,
  });
  assert.deepEqual(demoStepTarget('journey-edit'), {
    id: 'edit',
    anchor: 'journey-edit',
    index: 3,
    progress: 302 / DEMO_DURATION,
  });
  assert.equal(demoStepTarget('#keyboard'), null);
  assert.equal(demoStepTarget(''), null);
});

test('enhances only wide viewports without a reduced-motion preference', () => {
  assert.deepEqual(demoEligibility(1440, false), { mode: 'enhanced', reason: 'eligible' });
  assert.deepEqual(demoEligibility(1181, false), { mode: 'enhanced', reason: 'eligible' });
  assert.deepEqual(demoEligibility(1180, false), { mode: 'static', reason: 'viewport' });
  assert.deepEqual(demoEligibility(1440, true), { mode: 'static', reason: 'reduced-motion' });
});

test('returns the same frame for the same time whatever came before', () => {
  const forward = [0, 120, 372, 574].map(frameAt);
  const backward = [574, 372, 120, 0].map(frameAt).reverse();
  assert.deepEqual(forward, backward);
});

test('opens the tree level by level, then the orders tab', () => {
  assert.deepEqual(frameAt(0).tree.levels, [0, 0, 0, 0]);
  assert.equal(frameAt(0).tabOpen, false);
  assert.deepEqual(frameAt(56).tree.levels, [1, 1, 1, 1]);
  assert.equal(frameAt(56).tree.ordersSelected, false);
  assert.equal(frameAt(60).tree.ordersSelected, true);
  assert.equal(frameAt(60).tabOpen, true);
  assert.equal(frameAt(90).documentOpacity, 1);
});

test('types the WHERE clause one character at a time and clears it later', () => {
  assert.equal(frameAt(110).where.text, '');
  assert.equal(frameAt(110).where.placeholder, true);
  assert.equal(frameAt(132).where.text, 'status ');
  assert.equal(frameAt(150).where.text, "status = 'paid'");
  assert.equal(frameAt(150).where.focused, true);
  assert.equal(frameAt(200).where.focused, false);
  assert.deepEqual(frameAt(310).where, {
    text: "status = 'paid'",
    selected: true,
    focused: true,
    placeholder: false,
  });
  assert.equal(frameAt(330).where.text, '');
  assert.equal(frameAt(330).where.placeholder, true);
});

test('Refresh keeps only paid orders until the filter is cleared', () => {
  const filtered = frameAt(190);
  DEMO_ROWS.forEach((row, index) => {
    assert.equal(filtered.rows[index]?.keep, row.status === 'paid' ? 1 : 0, `row ${row.id}`);
  });
  assert.equal(filtered.footerRows, 'filtered');
  const restored = frameAt(340);
  assert.ok(restored.rows.every((row) => row.keep === 1));
  assert.equal(restored.footerRows, 'all');
});

test('the Builder opens on the same filter and folds away again', () => {
  assert.equal(frameAt(205).builder.open, 0);
  assert.equal(frameAt(240).builder.open, 1);
  assert.equal(frameAt(240).builder.active, true);
  assert.equal(frameAt(290).builder.open, 0);
  assert.equal(frameAt(290).builder.active, false);
});

test('editing row 2 goes from selected text to a pending change to saved', () => {
  assert.equal(editedRow(340)?.cell, 'normal');
  assert.equal(editedRow(340)?.status, 'pending');

  assert.equal(editedRow(348)?.cell, 'selecting');
  assert.equal(editedRow(348)?.status, 'pending');

  assert.equal(editedRow(357)?.cell, 'editing');
  assert.equal(editedRow(357)?.status, 'pa');
  assert.equal(editedRow(357)?.caret, true);

  const pending = frameAt(370);
  assert.equal(editedRow(370)?.cell, 'pending');
  assert.equal(editedRow(370)?.status, 'pending → paid');
  assert.equal(pending.toolbar.dirty, true);

  const saved = frameAt(390);
  assert.equal(editedRow(390)?.cell, 'normal');
  assert.equal(editedRow(390)?.status, 'paid');
  assert.equal(editedRow(390)?.tone, 'paid');
  assert.equal(saved.toolbar.dirty, false);
  assert.equal(saved.statusQuery, 'UPDATE · 1 row · 6 ms');
});

test('the chart replaces the grid and draws, then the audit viewer takes over', () => {
  assert.equal(frameAt(430).chart.visible, false);
  assert.equal(frameAt(440).chart.visible, true);
  assert.equal(frameAt(440).chart.draw, 0);
  const midway = frameAt(465).chart.draw;
  assert.ok(midway > 0 && midway < 1);
  assert.equal(frameAt(500).chart.draw, 1);

  assert.equal(frameAt(520).audit.visible, false);
  assert.equal(frameAt(530).audit.visible, true);
  assert.equal(frameAt(530).railAudit, true);
  assert.equal(frameAt(530).chart.visible, false);
  assert.equal(frameAt(545).audit.newRow, 1);
});

test('the camera starts on the whole window and never shows past its edges', () => {
  const first = frameAt(0).camera;
  assert.equal(first.scale, stage.width / DEMO_APP.width);
  assert.equal(first.x, 0);
  assert.ok(Math.abs(first.y) < 1, 'the whole window is in view, give or take rounding');
  assert.ok(frameAt(350).camera.scale > first.scale);

  for (let time = 0; time <= DEMO_DURATION; time += 0.5) {
    const { scale, x, y } = frameAt(time).camera;
    assert.ok(x <= 0 && x >= stage.width - DEMO_APP.width * scale - 1e-6, `x at ${time}`);
    assert.ok(
      y <= 0 && y >= Math.min(0, stage.height - DEMO_APP.height * scale) - 1e-6,
      `y at ${time}`,
    );
  }
});

test('the cursor presses where the click lands and rests out of the way', () => {
  const save = DEFAULT_DEMO_ANCHORS.save;
  const atSave = frameAt(384);
  assert.equal(atSave.cursor.pressed, true);
  const onScreen = {
    x: atSave.camera.x + (save.x + save.w / 2 - 12) * atSave.camera.scale,
    y: atSave.camera.y + (save.y + save.h / 2) * atSave.camera.scale,
  };
  assert.ok(Math.abs(atSave.cursor.x - onScreen.x) < 0.01);
  assert.ok(Math.abs(atSave.cursor.y - onScreen.y) < 0.01);
  assert.equal(frameAt(390).cursor.pressed, false);
  assert.equal(frameAt(0).cursor.opacity, 0);
  assert.equal(frameAt(DEMO_DURATION).cursor.opacity, 0);
});

test('a static snapshot freezes each step at its key moment', () => {
  const view = { width: 358, height: 220 };
  assert.equal(demoSnapshot('edit', DEFAULT_DEMO_ANCHORS, view).toolbar.dirty, true);
  assert.equal(demoSnapshot('filter', DEFAULT_DEMO_ANCHORS, view).footerRows, 'filtered');
  assert.equal(demoSnapshot('builder', DEFAULT_DEMO_ANCHORS, view).builder.open, 1);
  assert.equal(demoSnapshot('chart', DEFAULT_DEMO_ANCHORS, view).chart.draw, 1);
  assert.equal(demoSnapshot('audit', DEFAULT_DEMO_ANCHORS, view).audit.newRow, 1);
  assert.ok(
    demoSnapshot('explore', DEFAULT_DEMO_ANCHORS, view).camera.scale > view.width / DEMO_APP.width,
  );
});

test('a narrow static snapshot frames its step more tightly than a wide one', () => {
  const narrow = { width: 348, height: 218 };
  const wide = { width: 760, height: 475 };
  for (const step of DEMO_STEPS) {
    const tight = demoSnapshot(step.id, DEFAULT_DEMO_ANCHORS, narrow).camera.scale / narrow.width;
    const loose = demoSnapshot(step.id, DEFAULT_DEMO_ANCHORS, wide).camera.scale / wide.width;
    assert.ok(tight > loose, step.id);
  }
});
