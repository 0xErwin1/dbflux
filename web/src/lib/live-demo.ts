import { DEMO_ROWS, type DemoOrderStatus } from './live-demo-data.ts';

/** The app window is laid out at this size and scaled to fit, so the design's pixel values hold. */
export const DEMO_APP = { width: 1532, height: 900 } as const;

/** Demo time runs from 0 at the top of the pinned section to this value at its end. */
export const DEMO_DURATION = 600;

export const DEMO_STEPS = [
  { id: 'explore', anchor: 'journey-explore', start: 0, end: 100, hero: 86 },
  { id: 'filter', anchor: 'journey-filter', start: 100, end: 200, hero: 180 },
  { id: 'builder', anchor: 'journey-builder', start: 200, end: 300, hero: 252 },
  { id: 'edit', anchor: 'journey-edit', start: 300, end: 420, hero: 372 },
  { id: 'chart', anchor: 'journey-visualize', start: 420, end: 510, hero: 494 },
  { id: 'audit', anchor: 'governance', start: 510, end: 600, hero: 574 },
] as const;

export type DemoStepId = (typeof DEMO_STEPS)[number]['id'];

export type DemoAnchorKey =
  | 'tree'
  | 'where'
  | 'refresh'
  | 'builder-button'
  | 'status-header'
  | 'rows'
  | 'save'
  | 'chart-toggle'
  | 'rail-audit'
  | 'builder-sql'
  | 'audit-new';

/** A box in app coordinates. */
export interface DemoBox {
  x: number;
  y: number;
  w: number;
  h: number;
}

export type DemoAnchors = Record<DemoAnchorKey, DemoBox>;

/** Where each target sits in the app layout; the page replaces these with measured boxes. */
export const DEFAULT_DEMO_ANCHORS: DemoAnchors = {
  tree: { x: 63, y: 166, w: 288, h: 442 },
  where: { x: 373, y: 186, w: 800, h: 30 },
  refresh: { x: 1405, y: 186, w: 106, h: 30 },
  'builder-button': { x: 1305, y: 186, w: 92, h: 30 },
  'status-header': { x: 659, y: 224, w: 130, h: 34 },
  rows: { x: 361, y: 258, w: 1162, h: 548 },
  save: { x: 1363, y: 141, w: 94, h: 30 },
  'chart-toggle': { x: 527, y: 818, w: 68, h: 26 },
  'rail-audit': { x: 12, y: 220, w: 38, h: 38 },
  'builder-sql': { x: 1097, y: 520, w: 412, h: 150 },
  'audit-new': { x: 361, y: 270, w: 1162, h: 32 },
};

export interface DemoView {
  width: number;
  height: number;
}

export type DemoCellState = 'normal' | 'selecting' | 'editing' | 'pending';

export interface DemoRowFrame {
  id: number;
  /** 1 while the row belongs to the result, 0 once a filter removes it. */
  keep: number;
  /** 0 before the grid loads, 1 once the row has arrived. */
  load: number;
  selected: boolean;
  status: string;
  tone: DemoOrderStatus;
  cell: DemoCellState;
  caret: boolean;
}

export interface DemoFrame {
  time: number;
  camera: { scale: number; x: number; y: number };
  cursor: { x: number; y: number; pressed: boolean; opacity: number };
  ripple: { x: number; y: number; scale: number; opacity: number };
  enterKey: { x: number; y: number; opacity: number };
  tree: { levels: number[]; chevrons: number[]; ordersSelected: boolean };
  tabOpen: boolean;
  documentOpacity: number;
  where: { text: string; selected: boolean; focused: boolean; placeholder: boolean };
  builder: { open: number; active: boolean };
  rows: DemoRowFrame[];
  toolbar: { dirty: boolean };
  footerRows: 'all' | 'filtered';
  chart: { visible: boolean; draw: number; tip: number; toggleHover: boolean };
  audit: { visible: boolean; newRow: number; highlight: boolean };
  railAudit: boolean;
  statusQuery: string;
}

export type DemoEligibility =
  | { mode: 'enhanced'; reason: 'eligible' }
  | { mode: 'static'; reason: 'viewport' | 'reduced-motion' };

interface Point {
  x: number;
  y: number;
}

interface Region {
  cx: number;
  cy: number;
  w: number;
}

const CLICKS = [14, 26, 36, 46, 60, 114, 160, 211, 276, 306, 323, 345, 347, 384, 436, 526];
const FILTER = "status = 'paid'";
const EDITED_ROW = 2;

function clamp(value: number, low: number, high: number): number {
  return Math.min(high, Math.max(low, value));
}

function easeInOut(u: number): number {
  return u < 0.5 ? 4 * u * u * u : 1 - Math.pow(-2 * u + 2, 3) / 2;
}

/** Eased progress of `time` through [start, end], from 0 to 1. */
function span(time: number, start: number, end: number): number {
  return easeInOut(clamp((time - start) / (end - start), 0, 1));
}

function lerp(from: number, to: number, u: number): number {
  return from + (to - from) * u;
}

function typed(text: string, time: number, start: number, end: number): string {
  const count = Math.round(clamp((time - start) / (end - start), 0, 1) * text.length);
  return text.slice(0, count);
}

function round(value: number): number {
  return Math.round(value * 1e6) / 1e6;
}

function center(box: DemoBox, dx = 0, dy = 0): Point {
  return { x: box.x + box.w / 2 + dx, y: box.y + box.h / 2 + dy };
}

function along(box: DemoBox, fraction: number, dy = 0): Point {
  return { x: box.x + box.w * fraction, y: box.y + box.h / 2 + dy };
}

/** Maps scroll progress (0..1) to demo time; anything non-finite falls back to an end. */
export function demoTime(progress: number): number {
  if (Number.isNaN(progress)) return 0;
  return clamp(progress, 0, 1) * DEMO_DURATION;
}

export function demoStepAt(time: number): { index: number; id: DemoStepId; fill: number } {
  const clamped = clamp(time, 0, DEMO_DURATION);
  let index = DEMO_STEPS.findIndex((step) => clamped >= step.start && clamped < step.end);
  if (index === -1) index = DEMO_STEPS.length - 1;
  const step = DEMO_STEPS[index];
  return { index, id: step.id, fill: (clamped - step.start) / (step.end - step.start) };
}

/** Resolves a chapter fragment to the scroll progress just past that step's start. */
export function demoStepTarget(
  hash: string,
): { id: DemoStepId; anchor: string; index: number; progress: number } | null {
  const anchor = hash.startsWith('#') ? hash.slice(1) : hash;
  const index = DEMO_STEPS.findIndex((step) => step.anchor === anchor);
  if (index === -1) return null;
  const step = DEMO_STEPS[index];
  return { id: step.id, anchor: step.anchor, index, progress: (step.start + 2) / DEMO_DURATION };
}

/** The static document stays the default until both enhancement conditions pass. */
export function demoEligibility(viewportWidth: number, reducedMotion: boolean): DemoEligibility {
  if (reducedMotion) return { mode: 'static', reason: 'reduced-motion' };
  if (viewportWidth <= 1180) return { mode: 'static', reason: 'viewport' };
  return { mode: 'enhanced', reason: 'eligible' };
}

interface Paths {
  cursor: Array<[number, Point]>;
  camera: Array<[number, Region]>;
  snapshot: Record<DemoStepId, Region>;
  /** Closer framing for snapshots narrower than a tablet, where the wide one is unreadable. */
  closeUp: Record<DemoStepId, Region>;
}

function paths(anchors: DemoAnchors): Paths {
  const tree = anchors.tree;
  const treeRow = (index: number, depth: number): Point => ({
    x: tree.x + 68 + depth * 14,
    y: tree.y + 26 * index + 13,
  });
  const panelLeft = DEMO_APP.width - 8 - 441;
  const editedCell = { x: anchors['status-header'].x + 44, y: anchors.rows.y + 30 + 15 };
  const documentLeft = anchors.rows.x;
  const sql = anchors['builder-sql'];
  const newRow = anchors['audit-new'];

  const cursor: Array<[number, Point]> = [
    [0, { x: 1150, y: 780 }],
    [12, treeRow(1, 0)],
    [24, treeRow(2, 1)],
    [34, treeRow(3, 2)],
    [44, treeRow(4, 3)],
    [58, treeRow(7, 4)],
    [90, { x: documentLeft + 520, y: 520 }],
    [112, along(anchors.where, 0.12)],
    [116, along(anchors.where, 0.12, 22)],
    [150, along(anchors.where, 0.16, 24)],
    [158, center(anchors.refresh, -10)],
    [190, { x: documentLeft + 640, y: 600 }],
    [209, center(anchors['builder-button'], -8)],
    [244, { x: panelLeft + 250, y: sql.y - 92 }],
    [262, { x: panelLeft + 220, y: sql.y + 60 }],
    [272, { x: panelLeft + 441 - 30, y: 44 + 23 }],
    [296, { x: documentLeft + 600, y: 520 }],
    [304, along(anchors.where, 0.1)],
    [320, center(anchors.refresh, -10)],
    [342, editedCell],
    [362, { x: editedCell.x + 8, y: editedCell.y + 6 }],
    [380, center(anchors.save, -12)],
    [400, { x: documentLeft + 560, y: 420 }],
    [432, center(anchors['chart-toggle'])],
    [470, { x: documentLeft + 700, y: 560 }],
    [522, center(anchors['rail-audit'])],
    [560, { x: documentLeft + 560, y: newRow.y + 140 }],
    [DEMO_DURATION, { x: documentLeft + 560, y: newRow.y + 140 }],
  ];

  const full = { cx: DEMO_APP.width / 2, cy: DEMO_APP.height / 2, w: DEMO_APP.width };
  const sidebar = { cx: 380, cy: 300, w: 760 };
  const filter = { cx: anchors.where.x + 330, cy: anchors.where.y + 140, w: 900 };
  const filterWide = {
    cx: (anchors.where.x + anchors.refresh.x + anchors.refresh.w) / 2,
    cy: anchors.where.y + 200,
    w: anchors.refresh.x + anchors.refresh.w - anchors.where.x + 40,
  };
  const builderPanel = { cx: panelLeft + 120, cy: 330, w: 800 };
  const builderSql = { cx: panelLeft + 120, cy: sql.y + 40, w: 800 };
  const cell = { cx: editedCell.x + 160, cy: editedCell.y + 80, w: 760 };
  const toolbarLeft = documentLeft - 4;
  const toolbar = {
    cx: (toolbarLeft + DEMO_APP.width) / 2,
    cy: 260,
    w: DEMO_APP.width - toolbarLeft,
  };
  const chart = { cx: documentLeft + 560, cy: 420, w: 1150 };
  const audit = { cx: documentLeft + 490, cy: newRow.y + 120, w: 1000 };

  const camera: Array<[number, Region]> = [
    [0, full],
    [8, sidebar],
    [56, sidebar],
    [72, full],
    [104, full],
    [112, filter],
    [150, filter],
    [158, filterWide],
    [178, filterWide],
    [192, full],
    [222, full],
    [234, builderPanel],
    [250, builderPanel],
    [260, builderSql],
    [272, builderSql],
    [286, full],
    [326, full],
    [338, cell],
    [364, cell],
    [376, toolbar],
    [392, toolbar],
    [408, full],
    [438, full],
    [446, chart],
    [494, chart],
    [506, full],
    [528, full],
    [540, audit],
    [584, audit],
    [596, full],
    [DEMO_DURATION, full],
  ];

  const snapshot: Record<DemoStepId, Region> = {
    explore: { cx: 560, cy: 300, w: 900 },
    filter: filterWide,
    builder: builderSql,
    edit: toolbar,
    chart,
    audit,
  };

  const closeUp: Record<DemoStepId, Region> = {
    explore: { cx: 330, cy: 300, w: 560 },
    filter: { cx: anchors.where.x + 300, cy: anchors.where.y + 150, w: 620 },
    builder: { cx: panelLeft + 220, cy: sql.y + 20, w: 470 },
    edit: { cx: editedCell.x + 60, cy: editedCell.y + 40, w: 560 },
    chart: { cx: documentLeft + 500, cy: 420, w: 820 },
    audit: { cx: documentLeft + 330, cy: newRow.y + 90, w: 660 },
  };

  return { cursor, camera, snapshot, closeUp };
}

function track<T>(
  frames: Array<[number, T]>,
  time: number,
  mix: (from: T, to: T, t0: number, t1: number) => T,
): T {
  let index = 0;
  while (index < frames.length - 1 && frames[index + 1][0] <= time) index += 1;
  if (index >= frames.length - 1) return frames[frames.length - 1][1];
  const [t0, from] = frames[index];
  const [t1, to] = frames[index + 1];
  return mix(from, to, t0, t1);
}

function cameraFor(region: Region, view: DemoView) {
  const base = view.width / DEMO_APP.width;
  const scale = Math.max(base, view.width / region.w);
  const x = clamp(view.width / 2 - region.cx * scale, view.width - DEMO_APP.width * scale, 0);
  const y = clamp(
    view.height / 2 - region.cy * scale,
    Math.min(0, view.height - DEMO_APP.height * scale),
    0,
  );
  return { scale, x: round(x), y: round(y) };
}

function rowFrame(index: number, time: number): DemoRowFrame {
  const row = DEMO_ROWS[index];
  let keep = 1;
  if (row.status !== 'paid') {
    keep = time < 324 ? 1 - span(time, 161, 174) : span(time, 324, 336);
  }

  const frame: DemoRowFrame = {
    id: row.id,
    keep,
    load: span(time, 62 + index * 0.9, 68 + index * 0.9),
    selected: false,
    status: row.status,
    tone: row.status,
    cell: 'normal',
    caret: false,
  };
  if (row.id !== EDITED_ROW) return frame;

  frame.selected = time >= 343 && time < 404;
  if (time >= 345 && time < 352) {
    frame.cell = 'selecting';
  } else if (time >= 352 && time < 364) {
    frame.cell = 'editing';
    frame.status = typed('paid', time, 352, 361);
    frame.caret = true;
  } else if (time >= 364 && time < 386) {
    frame.cell = 'pending';
    frame.status = 'pending → paid';
  } else if (time >= 386) {
    frame.status = 'paid';
    frame.tone = 'paid';
  }
  return frame;
}

function statusQuery(time: number): string {
  if (time >= 384) return 'UPDATE · 1 row · 6 ms';
  if (time >= 323) return 'SELECT · 100 rows · 41 ms';
  if (time >= 160) return 'SELECT · 100 rows · 38 ms';
  return 'SELECT · 100 rows · 42 ms';
}

function whereFrame(time: number): DemoFrame['where'] {
  const text = time < 306 ? typed(FILTER, time, 116, 150) : time < 312 ? FILTER : '';
  const selected = time >= 308 && time < 312;
  const focused = (time >= 114 && time < 162) || (time >= 306 && time < 323);
  return { text, selected, focused, placeholder: text === '' };
}

/**
 * Everything the window shows at one moment. A pure function of time, so any
 * scroll position, reached from either direction, gives the same frame.
 */
export function demoFrame(
  time: number,
  anchors: DemoAnchors,
  view: DemoView,
  region?: Region,
): DemoFrame {
  const t = clamp(Number.isNaN(time) ? 0 : time, 0, DEMO_DURATION);
  const route = paths(anchors);
  const camera = cameraFor(
    region ??
      track(route.camera, t, (from, to, t0, t1) => {
        const u = span(t, t0, t1);
        return {
          cx: lerp(from.cx, to.cx, u),
          cy: lerp(from.cy, to.cy, u),
          w: lerp(from.w, to.w, u),
        };
      }),
    view,
  );

  const pointer = track(route.cursor, t, (from, to, t0, t1) => {
    const u = span(t, Math.max(t0, t1 - 9), t1);
    const arc = Math.sin(Math.PI * u) * 0.08;
    return {
      x: lerp(from.x, to.x, u) - (to.y - from.y) * arc,
      y: lerp(from.y, to.y, u) + (to.x - from.x) * arc,
    };
  });
  const screen = {
    x: round(camera.x + pointer.x * camera.scale),
    y: round(camera.y + pointer.y * camera.scale),
  };
  const ripple = CLICKS.find((click) => t >= click && t < click + 4);
  const rippleProgress = ripple === undefined ? 1 : (t - ripple) / 4;

  const builderOpen = t < 276 ? span(t, 211, 222) : 1 - span(t, 276, 286);

  return {
    time: t,
    camera,
    cursor: {
      ...screen,
      pressed: CLICKS.some((click) => t >= click - 0.6 && t < click + 0.8),
      opacity: Math.min(span(t, 1, 6), 1 - span(t, 592, 599)),
    },
    ripple: {
      ...screen,
      scale: 0.3 + rippleProgress * 0.9,
      opacity: ripple === undefined ? 0 : (1 - rippleProgress) * 0.8,
    },
    enterKey: {
      x: round(screen.x + 18 * camera.scale),
      y: round(screen.y + 14 * camera.scale),
      opacity: Math.min(span(t, 362, 363.5), 1 - span(t, 367, 369)),
    },
    tree: {
      levels: [span(t, 14, 18), span(t, 26, 30), span(t, 36, 40), span(t, 46, 52)],
      chevrons: [span(t, 14, 17), span(t, 26, 29), span(t, 36, 39), span(t, 46, 49)],
      ordersSelected: t >= 60,
    },
    tabOpen: t >= 60,
    documentOpacity: span(t, 60, 66),
    where: whereFrame(t),
    builder: { open: builderOpen, active: builderOpen > 0.5 },
    rows: DEMO_ROWS.map((_, index) => rowFrame(index, t)),
    toolbar: { dirty: t >= 364 && t < 386 },
    footerRows: t >= 160 && t < 323 ? 'filtered' : 'all',
    chart: {
      visible: t >= 437 && t < 527,
      draw: span(t, 440, 490),
      tip: span(t, 488, 494),
      toggleHover: t >= 430 && t < 437,
    },
    audit: { visible: t >= 527, newRow: span(t, 531, 541), highlight: t < 580 },
    railAudit: t >= 527,
    statusQuery: statusQuery(t),
  };
}

/** One step frozen at its key moment and framed on its action, for the static layout. */
export function demoSnapshot(id: DemoStepId, anchors: DemoAnchors, view: DemoView): DemoFrame {
  const step = DEMO_STEPS.find((item) => item.id === id) ?? DEMO_STEPS[0];
  const route = paths(anchors);
  const regions = view.width < 600 ? route.closeUp : route.snapshot;
  return demoFrame(step.hero, anchors, view, regions[step.id]);
}
