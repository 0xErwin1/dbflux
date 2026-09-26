import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import { isAboveViewport, parseTimeMs, staggerDelay } from './reveal.ts';

function revealBootstrap(): string {
  const path = fileURLToPath(new URL('../components/ScrollReveal.astro', import.meta.url));
  const source = readFileSync(path, 'utf8');
  const marker = source.indexOf('<script is:inline>');
  assert.notEqual(marker, -1, 'Could not find the reveal bootstrap in ScrollReveal.astro');

  const start = marker + '<script is:inline>'.length;
  const end = source.indexOf('</script>', start);
  assert.notEqual(end, -1, 'Could not find the end of the reveal bootstrap');
  return source.slice(start, end);
}

function runBootstrap(options: { reducedMotion: boolean; observer: boolean }) {
  const classes = new Set<string>();
  const timers: Array<() => void> = [];
  const documentElement = {
    classList: {
      add: (name: string) => classes.add(name),
      remove: (name: string) => classes.delete(name),
      contains: (name: string) => classes.has(name),
    },
    dataset: {} as Record<string, string>,
  };
  const window: Record<string, unknown> = {};
  if (options.observer) window.IntersectionObserver = function IntersectionObserver() {};

  new vm.Script(revealBootstrap(), { filename: 'inline-reveal-script.js' }).runInNewContext({
    window,
    document: { documentElement },
    matchMedia: (query: string) => ({
      matches: query.includes('reduce') ? options.reducedMotion : false,
    }),
    setTimeout: (callback: () => void) => timers.push(callback),
  });

  return { classes, timers, documentElement };
}

test('staggers the first element at zero and steps the rest', () => {
  assert.equal(staggerDelay(0, 60, 360), 0);
  assert.equal(staggerDelay(1, 60, 360), 60);
  assert.equal(staggerDelay(3, 60, 360), 180);
});

test('caps the stagger so a long list does not take seconds', () => {
  assert.equal(staggerDelay(6, 60, 360), 360);
  assert.equal(staggerDelay(40, 60, 360), 360);
});

test('treats invalid stagger inputs as no delay', () => {
  assert.equal(staggerDelay(-2, 60, 360), 0);
  assert.equal(staggerDelay(Number.NaN, 60, 360), 0);
  assert.equal(staggerDelay(3, 0, 360), 0);
  assert.equal(staggerDelay(3, 60, Number.NaN), 0);
});

test('parses CSS times in milliseconds and seconds', () => {
  assert.equal(parseTimeMs('60ms', 0), 60);
  assert.equal(parseTimeMs(' 0.36s ', 0), 360);
  assert.equal(parseTimeMs('.5S', 0), 500);
});

test('falls back when a CSS time is missing or malformed', () => {
  assert.equal(parseTimeMs('', 60), 60);
  assert.equal(parseTimeMs(undefined, 60), 60);
  assert.equal(parseTimeMs('60', 60), 60);
  assert.equal(parseTimeMs('-20ms', 60), 60);
});

test('only elements scrolled past count as above the viewport', () => {
  assert.equal(isAboveViewport(-10), true);
  assert.equal(isAboveViewport(0), true);
  assert.equal(isAboveViewport(1), false);
});

test('the head bootstrap enables reveals when motion is allowed', () => {
  const { classes } = runBootstrap({ reducedMotion: false, observer: true });
  assert.equal(classes.has('js-reveal'), true);
});

test('the head bootstrap leaves content visible under reduced motion', () => {
  const { classes, timers } = runBootstrap({ reducedMotion: true, observer: true });
  assert.equal(classes.has('js-reveal'), false);
  assert.equal(timers.length, 0);
});

test('the head bootstrap leaves content visible without IntersectionObserver', () => {
  const { classes } = runBootstrap({ reducedMotion: false, observer: false });
  assert.equal(classes.has('js-reveal'), false);
});

test('the head bootstrap shows content if the reveal script never starts', () => {
  const { classes, timers } = runBootstrap({ reducedMotion: false, observer: true });
  assert.equal(timers.length, 1);

  timers[0]();
  assert.equal(classes.has('js-reveal'), false);
});

test('the head bootstrap keeps reveals once the reveal script has started', () => {
  const { classes, timers, documentElement } = runBootstrap({
    reducedMotion: false,
    observer: true,
  });

  documentElement.dataset.revealReady = '';
  timers[0]();
  assert.equal(classes.has('js-reveal'), true);
});
