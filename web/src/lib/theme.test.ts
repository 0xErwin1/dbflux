import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import { normalizeThemePreference, resolveTheme } from './theme.ts';

function inlineScript(path: string, after: string): string {
  const source = readFileSync(fileURLToPath(new URL(path, import.meta.url)), 'utf8');
  const marker = source.indexOf(after);
  assert.notEqual(marker, -1, `Could not find ${after}`);

  const start = source.indexOf('>', source.indexOf('<script', marker)) + 1;
  const end = source.indexOf('</script>', start);
  assert.notEqual(end, -1, `Could not find closing script tag after ${after}`);
  return source.slice(start, end);
}

function runInlineScript(script: string, context: Record<string, unknown>): void {
  new vm.Script(script, { filename: 'inline-theme-script.js' }).runInNewContext(context);
}

test('normalizes missing and malformed preferences to system', () => {
  assert.equal(normalizeThemePreference(null), 'system');
  assert.equal(normalizeThemePreference(undefined), 'system');
  assert.equal(normalizeThemePreference('sepia'), 'system');
});

test('preserves explicit light, dark, mirage, and system preferences', () => {
  assert.equal(normalizeThemePreference('light'), 'light');
  assert.equal(normalizeThemePreference('dark'), 'dark');
  assert.equal(normalizeThemePreference('mirage'), 'mirage');
  assert.equal(normalizeThemePreference('system'), 'system');
});

test('resolves system preference from the operating system and honors explicit overrides', () => {
  assert.equal(resolveTheme('system', true), 'dark');
  assert.equal(resolveTheme('system', false), 'light');
  assert.equal(resolveTheme('dark', false), 'dark');
  assert.equal(resolveTheme('light', true), 'light');
  assert.equal(resolveTheme('mirage', false), 'mirage');
  assert.equal(resolveTheme('mirage', true), 'mirage');
});

test('the head theme bootstrap is executable and resolves the stored preference', () => {
  const documentElement = { dataset: {} as Record<string, string> };
  const script = inlineScript('../layouts/Base.astro', '<head>');

  runInlineScript(script, {
    document: { documentElement },
    localStorage: { getItem: () => 'mirage' },
    matchMedia: () => ({ matches: false }),
  });

  assert.deepEqual(documentElement.dataset, { themePreference: 'mirage', theme: 'mirage' });
});

test('the hero image script is executable and assigns a selected source before revealing it', () => {
  class ImageElement {
    hidden = true;
    src = '';
  }

  const hero = new ImageElement();
  const listeners = new Map<string, () => void>();
  const documentElement = { dataset: { theme: 'mirage' } };
  const script = inlineScript('../components/Landing.astro', '<img data-hero-shot');

  runInlineScript(script, {
    HTMLImageElement: ImageElement,
    document: {
      documentElement,
      querySelector: () => hero,
    },
    window: {
      addEventListener: (name: string, listener: () => void) => listeners.set(name, listener),
    },
  });

  assert.equal(hero.src, '/img/app-hero-mirage.png');
  assert.equal(hero.hidden, false);
  documentElement.dataset.theme = 'dark';
  listeners.get('dbflux-theme-change')?.();
  assert.equal(hero.src, '/img/app-hero-dark.png');
});
