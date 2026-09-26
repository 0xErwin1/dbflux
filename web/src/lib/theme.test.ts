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

test('normalizes missing, malformed and retired preferences to system', () => {
  assert.equal(normalizeThemePreference(null), 'system');
  assert.equal(normalizeThemePreference(undefined), 'system');
  assert.equal(normalizeThemePreference('sepia'), 'system');
  assert.equal(normalizeThemePreference('mirage'), 'system');
});

test('preserves explicit light, dark, and system preferences', () => {
  assert.equal(normalizeThemePreference('light'), 'light');
  assert.equal(normalizeThemePreference('dark'), 'dark');
  assert.equal(normalizeThemePreference('system'), 'system');
});

test('resolves system preference from the operating system and honors explicit overrides', () => {
  assert.equal(resolveTheme('system', true), 'dark');
  assert.equal(resolveTheme('system', false), 'light');
  assert.equal(resolveTheme('dark', false), 'dark');
  assert.equal(resolveTheme('light', true), 'light');
});

test('the head theme bootstrap is executable and resolves the stored preference', () => {
  const documentElement = { dataset: {} as Record<string, string> };
  const script = inlineScript('../layouts/Base.astro', '<head>');

  runInlineScript(script, {
    document: { documentElement },
    localStorage: { getItem: () => 'light' },
    matchMedia: () => ({ matches: true }),
  });

  assert.deepEqual(documentElement.dataset, { themePreference: 'light', theme: 'light' });
});

test('the head theme bootstrap falls back to the system theme for a retired preference', () => {
  const documentElement = { dataset: {} as Record<string, string> };
  const script = inlineScript('../layouts/Base.astro', '<head>');

  runInlineScript(script, {
    document: { documentElement },
    localStorage: { getItem: () => 'mirage' },
    matchMedia: () => ({ matches: true }),
  });

  assert.deepEqual(documentElement.dataset, { themePreference: 'system', theme: 'dark' });
});
