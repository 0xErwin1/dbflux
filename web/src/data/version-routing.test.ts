import assert from 'node:assert/strict';
import test from 'node:test';

import { currentVersionId, renderedLinkInputs, versionPrefix } from './version-routing.ts';

const sevenCurrent = [
  { id: 'nightly', noindex: true },
  { id: 'v0.8' },
  { id: 'v0.7', current: true },
  { id: 'v0.6' },
];

const eightCurrent = [
  { id: 'nightly', noindex: true },
  { id: 'v0.8', current: true },
  { id: 'v0.7' },
  { id: 'v0.6' },
];

/** The digest Astro keys its rendered-markdown cache on, reduced to the markdown section. */
function markdownConfigDigest(inputs: unknown): string {
  return JSON.stringify({ rehypePlugins: [[() => undefined, inputs], () => undefined] });
}

const prefixes = (versions: typeof sevenCurrent) => {
  const current = currentVersionId(versions);

  return Object.fromEntries(versions.map(({ id }) => [id, versionPrefix(id, current)]));
};

test('serves only the flagged release unprefixed, whichever release carries the flag', () => {
  assert.deepEqual(prefixes(sevenCurrent), {
    nightly: 'nightly',
    'v0.8': 'v0.8',
    'v0.7': '',
    'v0.6': 'v0.6',
  });

  assert.deepEqual(prefixes(eightCurrent), {
    nightly: 'nightly',
    'v0.8': '',
    'v0.7': 'v0.7',
    'v0.6': 'v0.6',
  });
});

test('falls back to the first entry and rejects an empty registry', () => {
  assert.equal(currentVersionId([{ id: 'nightly' }, { id: 'v0.8' }]), 'nightly');
  assert.throws(() => currentVersionId([]), /empty/);
});

test('moving current forward invalidates links rendered for the previous release', () => {
  const before = markdownConfigDigest(
    renderedLinkInputs(sevenCurrent, 'embedded', 'https://dbflux.dev'),
  );
  const after = markdownConfigDigest(
    renderedLinkInputs(eightCurrent, 'embedded', 'https://dbflux.dev'),
  );

  assert.notEqual(before, after);
});

test('moving current back invalidates links rendered for the newer release', () => {
  const before = markdownConfigDigest(
    renderedLinkInputs(eightCurrent, 'embedded', 'https://dbflux.dev'),
  );
  const after = markdownConfigDigest(
    renderedLinkInputs(sevenCurrent, 'embedded', 'https://dbflux.dev'),
  );

  assert.notEqual(before, after);
});

test('a plugin function alone serializes the same whichever release is current', () => {
  const pluginOnly = JSON.stringify({ rehypePlugins: [() => sevenCurrent, () => undefined] });
  const pluginOnlyAfter = JSON.stringify({ rehypePlugins: [() => eightCurrent, () => undefined] });

  assert.equal(pluginOnly, pluginOnlyAfter);
});

test('switching documentation host layout invalidates rendered links', () => {
  const embedded = renderedLinkInputs(eightCurrent, 'embedded', 'https://dbflux.dev');
  const site = renderedLinkInputs(eightCurrent, 'site', 'https://docs.dbflux.dev');
  const docs = renderedLinkInputs(eightCurrent, 'docs', 'https://docs.dbflux.dev');

  const digests = new Set([embedded, site, docs].map(markdownConfigDigest));

  assert.equal(digests.size, 3);
});

test('keeps the cache when nothing that shapes a link changed', () => {
  const unlisted = eightCurrent.map((version) => ({ ...version, noindex: !version.noindex }));

  assert.equal(
    markdownConfigDigest(renderedLinkInputs(eightCurrent, 'embedded', 'https://dbflux.dev')),
    markdownConfigDigest(renderedLinkInputs(unlisted, 'embedded', 'https://dbflux.dev')),
  );
});
