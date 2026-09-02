import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_LOCALE,
  LOCALE_REGISTRY,
  buildLocalizedDocumentMatrix,
  contentEntryId,
  docsRepoPatterns,
  isDocsRepoPath,
  localeForRepoPath,
  localizedDocPath,
  searchIndexPath,
  selectLocaleSearchEntries,
  splitContentEntryId,
  validateLocaleRegistry,
} from './locale-registry.ts';

const extendedRegistry = [
  { id: 'en', name: 'English', docsDirectory: null },
  { id: 'zh-Hans', name: '简体中文', docsDirectory: 'zh-hans' },
];

test('ships only the current English, Spanish and Simplified Chinese locale metadata', () => {
  assert.equal(DEFAULT_LOCALE, 'en');
  assert.deepEqual(
    LOCALE_REGISTRY.map(({ id, name, docsDirectory }) => ({ id, name, docsDirectory })),
    [
      { id: 'en', name: 'English', docsDirectory: null },
      { id: 'es', name: 'Español', docsDirectory: 'es' },
      { id: 'zh-Hans', name: '简体中文', docsDirectory: 'zh_Hans' },
    ],
  );
});

test('validates locale registry invariants', () => {
  assert.doesNotThrow(() => validateLocaleRegistry(extendedRegistry, 'en'));
  assert.throws(
    () => validateLocaleRegistry([{ id: 'es', name: 'Español', docsDirectory: 'es' }], 'en'),
    /default locale "en" is not registered/,
  );
  assert.throws(
    () =>
      validateLocaleRegistry(
        [
          { id: 'en', name: 'English', docsDirectory: null },
          { id: 'en', name: 'Duplicate', docsDirectory: 'duplicate' },
        ],
        'en',
      ),
    /duplicate locale id "en"/,
  );
  assert.throws(
    () =>
      validateLocaleRegistry(
        [
          { id: 'en', name: 'English', docsDirectory: null },
          { id: 'es', name: 'Español', docsDirectory: 'translated' },
          { id: 'fr', name: 'Français', docsDirectory: 'translated' },
        ],
        'en',
      ),
    /duplicate docs directory "translated"/,
  );
  assert.throws(
    () =>
      validateLocaleRegistry(
        [
          { id: 'en', name: 'English', docsDirectory: null },
          { id: 'es', name: 'Español', docsDirectory: null },
        ],
        'en',
      ),
    /non-default locale "es" must declare a docs directory/,
  );
});

test('derives only registered documentation include patterns', () => {
  assert.deepEqual(docsRepoPatterns(LOCALE_REGISTRY), [
    'docs/*.md',
    'docs/es/*.md',
    'docs/es/drivers/*.md',
    'docs/zh_Hans/*.md',
    'docs/zh_Hans/drivers/*.md',
  ]);
  assert.deepEqual(docsRepoPatterns(extendedRegistry), [
    'docs/*.md',
    'docs/zh-hans/*.md',
    'docs/zh-hans/drivers/*.md',
  ]);
});

test('ingests only existing files in registered localized docs directories', () => {
  assert.equal(isDocsRepoPath('docs/SETTINGS.md', extendedRegistry), true);
  assert.equal(isDocsRepoPath('docs/zh-hans/SETTINGS.md', extendedRegistry), true);
  assert.equal(isDocsRepoPath('docs/zh-hans/drivers/Postgres.md', extendedRegistry), true);
  assert.equal(isDocsRepoPath('docs/es/SETTINGS.md', extendedRegistry), false);
  assert.equal(isDocsRepoPath('docs/zh-hans/missing/nested.md', extendedRegistry), false);
});

test('preserves canonical locale identity while lowercasing only document paths', () => {
  assert.equal(
    contentEntryId('nightly/docs/zh-hans/Getting-Started.md', extendedRegistry, 'en'),
    'nightly/zh-Hans/getting-started',
  );
  assert.equal(
    contentEntryId('nightly/docs/zh-hans/drivers/Postgres.md', extendedRegistry, 'en'),
    'nightly/zh-Hans/drivers/postgres',
  );
  assert.equal(
    contentEntryId('nightly/docs/SETTINGS.md', extendedRegistry, 'en'),
    'nightly/settings',
  );
  assert.equal(
    contentEntryId('nightly/crates/dbflux_driver_Postgres/README.md', extendedRegistry, 'en'),
    'nightly/drivers/Postgres',
  );
});

test('routes the custom driver example to its own page id', () => {
  assert.equal(
    contentEntryId('nightly/examples/custom_driver/README.md', extendedRegistry, 'en'),
    'nightly/custom_driver_example',
  );
});

test('detects a localized repository path from its registered docs directory', () => {
  assert.equal(localeForRepoPath('docs/zh-hans/SETTINGS.md', extendedRegistry, 'en'), 'zh-Hans');
  assert.equal(localeForRepoPath('docs/SETTINGS.md', extendedRegistry, 'en'), 'en');
  assert.equal(localeForRepoPath('docs/zh-hans-extra/SETTINGS.md', extendedRegistry, 'en'), 'en');
});

test('splits content ids without changing canonical locale case', () => {
  const localized = splitContentEntryId('nightly/zh-Hans/getting-started', extendedRegistry, 'en');
  const canonical = splitContentEntryId('nightly/getting-started', extendedRegistry, 'en');

  assert.deepEqual(localized, {
    version: 'nightly',
    locale: 'zh-Hans',
    path: 'getting-started',
  });
  assert.deepEqual(canonical, {
    version: 'nightly',
    locale: 'en',
    path: 'getting-started',
  });
});

test('builds every locale route from canonical docs and marks absent content as fallback', () => {
  const english = { id: 'nightly/settings', body: '# Settings' };
  const translated = { id: 'nightly/zh-Hans/settings', body: '# 设置' };
  const englishOnly = { id: 'nightly/usage', body: '# Usage' };

  const matrix = buildLocalizedDocumentMatrix(
    [english, translated, englishOnly],
    extendedRegistry,
    'en',
  );
  const summary = matrix.map(({ version, path, locale, entry, translated, alternateLocales }) => ({
    version,
    path,
    locale,
    entry: entry.id,
    translated,
    alternateLocales,
  }));

  assert.deepEqual(summary, [
    {
      version: 'nightly',
      path: 'settings',
      locale: 'en',
      entry: 'nightly/settings',
      translated: true,
      alternateLocales: ['en', 'zh-Hans'],
    },
    {
      version: 'nightly',
      path: 'settings',
      locale: 'zh-Hans',
      entry: 'nightly/zh-Hans/settings',
      translated: true,
      alternateLocales: ['en', 'zh-Hans'],
    },
    {
      version: 'nightly',
      path: 'usage',
      locale: 'en',
      entry: 'nightly/usage',
      translated: true,
      alternateLocales: ['en'],
    },
    {
      version: 'nightly',
      path: 'usage',
      locale: 'zh-Hans',
      entry: 'nightly/usage',
      translated: false,
      alternateLocales: ['en'],
    },
  ]);
});

test('maps registered localized repository directories without Spanish special cases', () => {
  assert.deepEqual(localizedDocPath('docs/zh-hans/SETTINGS.md', extendedRegistry, 'en'), {
    locale: 'zh-Hans',
    path: 'settings',
  });
  assert.deepEqual(localizedDocPath('docs/SETTINGS.md', extendedRegistry, 'en'), {
    locale: 'en',
    path: 'settings',
  });
  assert.equal(localizedDocPath('docs/es/SETTINGS.md', extendedRegistry, 'en'), undefined);
});

test('selects exactly one localized or fallback search source per canonical document', () => {
  const english = { id: 'nightly/settings', body: '# Settings' };
  const translated = { id: 'nightly/zh-Hans/settings', body: '# 设置' };
  const englishOnly = { id: 'nightly/usage', body: '# Usage' };
  const otherVersion = { id: 'v0.7/settings', body: '# Old settings' };

  const selected = selectLocaleSearchEntries(
    [english, translated, englishOnly, otherVersion],
    'nightly',
    'zh-Hans',
    extendedRegistry,
    'en',
  );

  assert.deepEqual(
    selected.map(({ path, entry, translated }) => ({ path, entry: entry.id, translated })),
    [
      { path: 'settings', entry: 'nightly/zh-Hans/settings', translated: true },
      { path: 'usage', entry: 'nightly/usage', translated: false },
    ],
  );
  assert.deepEqual(
    selectLocaleSearchEntries(
      [english, translated, englishOnly, otherVersion],
      'nightly',
      'en',
      extendedRegistry,
      'en',
    ).map(({ entry }) => entry.id),
    ['nightly/settings', 'nightly/usage'],
  );
});

test('generates locale-aware search index URLs without changing English URLs', () => {
  assert.equal(searchIndexPath('v0.7', 'v0.7', 'en', 'en'), '/search-index.json');
  assert.equal(searchIndexPath('nightly', 'v0.7', 'en', 'en'), '/nightly/search-index.json');
  assert.equal(searchIndexPath('v0.7', 'v0.7', 'zh-Hans', 'en'), '/zh-Hans/search-index.json');
  assert.equal(
    searchIndexPath('nightly', 'v0.7', 'zh-Hans', 'en'),
    '/nightly/zh-Hans/search-index.json',
  );
});
