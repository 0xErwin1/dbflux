import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const LOCALES = ['en', 'es', 'ko', 'zh-Hans'];
const MARKER = 'data-locale-redirect';

/** The root locale redirect exactly as `LocaleRedirect.astro` inlines it into the page head. */
function redirectScript(): string {
  const source = readFileSync(
    fileURLToPath(new URL('../components/LocaleRedirect.astro', import.meta.url)),
    'utf8',
  );
  const marker = source.indexOf(MARKER);
  assert.notEqual(marker, -1, `Could not find the ${MARKER} script in LocaleRedirect.astro`);

  const start = source.indexOf('>', marker) + 1;
  const end = source.indexOf('</script>', start);
  assert.notEqual(end, -1, 'Could not find the closing script tag');
  return source.slice(start, end);
}

interface Visit {
  languages?: readonly string[];
  language?: string;
  stored?: string | null;
  storageFails?: boolean;
  search?: string;
  hash?: string;
}

/** Run the redirect for one visit to `/` and return where it sends the reader, if anywhere. */
function visit({
  languages,
  language = 'en-US',
  stored = null,
  storageFails = false,
  search = '',
  hash = '',
}: Visit): string | null {
  let destination: string | null = null;
  const context = {
    document: {
      currentScript: { dataset: { locales: LOCALES.join(' '), defaultLocale: 'en' } },
    },
    navigator: { languages, language },
    localStorage: {
      getItem: () => {
        if (storageFails) throw new Error('SecurityError');
        return stored;
      },
    },
    location: {
      search,
      hash,
      replace: (url: string) => {
        destination = url;
      },
    },
  };

  new vm.Script(redirectScript(), { filename: 'inline-locale-redirect.js' }).runInNewContext(
    context,
  );
  return destination;
}

test('opens the root in the first browser language the site translates', () => {
  assert.equal(visit({ languages: ['es-AR', 'en'] }), '/es/');
  assert.equal(visit({ languages: ['ko-KR'] }), '/ko/');
  assert.equal(visit({ languages: ['fr-FR', 'es'] }), '/es/');
});

test('keeps the default locale when the first matching language is English', () => {
  assert.equal(visit({ languages: ['en-GB', 'es'] }), null);
  assert.equal(visit({ languages: ['fr-FR', 'de'] }), null);
  assert.equal(visit({ languages: [], language: 'en-US' }), null);
});

test('maps simplified Chinese tags to zh-Hans and leaves traditional Chinese alone', () => {
  assert.equal(visit({ languages: ['zh-CN'] }), '/zh-Hans/');
  assert.equal(visit({ languages: ['zh'] }), '/zh-Hans/');
  assert.equal(visit({ languages: ['zh-Hans-SG'] }), '/zh-Hans/');
  assert.equal(visit({ languages: ['zh-TW'] }), null);
  assert.equal(visit({ languages: ['zh-Hant', 'ko'] }), '/ko/');
});

test('an explicit language choice wins over the browser languages', () => {
  assert.equal(visit({ languages: ['es-ES'], stored: 'en' }), null);
  assert.equal(visit({ languages: ['en-US'], stored: 'ko' }), '/ko/');
});

test('ignores a stored value that is not a site locale', () => {
  assert.equal(visit({ languages: ['es'], stored: 'fr' }), '/es/');
});

test('falls back to the browser languages when storage is unavailable', () => {
  assert.equal(visit({ languages: ['ko'], storageFails: true }), '/ko/');
});

test('uses navigator.language when navigator.languages is missing', () => {
  assert.equal(visit({ language: 'es-MX' }), '/es/');
});

test('keeps the query string and fragment on the redirect', () => {
  assert.equal(
    visit({ languages: ['es'], search: '?ref=x', hash: '#install' }),
    '/es/?ref=x#install',
  );
});
