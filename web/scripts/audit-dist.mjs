import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { relative, resolve } from 'node:path';
import { DEFAULT_LOCALE, LOCALE_REGISTRY } from '../src/i18n/locale-registry.mjs';
const mode =
  process.argv.at(-1) === 'docs' ? 'docs' : process.argv.at(-1) === 'site' ? 'site' : null;
if (!mode) throw new Error('usage: node scripts/audit-dist.mjs --mode site|docs');
const origin = mode === 'docs' ? 'https://docs.dbflux.dev' : 'https://dbflux.dev';
const fail = (message) => {
  throw Error(`SEO audit: ${message}`);
};
const text = (path) => (existsSync(path) ? readFileSync(path, 'utf8') : fail(`missing ${path}`));
const root = resolve('dist');
const fileFor = (pathname) => resolve(root, pathname.replace(/^\//, ''), 'index.html');
const pathnameFor = (path) => {
  const output = relative(root, path).split('\\').join('/');
  return output === 'index.html' ? '/' : `/${output.replace(/index\.html$/, '')}`;
};
const locales = LOCALE_REGISTRY.map(({ id }) => id);
const routeKey = (pathname) => {
  const [, first, ...rest] = pathname.split('/');
  return locales.includes(first)
    ? { locale: first, key: `/${rest.join('/')}` }
    : { locale: DEFAULT_LOCALE, key: pathname };
};
const [sitemap, robots, llms] = ['sitemap-0.xml', 'robots.txt', 'llms.txt'].map((path) =>
  text(`dist/${path}`),
);
const contentSignal = 'Content-Signal: search=yes,ai-train=no,use=reference';
for (const userAgent of ['Google-Extended', '*']) {
  const block = `User-agent: ${userAgent}\n${contentSignal}\nAllow: /\n\n`;
  if (!robots.includes(block)) fail(`robots has invalid ${userAgent} directive block`);
}
if (!robots.includes(`Sitemap: ${origin}/sitemap-index.xml`)) fail('robots has invalid sitemap');
const locations = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/g)].map(([, url]) => new URL(url));
const expected = new Set();
for (const file of readdirSync(root, { recursive: true })) {
  const path = resolve(root, file);
  if (!/[/\\]index\.html$/.test(path)) continue;
  const pathname = pathnameFor(path);
  const html = text(path);
  if (
    html.includes(`<link rel="canonical" href="${origin}${pathname}"`) &&
    !html.includes('name="robots" content="noindex, follow"')
  )
    expected.add(pathname);
}
const actual = new Set(locations.map(({ pathname }) => pathname));
if (
  actual.size !== locations.length ||
  actual.size !== expected.size ||
  [...expected].some((pathname) => !actual.has(pathname))
)
  fail(`sitemap route set differs: expected ${expected.size}, found ${locations.length}`);
const groups = new Map();
for (const pathname of expected) {
  const { locale, key } = routeKey(pathname);
  groups.set(key, [...(groups.get(key) ?? []), { locale, pathname }]);
}
for (const pathname of expected) {
  const siblings = groups.get(routeKey(pathname).key) ?? [];
  const wanted = new Map(siblings.map(({ locale, pathname }) => [locale, `${origin}${pathname}`]));
  const fallback = siblings.find(({ locale }) => locale === DEFAULT_LOCALE);
  if (fallback) wanted.set('x-default', `${origin}${fallback.pathname}`);
  const matches = [
    ...text(fileFor(pathname)).matchAll(/<link rel="alternate" hreflang="([^"]+)" href="([^"]+)"/g),
  ];
  const found = new Map(matches.map(([, label, href]) => [label, href]));
  if (
    found.size !== matches.length ||
    found.size !== wanted.size ||
    [...wanted].some(([label, href]) => found.get(label) !== href)
  )
    fail(`${pathname} hreflang set differs from real canonical siblings`);
}
for (const url of locations) {
  if (url.origin !== origin) fail(`sitemap host differs for ${url}`);
  const html = text(fileFor(url.pathname));
  if (!html.includes(`<link rel="canonical" href="${url.href}"`))
    fail(`non-self canonical ${url.pathname}`);
  if (html.includes('name="robots" content="noindex, follow"'))
    fail(`indexed noindex page ${url.pathname}`);
}
for (const path of ['/', '/install/', '/usage/']) {
  if (!llms.includes(`- ${mode === 'docs' ? path : `https://docs.dbflux.dev${path}`}`))
    fail(`llms lacks ${path}`);
}
for (const link of llms.matchAll(/^- (https?:\/\/[^\s]+)$/gm)) {
  const url = new URL(link[1]);
  if (url.hostname !== 'docs.dbflux.dev' || /nightly|v0\.6/.test(url.pathname))
    fail(`unsafe llms link ${url}`);
}
if (
  mode === 'docs' &&
  /<meta (?:name="description"|property="og:description")[^>]*(?:```|bash|curl|\$ )/i.test(
    text('dist/install/index.html'),
  )
)
  fail('/install metadata leaks code or shell content');
console.log(`ok: SEO foundation audit (${mode})`);
