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
  const output = relative(root, path).replaceAll('\\', '/');
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
  if (!path.endsWith('/index.html')) continue;
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
const thinRoutes =
  '/install/postgresql/ /install/mysql/ /install/mongodb/ /features/sql-editor/ /features/local-mcp-governance/'.split(
    ' ',
  );
if (mode === 'site')
  for (const route of thinRoutes)
    if (existsSync(fileFor(route))) fail(`thin route appears in site output: ${route}`);
const markdownFiles = new Set(
  readdirSync(root, { recursive: true })
    .filter((file) => file.endsWith('index.md'))
    .map((file) => `/${file.slice(0, -8)}`),
);
if (mode === 'docs') {
  const markdownExpected = new Set(
    [...expected].filter((path) => routeKey(path).locale === DEFAULT_LOCALE && path !== '/'),
  );
  if (
    markdownFiles.size !== markdownExpected.size ||
    [...markdownExpected].some((file) => !markdownFiles.has(file))
  )
    fail('Markdown sibling set differs from current English documents');
  if (
    locations.some(({ pathname }) => pathname.endsWith('.md')) ||
    !text('dist/install/index.md').includes('](/release/)')
  )
    fail('invalid Markdown discovery or links');
} else if (markdownFiles.size) fail('Markdown appears outside docs mode');
for (const config of ['wrangler.jsonc', 'wrangler.docs.jsonc'])
  if (
    !/"main": "worker.mjs"[\s\S]*"binding": "ASSETS"[\s\S]*"run_worker_first": true/.test(
      text(config),
    )
  )
    fail(`invalid ${config}`);
const { default: worker } = await import('../worker.mjs');
const assets = {
  fetch(request) {
    const path = new URL(request.url).pathname;
    if (path.endsWith('index.md'))
      return path.includes('missing-sibling')
        ? new Response('', { status: 404 })
        : new Response('markdown');
    if (path.includes('redirect'))
      return new Response(null, { status: 302, headers: { Location: '/' } });
    if (path.includes('error')) return new Response('error', { status: 500 });
    return new Response('html', {
      headers: {
        'content-type': 'text/html',
        Vary: 'Origin',
        Link: '<old>; rel="next"',
        'cache-control': 'max-age=60',
      },
    });
  },
};
const request = (accept, method = 'GET', path = '/usage/') =>
  worker.fetch(
    new Request(`https://docs.dbflux.dev${path}`, { method, headers: { Accept: accept } }),
    { ASSETS: assets },
  );
for (const [accept, body] of [
  ['text/markdown', 'markdown'],
  ['text/html', 'html'],
  ['', 'html'],
  ['*/*', 'html'],
  ['application/json', 'html'],
  ['text/markdown;q=no', 'html'],
  ['text/html;q=0.4,text/markdown;q=0.9', 'markdown'],
  ['text/html,text/markdown', 'markdown'],
  ['text/markdown;q=0', 'html'],
]) {
  const response = await request(accept);
  if (
    (await response.text()) !== body ||
    !response.headers.get('vary')?.includes('Accept') ||
    !response.headers.get('link')?.includes('old')
  )
    fail(`Worker negotiation failed for ${accept || 'absent'}`);
}
for (const [method, path, status, body] of [
  ['HEAD', '/usage/', 200, ''],
  ['POST', '/usage/', 200, 'html'],
  ['GET', '/redirect/', 302, ''],
  ['GET', '/error/', 500, 'error'],
  ['GET', '/missing-sibling/', 200, 'html'],
]) {
  const response = await request('text/markdown', method, path);
  if (response.status !== status || (await response.text()) !== body)
    fail(`Worker response semantics failed for ${method} ${path}`);
}
console.log(`ok: SEO foundation audit (${mode})`);
