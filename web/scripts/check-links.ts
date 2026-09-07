/**
 * Fail the build on an internal link that points at a page the site does not
 * render, and on a relative link the rewriting never resolved.
 *
 * The docs are authored to be read on GitHub and link to each other and to
 * source files by repository path; those hrefs are rewritten at build time.
 * This is the check that the rewriting kept up when a document is added,
 * renamed or removed. A surviving relative href is its own failure: it resolves
 * against whichever page happens to render it, so the same link is broken on
 * one page and not another, and checking absolute hrefs alone never sees it.
 *
 * Only same-origin links are checked. In a split deployment the landing page
 * links to the documentation host on purpose, and those targets are not in this
 * build to verify.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const DIST = fileURLToPath(new URL('../dist/', import.meta.url));

async function htmlFiles(dir: string): Promise<string[]> {
  const found: string[] = [];

  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);

    if (entry.isDirectory()) found.push(...(await htmlFiles(path)));
    else if (entry.name.endsWith('.html')) found.push(path);
  }

  return found;
}

const files = await htmlFiles(DIST);

/**
 * The document without its scripts.
 *
 * Client bundles contain `href="..."` inside template literals and string
 * assignments. Those are code that computes a link at runtime, not a link in
 * the page, and reading them as markup reports every rendered template as a
 * broken relative href.
 */
const markup = (html: string) => html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '');

/** An href resolved against the current page rather than the site root. */
const relativeHref = (href: string) =>
  href !== '' && !/^[a-z][a-z0-9+.-]*:|^\/\/|^\/|^#/i.test(href);

/** `relative()` is platform-separator aware; URLs are always forward slashes. */
const toPosix = (path: string) => relative(DIST, path).split('\\').join('/');

const pages = new Set(
  files.map((file) => `/${toPosix(file).replace(/index\.html$/, '')}`.replace(/\/$/, '/')),
);

const broken: string[] = [];
const unrewritten: string[] = [];

for (const file of files) {
  const html = await readFile(file, 'utf8');
  const from = `/${toPosix(file)}`;

  for (const match of markup(html).matchAll(/href="([^"]*)"/g)) {
    const href = match[1];

    // A relative href is a repository path the rewriting failed to resolve. It
    // resolves against the page's own URL, so it points at a page that was
    // never built and no absolute-href check would ever see it.
    if (relativeHref(href)) {
      unrewritten.push(`${from} -> ${href}`);
      continue;
    }

    if (!href.startsWith('/')) continue;

    const path = href.split(/[#?]/)[0];

    if (path.startsWith('/_astro/') || /\.[a-z0-9]+$/i.test(path)) continue;

    const target = path.endsWith('/') ? path : `${path}/`;

    if (!pages.has(target)) broken.push(`${from} -> ${path}`);
  }
}

if (unrewritten.length > 0) {
  console.error(`${unrewritten.length} relative link(s) were never rewritten to a site route:\n`);
  for (const entry of [...new Set(unrewritten)].sort()) console.error(`  ${entry}`);
}

if (broken.length > 0) {
  console.error(`${broken.length} internal link(s) point at a page that is not built:\n`);
  for (const entry of [...new Set(broken)].sort()) console.error(`  ${entry}`);
}

if (broken.length + unrewritten.length > 0) process.exit(1);

console.log(`ok: ${files.length} pages, every internal link resolves`);
