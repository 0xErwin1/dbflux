/**
 * Fail the build on an internal link that points at a page the site does not
 * render.
 *
 * The docs are authored to be read on GitHub and link to each other by
 * filename; those hrefs are rewritten at build time. This is the check that the
 * rewriting kept up when a document is added, renamed or removed.
 *
 * Only same-origin links are checked. In a split deployment the landing page
 * links to the documentation host on purpose, and those targets are not in this
 * build to verify.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join, relative } from 'node:path';

const DIST = new URL('../dist/', import.meta.url).pathname;

async function htmlFiles(dir) {
  const found = [];

  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);

    if (entry.isDirectory()) found.push(...(await htmlFiles(path)));
    else if (entry.name.endsWith('.html')) found.push(path);
  }

  return found;
}

const files = await htmlFiles(DIST);

const pages = new Set(
  files.map((file) => `/${relative(DIST, file).replace(/index\.html$/, '')}`.replace(/\/$/, '/')),
);

const broken = [];

for (const file of files) {
  const html = await readFile(file, 'utf8');
  const from = `/${relative(DIST, file)}`;

  for (const match of html.matchAll(/href="(\/[^"#?]*)/g)) {
    const href = match[1];

    if (href.startsWith('/_astro/') || /\.[a-z0-9]+$/i.test(href)) continue;

    const target = href.endsWith('/') ? href : `${href}/`;

    if (!pages.has(target)) broken.push(`${from} -> ${href}`);
  }
}

if (broken.length > 0) {
  console.error(`${broken.length} internal link(s) point at a page that is not built:\n`);
  for (const entry of [...new Set(broken)].sort()) console.error(`  ${entry}`);
  process.exit(1);
}

console.log(`ok: ${files.length} pages, every internal link resolves`);
