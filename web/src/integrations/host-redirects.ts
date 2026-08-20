import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import type { AstroIntegration } from 'astro';
import { DOCS_MODE, SITE_ORIGIN, docsPath, docsUrl } from '../data/site';
import { CURRENT, VERSIONS } from '../data/versions';

/**
 * Tell the two hosts about each other with a status code.
 *
 * A static build cannot answer 301 by itself, so the site used to ship a page
 * at every moved documentation URL carrying a meta refresh. That answers 200
 * with an empty document: a person arrives, but anything reading the site
 * mechanically — a crawler, an agent — sees a live page with no content at a
 * URL the repository still links to, and concludes the documentation is being
 * moved around. Cloudflare reads this file from the asset directory and
 * answers before it looks for an asset, which lets the site say the one true
 * thing: the page moved, permanently, to there.
 */
function rules(): string[] {
  if (DOCS_MODE === 'site') {
    const moved = VERSIONS.map((version) => {
      const prefix = version.id === CURRENT.id ? '' : version.id;

      return { from: `/${docsPath('', prefix)}`, to: docsUrl('', prefix) };
    });

    // Cloudflare matches in file order and asks that the static rules come
    // first, so the exact paths are written before the wildcards.
    return [
      ...moved.map(({ from, to }) => `${from} ${to} 301`),
      ...moved.map(({ from, to }) => `${from}/* ${to}:splat 301`),
    ];
  }

  // The landing pages belong to the site host. This build emits `/about/` only
  // because one source tree builds both; it already canonicalises off-host.
  if (DOCS_MODE === 'docs') {
    return [`/about ${SITE_ORIGIN}/about/ 301`, `/about/ ${SITE_ORIGIN}/about/ 301`];
  }

  return [];
}

export function hostRedirects(): AstroIntegration {
  return {
    name: 'dbflux:host-redirects',
    hooks: {
      'astro:build:done': async ({ dir }) => {
        const lines = rules();

        if (lines.length === 0) return;

        const path = fileURLToPath(new URL('_redirects', dir));

        await mkdir(dirname(path), { recursive: true });
        await writeFile(path, `${lines.join('\n')}\n`, 'utf8');
      },
    },
  };
}
