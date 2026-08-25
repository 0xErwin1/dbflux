import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import manifest from './.versions/manifest.json';
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import { routeForRepoPath, titleForRepoPath } from './src/data/nav';
import { sitemapPathsFor } from './src/data/docs';
import { CURRENT } from './src/data/versions';
import { DOCS_MODE, ORIGIN } from './src/data/site';
import { hostRedirects } from './src/integrations/host-redirects';
import { DEFAULT_LOCALE } from './src/i18n';
import type { Locale } from './src/i18n';
import { LOCALE_REGISTRY, localeForRepoPath } from './src/i18n/locale-registry.mjs';

const REPO_ROOT = fileURLToPath(new URL('../', import.meta.url));
const sitemapPaths = sitemapPathsFor(
  manifest.find((entry) => entry.id === CURRENT.id)?.sourceFacts ?? [],
);

/** Collect the text of a node back into a plain string. */
function textOf(node: any): string {
  if (node.type === 'text') return node.value;
  return (node.children ?? []).map(textOf).join('');
}

/**
 * The repository root a rendered doc file was materialised from, and the
 * locale it renders in.
 *
 * `content.config.ts` mirrors each version's repository tree under
 * `.versions/<version>/` (see `scripts/fetch-docs.mjs`), so a source file's
 * `fromDir` sits inside that per-version mirror, not inside the actual
 * checkout `REPO_ROOT` points at. Resolving a relative link against
 * `REPO_ROOT` therefore left `web/.versions/<version>/` in the computed repo
 * path, which never matched `routeForRepoPath`'s patterns and silently fell
 * back to a broken `github.com/.../blob/main/web/.versions/...` link. The
 * per-version mirror root is the correct base for that resolution.
 *
 * The locale is derived from the registered localized docs directories inside
 * the mirror. The version is the mirror's own directory name, needed so a link
 * written inside a version-exclusive page (e.g. a driver only shipped in `nightly`)
 * resolves within that same version instead of silently falling back to the
 * current release.
 */
function versionContext(
  filePath: string,
): { root: string; locale: Locale; version: string } | null {
  const marker = '/.versions/';
  const markerIndex = filePath.indexOf(marker);
  if (markerIndex === -1) return null;

  const afterMarker = filePath.slice(markerIndex + marker.length);
  const versionEnd = afterMarker.indexOf('/');
  if (versionEnd === -1) return null;

  const version = afterMarker.slice(0, versionEnd);
  const root = filePath.slice(0, markerIndex + marker.length) + version;
  const repoPath = afterMarker.slice(versionEnd + 1);
  const locale = localeForRepoPath(repoPath) as Locale;

  return { root, locale, version };
}

/**
 * Point the repository's relative markdown links at the pages rendering them,
 * and give them a title a reader can act on.
 *
 * The docs are written to be read on GitHub, where `SETTINGS.md` is both a
 * working href and a sensible label. Served under `/docs/usage/` that href
 * resolves to a page that does not exist, and the label names a file the reader
 * does not have. Both are rewritten here so the markdown stays correct in the
 * repository and reads correctly on the site.
 */
function rehypeRepoLinks() {
  return (tree: any, file: any) => {
    const filePath = file.path ?? file.history?.[0] ?? '';
    const context = versionContext(filePath);
    const root = context?.root ?? REPO_ROOT;
    const locale = context?.locale ?? DEFAULT_LOCALE;
    const version = context?.version;
    const fromDir = dirname(filePath);

    const visit = (node: any) => {
      if (node.type === 'element' && node.tagName === 'a') {
        const href = node.properties?.href;

        if (typeof href === 'string' && !/^[a-z]+:|^\/|^#/i.test(href)) {
          const [target, hash] = href.split('#');

          if (target.endsWith('.md')) {
            const repoPath = relative(root, resolve(fromDir, target)).split('\\').join('/');

            node.properties.href =
              routeForRepoPath(repoPath, locale, version) + (hash ? `#${hash}` : '');

            // Only relabel when the text is the path itself. A link already
            // written as a sentence is the author's wording and stays.
            const label = textOf(node).trim();
            const title = titleForRepoPath(repoPath);

            if (title && /\.md$/.test(label)) {
              node.children = [{ type: 'text', value: title }];
            }
          }
        }
      }

      // A bare `docs/AUDIT.md` in prose is a file reference, which means
      // nothing to a reader who has no checkout. When the site renders that
      // file, turn the mention into a link to the page.
      if (node.type === 'element' && node.tagName !== 'a' && Array.isArray(node.children)) {
        node.children = node.children.map((child: any) => {
          if (child.type !== 'element' || child.tagName !== 'code') return child;

          const mention = textOf(child).trim();
          if (!/^[\w./-]+\.md$/.test(mention)) return child;

          const repoPath = mention.replace(/^\.\//, '');
          const title = titleForRepoPath(repoPath);
          if (!title) return child;

          return {
            type: 'element',
            tagName: 'a',
            properties: { href: routeForRepoPath(repoPath, locale, version) },
            children: [{ type: 'text', value: title }],
          };
        });
      }

      for (const child of node.children ?? []) visit(child);
    };

    visit(tree);
  };
}

/**
 * Hand mermaid fences to the client renderer instead of the syntax highlighter,
 * so diagrams draw as diagrams rather than as a listing of their own source.
 */
function rehypeMermaid() {
  return (tree: any) => {
    const visit = (node: any) => {
      if (!Array.isArray(node.children)) return;

      node.children = node.children.map((child: any) => {
        visit(child);

        const isMermaid =
          child.type === 'element' &&
          child.tagName === 'pre' &&
          child.properties?.dataLanguage === 'mermaid';

        if (!isMermaid) return child;

        return {
          type: 'element',
          tagName: 'div',
          properties: { className: ['mermaid'] },
          children: [{ type: 'text', value: textOf(child) }],
        };
      });
    };

    visit(tree);
  };
}

export default defineConfig({
  site: ORIGIN,
  i18n: {
    defaultLocale: DEFAULT_LOCALE,
    locales: LOCALE_REGISTRY.map(({ id }) => id),
    routing: 'manual',
  },
  integrations: [
    sitemap({
      filter: (page) => DOCS_MODE !== 'docs' || sitemapPaths.has(new URL(page).pathname),
    }),
    hostRedirects(),
  ],
  markdown: {
    rehypePlugins: [rehypeRepoLinks, rehypeMermaid],
    shikiConfig: {
      theme: 'ayu-dark',
      wrap: false,
    },
  },
});
