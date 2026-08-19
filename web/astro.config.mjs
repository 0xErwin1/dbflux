import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

const REPO_ROOT = fileURLToPath(new URL('../', import.meta.url));
const REPO_URL = 'https://github.com/0xErwin1/dbflux';

/**
 * Map a repository path to the page that renders it, or to the repository when
 * the site does not host it.
 *
 * Kept in step with the patterns in `src/content.config.ts`.
 */
function routeForRepoPath(path) {
  const driver = path.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
  if (driver) return `/docs/drivers/${driver[1]}/`;

  const doc = path.match(/^docs\/([^/]+)\.md$/);
  if (doc) return `/docs/${doc[1].toLowerCase()}/`;

  if (path === 'ARCHITECTURE.md') return '/docs/architecture/';
  if (path === 'CONTRIBUTING.md') return '/docs/contributing/';

  return `${REPO_URL}/blob/main/${path}`;
}

/**
 * Rewrite the repository's relative markdown links to site routes.
 *
 * The docs are written to be read on GitHub, where `SETTINGS.md` is a sibling
 * file. Served under `/docs/usage/` that same href resolves to a page that does
 * not exist, so every link is re-pointed at the page rendering that file.
 */
function rehypeRepoLinks() {
  return (tree, file) => {
    const fromDir = dirname(file.path ?? file.history?.[0] ?? '');

    const visit = (node) => {
      if (node.type === 'element' && node.tagName === 'a') {
        const href = node.properties?.href;

        if (typeof href === 'string' && !/^[a-z]+:|^\/|^#/i.test(href)) {
          const [target, hash] = href.split('#');

          if (target.endsWith('.md')) {
            const repoPath = relative(REPO_ROOT, resolve(fromDir, target)).split('\\').join('/');
            node.properties.href = routeForRepoPath(repoPath) + (hash ? `#${hash}` : '');
          }
        }
      }

      for (const child of node.children ?? []) visit(child);
    };

    visit(tree);
  };
}

/** Collect the text of a highlighted code block back into its original source. */
function textOf(node) {
  if (node.type === 'text') return node.value;
  return (node.children ?? []).map(textOf).join('');
}

/**
 * Hand mermaid fences to the client renderer instead of the syntax highlighter,
 * so diagrams draw as diagrams rather than as a listing of their own source.
 */
function rehypeMermaid() {
  return (tree) => {
    const visit = (node) => {
      if (!Array.isArray(node.children)) return;

      node.children = node.children.map((child) => {
        visit(child);

        const isMermaid =
          child.type === 'element' &&
          child.tagName === 'pre' &&
          child.properties?.['dataLanguage'] === 'mermaid';

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
  site: 'https://dbflux.dev',
  integrations: [sitemap()],
  markdown: {
    rehypePlugins: [rehypeRepoLinks, rehypeMermaid],
    shikiConfig: {
      theme: 'ayu-dark',
      wrap: false,
    },
  },
});
