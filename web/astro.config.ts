import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import { routeForRepoPath, titleForRepoPath } from './src/data/nav';

const REPO_ROOT = fileURLToPath(new URL('../', import.meta.url));

/** Collect the text of a node back into a plain string. */
function textOf(node: any): string {
  if (node.type === 'text') return node.value;
  return (node.children ?? []).map(textOf).join('');
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
    const fromDir = dirname(file.path ?? file.history?.[0] ?? '');

    const visit = (node: any) => {
      if (node.type === 'element' && node.tagName === 'a') {
        const href = node.properties?.href;

        if (typeof href === 'string' && !/^[a-z]+:|^\/|^#/i.test(href)) {
          const [target, hash] = href.split('#');

          if (target.endsWith('.md')) {
            const repoPath = relative(REPO_ROOT, resolve(fromDir, target)).split('\\').join('/');

            node.properties.href = routeForRepoPath(repoPath) + (hash ? `#${hash}` : '');

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
