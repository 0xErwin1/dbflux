/**
 * Screenshots and diagrams the documentation embeds from `docs/images/`.
 *
 * Kept free of build-time imports so Node's test runner can load it without
 * Vite. `scripts/fetch-docs.ts` uses `isDocImagePath` to mirror the images of
 * every version, and `astro.config.ts` wires the rewrite and the integration
 * below to that version's served URL.
 */
import { createReadStream } from 'node:fs';
import { copyFile, mkdir, readdir } from 'node:fs/promises';
import { dirname, extname, join, relative, resolve } from 'node:path';
import type { AstroIntegration } from 'astro';

const CONTENT_TYPES: Readonly<Record<string, string>> = {
  '.webp': 'image/webp',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
};

/** A repository path the site publishes as a documentation image. */
export function isDocImagePath(path: string): boolean {
  return /^docs\/images\/.+\.(?:webp|png|jpe?g|svg)$/i.test(path);
}

/** Maps an image's repository path in a version to the URL a page should load it from. */
export type DocImageUrl = (repoPath: string, versionId: string) => string;

/**
 * The version mirror a rendered file sits in, as `.versions/<version>/<repoPath>`.
 *
 * Accepts the absolute path the markdown pipeline reports and the path
 * relative to `web/` a content entry carries.
 */
function mirrorOf(filePath: string): { root: string; version: string } | null {
  const marker = filePath.startsWith('.versions/') ? '.versions/' : '/.versions/';
  const markerIndex = filePath.indexOf(marker);
  if (markerIndex === -1) return null;

  const afterMarker = filePath.slice(markerIndex + marker.length);
  const versionEnd = afterMarker.indexOf('/');
  if (versionEnd === -1) return null;

  const version = afterMarker.slice(0, versionEnd);

  return { root: filePath.slice(0, markerIndex + marker.length) + version, version };
}

/**
 * Map an image source written in the page at `filePath` to its served URL.
 *
 * Only a path that resolves into `docs/images/` of the page's own version is
 * rewritten. Anything else comes back as written, which the link check reports
 * instead of the page shipping a broken image. A `srcset` with several
 * candidates comes back unchanged too: the authoring convention is one image
 * per `<source>`. Returns null when the file is not inside a version mirror.
 */
function sourceRewriter(
  filePath: string,
  urlFor: DocImageUrl,
): ((source: string) => string) | null {
  const mirror = mirrorOf(filePath);
  if (!mirror) return null;

  const fromDir = dirname(filePath);

  return (source) => {
    if (source === '' || /^[a-z][a-z0-9+.-]*:|^\/|^#/i.test(source)) return source;
    const trimmed = source.trim();
    if (/[\s,]/.test(trimmed)) return source;

    const cut = trimmed.search(/[?#]/);
    const path = cut === -1 ? trimmed : trimmed.slice(0, cut);
    const suffix = cut === -1 ? '' : trimmed.slice(cut);
    let decoded: string;

    try {
      decoded = decodeURI(path);
    } catch {
      return source;
    }

    const repoPath = relative(mirror.root, resolve(fromDir, decoded)).split('\\').join('/');
    if (!isDocImagePath(repoPath)) return source;

    return urlFor(repoPath, mirror.version) + suffix;
  };
}

const IMAGE_TAG = /<(?:img|source)\b[^>]*>/gi;

/** Rewrite the `src` and `srcset` attributes of one `<img>` or `<source>` tag. */
const rewriteTag = (tag: string, rewrite: (source: string) => string) =>
  tag.replace(
    /(\s(?:src|srcset)\s*=\s*)(["'])(.*?)\2/gi,
    (_, prefix: string, quote: string, value: string) =>
      `${prefix}${quote}${rewrite(value)}${quote}`,
  );

/**
 * Point a page's relative `<img src>` and `<source srcset>` at the served copy
 * of the image in the page's own version.
 *
 * Pages use a `<picture>` element so GitHub can pick the light or dark
 * screenshot, and write the path relative to the markdown file. That HTML is
 * still a `raw` node at this stage, because Astro parses it after the
 * configured rehype plugins run, so its attributes are rewritten in the markup
 * itself. A markdown `![alt](path)` image is already an `img` element; it is
 * rewritten too and removed from `data.astro.localImagePaths`, the list Astro's
 * own image pipeline imports and optimises from. Left there, every screenshot
 * would also be emitted under `/_astro/`, and optimising it needs `sharp`,
 * which this site does not install.
 *
 * `data` is the rendered file's `vfile.data`.
 */
export function rewriteDocImages(
  tree: any,
  filePath: string,
  urlFor: DocImageUrl,
  data?: any,
): void {
  const rewrite = sourceRewriter(filePath, urlFor);
  if (!rewrite) return;

  const rewrittenMarkdownImages = new Set<string>();

  const visit = (node: any) => {
    if (node.type === 'raw' && typeof node.value === 'string') {
      node.value = node.value.replace(IMAGE_TAG, (tag: string) => rewriteTag(tag, rewrite));
    }

    if (node.type === 'element' && node.tagName === 'img') {
      const source = node.properties?.src;

      if (typeof source === 'string') {
        const rewritten = rewrite(source);

        if (rewritten !== source) {
          node.properties.src = rewritten;
          rewrittenMarkdownImages.add(source);
        }
      }
    }

    for (const child of node.children ?? []) visit(child);
  };

  visit(tree);

  const imported = data?.astro?.localImagePaths;
  if (Array.isArray(imported) && rewrittenMarkdownImages.size > 0) {
    data.astro.localImagePaths = imported.filter(
      (path: string) => !rewrittenMarkdownImages.has(path),
    );
  }
}

/**
 * The same rewrite as `rewriteDocImages`, applied to the markdown source of a
 * page, for the plain-markdown copy published next to it.
 *
 * Covers `![alt](path "title")` images and `<img>` / `<source>` tags. Fenced
 * code blocks and inline code spans are left exactly as written, as the HTML
 * page leaves them.
 */
export function rewriteMarkdownImages(body: string, filePath: string, urlFor: DocImageUrl): string {
  const rewrite = sourceRewriter(filePath, urlFor);
  if (!rewrite) return body;

  const prose = (text: string) =>
    text.replace(
      /(`+)[\s\S]*?\1|<(?:img|source)\b[^>]*>|(!\[[^\]]*\]\()([^)\s]+)((?:\s+"[^"]*")?\))/gi,
      (match: string, code?: string, opening?: string, source?: string, closing?: string) => {
        if (code) return match;
        if (opening && source !== undefined && closing) {
          return `${opening}${rewrite(source)}${closing}`;
        }

        return rewriteTag(match, rewrite);
      },
    );

  // Odd segments are fenced code blocks, captured whole by the split.
  return body
    .split(/^((?:```|~~~)[^\n]*\n[\s\S]*?^(?:```|~~~)[ \t]*$)/m)
    .map((segment, index) => (index % 2 === 1 ? segment : prose(segment)))
    .join('');
}

/** Every documentation image under a directory, as paths relative to it. */
async function imagesUnder(root: string): Promise<string[]> {
  const directory = join(root, 'docs/images');
  let entries: string[];

  try {
    entries = (await readdir(directory, { recursive: true })) as string[];
  } catch {
    return [];
  }

  return entries
    .map((entry) => `docs/images/${entry.split('\\').join('/')}`)
    .filter(isDocImagePath);
}

/**
 * Serve each version's `docs/images/` at the path `outputPathFor` gives it:
 * copied into the build output, and answered by the dev server.
 *
 * Each version publishes the images of its own ref, so a page of an older
 * release never loads a screenshot that only exists in nightly. A version
 * without `docs/images/` simply contributes nothing.
 *
 * `outputPathFor` returns a path relative to the output root, without a
 * leading slash.
 */
export function docImages(options: {
  versionsDir: string;
  versionIds: readonly string[];
  outputPathFor: (repoPath: string, versionId: string) => string;
}): AstroIntegration {
  const published = async () => {
    const entries: Array<{ source: string; output: string }> = [];

    for (const versionId of options.versionIds) {
      const root = join(options.versionsDir, versionId);

      for (const repoPath of await imagesUnder(root)) {
        entries.push({
          source: join(root, repoPath),
          output: options.outputPathFor(repoPath, versionId),
        });
      }
    }

    return entries;
  };

  return {
    name: 'dbflux:doc-images',
    hooks: {
      'astro:server:setup': async ({ server }) => {
        const byUrl = new Map(
          (await published()).map(({ source, output }) => [`/${output}`, source]),
        );

        server.middlewares.use((request, response, next) => {
          let pathname: string;

          try {
            pathname = decodeURI(new URL(request.url ?? '/', 'http://localhost').pathname);
          } catch {
            return next();
          }

          const source = byUrl.get(pathname);
          if (!source) return next();

          response.setHeader(
            'content-type',
            CONTENT_TYPES[extname(source).toLowerCase()] ?? 'application/octet-stream',
          );
          createReadStream(source).pipe(response);
        });
      },
      'astro:build:done': async ({ dir }) => {
        const outputRoot = new URL('./', dir);

        for (const { source, output } of await published()) {
          const target = new URL(output, outputRoot);

          await mkdir(new URL('./', target), { recursive: true });
          await copyFile(source, target);
        }
      },
    },
  };
}
