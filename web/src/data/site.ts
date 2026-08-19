/**
 * Where the documentation lives.
 *
 * `embedded` keeps everything on one origin, documentation under `/docs/`. It is
 * the default because it is what a single local server can serve: splitting the
 * site in two should not mean running two of them, and editing a paragraph
 * should stay one command.
 *
 * `site` and `docs` are the two halves of a split deployment — one build of this
 * same source each, differing only in this variable.
 */
export type DocsMode = 'embedded' | 'site' | 'docs';

const MODES: readonly DocsMode[] = ['embedded', 'site', 'docs'];

function readMode(): DocsMode {
  const raw = process.env.DOCS_MODE ?? 'embedded';

  if (!MODES.includes(raw as DocsMode)) {
    throw new Error(`DOCS_MODE must be one of ${MODES.join(', ')}, got "${raw}"`);
  }

  return raw as DocsMode;
}

export const DOCS_MODE = readMode();

export const SITE_ORIGIN = process.env.SITE_ORIGIN ?? 'https://dbflux.dev';
export const DOCS_ORIGIN =
  process.env.DOCS_ORIGIN ?? (DOCS_MODE === 'embedded' ? SITE_ORIGIN : 'https://docs.dbflux.dev');

/** The origin this build is served from. */
export const ORIGIN = DOCS_MODE === 'docs' ? DOCS_ORIGIN : SITE_ORIGIN;

/** True when the documentation is served from the root of its own origin. */
export const DOCS_AT_ROOT = DOCS_MODE === 'docs';

/**
 * The path a documentation page is built at, without an origin.
 *
 * `path` is everything after the version, empty for a version's index. The
 * `/docs` segment exists only while the documentation shares the site's origin;
 * on its own host it is the root, which is the point of moving it there.
 */
export function docsPath(path: string, versionPrefix = ''): string {
  const parts = [versionPrefix, DOCS_AT_ROOT ? '' : 'docs', path].filter(Boolean);

  return parts.join('/');
}

/**
 * A documentation URL to link to.
 *
 * Distinct from `docsPath`, which says where *this* build emits a page. In
 * `site` mode the reader is being sent to the other host, where the
 * documentation sits at the root — so the link must not carry the `/docs`
 * segment this build still uses for its own redirect stubs.
 */
export function docsUrl(path: string, versionPrefix = ''): string {
  if (DOCS_MODE === 'site') {
    const remote = [versionPrefix, path].filter(Boolean).join('/');

    return `${DOCS_ORIGIN}/${remote}${remote ? '/' : ''}`;
  }

  return `/${docsPath(path, versionPrefix)}/`.replace(/\/+/g, '/');
}

/** A landing-page URL, absolute when this build does not contain it. */
export function siteUrl(path = ''): string {
  const absolute = `/${path}`.replace(/\/+/g, '/');

  return DOCS_AT_ROOT ? `${SITE_ORIGIN}${absolute}` : absolute;
}
