// Vite inlines this at build time. Reading it with `fs` instead breaks once the
// module is bundled, because the relative path no longer points at the file.
import { docsPath, docsUrl } from './site';
import { DEFAULT_LOCALE } from '../i18n';
import type { Locale } from '../i18n';
import registry from '../../versions.json';
import manifest from '../../.versions/manifest.json';

export interface DocsVersion {
  /** Directory name under `.versions/`, and the URL prefix for non-current versions. */
  readonly id: string;
  /**
   * Git ref the documentation is read from.
   *
   * While a minor is supported this is its release branch. Once the branch is
   * discarded at EOL, repoint it at that minor's last tag: a tag is permanent,
   * a deleted branch breaks the next build.
   */
  readonly ref: string;
  /** Excluded from search engines. Nightly documents behaviour nobody is running yet. */
  readonly noindex?: boolean;
  /**
   * The release served unprefixed at `/docs/`.
   *
   * Separate from list order on purpose: the list is what the selector shows,
   * newest first, and nightly leads it. Making position decide which release is
   * current would have put unreleased documentation at `/docs/`.
   */
  readonly current?: boolean;
}

/**
 * Which documentation the site publishes.
 *
 * Granularity is the minor series, not the patch. A `release/vX.Y` branch takes
 * cherry-picked fixes only, so a documentation change between two patches is a
 * correction — someone on X.Y.7 should read it, not be pinned to the wrong text.
 *
 * Order is what the version selector shows, newest first. Which entry is the
 * current release is the `current` flag, not the position: that release is
 * served unprefixed at `/docs/`, so a link to `/docs/usage/` always means
 * "whatever is current". Every other entry is served under its id and keeps
 * that URL for good.
 *
 * Note what is *not* here: the product version. It is read from each ref's
 * Cargo.toml at build time, because a number typed here is a number that goes
 * quietly wrong at the next release.
 */
export const VERSIONS: readonly DocsVersion[] = registry;

export const CURRENT = VERSIONS.find((version) => version.current) ?? VERSIONS[0];

/** One record written by `scripts/fetch-docs.mjs` while it pulls that ref. */
interface ManifestEntry {
  id: string;
  ref: string;
  version: string;
  commit: string;
  date: string;
}

function entryFor(id: string): ManifestEntry {
  const entry = (manifest as ManifestEntry[]).find((candidate) => candidate.id === id);

  if (!entry) throw new Error(`No materialised documentation for version "${id}"`);

  return entry;
}

/** The product version a documentation set describes, from its own Cargo.toml. */
export function productVersion(id: string): string {
  return entryFor(id).version;
}

/**
 * What exactly this documentation was built from.
 *
 * A version number is not enough to identify a moving target: nightly reports
 * `0.8.0-dev.0` for months, and a release branch keeps its number across
 * cherry-picked fixes. The commit says which one you are reading.
 */
export function buildInfo(id: string): { version: string; commit: string; date: string } {
  const entry = entryFor(id);

  return { version: entry.version, commit: entry.commit, date: entry.date };
}

/** What the version selector and page titles show. */
export function versionLabel(version: DocsVersion): string {
  if (version.id === 'nightly') return 'nightly';

  return version.id.replace(/^v/, '');
}

export const versionById = (id: string): DocsVersion | undefined =>
  VERSIONS.find((version) => version.id === id);

/** Prefix a version occupies in a URL — empty for the current release. */
function prefixFor(versionId: string): string {
  return versionId === CURRENT.id ? '' : versionId;
}

/**
 * Documentation URL for an entry id of the form `<version>/<path>`.
 *
 * `entryId` is always the canonical English id, never the `<version>/es/<path>`
 * shape a Spanish collection entry carries — `locale` is what selects the
 * `/es/` URL prefix, independent of which entry's content is being linked to.
 */
export function docsHref(entryId: string, locale: Locale = DEFAULT_LOCALE): string {
  const separator = entryId.indexOf('/');

  return docsUrl(entryId.slice(separator + 1), prefixFor(entryId.slice(0, separator)), locale);
}

/** Root of a version's documentation. */
export function versionHome(version: DocsVersion, locale: Locale = DEFAULT_LOCALE): string {
  return docsUrl('', prefixFor(version.id), locale);
}

/**
 * The path a documentation page is generated at.
 *
 * Distinct from `docsHref`, which is for linking and may point at another host.
 */
export function docsRoute(
  versionId: string,
  path: string,
  locale: Locale = DEFAULT_LOCALE,
): string {
  return docsPath(path, prefixFor(versionId), locale);
}
