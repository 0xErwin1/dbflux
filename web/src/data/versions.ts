// Vite inlines this at build time. Reading it with `fs` instead breaks once the
// module is bundled, because the relative path no longer points at the file.
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
}

/**
 * Which documentation the site publishes.
 *
 * Granularity is the minor series, not the patch. A `release/vX.Y` branch takes
 * cherry-picked fixes only, so a documentation change between two patches is a
 * correction — someone on X.Y.7 should read it, not be pinned to the wrong text.
 *
 * The first entry is the current release. It is served unprefixed at `/docs/`,
 * so a link to `/docs/usage/` always means "whatever is current". Every other
 * entry is served under its id and keeps that URL for good.
 *
 * Note what is *not* here: the product version. It is read from each ref's
 * Cargo.toml at build time, because a number typed here is a number that goes
 * quietly wrong at the next release.
 */
export const VERSIONS: readonly DocsVersion[] = registry;

export const CURRENT = VERSIONS[0];

/** The product version a documentation set describes, from its own Cargo.toml. */
export function productVersion(id: string): string {
  const entry = manifest.find((candidate) => candidate.id === id);

  if (!entry) throw new Error(`No materialised documentation for version "${id}"`);

  return entry.version;
}

/** What the version selector and page titles show. */
export function versionLabel(version: DocsVersion): string {
  if (version.id === 'nightly') return 'nightly';

  return version.id.replace(/^v/, '');
}

export const versionById = (id: string): DocsVersion | undefined =>
  VERSIONS.find((version) => version.id === id);

/** Documentation URL for an entry id of the form `<version>/<path>`. */
export function docsHref(entryId: string): string {
  const separator = entryId.indexOf('/');
  const versionId = entryId.slice(0, separator);
  const path = entryId.slice(separator + 1);

  return versionId === CURRENT.id ? `/docs/${path}/` : `/${versionId}/docs/${path}/`;
}

/** Root of a version's documentation. */
export function versionHome(version: DocsVersion): string {
  return version.id === CURRENT.id ? '/docs/' : `/${version.id}/docs/`;
}
