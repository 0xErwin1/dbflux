export interface DocsVersion {
  /** Directory name under `.versions/`, and the URL prefix for non-current versions. */
  readonly id: string;
  /** What the selector shows. */
  readonly label: string;
  /**
   * Git ref the documentation is read from.
   *
   * While a minor is supported this is its release branch. Once the branch is
   * discarded at EOL, repoint it at that minor's last tag: a tag is permanent,
   * a deleted branch breaks the next build.
   */
  readonly ref: string;
  /** Product version this documents, shown on the page and in the hero. */
  readonly version: string;
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
 */
export const VERSIONS: readonly DocsVersion[] = [
  { id: 'v0.7', label: '0.7 — current', ref: 'release/v0.7', version: '0.7.7' },
  { id: 'nightly', label: 'nightly', ref: 'main', version: 'main', noindex: true },
  { id: 'v0.6', label: '0.6', ref: 'release/v0.6', version: '0.6.4' },
];

export const CURRENT = VERSIONS[0];

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
