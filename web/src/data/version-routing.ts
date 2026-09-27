/**
 * Version routing rules with no build-time imports, so Node's test runner can
 * load them without Vite. `versions.ts` and `nav.ts` apply them to the real
 * registry; `astro.config.ts` feeds `renderedLinkInputs` to the markdown
 * pipeline.
 */

export interface VersionRoute {
  readonly id: string;
  readonly current?: boolean;
}

/** The release served unprefixed: the flagged entry, else the first one listed. */
export function currentVersionId(versions: readonly VersionRoute[]): string {
  const current = versions.find((version) => version.current === true) ?? versions[0];

  if (current === undefined) throw new Error('The documentation version registry is empty');

  return current.id;
}

/** URL prefix a version occupies: empty for the current release, its id otherwise. */
export function versionPrefix(versionId: string, currentId: string): string {
  return versionId === currentId ? '' : versionId;
}

export interface RenderedLinkInputs {
  readonly currentVersion: string;
  readonly versions: readonly string[];
  readonly docsMode: string;
  readonly docsOrigin: string;
}

/**
 * Everything a rendered documentation link depends on besides the markdown
 * itself.
 *
 * Astro caches each entry's rendered HTML in its content store and reuses it
 * while the entry's source is unchanged. It only discards that cache when the
 * serialized Astro config changes, and the functions in `rehypePlugins` do not
 * serialize. So these inputs are handed to the link plugin as its options,
 * which do serialize: moving `current` to another release, or building for a
 * different `DOCS_MODE`, then re-renders every entry instead of serving links
 * computed for the previous layout.
 */
export function renderedLinkInputs(
  versions: readonly VersionRoute[],
  docsMode: string,
  docsOrigin: string,
): RenderedLinkInputs {
  return {
    currentVersion: currentVersionId(versions),
    versions: versions.map((version) => version.id),
    docsMode,
    docsOrigin,
  };
}
