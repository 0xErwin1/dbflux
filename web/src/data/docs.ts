import type { CollectionEntry } from 'astro:content';
import type { Locale } from '../i18n';
import { DOCS_SECTIONS, docTitle } from './nav';

/**
 * The nav/rail/breadcrumb label for a page in `locale`.
 *
 * Spanish uses the translated doc's own H1 when one exists (`esTitles`);
 * otherwise — untranslated pages, or the English locale — it falls back to
 * the fixed `DOC_TITLES` rail label so navigation never renders a blank spot
 * for a page that has not been translated yet.
 */
export function localizedDocTitle(
  id: string,
  locale: Locale,
  esTitles: Readonly<Record<string, string>>,
): string {
  return (locale === 'es' ? esTitles[id] : undefined) ?? docTitle(id);
}

/**
 * Split a collection id of the form `<version>/<path>`, or its Spanish
 * counterpart `<version>/es/<path>` (see `content.config.ts`). The locale
 * segment is reported separately so callers never have to special-case it out
 * of `path` themselves.
 */
export function splitId(id: string): { version: string; locale: Locale; path: string } {
  const separator = id.indexOf('/');
  const version = id.slice(0, separator);
  const rest = id.slice(separator + 1);

  if (rest.startsWith('es/')) return { version, locale: 'es', path: rest.slice(3) };

  return { version, locale: 'en', path: rest };
}

/**
 * The pages a version actually ships, as version-less paths.
 *
 * Restricted to the English entries: this drives the sidebar tree and the
 * version-switcher's "does this page exist there" check, both of which stay
 * on the canonical English page set until W2b translates navigation itself.
 */
export function pathsForVersion(
  entries: readonly CollectionEntry<'docs'>[],
  versionId: string,
): string[] {
  return entries
    .filter((entry) => {
      const parsed = splitId(entry.id);
      return parsed.version === versionId && parsed.locale === 'en';
    })
    .map((entry) => splitId(entry.id).path);
}

export interface DocsSectionView {
  readonly id: string;
  readonly title: string;
  readonly entries: readonly string[];
}

/**
 * The reading order, restricted to what this version has.
 *
 * The order is declared once, for the current release. An older version that is
 * missing a page simply drops it, and anything it ships that the order does not
 * mention is surfaced separately rather than hidden.
 */
export function sectionsFor(available: readonly string[]): {
  sections: DocsSectionView[];
  unfiled: string[];
} {
  const known = new Set(available);

  const sections = DOCS_SECTIONS.map((section) => ({
    id: section.id,
    title: section.title,
    entries: section.entries.filter((path) => known.has(path)),
  })).filter((section) => section.entries.length > 0);

  const listed = new Set(sections.flatMap((section) => section.entries));

  return { sections, unfiled: available.filter((path) => !listed.has(path)).sort() };
}

/**
 * The first-level heading of a doc's raw markdown body, if it has one.
 *
 * Every page in this collection is authored with its own H1 as the first
 * line, so this is a plain first-match rather than a full markdown parse.
 */
export function firstHeading(body: string | undefined): string | undefined {
  const match = body?.match(/^#\s+(.+)$/m);

  return match?.[1].trim();
}

/**
 * Spanish page titles, sourced from each translated doc's own H1.
 *
 * The English `DOC_TITLES` dictionary in `nav.ts` is a fixed rail label, not
 * the page's own heading, so it never doubles as the Spanish title — pages
 * without an `es` sibling are simply absent from the returned map and callers
 * fall back to `docTitle(path)`.
 */
export function esTitlesFor(
  entries: readonly CollectionEntry<'docs'>[],
  versionId: string,
): Record<string, string> {
  const titles: Record<string, string> = {};

  for (const entry of entries) {
    const parsed = splitId(entry.id);
    if (parsed.version !== versionId || parsed.locale !== 'es') continue;

    const heading = firstHeading(entry.body);
    if (heading) titles[parsed.path] = heading;
  }

  return titles;
}

/**
 * The repository path a materialised doc file was copied from, for the
 * "Edit this page" link.
 *
 * `entry.filePath` (from the `glob` loader) is relative to the site root,
 * e.g. `.versions/v0.7/docs/es/SETTINGS.md` or
 * `.versions/v0.7/crates/dbflux_driver_postgres/README.md` — stripping the
 * `.versions/<versionId>/` mirror prefix recovers the real path in the
 * repository (`docs/es/SETTINGS.md`, `crates/dbflux_driver_postgres/README.md`).
 */
export function repoPathFor(filePath: string, versionId: string): string {
  const prefix = `.versions/${versionId}/`;

  return filePath.startsWith(prefix) ? filePath.slice(prefix.length) : filePath;
}

export { docTitle };
