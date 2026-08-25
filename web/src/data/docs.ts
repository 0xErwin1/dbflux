import type { CollectionEntry } from 'astro:content';
import { DEFAULT_LOCALE, LOCALES } from '../i18n';
import type { Locale } from '../i18n';
import { splitContentEntryId } from '../i18n/locale-registry.mjs';
import { DOCS_SECTIONS, docTitle } from './nav';

export type DocTitlesByLocale = Readonly<Partial<Record<Locale, Readonly<Record<string, string>>>>>;

/** The translated H1 when it exists, otherwise the stable English rail label. */
export function localizedDocTitle(
  id: string,
  locale: Locale,
  titlesByLocale: DocTitlesByLocale,
): string {
  return (locale === DEFAULT_LOCALE ? undefined : titlesByLocale[locale]?.[id]) ?? docTitle(id);
}

/** Split a collection id while preserving the registry's canonical locale id. */
export function splitId(id: string): { version: string; locale: Locale; path: string } {
  const parsed = splitContentEntryId(id);

  if (!(LOCALES as readonly string[]).includes(parsed.locale)) {
    throw new Error(`Content entry "${id}" uses unregistered locale "${parsed.locale}"`);
  }

  return { ...parsed, locale: parsed.locale as Locale };
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

/** Localized page titles, sourced only from translated documents that exist. */
export function localizedTitlesFor(
  entries: readonly CollectionEntry<'docs'>[],
  versionId: string,
): DocTitlesByLocale {
  const titles: Partial<Record<Locale, Record<string, string>>> = {};

  for (const entry of entries) {
    const parsed = splitId(entry.id);
    if (parsed.version !== versionId || parsed.locale === DEFAULT_LOCALE) continue;

    const heading = firstHeading(entry.body);
    if (!heading) continue;

    const localeTitles = titles[parsed.locale] ?? {};
    localeTitles[parsed.path] = heading;
    titles[parsed.locale] = localeTitles;
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
