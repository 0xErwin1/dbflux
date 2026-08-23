import type { CollectionEntry } from 'astro:content';
import type { Locale } from '../i18n';
import { DOCS_SECTIONS, docTitle } from './nav';

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

export { docTitle };
