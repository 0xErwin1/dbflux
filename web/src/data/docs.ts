import type { CollectionEntry } from 'astro:content';
import { DOCS_SECTIONS, docTitle } from './nav';

/** Split a collection id of the form `<version>/<path>`. */
export function splitId(id: string): { version: string; path: string } {
  const separator = id.indexOf('/');

  return { version: id.slice(0, separator), path: id.slice(separator + 1) };
}

/** The pages a version actually ships, as version-less paths. */
export function pathsForVersion(
  entries: readonly CollectionEntry<'docs'>[],
  versionId: string,
): string[] {
  return entries
    .filter((entry) => splitId(entry.id).version === versionId)
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
