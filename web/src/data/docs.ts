import type { CollectionEntry } from 'astro:content';
import { DEFAULT_LOCALE, LOCALES } from '../i18n';
import type { Locale } from '../i18n';
import { splitContentEntryId } from '../i18n/locale-registry.mjs';
import { CURRENT, docsRoute } from './versions';
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

export interface DocRoutePolicy {
  readonly canonicalPath?: string;
  readonly indexable: boolean;
  readonly alternateLocales: readonly Locale[];
}

/** Derived from the PR 1 policy: only current, real English documents get Markdown. */
export const markdownPathFor = (
  versionId: string,
  path: string,
  locale: Locale,
  translated: boolean,
  policy: DocRoutePolicy,
): string | undefined =>
  versionId === CURRENT.id && locale === DEFAULT_LOCALE && translated && policy.indexable
    ? `/${docsRoute(versionId, path, locale)}/index.md`.replace(/\/+/g, '/')
    : undefined;

const routePath = (versionId: string, path: string, locale: Locale): string =>
  `/${docsRoute(versionId, path, locale)}/`.replace(/\/+/g, '/');

export const sitemapPathsFor = (
  facts: readonly { readonly path: string; readonly locales: readonly string[] }[],
): Set<string> =>
  new Set([
    ...LOCALES.map((locale) => routePath(CURRENT.id, '', locale)),
    ...facts.flatMap(({ path, locales }) =>
      locales
        .filter((locale): locale is Locale => (LOCALES as readonly string[]).includes(locale))
        .map((locale) => routePath(CURRENT.id, path, locale)),
    ),
  ]);

export function docRoutePolicy(
  entries: readonly CollectionEntry<'docs'>[],
  versionId: string,
  path: string | undefined,
  locale: Locale,
  translated: boolean,
): DocRoutePolicy {
  const page = path ?? '';
  const currentLocales =
    path === undefined
      ? LOCALES
      : LOCALES.filter((target) =>
          entries.some((entry) => {
            const id = splitId(entry.id);
            return id.version === CURRENT.id && id.locale === target && id.path === page;
          }),
        );
  const indexable = versionId === CURRENT.id && translated;
  const canonicalLocale = currentLocales.includes(locale)
    ? locale
    : currentLocales.includes(DEFAULT_LOCALE)
      ? DEFAULT_LOCALE
      : undefined;
  const canonicalPath = indexable
    ? routePath(versionId, page, locale)
    : canonicalLocale
      ? routePath(CURRENT.id, page, canonicalLocale)
      : undefined;
  const alternateLocales = indexable ? currentLocales : [];
  return { canonicalPath, indexable, alternateLocales };
}

/** Extract only human prose from markdown; code, headings, commands, and navigation cannot become metadata. */
export function safeDescription(body: string | undefined): string | undefined {
  if (!body) return undefined;
  let fenced = false;
  for (const raw of body.split('\n')) {
    const line = raw.trim();
    if (/^(?:`{3,}|~{3,})/.test(line)) {
      fenced = !fenced;
      continue;
    }
    if (
      fenced ||
      !line ||
      /^(?:#{1,6}\s|[-*+]\s|\d+[.)]\s|>|---+$|<[^>]+>$)/.test(line) ||
      /^\$\s|^(?:curl|wget|sudo|npm|pnpm|cargo|git)\b/.test(line) ||
      line.includes('|')
    )
      continue;
    const prose = line
      .replace(/[`*_\[\]]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (prose.length >= 24) return prose.slice(0, 180).replace(/\s+\S*$/, '');
  }
  return undefined;
}

export { docTitle };
