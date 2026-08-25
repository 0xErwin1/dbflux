import { en } from './en';
import { es } from './es';
import { DEFAULT_LOCALE as REGISTRY_DEFAULT_LOCALE, LOCALE_REGISTRY } from './locale-registry.mjs';

const DICTIONARY_MODULES = { en, es };

export type Locale = keyof typeof DICTIONARY_MODULES;

const registeredLocaleIds = LOCALE_REGISTRY.map(({ id }) => id);
const dictionaryLocaleIds = Object.keys(DICTIONARY_MODULES);

if (
  registeredLocaleIds.length !== dictionaryLocaleIds.length ||
  registeredLocaleIds.some((id) => !dictionaryLocaleIds.includes(id))
) {
  throw new Error('Locale registry and translated dictionaries must declare the same locale ids');
}

export const LOCALES = registeredLocaleIds as readonly Locale[];

export const DEFAULT_LOCALE = REGISTRY_DEFAULT_LOCALE as Locale;

/** Native display name for each locale, used by the language picker. */
export const LOCALE_NAMES = Object.fromEntries(
  LOCALE_REGISTRY.map(({ id, name }) => [id, name]),
) as Record<Locale, string>;

/**
 * The complete shape of every chrome string the site renders.
 *
 * `en.ts` is the canonical `satisfies Dictionary` object. `es.ts` is typed
 * `Dictionary` directly (not `satisfies`), so a key missing from the Spanish
 * dictionary is a compile error rather than a silently widened type.
 */
export interface Dictionary {
  nav: {
    features: string;
    drivers: string;
    docs: string;
    about: string;
    github: string;
    download: string;
    menu: string;
    language: string;
  };
  footer: {
    product: string;
    features: string;
    drivers: string;
    releases: string;
    docs: string;
    usage: string;
    connecting: string;
    mcp: string;
    project: string;
    about: string;
    contributing: string;
    source: string;
    tagline: string;
    license: string;
  };
  search: {
    placeholder: string;
    move: string;
    open: string;
    close: string;
    no_results: string;
    unavailable: string;
    result_count_one: string;
    result_count_other: string;
  };
  versions: {
    label: string;
    index_tag_title: string;
    index_tag: string;
    default_tag: string;
  };
  docs_sections: {
    start: string;
    using: string;
    configure: string;
    integrate: string;
    reference: string;
    drivers: string;
    contribute: string;
  };
  docs_tree: {
    search_cta: string;
    rail_toggle: string;
    on_this_page: string;
    crumb_docs: string;
    crumb_overview: string;
    edit_page: string;
    report_issue: string;
    not_translated: string;
    view_in_english: string;
  };
  docs_index: {
    title: string;
    intro: string;
    unfiled_title: string;
    unfiled_body: string;
  };
  landing: {
    title: string;
    lede: string;
    download_linux: string;
    download_macos: string;
    download_windows: string;
    view_source: string;
    platforms_meta: string;
    hero_caption: string;
    hero_alt: string;
    drivers_eyebrow: string;
    drivers_link: string;
    drivers_note: string;
    features_eyebrow: string;
    feature: {
      editor: { title: string; body: string };
      grid: { title: string; body: string };
      charts: { title: string; body: string };
      hooks: { title: string; body: string };
      reach: { title: string; body: string };
      audit: { title: string; body: string };
    };
    keyboard_eyebrow: string;
    keyboard_title: string;
    keyboard_body: string;
    keyboard_link: string;
    shortcut: {
      new_query: string;
      command_palette: string;
      open_script: string;
      new_connection: string;
    };
    governance_eyebrow: string;
    governance_title: string;
    governance_body: string;
    audit_eyebrow: string;
    audit_title: string;
    audit_body: string;
    docs_eyebrow: string;
    docs_link: string;
    doc_card: {
      usage: { title: string; body: string };
      connecting: { title: string; body: string };
      mcp: { title: string; body: string };
    };
  };
  install: {
    all_downloads: string;
    copy: string;
    copied: string;
    copy_fallback: string;
    hint: {
      tarball: string;
      aur: string;
      deb: string;
      appimage: string;
      nix: string;
      dmg: string;
      installer: string;
      portable: string;
    };
    steps: {
      dmg: [string, string, string];
      installer: [string, string, string];
      portable: [string, string, string];
    };
  };
  about: {
    page_title: string;
    page_description: string;
    h1: string;
    intro_p1: string;
    intro_p2: string;
    intro_p3: string;
    principles_eyebrow: string;
    principle: {
      p01: { title: string; body: string };
      p02: { title: string; body: string };
      p03: { title: string; body: string };
      p04: { title: string; body: string };
    };
    layers_eyebrow: string;
    layer: {
      ui: { detail: string };
      app: { detail: string };
      core: { detail: string };
      drivers: { detail: string };
    };
    muted_links: {
      prefix: string;
      architecture: string;
      middle: string;
      driver_authoring: string;
      suffix: string;
    };
    maintainer_title: string;
    maintainer_body: string;
    contribute_title: string;
    contribute_body: string;
    contribute_link: string;
  };
  notfound: {
    title: string;
    lede: string;
    docs_button: string;
    home_button: string;
    versions_label: string;
  };
  banner: {
    skip_link: string;
  };
}

type DotPaths<T, Prefix extends string = ''> = {
  [K in keyof T & string]: T[K] extends string ? `${Prefix}${K}` : DotPaths<T[K], `${Prefix}${K}.`>;
}[keyof T & string];

/** Every dot-delimited key path a `Dictionary` exposes, e.g. `"nav.features"`. */
export type DictionaryKey = DotPaths<Dictionary>;

const DICTIONARIES: Record<Locale, Dictionary> = DICTIONARY_MODULES;

/** Resolve a dot-delimited key path against the dictionary for `locale`. */
export function t(locale: Locale, key: DictionaryKey): string {
  const segments = key.split('.');
  let value: unknown = DICTIONARIES[locale];

  for (const segment of segments) {
    if (typeof value !== 'object' || value === null || !(segment in value)) {
      throw new Error(`Missing i18n key "${key}" for locale "${locale}"`);
    }

    value = (value as Record<string, unknown>)[segment];
  }

  if (typeof value !== 'string') {
    throw new Error(`i18n key "${key}" does not resolve to a string for locale "${locale}"`);
  }

  return value;
}

/**
 * The translated title for a docs rail section, keyed by `DocsSection.id`.
 *
 * `DocsSection.id` is typed as a plain `string` in `data/nav.ts` (it doubles
 * as an anchor id and a `<details>` key), so this resolves it against
 * `docs_sections` with a runtime check rather than widening `t()`'s key type
 * for one caller.
 */
export function sectionTitle(locale: Locale, id: string): string {
  const key = id as keyof Dictionary['docs_sections'];
  const value = DICTIONARIES[locale].docs_sections[key];

  if (typeof value !== 'string') {
    throw new Error(`Missing docs section title for id "${id}"`);
  }

  return value;
}

/**
 * The full dictionary for a locale, typed as `Dictionary`.
 *
 * `t()` only resolves string leaves — `install.steps.*` are tuples, not
 * strings, so a caller that needs one of those arrays reads it directly off
 * this object instead.
 */
export function dictionary(locale: Locale): Dictionary {
  return DICTIONARIES[locale];
}

/**
 * Derive the active locale from a request path.
 *
 * Routing is manual (`i18n.routing: 'manual'`), so the locale is never in
 * `Astro.currentLocale` — it is read back from the leading `/es/` segment the
 * same way `[...path].astro` will compute it when emitting that segment.
 */
export function localeFromPathname(pathname: string): Locale {
  const [, first] = pathname.split('/');

  return (LOCALES as readonly string[]).includes(first) ? (first as Locale) : DEFAULT_LOCALE;
}

/**
 * The same pathname with its locale segment swapped, e.g. `/about/` becomes
 * `/es/about/` for `'es'`, and `/es/about/` becomes `/about/` for `'en'`.
 *
 * Used to compute hreflang alternates and the locale switcher target — both
 * need the equivalent path in another locale without knowing anything about
 * what kind of page it is.
 */
export function withLocale(pathname: string, locale: Locale): string {
  const current = localeFromPathname(pathname);
  const rest = current === DEFAULT_LOCALE ? pathname : pathname.slice(current.length + 1) || '/';

  if (locale === DEFAULT_LOCALE) return rest;

  return `/${locale}${rest}`.replace(/\/+/g, '/');
}
