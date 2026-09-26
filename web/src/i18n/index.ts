import { en } from './en';
import { es } from './es';
import { ko } from './ko';
import { zh_Hans } from './zh_Hans';
import { DEFAULT_LOCALE as REGISTRY_DEFAULT_LOCALE, LOCALE_REGISTRY } from './locale-registry.ts';

const DICTIONARY_MODULES = { en, es, ko, 'zh-Hans': zh_Hans };

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
 * `en.ts` is the canonical `satisfies Dictionary` object. Every translated
 * dictionary (`es.ts`, `zh_Hans.ts`) is typed `Dictionary` directly (not
 * `satisfies`), so a key missing from a translation is a compile error rather
 * than a silently widened type.
 */
export interface Dictionary {
  nav: {
    drivers: string;
    keyboard: string;
    governance: string;
    compare: string;
    docs: string;
    about: string;
    source: string;
    download: string;
    menu: string;
    language: string;
    theme: string;
    theme_system: string;
    theme_light: string;
    theme_dark: string;
  };
  footer: {
    product: string;
    drivers: string;
    releases: string;
    changelog: string;
    docs: string;
    usage: string;
    connecting: string;
    mcp: string;
    driver_authoring: string;
    project: string;
    about: string;
    contributing: string;
    compare: string;
    source: string;
    trademark: string;
    privacy: string;
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
    previous: string;
    next: string;
  };
  docs_index: {
    title: string;
    intro: string;
    unfiled_title: string;
    unfiled_body: string;
  };
  landing: {
    eyebrow: string;
    title_1: string;
    title_2: string;
    title_3: string;
    lede: string;
    lede_short: string;
    read_docs: string;
    console: {
      search: string;
      dialect: string;
      run: string;
      executing: string;
      result_grid: string;
      pick_hint: string;
      connected: string;
      audit_log: string;
      tasks: string;
    };
    drivers_label: string;
    and_more: string;
    core: {
      eyebrow: string;
      title: string;
      body: string;
      rpc_more: string;
      note: string;
      view_chosen: string;
      view_short: string;
      views: {
        grid: string;
        tree: string;
        keys: string;
        events: string;
        range: string;
        objects: string;
      };
    };
    keyboard: {
      eyebrow: string;
      title: string;
      body: string;
      mac_note: string;
      link: string;
      placeholder: string;
      palette_label: string;
      count: string;
      empty: string;
      ran: string;
      hint: string;
    };
    commands: {
      palette: string;
      new_tab: string;
      run: string;
      run_new_tab: string;
      open_script: string;
      saved_queries: string;
      audit: string;
      sidebar: string;
      focus_sidebar: string;
      focus_editor: string;
      focus_results: string;
      close_tab: string;
      comment: string;
      save: string;
    };
    governance: {
      eyebrow: string;
      title: string;
      body: string;
      outcome: string;
      example: string;
      stage: {
        agent: string;
        classify: string;
        policy: string;
        approval: string;
        execute: string;
        audit: string;
      };
      note: {
        agent: string;
        classify: string;
        policy: string;
        approval: string;
        execute: string;
        audit: string;
      };
      hint: {
        schema: string;
        read: string;
        write: string;
        destructive: string;
      };
      verdict: {
        allowed: string;
        waits: string;
        denied: string;
      };
      detail: {
        metadata: string;
        read: string;
        write: string;
        destructive: string;
      };
    };
    audit: {
      eyebrow: string;
      title: string;
      body: string;
      fingerprint: string;
      redacted: string;
      local: string;
    };
    compare: {
      eyebrow: string;
      title: string;
      body: string;
      feature: string;
      why: string;
      leads: string;
      reviewed: string;
      full: string;
      legend: {
        included: string;
        limited: string;
        paid: string;
        none: string;
        unknown: string;
      };
      edition: {
        community: string;
        commercial: string;
        free_trial: string;
      };
      row: {
        open_source: string;
        limits: string;
        builder: string;
        charts: string;
        s3: string;
        mcp: string;
        builder_short: string;
        s3_short: string;
        table_editor: string;
        query_plan: string;
        import_connections: string;
      };
      row_note: {
        limits: string;
        dynamodb: string;
        builder: string;
        s3: string;
      };
      group: {
        licence: string;
        engines: string;
        query: string;
        cloud: string;
      };
      cell: {
        included: string;
        none: string;
        no: string;
        not_reviewed: string;
        paid_licence: string;
        lite_up: string;
        paid_editions: string;
        paid_editions_beta: string;
        beta: string;
        beta_macos: string;
        tabs_limit: string;
        with_roles: string;
        mcp_dbeaver: string;
        mcp_dbgate: string;
        not_yet: string;
        not_included: string;
      };
      gain: {
        dbeaver: string;
        datagrip: string;
        tableplus: string;
        beekeeper: string;
        dbgate: string;
      };
      keep: {
        dbeaver: string;
        datagrip: string;
        tableplus: string;
        beekeeper: string;
        dbgate: string;
      };
    };
    rules: {
      eyebrow: string;
      r1_title: string;
      r1_body: string;
      r2_title: string;
      r2_body: string;
      r3_title: string;
      r3_body: string;
      r4_title: string;
      r4_body: string;
    };
  };
  install: {
    eyebrow: string;
    title: string;
    body: string;
    all_downloads: string;
    copy: string;
    copied: string;
    copy_command: string;
    platforms_label: string;
    note: {
      linux: string;
      arch: string;
      nix: string;
      macos: string;
      windows: string;
    };
  };
  compare_page: {
    page_title: string;
    page_description: string;
    eyebrow: string;
    h1_1: string;
    h1_2: string;
    lede: string;
    meta: string;
    meta_body: string;
    legend: {
      included: string;
      limited: string;
      paid: string;
      none: string;
      unknown: string;
    };
    highlight_hint: string;
    edition_compared: string;
    note_dynamodb: string;
    note_datagrip: string;
    sources_title: string;
    per_client: string;
    read_comparison: string;
    feedback: string;
    feedback_link: string;
    vs_dbeaver: {
      page_title: string;
      page_description: string;
      crumb: string;
      h1: string;
      lede: string;
      download: string;
      bring: string;
      on_page: string;
      toc: {
        edition: string;
        gain: string;
        gaps: string;
        moving: string;
      };
      meta: string;
      edition_title: string;
      edition_body: string;
      col_feature: string;
      col_community: string;
      col_paid: string;
      sources: string;
      editions_page: string;
      dynamodb_note: string;
      gain_title: string;
      gain: {
        builder: { title: string; body: string; link: string };
        redis: { title: string; body: string; link: string };
        aws: { title: string; body: string; link: string };
      };
      mockup: {
        columns: string;
        joins: string;
        filters: string;
        run: string;
        keys: string;
        preview_first: string;
        unsaved: string;
      };
      gaps_title: string;
      gaps_body: string;
      not_yet: string;
      gap: {
        table_editor: { name: string; detail: string };
        formats: { name: string; detail: string };
        query_plan: { name: string; detail: string };
        users: { name: string; detail: string };
        backup: { name: string; detail: string };
        data_compare: { name: string; detail: string };
      };
      moving_title: string;
      step: {
        install: { title: string; body: string };
        import: { title: string; body: string };
        query: { title: string; body: string };
      };
      where_title: string;
      term: {
        sidebar: string;
        query_tab: string;
        diagram: string;
        transfer: string;
        schema_diff: string;
      };
    };
  };
  about: {
    page_title: string;
    page_description: string;
    eyebrow: string;
    h1: string;
    lede: string;
    p1: string;
    p2: string;
    card_body: string;
    options_eyebrow: string;
    option: {
      one: { label: string; title: string; body: string };
      two: { label: string; title: string; body: string };
      three: { label: string; title: string; body: string };
    };
    principles_eyebrow: string;
    principle: {
      p01: { title: string; body: string };
      p02: { title: string; body: string };
      p03: { title: string; body: string };
      p04: { title: string; body: string };
    };
    arch_eyebrow: string;
    arch_title: string;
    arch_body: string;
    arch_link: string;
    authoring_link: string;
    layer: {
      ui: { name: string; detail: string };
      app: { name: string; detail: string };
      core: { name: string; detail: string };
      drivers: { name: string; detail: string };
    };
    maintainer_title: string;
    maintainer_body: string;
    maintainer_link: string;
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
 * `t()` only resolves string leaves, so a caller that needs a whole group of
 * strings (the landing page's demo data, for one) reads it directly off this
 * object instead.
 */
export function dictionary(locale: Locale): Dictionary {
  return DICTIONARIES[locale];
}

/**
 * Derive the active locale from a request path.
 *
 * Routing is manual (`i18n.routing: 'manual'`), so the locale is never in
 * `Astro.currentLocale` — it is read back from the leading locale segment (e.g.
 * `/es/`, `/zh-Hans/`) the same way `[...path].astro` will compute it when
 * emitting that segment.
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
