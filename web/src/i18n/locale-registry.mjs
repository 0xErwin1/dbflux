/**
 * Runtime locale metadata shared by Node scripts, Astro configuration, and the
 * TypeScript i18n facade. A docs directory is deliberately separate from the
 * canonical locale id so filesystem naming cannot accidentally change URLs.
 */
export const DEFAULT_LOCALE = 'en';

export const LOCALE_REGISTRY = validateLocaleRegistry(
  [
    { id: 'en', name: 'English', docsDirectory: null },
    { id: 'es', name: 'Español', docsDirectory: 'es' },
    { id: 'zh-Hans', name: '简体中文', docsDirectory: 'zh_Hans' },
  ],
  DEFAULT_LOCALE,
);

/**
 * @typedef {{ id: string, name: string, docsDirectory: string | null }} LocaleDefinition
 */

/**
 * Validate and freeze locale metadata at its shared runtime boundary.
 *
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {ReadonlyArray<Readonly<LocaleDefinition>>}
 */
export function validateLocaleRegistry(registry, defaultLocale) {
  const ids = new Set();
  const directories = new Set();

  for (const locale of registry) {
    if (ids.has(locale.id)) {
      throw new Error(`Invalid locale registry: duplicate locale id "${locale.id}"`);
    }
    ids.add(locale.id);

    if (locale.id !== defaultLocale && !locale.docsDirectory) {
      throw new Error(
        `Invalid locale registry: non-default locale "${locale.id}" must declare a docs directory`,
      );
    }

    if (locale.docsDirectory !== null) {
      if (!locale.docsDirectory || locale.docsDirectory.includes('/')) {
        throw new Error(
          `Invalid locale registry: docs directory for "${locale.id}" must be one path segment`,
        );
      }
      if (directories.has(locale.docsDirectory)) {
        throw new Error(
          `Invalid locale registry: duplicate docs directory "${locale.docsDirectory}"`,
        );
      }
      directories.add(locale.docsDirectory);
    }
  }

  if (!ids.has(defaultLocale)) {
    throw new Error(`Invalid locale registry: default locale "${defaultLocale}" is not registered`);
  }

  return Object.freeze(registry.map((locale) => Object.freeze({ ...locale })));
}

/**
 * Repository-relative glob patterns for documentation tracked by each locale.
 * The default locale remains directly under docs/; localized files must exist
 * in their explicitly registered directories.
 *
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @returns {string[]}
 */
export function docsRepoPatterns(registry = LOCALE_REGISTRY) {
  return [
    'docs/*.md',
    ...registry.flatMap(({ id, docsDirectory }) =>
      id === DEFAULT_LOCALE || docsDirectory === null
        ? []
        : [`docs/${docsDirectory}/*.md`, `docs/${docsDirectory}/drivers/*.md`],
    ),
  ];
}

/**
 * Match the concrete path shapes represented by docsRepoPatterns().
 *
 * @param {string} path
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @returns {boolean}
 */
export function isDocsRepoPath(path, registry = LOCALE_REGISTRY) {
  if (/^docs\/[^/]+\.md$/.test(path)) return true;

  return registry.some(({ id, docsDirectory }) => {
    if (id === DEFAULT_LOCALE || docsDirectory === null) return false;

    const prefix = `docs/${docsDirectory}/`;
    if (!path.startsWith(prefix)) return false;

    const document = path.slice(prefix.length);
    return /^[^/]+\.md$/.test(document) || /^drivers\/[^/]+\.md$/.test(document);
  });
}

/**
 * @param {string} repoPath
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {string}
 */
export function localeForRepoPath(
  repoPath,
  registry = LOCALE_REGISTRY,
  defaultLocale = DEFAULT_LOCALE,
) {
  const localized = registry.find(
    ({ id, docsDirectory }) =>
      id !== defaultLocale &&
      docsDirectory !== null &&
      repoPath.startsWith(`docs/${docsDirectory}/`),
  );

  return localized?.id ?? defaultLocale;
}

/**
 * Resolve a repository documentation path to its canonical locale and route path.
 * Unregistered nested docs directories are deliberately not treated as English.
 *
 * @param {string} repoPath
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {{ locale: string, path: string } | undefined}
 */
export function localizedDocPath(
  repoPath,
  registry = LOCALE_REGISTRY,
  defaultLocale = DEFAULT_LOCALE,
) {
  const localized = registry.find(
    ({ id, docsDirectory }) =>
      id !== defaultLocale &&
      docsDirectory !== null &&
      repoPath.startsWith(`docs/${docsDirectory}/`),
  );

  if (localized?.docsDirectory) {
    const document = repoPath.slice(`docs/${localized.docsDirectory}/`.length);
    if (!/^(?:drivers\/)?[^/]+\.md$/.test(document)) return undefined;

    return { locale: localized.id, path: document.replace(/\.md$/, '').toLowerCase() };
  }

  const document = repoPath.match(/^docs\/([^/]+)\.md$/);
  return document ? { locale: defaultLocale, path: document[1].toLowerCase() } : undefined;
}

/**
 * Split a generated collection id while preserving the registered locale id.
 *
 * @param {string} id
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {{ version: string, locale: string, path: string }}
 */
export function splitContentEntryId(
  id,
  registry = LOCALE_REGISTRY,
  defaultLocale = DEFAULT_LOCALE,
) {
  const [version, ...segments] = id.split('/');
  const localized = registry.find(
    ({ id: localeId }) => localeId !== defaultLocale && localeId === segments[0],
  );

  return localized
    ? { version, locale: localized.id, path: segments.slice(1).join('/') }
    : { version, locale: defaultLocale, path: segments.join('/') };
}

/**
 * Materialize one route descriptor per canonical document and registered locale.
 * A missing localized entry reuses the English entry but remains explicitly
 * untranslated. alternateLocales contains only real content siblings.
 *
 * @template {{ id: string }} T
 * @param {ReadonlyArray<T>} entries
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {Array<{
 *   version: string,
 *   path: string,
 *   locale: string,
 *   entry: T,
 *   translated: boolean,
 *   alternateLocales: string[],
 * }>}
 */
export function buildLocalizedDocumentMatrix(
  entries,
  registry = LOCALE_REGISTRY,
  defaultLocale = DEFAULT_LOCALE,
) {
  const entriesByKey = new Map(
    entries.map((entry) => {
      const { version, locale, path } = splitContentEntryId(entry.id, registry, defaultLocale);
      return [`${version}\0${locale}\0${path}`, entry];
    }),
  );
  const canonicalEntries = entries.filter(
    (entry) => splitContentEntryId(entry.id, registry, defaultLocale).locale === defaultLocale,
  );

  return canonicalEntries.flatMap((canonicalEntry) => {
    const { version, path } = splitContentEntryId(canonicalEntry.id, registry, defaultLocale);
    const alternateLocales = registry
      .filter(({ id: locale }) => entriesByKey.has(`${version}\0${locale}\0${path}`))
      .map(({ id }) => id);

    return registry.map(({ id: locale }) => {
      const localizedEntry = entriesByKey.get(`${version}\0${locale}\0${path}`);

      return {
        version,
        path,
        locale,
        entry: localizedEntry ?? canonicalEntry,
        translated: locale === defaultLocale || localizedEntry !== undefined,
        alternateLocales,
      };
    });
  });
}

/**
 * Select one search source per canonical document for a version and locale.
 * Missing translations deliberately retain the English source while the locale
 * remains on the descriptor so callers can link to the localized fallback route.
 *
 * @template {{ id: string }} T
 * @param {ReadonlyArray<T>} entries
 * @param {string} versionId
 * @param {string} locale
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 */
export function selectLocaleSearchEntries(
  entries,
  versionId,
  locale,
  registry = LOCALE_REGISTRY,
  defaultLocale = DEFAULT_LOCALE,
) {
  if (!registry.some(({ id }) => id === locale)) {
    throw new Error(`Cannot build search index for unregistered locale "${locale}"`);
  }

  return buildLocalizedDocumentMatrix(entries, registry, defaultLocale).filter(
    (document) => document.version === versionId && document.locale === locale,
  );
}

/** Preserve the existing English index paths while adding canonical locale segments. */
export function searchIndexPath(
  versionId,
  currentVersionId,
  locale,
  defaultLocale = DEFAULT_LOCALE,
) {
  const versionPrefix = versionId === currentVersionId ? '' : `/${versionId}`;
  const localeSuffix = locale === defaultLocale ? '' : `/${locale}`;

  return `${versionPrefix}${localeSuffix}/search-index.json`;
}

/**
 * Generate a content collection id without normalizing the canonical locale id.
 * Only the document portion is lowercased to preserve the site's existing URLs.
 *
 * @param {string} entry
 * @param {ReadonlyArray<LocaleDefinition>} registry
 * @param {string} defaultLocale
 * @returns {string}
 */
export function contentEntryId(entry, registry = LOCALE_REGISTRY, defaultLocale = DEFAULT_LOCALE) {
  const [version, ...rest] = entry.split('/');
  const repoPath = rest.join('/');

  const driver = repoPath.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
  if (driver) return `${version}/drivers/${driver[1]}`;

  const locale = localeForRepoPath(repoPath, registry, defaultLocale);
  const localeDefinition = registry.find(({ id }) => id === locale);
  let documentPath = repoPath.replace(/^docs\//, '');

  if (locale !== defaultLocale && localeDefinition?.docsDirectory) {
    documentPath = documentPath.slice(localeDefinition.docsDirectory.length + 1);
  }

  const normalizedDocument = documentPath.replace(/\.md$/, '').toLowerCase();
  const localePrefix = locale === defaultLocale ? '' : `${locale}/`;

  return `${version}/${localePrefix}${normalizedDocument}`;
}
