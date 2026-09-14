import type { APIRoute } from 'astro';
import { asJson, buildSearchIndex } from '../../lib/search-index';
import { DOCS_MODE } from '../../data/site';
import { CURRENT, VERSIONS } from '../../data/versions';
import { DEFAULT_LOCALE, LOCALES } from '../../i18n';
import type { Locale } from '../../i18n';

export function getStaticPaths() {
  const versions = VERSIONS.filter((version) => version.id !== CURRENT.id).map((version) => ({
    params: { version: version.id },
    props: { versionId: version.id, locale: DEFAULT_LOCALE },
  }));
  const currentLocales =
    DOCS_MODE === 'site'
      ? []
      : LOCALES.filter((locale) => locale !== DEFAULT_LOCALE).map((locale) => ({
          params: { version: locale },
          props: { versionId: CURRENT.id, locale },
        }));

  return [...versions, ...currentLocales];
}

export const GET: APIRoute = async ({ props }) => {
  const { versionId, locale } = props as { versionId: string; locale: Locale };

  return asJson(await buildSearchIndex(versionId, locale));
};
