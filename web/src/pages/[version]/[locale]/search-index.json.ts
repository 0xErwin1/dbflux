import type { APIRoute } from 'astro';
import { DOCS_MODE } from '../../../data/site';
import { CURRENT, VERSIONS } from '../../../data/versions';
import { DEFAULT_LOCALE, LOCALES } from '../../../i18n';
import type { Locale } from '../../../i18n';
import { asJson, buildSearchIndex } from '../../../lib/search-index';

export function getStaticPaths() {
  if (DOCS_MODE === 'site') return [];

  return VERSIONS.filter((version) => version.id !== CURRENT.id).flatMap((version) =>
    LOCALES.filter((locale) => locale !== DEFAULT_LOCALE).map((locale) => ({
      params: { version: version.id, locale },
      props: { versionId: version.id, locale },
    })),
  );
}

export const GET: APIRoute = async ({ props }) => {
  const { versionId, locale } = props as { versionId: string; locale: Locale };

  return asJson(await buildSearchIndex(versionId, locale));
};
