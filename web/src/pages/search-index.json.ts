import type { APIRoute } from 'astro';
import { asJson, buildSearchIndex } from '../lib/search-index';
import { CURRENT } from '../data/versions';
import { DEFAULT_LOCALE } from '../i18n';

export const GET: APIRoute = async () => asJson(await buildSearchIndex(CURRENT.id, DEFAULT_LOCALE));
