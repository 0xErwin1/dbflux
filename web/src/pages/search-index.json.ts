import type { APIRoute } from 'astro';
import { asJson, buildSearchIndex } from '../lib/search-index';
import { CURRENT } from '../data/versions';

export const GET: APIRoute = async () => asJson(await buildSearchIndex(CURRENT.id));
