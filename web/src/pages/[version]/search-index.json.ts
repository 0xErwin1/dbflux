import type { APIRoute } from 'astro';
import { asJson, buildSearchIndex } from '../../lib/search-index';
import { CURRENT, VERSIONS } from '../../data/versions';

export function getStaticPaths() {
  return VERSIONS.filter((version) => version.id !== CURRENT.id).map((version) => ({
    params: { version: version.id },
  }));
}

export const GET: APIRoute = async ({ params }) => asJson(await buildSearchIndex(params.version!));
