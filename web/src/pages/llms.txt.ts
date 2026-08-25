import type { APIRoute } from 'astro';
import { DOCS_MODE, docsUrl, siteUrl } from '../data/site';
import { ACQUISITION } from '../data/acquisition';

export const GET: APIRoute = () => {
  const links = [docsUrl(''), docsUrl('install'), docsUrl('usage')];
  if (DOCS_MODE === 'site') links.push(...ACQUISITION.map(({ route }) => siteUrl(route.slice(1))));
  return new Response(
    `# DBFlux\n\nCurrent documentation:\n${links.map((link) => `- ${link}`).join('\n')}\n`,
    {
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    },
  );
};
