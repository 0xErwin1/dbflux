import type { APIRoute } from 'astro';
import { docsUrl } from '../data/site';

export const GET: APIRoute = () =>
  new Response(
    `# DBFlux\n\nCurrent documentation:\n${[docsUrl(''), docsUrl('install'), docsUrl('usage')].map((link) => `- ${link}`).join('\n')}\n`,
    {
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    },
  );
