import type { APIRoute } from 'astro';
import { ORIGIN } from '../data/site';

/**
 * Point crawlers at the sitemap this host actually publishes.
 *
 * Two hosts serve this source and each has its own sitemap, so the file is
 * generated rather than dropped in `public/`. Note that a robots.txt managed at
 * the CDN takes precedence over this one: check what the live URL returns
 * before concluding a rule is in effect.
 */
export const GET: APIRoute = () =>
  new Response(`User-agent: *\nAllow: /\n\nSitemap: ${ORIGIN}/sitemap-index.xml\n`, {
    headers: { 'content-type': 'text/plain; charset=utf-8' },
  });
