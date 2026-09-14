import type { APIRoute } from 'astro';
import { ORIGIN } from '../data/site';

/**
 * Point crawlers at the sitemap this host publishes, and state what the content
 * may be used for.
 *
 * Two hosts serve this source and each has its own sitemap, so the file is
 * generated rather than dropped in `public/`.
 *
 * The Content-Signal line separates two preferences that are easy to conflate:
 * being indexed and being trained on. Blocking a crawler outright costs both,
 * and indexing is the one that brings readers here. This asks for the first and
 * declines the second, which is a request rather than an enforcement — a
 * crawler is free to ignore it.
 *
 * A robots.txt managed at the CDN takes precedence over this file, so check
 * what the live URL returns before concluding a rule is in effect.
 */
const BODY = [
  'User-agent: Google-Extended',
  'Content-Signal: search=yes,ai-train=no,use=reference',
  'Allow: /',
  '',
  'User-agent: *',
  'Content-Signal: search=yes,ai-train=no,use=reference',
  'Allow: /',
  '',
  `Sitemap: ${ORIGIN}/sitemap-index.xml`,
  '',
].join('\n');

export const GET: APIRoute = () =>
  new Response(BODY, { headers: { 'content-type': 'text/plain; charset=utf-8' } });
