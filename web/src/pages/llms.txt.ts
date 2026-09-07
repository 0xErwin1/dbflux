import type { APIRoute } from 'astro';
import { docsUrl } from '../data/site';

/**
 * llms.txt per https://llmstxt.org/: an H1, a one-line blockquote summary, and
 * sections whose entries are markdown links with a short description. Agents
 * and audits parse the link syntax, so bare URLs are not equivalent.
 */
export const GET: APIRoute = () =>
  new Response(
    [
      '# DBFlux',
      '',
      '> DBFlux is a keyboard-first, open-source database client for PostgreSQL, MySQL/MariaDB, SQL Server, SQLite, MongoDB, Redis, DynamoDB, CloudWatch, InfluxDB, Redshift, and S3, built in Rust on GPUI.',
      '',
      'Every documentation page also serves a Markdown version: request it with `Accept: text/markdown` or append `index.md` to the page URL.',
      '',
      '## Documentation',
      '',
      `- [Documentation home](${docsUrl('')}): entry point to the current documentation`,
      `- [Install](${docsUrl('install')}): platform installers and package managers`,
      `- [Usage guide](${docsUrl('usage')}): connecting, browsing schemas, running queries, and charting`,
      '',
      '## Agent resources',
      '',
      '- [MCP server card](https://dbflux.dev/.well-known/mcp/server-card.json): remote MCP endpoint exposing this documentation as searchable tools',
      '- [AI catalog](https://dbflux.dev/.well-known/ai-catalog.json): ARD capability manifest for this site',
      '',
    ].join('\n'),
    {
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    },
  );
