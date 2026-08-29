import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';
import { z } from 'zod';

interface Env {
  DBFLUX_DOCS_MCP: DurableObjectNamespace<DbfluxDocsMcp>;
}

interface DocSection {
  p: string;
  t: string;
  h: string;
  a: string;
  s: string;
}

interface SearchResult {
  path: string;
  title: string;
  heading: string;
  url: string;
  snippet: string;
}

const SEARCH_INDEX_URL = 'https://docs.dbflux.dev/search-index.json';
const DOCS_ORIGIN = 'https://docs.dbflux.dev';

function sectionUrl(section: DocSection): string {
  return `${DOCS_ORIGIN}${section.p}${section.a ? `#${section.a}` : ''}`;
}

async function fetchSearchIndex(): Promise<DocSection[]> {
  const response = await fetch(SEARCH_INDEX_URL, {
    cf: { cacheTtl: 300, cacheEverything: true },
  });
  if (!response.ok)
    throw new Error(
      `Failed to fetch ${SEARCH_INDEX_URL}: ${response.status} ${response.statusText}`,
    );
  return (await response.json()) as DocSection[];
}

function tokenize(query: string): string[] {
  return query
    .toLowerCase()
    .split(/\W+/)
    .filter((token) => token.length > 0);
}

function scoreSection(section: DocSection, tokens: string[]): number {
  const title = section.t.toLowerCase();
  const heading = section.h.toLowerCase();
  const snippet = section.s.toLowerCase();
  let score = 0;
  for (const token of tokens) {
    score += countOccurrences(title, token) * 5;
    score += countOccurrences(heading, token) * 3;
    score += countOccurrences(snippet, token) * 1;
  }
  return score;
}

function countOccurrences(haystack: string, needle: string): number {
  if (needle.length === 0) return 0;
  let count = 0;
  let index = haystack.indexOf(needle);
  while (index !== -1) {
    count += 1;
    index = haystack.indexOf(needle, index + needle.length);
  }
  return count;
}

function errorContent(message: string) {
  return {
    isError: true as const,
    content: [{ type: 'text' as const, text: message }],
  };
}

function jsonContent(value: unknown) {
  return {
    content: [{ type: 'text' as const, text: JSON.stringify(value, null, 2) }],
  };
}

function normalizePagePath(path: string): string {
  const withLeadingSlash = path.startsWith('/') ? path : `/${path}`;
  return withLeadingSlash.endsWith('/') ? withLeadingSlash : `${withLeadingSlash}/`;
}

export class DbfluxDocsMcp extends McpAgent<Env> {
  server = new McpServer({ name: 'dbflux-docs', version: '0.1.0' });

  async init() {
    this.server.tool(
      'search_docs',
      'Full-text search over the DBFlux documentation. Returns ranked sections with a URL and a plain-text snippet.',
      {
        query: z
          .string()
          .describe('The search query, e.g. "MCP governance" or "connect to PostgreSQL".'),
        limit: z
          .number()
          .int()
          .min(1)
          .max(20)
          .default(5)
          .describe('Maximum number of results to return.'),
      },
      async ({ query, limit }) => {
        let sections: DocSection[];
        try {
          sections = await fetchSearchIndex();
        } catch (error) {
          return errorContent(
            `Could not fetch the DBFlux documentation search index: ${String(error)}`,
          );
        }

        const tokens = tokenize(query);
        const results: SearchResult[] = sections
          .map((section) => ({ section, score: scoreSection(section, tokens) }))
          .filter(({ score }) => score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, limit)
          .map(({ section }) => ({
            path: section.p,
            title: section.t,
            heading: section.h,
            url: sectionUrl(section),
            snippet: section.s,
          }));

        return jsonContent(results);
      },
    );

    this.server.tool(
      'read_page',
      'Fetch the full Markdown content of a DBFlux documentation page.',
      {
        path: z.string().describe('The page path, e.g. "/usage/" or "/install/postgresql/".'),
      },
      async ({ path }) => {
        let sections: DocSection[];
        try {
          sections = await fetchSearchIndex();
        } catch (error) {
          return errorContent(
            `Could not fetch the DBFlux documentation search index: ${String(error)}`,
          );
        }

        const normalizedPath = normalizePagePath(path);
        const knownPaths = [...new Set(sections.map((section) => section.p))];
        if (!knownPaths.includes(normalizedPath)) {
          const closeMatches = knownPaths
            .filter((known) => known.includes(normalizedPath) || normalizedPath.includes(known))
            .slice(0, 5);
          const suggestion =
            closeMatches.length > 0 ? ` Close matches: ${closeMatches.join(', ')}.` : '';
          return errorContent(
            `No DBFlux documentation page exists at "${normalizedPath}".${suggestion}`,
          );
        }

        const markdownUrl = `${DOCS_ORIGIN}${normalizedPath}index.md`;
        let response: Response;
        try {
          response = await fetch(markdownUrl, { cf: { cacheTtl: 300, cacheEverything: true } });
        } catch (error) {
          return errorContent(`Could not fetch ${markdownUrl}: ${String(error)}`);
        }
        if (!response.ok)
          return errorContent(
            `Could not fetch ${markdownUrl}: ${response.status} ${response.statusText}`,
          );

        return { content: [{ type: 'text' as const, text: await response.text() }] };
      },
    );

    this.server.tool(
      'list_pages',
      'List every page in the DBFlux documentation with its title.',
      {},
      async () => {
        let sections: DocSection[];
        try {
          sections = await fetchSearchIndex();
        } catch (error) {
          return errorContent(
            `Could not fetch the DBFlux documentation search index: ${String(error)}`,
          );
        }

        const pages = new Map<string, string>();
        for (const section of sections) if (!pages.has(section.p)) pages.set(section.p, section.t);
        const list = [...pages.entries()].map(([path, title]) => ({ path, title }));

        return jsonContent(list);
      },
    );
  }
}

const LANDING_TEXT =
  'dbflux-docs MCP server. Connect to https://mcp.dbflux.dev/mcp. Server card: https://dbflux.dev/.well-known/mcp/server-card.json\n';

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/mcp' || url.pathname.startsWith('/mcp/'))
      return DbfluxDocsMcp.serve('/mcp', { binding: 'DBFLUX_DOCS_MCP' }).fetch(request, env, ctx);

    if (url.pathname === '/')
      return new Response(LANDING_TEXT, {
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      });

    return new Response('Not found', { status: 404 });
  },
};
