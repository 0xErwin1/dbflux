// Registers DBFlux docs tools with any browser-side WebMCP host (navigator.modelContext).
// No-op when the API is absent, so this file is safe to load unconditionally.
(function () {
  const modelContext = navigator.modelContext;
  if (!modelContext) return;

  const textResult = (text) => ({ content: [{ type: 'text', text }] });

  const searchDocs = {
    name: 'search_docs',
    description: 'Search the DBFlux documentation and return the top matching sections.',
    inputSchema: {
      type: 'object',
      properties: { query: { type: 'string' } },
      required: ['query'],
    },
    execute: async ({ query }) => {
      try {
        const response = await fetch('https://docs.dbflux.dev/search-index.json');
        if (!response.ok) return textResult(`Search index unavailable (${response.status}).`);
        const sections = await response.json();
        const tokens = query.toLowerCase().split(/\W+/).filter(Boolean);

        const scored = sections
          .map((section) => {
            const title = (section.t ?? '').toLowerCase();
            const heading = (section.h ?? '').toLowerCase();
            const snippet = (section.s ?? '').toLowerCase();
            let score = 0;
            for (const token of tokens) {
              score += count(title, token) * 5;
              score += count(heading, token) * 3;
              score += count(snippet, token);
            }
            return { section, score };
          })
          .filter(({ score }) => score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 5)
          .map(({ section }) => ({
            path: section.p,
            title: section.t,
            heading: section.h,
            url: `https://docs.dbflux.dev${section.p}${section.a ? `#${section.a}` : ''}`,
            snippet: section.s,
          }));

        return textResult(JSON.stringify(scored));
      } catch (error) {
        return textResult(`Search failed: ${error}`);
      }
    },
  };

  const readDocsPage = {
    name: 'read_docs_page',
    description: 'Fetch a DBFlux documentation page as markdown.',
    inputSchema: {
      type: 'object',
      properties: { path: { type: 'string', description: 'Docs page path such as /usage/' } },
      required: ['path'],
    },
    execute: async ({ path }) => {
      try {
        const normalized = normalizePath(path);
        const response = await fetch(`https://docs.dbflux.dev${normalized}index.md`);
        if (!response.ok) return textResult(`Docs page unavailable (${response.status}).`);
        return textResult(await response.text());
      } catch (error) {
        return textResult(`Fetch failed: ${error}`);
      }
    },
  };

  const openDocsPage = {
    name: 'open_docs_page',
    description: 'Navigate the browser to a DBFlux documentation page.',
    inputSchema: {
      type: 'object',
      properties: { path: { type: 'string', description: 'Docs page path such as /usage/' } },
      required: ['path'],
    },
    execute: async ({ path }) => {
      try {
        const normalized = normalizePath(path);
        window.location.assign(`https://docs.dbflux.dev${normalized}`);
        return textResult(`Navigating to https://docs.dbflux.dev${normalized}`);
      } catch (error) {
        return textResult(`Navigation failed: ${error}`);
      }
    },
  };

  function normalizePath(path) {
    const withLeading = path.startsWith('/') ? path : `/${path}`;
    return withLeading.endsWith('/') ? withLeading : `${withLeading}/`;
  }

  function count(haystack, token) {
    if (!token) return 0;
    return haystack.split(token).length - 1;
  }

  const tools = [searchDocs, readDocsPage, openDocsPage];

  if (typeof modelContext.registerTool === 'function') {
    for (const tool of tools) modelContext.registerTool(tool);
  } else if (typeof modelContext.provideContext === 'function') {
    modelContext.provideContext({ tools });
  }
})();
