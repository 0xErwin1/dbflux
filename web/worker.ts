interface Env {
  ASSETS: { fetch(input: Request | string): Promise<Response> };
}

const quality = (accept: string | null, type: string): number | undefined => {
  const values = (accept ?? '').split(',').flatMap((part) => {
    const [media, ...params] = part
      .trim()
      .split(';')
      .map((value) => value.trim());
    if (media !== type) return [];
    const raw = params.find((value) => value.startsWith('q='))?.slice(2);
    return raw === undefined ? [1] : /^(?:0(?:\.\d+)?|1(?:\.0+)?)$/.test(raw) ? [Number(raw)] : [];
  });
  return values.length ? Math.max(...values) : undefined;
};

const merge = (headers: Headers, name: string, value: string) =>
  headers.set(name, [headers.get(name), value].filter(Boolean).join(', '));

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!['GET', 'HEAD'].includes(request.method)) return env.ASSETS.fetch(request);

    const url = new URL(request.url);

    if (url.pathname === '/.well-known/api-catalog') {
      const apiCatalog = await env.ASSETS.fetch(request);
      if (!apiCatalog.ok) return apiCatalog;
      const headers = new Headers(apiCatalog.headers);
      headers.set('content-type', 'application/linkset+json');
      return new Response(request.method === 'HEAD' ? null : apiCatalog.body, {
        status: apiCatalog.status,
        statusText: apiCatalog.statusText,
        headers,
      });
    }

    const html = await env.ASSETS.fetch(request);
    // Negotiation keys on the pathname, not the raw URL: clients (and scanners)
    // append cache-busting query strings, and those must still negotiate.
    if (
      !html.ok ||
      !url.pathname.endsWith('/') ||
      !html.headers.get('content-type')?.includes('text/html')
    ) {
      if (url.pathname.endsWith('.md') || url.pathname === '/search-index.json') {
        const headers = new Headers(html.headers);
        headers.set('Access-Control-Allow-Origin', '*');
        return new Response(request.method === 'HEAD' ? null : html.body, {
          status: html.status,
          statusText: html.statusText,
          headers,
        });
      }
      return html;
    }
    const markdownUrl = new URL(`${url.pathname}index.md`, url.origin).href;
    const markdown = await env.ASSETS.fetch(new Request(markdownUrl));
    if (!markdown.ok) return html;
    const headers = new Headers(html.headers);
    merge(headers, 'Vary', 'Accept');
    merge(headers, 'Link', `<${markdownUrl}>; rel="alternate"; type="text/markdown"`);
    const markdownQuality = quality(request.headers.get('Accept'), 'text/markdown');
    const selected = markdownQuality !== undefined && markdownQuality > 0;
    if (selected) {
      headers.set('content-type', 'text/markdown; charset=utf-8');
      headers.delete('content-length');
      headers.set(
        'Link',
        `${html.headers.get('Link') ? `${html.headers.get('Link')}, ` : ''}<${url.origin}${url.pathname}>; rel="alternate"; type="text/html"`,
      );
      const markdownText = await markdown.text();
      headers.set('x-markdown-tokens', String(Math.ceil(markdownText.length / 4)));
      return new Response(request.method === 'HEAD' ? null : markdownText, {
        status: html.status,
        statusText: html.statusText,
        headers,
      });
    }
    return new Response(request.method === 'HEAD' ? null : html.body, {
      status: html.status,
      statusText: html.statusText,
      headers,
    });
  },
};
