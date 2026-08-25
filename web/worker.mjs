const quality = (accept, type) => {
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

const merge = (headers, name, value) =>
  headers.set(name, [headers.get(name), value].filter(Boolean).join(', '));

export default {
  async fetch(request, env) {
    if (!['GET', 'HEAD'].includes(request.method)) return env.ASSETS.fetch(request);
    const html = await env.ASSETS.fetch(request);
    if (
      !html.ok ||
      !request.url.endsWith('/') ||
      !html.headers.get('content-type')?.includes('text/html')
    )
      return html;
    const markdownUrl = new URL('index.md', request.url).href;
    const markdown = await env.ASSETS.fetch(new Request(markdownUrl));
    if (!markdown.ok) return html;
    const headers = new Headers(html.headers);
    merge(headers, 'Vary', 'Accept');
    merge(headers, 'Link', `<${markdownUrl}>; rel="alternate"; type="text/markdown"`);
    const markdownQuality = quality(request.headers.get('Accept'), 'text/markdown');
    const htmlQuality = quality(request.headers.get('Accept'), 'text/html');
    const selected =
      markdownQuality > 0 && (htmlQuality === undefined || markdownQuality >= htmlQuality);
    if (selected) {
      headers.set('content-type', 'text/markdown; charset=utf-8');
      headers.delete('content-length');
      headers.set(
        'Link',
        `${html.headers.get('Link') ? `${html.headers.get('Link')}, ` : ''}<${request.url}>; rel="alternate"; type="text/html"`,
      );
    }
    return new Response(request.method === 'HEAD' ? null : (selected ? markdown : html).body, {
      status: html.status,
      statusText: html.statusText,
      headers,
    });
  },
};
