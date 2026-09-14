# @dbflux/mcp

A remote MCP (Model Context Protocol) server that exposes the DBFlux documentation as a
queryable information base. It reads the same machine-readable artifacts the docs site
publishes (`/search-index.json` and per-page `index.md` files at `docs.dbflux.dev`), so
there is no separate content pipeline to maintain.

Deployed to Cloudflare Workers at `https://mcp.dbflux.dev/mcp`.

## Tools

- **`search_docs`** — full-text search over the documentation. Takes `query` and an
  optional `limit` (1-20, default 5). Returns ranked `{ path, title, heading, url,
snippet }` results.
- **`read_page`** — fetches the full Markdown of a documentation page by `path`
  (e.g. `/usage/`).
- **`list_pages`** — lists every documentation page as `{ path, title }`.

All tools are read-only.

## Development

```bash
pnpm install
pnpm dev      # wrangler dev, http://localhost:8787/mcp
pnpm check    # typecheck
pnpm deploy   # wrangler deploy
```
