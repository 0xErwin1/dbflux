import type { APIRoute } from 'astro';
import { DOCS_MODE } from '../data/site';

const BODY = `# DBFlux

DBFlux is a keyboard-first database client for SQLite, PostgreSQL, MySQL/MariaDB, SQL Server,
MongoDB, Redis, DynamoDB, CloudWatch, InfluxDB, Redshift, and S3.

- [Documentation](https://docs.dbflux.dev/)
- [Install](https://docs.dbflux.dev/install/)
- [Source](https://github.com/0xErwin1/dbflux)

Agent resources are available at [/llms.txt](/llms.txt) and under
[/.well-known/](/.well-known/api-catalog), including the API catalog, the
AI catalog manifest, and agent skills.
`;

export function getStaticPaths() {
  return DOCS_MODE === 'docs' ? [] : [{ params: { home: 'index' } }];
}

export const GET: APIRoute = () =>
  new Response(BODY, {
    headers: { 'content-type': 'text/markdown; charset=utf-8' },
  });
