export const REPO = 'https://github.com/0xErwin1/dbflux';
export const VERSION = '0.7.7';

export interface DocsSection {
  readonly id: string;
  readonly title: string;
  /** Collection entry ids, in reading order. */
  readonly entries: readonly string[];
}

/**
 * Reading order for the documentation rail.
 *
 * The repository's `docs/` files carry no ordering metadata, so the sequence is
 * declared here rather than inferred from filenames. An entry listed here but
 * missing from disk is reported at build time by `docsSections()`.
 */
export const DOCS_SECTIONS: readonly DocsSection[] = [
  { id: 'start', title: 'Start here', entries: ['install', 'usage', 'connections'] },
  { id: 'using', title: 'Using DBFlux', entries: ['charts', 'dashboards', 'dashboards_and_audit'] },
  { id: 'configure', title: 'Configuring', entries: ['settings', 'lua', 'data_and_privacy'] },
  { id: 'integrate', title: 'Integrations', entries: ['mcp_ai_integration', 'audit'] },
  { id: 'reference', title: 'Reference', entries: ['drivers', 'concepts'] },
  {
    id: 'drivers',
    title: 'Driver reference',
    entries: [
      'drivers/postgres',
      'drivers/mysql',
      'drivers/mssql',
      'drivers/sqlite',
      'drivers/redshift',
      'drivers/clickhouse',
      'drivers/mongodb',
      'drivers/redis',
      'drivers/dynamodb',
      'drivers/influxdb',
      'drivers/cloudwatch',
      'drivers/s3',
      'drivers/ipc',
    ],
  },
  {
    id: 'contribute',
    title: 'Contributing',
    entries: [
      'contributing',
      'architecture',
      'driver_authoring',
      'driver_rpc_protocol',
      'rpc_services_config',
      'release',
    ],
  },
];

/** Display titles for the rail. The markdown H1 stays the page heading. */
export const DOC_TITLES: Readonly<Record<string, string>> = {
  install: 'Installing',
  usage: 'Usage guide',
  connections: 'Connecting',
  charts: 'Charts',
  dashboards: 'Dashboards',
  dashboards_and_audit: 'Dashboards & audit',
  settings: 'Settings & hooks',
  lua: 'Lua scripting',
  data_and_privacy: 'Data & privacy',
  mcp_ai_integration: 'AI + MCP',
  audit: 'Audit events',
  drivers: 'Drivers',
  concepts: 'Key concepts',
  driver_authoring: 'Driver authoring',
  driver_rpc_protocol: 'Driver RPC protocol',
  rpc_services_config: 'RPC services config',
  release: 'Release process',
  architecture: 'Architecture',
  contributing: 'Contributing',
  'drivers/postgres': 'PostgreSQL',
  'drivers/mysql': 'MySQL / MariaDB',
  'drivers/mssql': 'SQL Server',
  'drivers/sqlite': 'SQLite',
  'drivers/redshift': 'Amazon Redshift',
  'drivers/clickhouse': 'ClickHouse',
  'drivers/mongodb': 'MongoDB',
  'drivers/redis': 'Redis',
  'drivers/dynamodb': 'DynamoDB',
  'drivers/influxdb': 'InfluxDB',
  'drivers/cloudwatch': 'CloudWatch',
  'drivers/s3': 'S3',
  'drivers/ipc': 'External RPC drivers',
};

export const docTitle = (id: string): string => DOC_TITLES[id] ?? id;

export const REPO_URL = REPO;

/**
 * Map a repository path to the page that renders it, or to the repository when
 * the site does not host it.
 *
 * Kept in step with the patterns in `src/content.config.ts`.
 */
export function routeForRepoPath(path: string): string {
  const driver = path.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
  if (driver) return `/docs/drivers/${driver[1]}/`;

  const doc = path.match(/^docs\/([^/]+)\.md$/);
  if (doc) return `/docs/${doc[1].toLowerCase()}/`;

  if (path === 'ARCHITECTURE.md') return '/docs/architecture/';
  if (path === 'CONTRIBUTING.md') return '/docs/contributing/';

  return `${REPO}/blob/main/${path}`;
}

/**
 * The display title for a repository path, when the site renders it as a page.
 *
 * The docs are written to be read on GitHub, so they link to each other by
 * filename. "See `SETTINGS.md`" is the right sentence in a repository and the
 * wrong one on a documentation site, where the reader has no files.
 */
export function titleForRepoPath(path: string): string | null {
  const driver = path.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
  if (driver) return DOC_TITLES[`drivers/${driver[1]}`] ?? null;

  const doc = path.match(/^docs\/([^/]+)\.md$/);
  if (doc) return DOC_TITLES[doc[1].toLowerCase()] ?? null;

  if (path === 'ARCHITECTURE.md') return DOC_TITLES.architecture ?? null;
  if (path === 'CONTRIBUTING.md') return DOC_TITLES.contributing ?? null;

  return null;
}
