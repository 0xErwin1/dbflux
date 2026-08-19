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
  { id: 'start', title: 'Start here', entries: ['usage', 'connections'] },
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
