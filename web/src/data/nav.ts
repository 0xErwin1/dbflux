import { docsUrl } from './site';
import { CURRENT } from './versions';
import { DEFAULT_LOCALE } from '../i18n';
import type { Locale } from '../i18n';
import { localizedDocPath } from '../i18n/locale-registry.ts';

export const REPO = 'https://github.com/0xErwin1/dbflux';

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
      'security',
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
  security: 'Security',
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
 * `locale` selects the URL prefix for a repository path that does not itself
 * say which locale it belongs to (a driver README, `ARCHITECTURE.md`, or an
 * English `docs/<page>.md`). A path in any registered localized docs directory
 * is unambiguous and resolves to that locale's canonical route id.
 *
 * `versionId` selects the URL's version prefix, matching the version of the
 * page doing the linking (e.g. `nightly` for a link written inside nightly's
 * own documentation). Left unset, it resolves against the current release —
 * correct for the common case, but wrong for a version-exclusive page (a
 * driver only shipped in `nightly`) linking to another page that only exists
 * in that same version.
 *
 * Kept in step with the patterns in `src/content.config.ts`.
 */
export function routeForRepoPath(
  path: string,
  locale: Locale = DEFAULT_LOCALE,
  versionId?: string,
): string {
  const versionPrefix =
    versionId === undefined ? undefined : versionId === CURRENT.id ? '' : versionId;

  const driver = path.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
  if (driver) return docsUrl(`drivers/${driver[1]}`, versionPrefix, locale);

  const doc = localizedDocPath(path);
  if (doc) {
    const routeLocale = doc.locale === DEFAULT_LOCALE ? locale : (doc.locale as Locale);
    return docsUrl(doc.path, versionPrefix, routeLocale);
  }

  if (path === 'ARCHITECTURE.md') return docsUrl('architecture', versionPrefix, locale);
  if (path === 'CONTRIBUTING.md') return docsUrl('contributing', versionPrefix, locale);
  if (path === 'SECURITY.md') return docsUrl('security', versionPrefix, locale);

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

  const doc = localizedDocPath(path);
  if (doc) return DOC_TITLES[doc.path] ?? null;

  if (path === 'ARCHITECTURE.md') return DOC_TITLES.architecture ?? null;
  if (path === 'CONTRIBUTING.md') return DOC_TITLES.contributing ?? null;
  if (path === 'SECURITY.md') return DOC_TITLES.security ?? null;

  return null;
}
