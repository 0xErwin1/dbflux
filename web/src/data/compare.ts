import type { Dictionary } from '../i18n';

/**
 * What each client ships, and in which edition.
 *
 * Every competitor cell here was checked against the vendor's own
 * documentation, pricing page or repository on {@link COMPARE_REVIEWED}, and
 * each row carries the pages it was checked against in `sources`; a cell
 * nobody checked says "not reviewed" rather than guessing. When a vendor
 * changes an edition, update the cell, its source and the date together.
 * DBFlux's own cells describe what the current release ships, and say "not
 * yet" where it does not, without promising a release.
 *
 * The home page teaser, the comparison page and the per-client pages all read
 * from this module, so a correction lands everywhere at once.
 */
export const COMPARE_REVIEWED = '2026-09-26';

/** The month the DBeaver edition split was read from dbeaver.com/edition. */
export const DBEAVER_EDITIONS_AS_OF = '2026-09';

export type CompareTone = 'included' | 'limited' | 'paid' | 'none' | 'unknown';

type CompareStrings = Dictionary['landing']['compare'];
type CellKey = keyof CompareStrings['cell'];
type RowKey = keyof CompareStrings['row'];
type RowNoteKey = keyof CompareStrings['row_note'];

/** A cell is either a translated phrase or a literal, such as an edition or licence name. */
export type CompareCell = readonly [CompareTone, { key: CellKey } | { text: string }];

export type ClientId = 'dbeaver' | 'datagrip' | 'tableplus' | 'beekeeper' | 'dbgate';

export interface CompareClient {
  readonly id: ClientId;
  readonly name: string;
  readonly edition: keyof CompareStrings['edition'];
}

export const COMPARE_CLIENTS: readonly CompareClient[] = [
  { id: 'dbeaver', name: 'DBeaver', edition: 'community' },
  { id: 'datagrip', name: 'DataGrip', edition: 'commercial' },
  { id: 'tableplus', name: 'TablePlus', edition: 'free_trial' },
  { id: 'beekeeper', name: 'Beekeeper', edition: 'community' },
  { id: 'dbgate', name: 'DbGate', edition: 'community' },
];

/** A vendor page a row's cell was checked against. */
export interface CompareSource {
  readonly client: ClientId;
  readonly href: string;
  /** Distinguishes two sources for the same client, e.g. a CLI next to the desktop app. */
  readonly detail?: string;
}

export type CompareRowId =
  | 'open_source'
  | 'limits'
  | 'mongodb'
  | 'redis'
  | 'dynamodb'
  | 'builder'
  | 'charts'
  | 'table_editor'
  | 'query_plan'
  | 's3'
  | 'mcp'
  | 'import_connections'
  | 'commercial_use'
  | 'diagram'
  | 'schema_diff'
  | 'query_log'
  | 'data_compare'
  | 'command_palette'
  | 'ai_approval';

export interface CompareRow {
  readonly id: CompareRowId;
  /** A translated row label, or a product name that is never translated. */
  readonly label: { key: RowKey } | { text: string };
  /** The muted second line under the label, when the row needs one. */
  readonly note?: RowNoteKey;
  readonly icon: string;
  readonly dbflux: CompareCell;
  readonly clients: Readonly<Record<ClientId, CompareCell>>;
  readonly sources: readonly CompareSource[];
}

const included: CompareCell = ['included', { key: 'included' }];
const notReviewed: CompareCell = ['unknown', { key: 'not_reviewed' }];
const notYet: CompareCell = ['none', { key: 'not_yet' }];

const notAvailable: CompareCell = ['none', { key: 'not_available' }];
const notIncluded: CompareCell = ['none', { key: 'not_included' }];

const BEEKEEPER_README =
  'https://raw.githubusercontent.com/beekeeper-studio/beekeeper-studio/master/README.md';
const TABLEPLUS_CHANGELOG = 'https://tableplus.com/blog/2017/02/changelogs.html';
const DATAGRIP_LICENSING =
  'https://blog.jetbrains.com/datagrip/2025/10/01/datagrip-is-now-free-for-non-commercial-use/';
const DBGATE_TEAM_PREMIUM = 'https://www.dbgate.io/editions/team-premium/';

const ROWS: Readonly<Record<CompareRowId, CompareRow>> = {
  open_source: {
    id: 'open_source',
    label: { key: 'open_source' },
    icon: 'scale',
    dbflux: ['included', { text: 'MIT / Apache-2.0' }],
    clients: {
      dbeaver: ['included', { text: 'Apache-2.0' }],
      datagrip: ['none', { key: 'no' }],
      tableplus: ['none', { key: 'no' }],
      beekeeper: ['included', { text: 'GPLv3' }],
      dbgate: ['included', { text: 'GPLv3' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://github.com/dbeaver/dbeaver' },
      { client: 'datagrip', href: 'https://www.jetbrains.com/datagrip/buy/' },
      { client: 'tableplus', href: 'https://tableplus.com/pricing' },
      { client: 'beekeeper', href: 'https://github.com/beekeeper-studio/beekeeper-studio' },
      { client: 'dbgate', href: 'https://github.com/dbgate/dbgate' },
    ],
  },
  limits: {
    id: 'limits',
    label: { key: 'limits' },
    note: 'limits',
    icon: 'tags',
    dbflux: ['included', { key: 'none' }],
    clients: {
      dbeaver: ['included', { key: 'none' }],
      datagrip: ['paid', { key: 'paid_licence' }],
      tableplus: ['limited', { key: 'tabs_limit' }],
      beekeeper: ['included', { key: 'none' }],
      dbgate: ['included', { key: 'none' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.io/' },
      { client: 'datagrip', href: DATAGRIP_LICENSING },
      { client: 'tableplus', href: 'https://tableplus.com/pricing' },
      { client: 'beekeeper', href: 'https://www.beekeeperstudio.io/pricing' },
      { client: 'dbgate', href: 'https://dbgate.io/pricing/' },
    ],
  },
  mongodb: {
    id: 'mongodb',
    label: { text: 'MongoDB' },
    icon: 'brand/mongodb',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: included,
      tableplus: ['limited', { key: 'beta' }],
      beekeeper: ['paid', { key: 'paid_editions' }],
      dbgate: included,
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/MongoDB/' },
      { client: 'datagrip', href: 'https://www.jetbrains.com/datagrip/features/mongodb/' },
      { client: 'tableplus', href: 'https://docs.tableplus.com/' },
      { client: 'beekeeper', href: BEEKEEPER_README },
      { client: 'dbgate', href: 'https://dbgate.io/pricing/' },
    ],
  },
  redis: {
    id: 'redis',
    label: { text: 'Redis' },
    icon: 'brand/redis',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: included,
      tableplus: included,
      beekeeper: included,
      dbgate: included,
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Redis/' },
      { client: 'datagrip', href: 'https://www.jetbrains.com/datagrip/features/redis/' },
      { client: 'tableplus', href: 'https://docs.tableplus.com/' },
      { client: 'beekeeper', href: BEEKEEPER_README },
      { client: 'dbgate', href: 'https://dbgate.io/pricing/' },
    ],
  },
  dynamodb: {
    id: 'dynamodb',
    label: { text: 'DynamoDB' },
    note: 'dynamodb',
    icon: 'braces',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: included,
      tableplus: ['limited', { key: 'beta_macos' }],
      beekeeper: ['paid', { key: 'paid_editions_beta' }],
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/AWS-DynamoDB/' },
      { client: 'datagrip', href: 'https://www.jetbrains.com/help/datagrip/dynamodb.html' },
      { client: 'tableplus', href: TABLEPLUS_CHANGELOG },
      { client: 'beekeeper', href: BEEKEEPER_README },
      {
        client: 'beekeeper',
        href: 'https://docs.beekeeperstudio.io/user_guide/connecting/dynamodb',
        detail: 'beta',
      },
      {
        client: 'dbgate',
        href: 'https://www.dbgate.io/news/2026-02-24-7-1-0-dynamodb-api-endpoints/',
      },
    ],
  },
  builder: {
    id: 'builder',
    label: { key: 'builder' },
    note: 'builder',
    icon: 'square-function',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: notAvailable,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Visual-Query-Builder/' },
      {
        client: 'datagrip',
        href: 'https://youtrack.jetbrains.com/issue/DBE-3897/Visual-Query-Builder',
      },
      {
        client: 'dbgate',
        href: 'https://docs.dbgate.io/dbgate/sql-and-queries/query-designer/index.html',
      },
    ],
  },
  charts: {
    id: 'charts',
    label: { key: 'charts' },
    icon: 'chart-spline',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: included,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Managing-Charts/' },
      {
        client: 'datagrip',
        href: 'https://blog.jetbrains.com/datagrip/2023/12/06/datagrip-2023-3-data-visualization-with-the-lets-plot-library-new-import-functionality-numerous-improvements-in-introspection-dynamodb-support-and-more/',
      },
      { client: 'dbgate', href: 'https://dbgate.io/pricing/' },
    ],
  },
  table_editor: {
    id: 'table_editor',
    label: { key: 'table_editor' },
    icon: 'table',
    dbflux: notYet,
    clients: {
      dbeaver: included,
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: included,
      dbgate: notReviewed,
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Database-Object-Editor/' },
      { client: 'beekeeper', href: 'https://www.beekeeperstudio.io/community/' },
    ],
  },
  query_plan: {
    id: 'query_plan',
    label: { key: 'query_plan' },
    icon: 'layers',
    dbflux: notYet,
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [],
  },
  s3: {
    id: 's3',
    label: { key: 's3' },
    note: 's3',
    icon: 'boxes',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { text: 'Ultimate' }],
      datagrip: ['included', { key: 'big_data_tools' }],
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Cloud-Storage/' },
      {
        client: 'datagrip',
        href: 'https://www.jetbrains.com/help/datagrip/big-data-tools-aws-s3.html',
      },
      {
        client: 'datagrip',
        href: 'https://www.jetbrains.com/help/datagrip/big-data-tools-minio.html',
        detail: 'MinIO',
      },
    ],
  },
  mcp: {
    id: 'mcp',
    label: { key: 'mcp' },
    icon: 'bot',
    dbflux: ['included', { key: 'with_roles' }],
    clients: {
      dbeaver: ['limited', { key: 'mcp_dbeaver' }],
      datagrip: included,
      tableplus: ['included', { text: 'macOS' }],
      beekeeper: notReviewed,
      dbgate: ['limited', { key: 'mcp_dbgate' }],
    },
    sources: [
      {
        client: 'dbeaver',
        href: 'https://dbeaver.com/docs/team-edition/web/Model-Context-Protocol-Server/',
      },
      { client: 'dbeaver', href: 'https://dbeaver.io/dbvr/', detail: 'dbvr' },
      { client: 'datagrip', href: 'https://www.jetbrains.com/help/datagrip/mcp-server.html' },
      { client: 'tableplus', href: TABLEPLUS_CHANGELOG },
      { client: 'dbgate', href: 'https://www.dbgate.io/news/2026-07-21-7-2-3/' },
    ],
  },
  import_connections: {
    id: 'import_connections',
    label: { key: 'import_connections' },
    icon: 'arrow-left-right',
    dbflux: ['included', { text: 'DBeaver, Beekeeper' }],
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [],
  },
  commercial_use: {
    id: 'commercial_use',
    label: { key: 'commercial_use' },
    icon: 'file-text',
    dbflux: included,
    clients: {
      dbeaver: notReviewed,
      datagrip: ['paid', { key: 'paid_licence' }],
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [
      { client: 'datagrip', href: DATAGRIP_LICENSING },
      {
        client: 'datagrip',
        href: 'https://sales.jetbrains.com/hc/en-gb/articles/18950890312210-The-free-non-commercial-licensing-FAQ',
        detail: 'FAQ',
      },
    ],
  },
  diagram: {
    id: 'diagram',
    label: { key: 'diagram' },
    icon: 'chart-network',
    dbflux: included,
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: ['limited', { key: 'community_plugin' }],
      beekeeper: ['paid', { key: 'paid_editions' }],
      dbgate: notReviewed,
    },
    sources: [
      { client: 'tableplus', href: 'https://github.com/TablePlus/diagram-plugin' },
      {
        client: 'beekeeper',
        href: 'https://docs.beekeeperstudio.io/user_guide/entity-relationship-diagrams-erd/',
      },
    ],
  },
  schema_diff: {
    id: 'schema_diff',
    label: { key: 'schema_diff' },
    icon: 'copy',
    dbflux: included,
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: notAvailable,
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'tableplus', href: 'https://github.com/TablePlus/TablePlus/issues/3232' },
      { client: 'dbgate', href: 'https://docs.dbgate.io/dbgate/database-operations/index.html' },
    ],
  },
  query_log: {
    id: 'query_log',
    label: { key: 'query_log' },
    icon: 'scroll-text',
    dbflux: ['included', { key: 'audit_log' }],
    clients: {
      dbeaver: notReviewed,
      datagrip: ['included', { key: 'sql_log' }],
      tableplus: ['included', { key: 'console_log' }],
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Team Premium' }],
    },
    sources: [
      {
        client: 'datagrip',
        href: 'https://www.jetbrains.com/help/datagrip/find-recent-queries-and-files.html',
      },
      {
        client: 'tableplus',
        href: 'https://docs.tableplus.com/gui-tools/the-interface/console-log.md',
      },
      { client: 'dbgate', href: DBGATE_TEAM_PREMIUM },
    ],
  },
  data_compare: {
    id: 'data_compare',
    label: { key: 'data_compare' },
    icon: 'arrow-left-right',
    dbflux: notYet,
    clients: {
      dbeaver: notReviewed,
      datagrip: included,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [
      { client: 'datagrip', href: 'https://www.jetbrains.com/help/datagrip/compare-data.html' },
    ],
  },
  command_palette: {
    id: 'command_palette',
    label: { key: 'command_palette' },
    icon: 'search',
    dbflux: included,
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: ['limited', { key: 'objects_only' }],
      beekeeper: notReviewed,
      dbgate: included,
    },
    sources: [
      { client: 'tableplus', href: 'https://docs.tableplus.com/gui-tools/open-anything.md' },
      { client: 'dbgate', href: 'https://www.dbgate.io/features/interface/' },
    ],
  },
  ai_approval: {
    id: 'ai_approval',
    label: { key: 'ai_approval' },
    icon: 'clock-check',
    dbflux: ['included', { key: 'per_policy' }],
    clients: {
      dbeaver: notReviewed,
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: ['paid', { key: 'paid_editions' }],
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'beekeeper', href: 'https://docs.beekeeperstudio.io/user_guide/sql-ai-shell/' },
      { client: 'dbgate', href: DBGATE_TEAM_PREMIUM },
    ],
  },
};

const rows = (ids: readonly CompareRowId[]): readonly CompareRow[] => ids.map((id) => ROWS[id]);

/** The rows the home page teaser shows, one client at a time. */
export const COMPARE_ROWS: readonly CompareRow[] = rows([
  'open_source',
  'limits',
  'mongodb',
  'redis',
  'dynamodb',
  'builder',
  'charts',
  's3',
  'mcp',
]);

export interface CompareGroup {
  readonly id: keyof CompareStrings['group'];
  readonly icon: string;
  readonly rows: readonly CompareRow[];
}

/** The full comparison page, every client side by side. */
export const COMPARE_GROUPS: readonly CompareGroup[] = [
  { id: 'licence', icon: 'scale', rows: rows(['open_source', 'limits']) },
  { id: 'engines', icon: 'database', rows: rows(['mongodb', 'redis', 'dynamodb']) },
  {
    id: 'query',
    icon: 'table',
    rows: rows(['builder', 'charts', 'table_editor', 'query_plan']),
  },
  { id: 'cloud', icon: 'boxes', rows: rows(['s3', 'mcp', 'import_connections']) },
];

/** The five rows the phone layout keeps, against DBeaver Community only. */
export const COMPARE_MOBILE_ROWS: readonly {
  label: CompareRow['label'];
  icon: string;
  dbflux: CompareTone;
  dbeaver: CompareTone;
}[] = [
  { label: { text: 'MongoDB' }, icon: 'brand/mongodb', dbflux: 'included', dbeaver: 'paid' },
  { label: { text: 'Redis' }, icon: 'brand/redis', dbflux: 'included', dbeaver: 'paid' },
  { label: { text: 'DynamoDB' }, icon: 'braces', dbflux: 'included', dbeaver: 'paid' },
  {
    label: { key: 'builder_short' },
    icon: 'square-function',
    dbflux: 'included',
    dbeaver: 'paid',
  },
  { label: { key: 's3_short' }, icon: 'boxes', dbflux: 'included', dbeaver: 'paid' },
];

type VsStrings = Dictionary['compare_page']['vs'];

export type VsColumn = keyof VsStrings['column'];
export type VsGain = 'builder' | 'redis' | 'aws' | 'governance';
export type VsGap = keyof VsStrings['gap'];
export type VsTerm = keyof VsStrings['term'];

/** One row of a per-client table: DBFlux first, then one cell per column of that client. */
export interface VsRow {
  readonly row: CompareRow;
  readonly cells: readonly CompareCell[];
}

/**
 * One client's page under `/compare/`.
 *
 * `columns` names the client's editions or licences in the order the table
 * shows them, and every row carries one cell per column. A cell equal to what
 * the full comparison shows for that client reads it from the shared row, so
 * the two pages cannot disagree about the compared edition.
 *
 * `gaps` lists only what DBFlux lacks and this client has, per its own
 * documentation. `terms` maps the client's own UI names to DBFlux's.
 */
export interface VsPage {
  readonly client: ClientId;
  readonly path: string;
  /** The vendor page the editions or licences were read from. */
  readonly editionsSource: string;
  readonly columns: readonly VsColumn[];
  readonly rows: readonly VsRow[];
  readonly gains: readonly VsGain[];
  readonly gaps: readonly VsGap[];
  readonly terms: readonly { from: string; to: VsTerm }[];
}

const vsRow = (id: CompareRowId, ...cells: CompareCell[]): VsRow => ({ row: ROWS[id], cells });

const compared = (id: CompareRowId, client: ClientId): CompareCell => ROWS[id].clients[client];

/**
 * DBeaver: Community, and the cheapest paid DBeaver edition that has the
 * feature. "Not included" in Community follows from each vendor page saying
 * the feature is available in the listed paid editions only.
 */
const DBEAVER_PAGE: VsPage = {
  client: 'dbeaver',
  path: 'compare/dbeaver/',
  editionsSource: 'https://dbeaver.com/edition/',
  columns: ['dbeaver_community', 'dbeaver_paid'],
  rows: (['mongodb', 'redis', 'dynamodb', 'builder', 'charts', 's3'] as const).map((id) =>
    vsRow(id, notIncluded, compared(id, 'dbeaver')),
  ),
  gains: ['builder', 'redis', 'aws'],
  gaps: ['table_editor', 'formats', 'query_plan', 'users', 'backup', 'data_compare'],
  terms: [
    { from: 'Database Navigator', to: 'sidebar' },
    { from: 'SQL Editor', to: 'query_tab' },
    { from: 'ER Diagram', to: 'diagram' },
    { from: 'Data Transfer', to: 'transfer' },
    { from: 'Schema Compare', to: 'schema_diff' },
  ],
};

/**
 * DataGrip: one feature set under two licences. The non-commercial licence has
 * every feature of the commercial one, so the difference is who may use it
 * for what, and the features DataGrip has in both columns are shown as such.
 */
const DATAGRIP_PAGE: VsPage = {
  client: 'datagrip',
  path: 'compare/datagrip/',
  editionsSource: DATAGRIP_LICENSING,
  columns: ['datagrip_noncommercial', 'datagrip_commercial'],
  rows: [
    vsRow(
      'commercial_use',
      ['none', { key: 'noncommercial_only' }],
      compared('commercial_use', 'datagrip'),
    ),
    vsRow('open_source', ['none', { key: 'no' }], compared('open_source', 'datagrip')),
    ...(
      [
        'mongodb',
        'redis',
        'dynamodb',
        'builder',
        'charts',
        's3',
        'mcp',
        'query_log',
        'data_compare',
      ] as const
    ).map((id) => vsRow(id, compared(id, 'datagrip'), compared(id, 'datagrip'))),
  ],
  gains: ['builder', 'governance', 'aws'],
  gaps: ['data_compare', 'query_plan'],
  terms: [
    { from: 'Database Explorer', to: 'sidebar' },
    { from: 'Query console', to: 'query_tab' },
    { from: 'Diagrams', to: 'diagram' },
    { from: 'Import/Export', to: 'transfer' },
    { from: 'Compare Structure', to: 'schema_diff' },
    { from: 'Query history', to: 'history' },
    { from: 'Query files', to: 'saved' },
    { from: 'Use SSH tunnel', to: 'ssh' },
    { from: 'Find Action', to: 'palette' },
    { from: 'Startup script', to: 'no_equivalent' },
  ],
};

/**
 * TablePlus: the free version is a trial without a time limit, and paid
 * licences are perpetual. Features are split by platform rather than by plan,
 * so a fact found only in the macOS changelog is labelled macOS.
 */
const TABLEPLUS_PAGE: VsPage = {
  client: 'tableplus',
  path: 'compare/tableplus/',
  editionsSource: 'https://tableplus.com/pricing',
  columns: ['tableplus_trial', 'tableplus_paid'],
  rows: [
    vsRow('limits', compared('limits', 'tableplus'), ['included', { key: 'none' }]),
    ...(
      [
        'open_source',
        'mongodb',
        'redis',
        'dynamodb',
        'diagram',
        'schema_diff',
        'mcp',
        'query_log',
        'command_palette',
      ] as const
    ).map((id) => vsRow(id, compared(id, 'tableplus'), compared(id, 'tableplus'))),
  ],
  gains: ['builder', 'governance', 'aws'],
  gaps: ['formats', 'backup'],
  terms: [
    { from: 'Left sidebar', to: 'sidebar' },
    { from: 'Query Editor', to: 'query_tab' },
    { from: 'File > Import, File > Export', to: 'transfer' },
    { from: 'History', to: 'history' },
    { from: 'Favorite', to: 'saved' },
    { from: 'Use SSH key', to: 'ssh' },
    { from: 'Open Anything', to: 'palette' },
  ],
};

/**
 * Beekeeper Studio: Community under GPLv3, and paid editions. Its README says
 * which databases need a paid edition without naming the plan, so those cells
 * say "Paid editions".
 */
const BEEKEEPER_PAGE: VsPage = {
  client: 'beekeeper',
  path: 'compare/beekeeper/',
  editionsSource: BEEKEEPER_README,
  columns: ['beekeeper_community', 'beekeeper_paid'],
  rows: [
    vsRow('open_source', compared('open_source', 'beekeeper'), [
      'none',
      { key: 'commercial_licence' },
    ]),
    vsRow('mongodb', notIncluded, compared('mongodb', 'beekeeper')),
    vsRow('redis', compared('redis', 'beekeeper'), compared('redis', 'beekeeper')),
    vsRow('dynamodb', notIncluded, compared('dynamodb', 'beekeeper')),
    vsRow('diagram', notIncluded, compared('diagram', 'beekeeper')),
    vsRow('table_editor', compared('table_editor', 'beekeeper'), included),
    vsRow('ai_approval', notIncluded, ['paid', { text: 'AI Shell' }]),
  ],
  gains: ['builder', 'governance', 'aws'],
  gaps: ['table_editor', 'backup'],
  terms: [
    { from: 'Saved connections', to: 'sidebar' },
    { from: 'SQL Editor', to: 'query_tab' },
    { from: 'Open ER Diagram', to: 'diagram' },
    { from: 'Data Import, Data Export', to: 'transfer' },
    { from: 'Query history', to: 'history' },
    { from: 'Saved Queries', to: 'saved' },
    { from: 'SSH Tunnel', to: 'ssh' },
    { from: 'Quick Search', to: 'palette' },
    { from: 'Cloud Workspaces', to: 'no_equivalent' },
  ],
};

/** DbGate: Community under GPL-3.0, Premium for one user, and Team Premium. */
const DBGATE_PAGE: VsPage = {
  client: 'dbgate',
  path: 'compare/dbgate/',
  editionsSource: 'https://dbgate.io/pricing/',
  columns: ['dbgate_community', 'dbgate_premium', 'dbgate_team'],
  rows: [
    vsRow('mongodb', compared('mongodb', 'dbgate'), included, included),
    vsRow('redis', compared('redis', 'dbgate'), included, included),
    vsRow('dynamodb', notIncluded, included, included),
    vsRow('builder', notIncluded, included, included),
    vsRow('charts', notIncluded, included, included),
    vsRow('schema_diff', notIncluded, included, included),
    vsRow(
      'mcp',
      compared('mcp', 'dbgate'),
      ['unknown', { key: 'not_stated' }],
      ['included', { key: 'mcp_dbgate_team' }],
    ),
    vsRow('query_log', notIncluded, notIncluded, ['included', { key: 'user_actions' }]),
    vsRow('command_palette', compared('command_palette', 'dbgate'), included, included),
    vsRow(
      'ai_approval',
      notIncluded,
      ['included', { key: 'db_chat' }],
      ['included', { key: 'db_chat' }],
    ),
  ],
  gains: ['builder', 'governance', 'aws'],
  gaps: ['table_editor', 'formats', 'backup'],
  terms: [
    { from: 'CONNECTIONS', to: 'sidebar' },
    { from: 'SQL editor', to: 'query_tab' },
    { from: 'ER diagrams', to: 'diagram' },
    { from: 'Export & import', to: 'transfer' },
    { from: 'Compare & deploy models', to: 'schema_diff' },
    { from: 'Saved queries', to: 'saved' },
    { from: 'Use SSH tunnel', to: 'ssh' },
    { from: 'Command palette', to: 'palette' },
    { from: 'Perspectives, Maps', to: 'no_equivalent' },
  ],
};

/** Every client's page, in the order the comparison lists the clients. */
export const VS_PAGES: Readonly<Record<ClientId, VsPage>> = {
  dbeaver: DBEAVER_PAGE,
  datagrip: DATAGRIP_PAGE,
  tableplus: TABLEPLUS_PAGE,
  beekeeper: BEEKEEPER_PAGE,
  dbgate: DBGATE_PAGE,
};

/** Clients with a page of their own under `/compare/`. */
export const COMPARE_PAGES: Readonly<Partial<Record<ClientId, string>>> = Object.fromEntries(
  COMPARE_CLIENTS.map((client) => [client.id, VS_PAGES[client.id].path]),
);

export const GAP_ICON: Readonly<Record<VsGap, string>> = {
  table_editor: 'table',
  formats: 'file-down',
  query_plan: 'layers',
  users: 'key-round',
  backup: 'hard-drive',
  data_compare: 'arrow-left-right',
};

export const TONE_ICON: Readonly<Record<CompareTone, string>> = {
  included: 'circle-check',
  limited: 'circle-alert',
  paid: 'lock',
  none: 'circle-slash',
  unknown: 'circle-question-mark',
};
