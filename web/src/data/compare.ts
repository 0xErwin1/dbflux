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
  | 'import_connections';

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

const BEEKEEPER_README =
  'https://raw.githubusercontent.com/beekeeper-studio/beekeeper-studio/master/README.md';
const TABLEPLUS_CHANGELOG = 'https://tableplus.com/blog/2017/02/changelogs.html';

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
      {
        client: 'datagrip',
        href: 'https://blog.jetbrains.com/datagrip/2025/10/01/datagrip-is-now-free-for-non-commercial-use/',
      },
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
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Premium' }],
    },
    sources: [
      { client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Visual-Query-Builder/' },
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
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
    sources: [{ client: 'dbeaver', href: 'https://dbeaver.com/docs/dbeaver/Cloud-Storage/' }],
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

/** Clients with a page of their own under `/compare/`. */
export const COMPARE_PAGES: Readonly<Partial<Record<ClientId, string>>> = {
  dbeaver: 'compare/dbeaver/',
};

/**
 * The DBeaver page's edition table: DBFlux, DBeaver Community, and the cheapest
 * paid DBeaver edition that has the feature. "Not included" in Community
 * follows from each vendor page saying the feature is available in the listed
 * paid editions only.
 */
export const DBEAVER_EDITION_ROWS: readonly {
  row: CompareRow;
  community: CompareCell;
  paid: CompareCell;
}[] = rows(['mongodb', 'redis', 'dynamodb', 'builder', 'charts', 's3']).map((row) => ({
  row,
  community: ['none', { key: 'not_included' }],
  paid: row.clients.dbeaver,
}));

export const DBEAVER_EDITIONS_SOURCE = 'https://dbeaver.com/edition/';

type VsStrings = Dictionary['compare_page']['vs_dbeaver'];

/**
 * What DBFlux does not do today that a DBeaver user may rely on.
 *
 * Each entry was checked against the repository: only what is missing is
 * listed, and nothing here carries a release target.
 */
export const DBEAVER_GAPS: readonly { id: keyof VsStrings['gap']; icon: string }[] = [
  { id: 'table_editor', icon: 'table' },
  { id: 'formats', icon: 'file-down' },
  { id: 'query_plan', icon: 'layers' },
  { id: 'users', icon: 'key-round' },
  { id: 'backup', icon: 'hard-drive' },
  { id: 'data_compare', icon: 'arrow-left-right' },
];

/** DBeaver's names for things, and where the same thing lives in DBFlux. */
export const DBEAVER_TERMS: readonly { from: string; to: keyof VsStrings['term'] }[] = [
  { from: 'Database Navigator', to: 'sidebar' },
  { from: 'SQL Editor', to: 'query_tab' },
  { from: 'ER Diagram', to: 'diagram' },
  { from: 'Data Transfer', to: 'transfer' },
  { from: 'Schema Compare', to: 'schema_diff' },
];

export const TONE_ICON: Readonly<Record<CompareTone, string>> = {
  included: 'circle-check',
  limited: 'circle-alert',
  paid: 'lock',
  none: 'circle-slash',
  unknown: 'circle-question-mark',
};
