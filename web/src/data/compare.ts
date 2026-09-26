import type { Dictionary } from '../i18n';

/**
 * What each client ships, and in which edition.
 *
 * Every competitor cell here was checked against the vendor's own
 * documentation, pricing page or repository on {@link COMPARE_REVIEWED}; a cell
 * nobody checked says "not reviewed" rather than guessing. When a vendor
 * changes an edition, update the cell and the date together. DBFlux's own
 * cells describe what the current release ships.
 */
export const COMPARE_REVIEWED = '2026-09-26';

export type CompareTone = 'included' | 'limited' | 'paid' | 'none' | 'unknown';

type CellKey = keyof Dictionary['landing']['compare']['cell'];

/** A cell is either a translated phrase or a literal, such as an edition or licence name. */
export type CompareCell = readonly [CompareTone, { key: CellKey } | { text: string }];

export type ClientId = 'dbeaver' | 'datagrip' | 'tableplus' | 'beekeeper' | 'dbgate';

export interface CompareClient {
  readonly id: ClientId;
  readonly name: string;
  readonly edition: keyof Dictionary['landing']['compare']['edition'];
}

export const COMPARE_CLIENTS: readonly CompareClient[] = [
  { id: 'dbeaver', name: 'DBeaver', edition: 'community' },
  { id: 'datagrip', name: 'DataGrip', edition: 'commercial' },
  { id: 'tableplus', name: 'TablePlus', edition: 'free_trial' },
  { id: 'beekeeper', name: 'Beekeeper', edition: 'community' },
  { id: 'dbgate', name: 'DbGate', edition: 'community' },
];

export interface CompareRow {
  /** A translated row label, or a product name that is never translated. */
  readonly label: { key: keyof Dictionary['landing']['compare']['row'] } | { text: string };
  readonly icon: string;
  readonly dbflux: CompareCell;
  readonly clients: Readonly<Record<ClientId, CompareCell>>;
}

const included: CompareCell = ['included', { key: 'included' }];
const notReviewed: CompareCell = ['unknown', { key: 'not_reviewed' }];

export const COMPARE_ROWS: readonly CompareRow[] = [
  {
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
  },
  {
    label: { key: 'limits' },
    icon: 'tags',
    dbflux: ['included', { key: 'none' }],
    clients: {
      dbeaver: ['included', { key: 'none' }],
      datagrip: ['paid', { key: 'paid_licence' }],
      tableplus: ['limited', { key: 'tabs_limit' }],
      beekeeper: ['included', { key: 'none' }],
      dbgate: ['included', { key: 'none' }],
    },
  },
  {
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
  },
  {
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
  },
  {
    label: { text: 'DynamoDB' },
    icon: 'braces',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: included,
      tableplus: ['limited', { key: 'beta_macos' }],
      beekeeper: ['paid', { key: 'paid_editions_beta' }],
      dbgate: ['paid', { text: 'Premium' }],
    },
  },
  {
    label: { key: 'builder' },
    icon: 'square-function',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { key: 'lite_up' }],
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: ['paid', { text: 'Premium' }],
    },
  },
  {
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
  },
  {
    label: { key: 's3' },
    icon: 'boxes',
    dbflux: included,
    clients: {
      dbeaver: ['paid', { text: 'Ultimate' }],
      datagrip: notReviewed,
      tableplus: notReviewed,
      beekeeper: notReviewed,
      dbgate: notReviewed,
    },
  },
  {
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
  },
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

export const TONE_ICON: Readonly<Record<CompareTone, string>> = {
  included: 'circle-check',
  limited: 'circle-alert',
  paid: 'lock',
  none: 'circle-slash',
  unknown: 'circle-question-mark',
};
