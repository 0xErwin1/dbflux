const records = [
  [
    '/install/postgresql/',
    'PostgreSQL',
    'PostgreSQL connections use DBFlux driver documentation.',
    'drivers/postgres',
  ],
  [
    '/install/mysql/',
    'MySQL and MariaDB',
    'MySQL and MariaDB connections use DBFlux driver documentation.',
    'drivers/mysql',
  ],
  [
    '/install/mongodb/',
    'MongoDB',
    'MongoDB connections use DBFlux driver documentation.',
    'drivers/mongodb',
  ],
  [
    '/features/sql-editor/',
    'SQL editor',
    'Use DBFlux documentation to install and begin querying.',
    'usage',
  ],
  [
    '/features/local-mcp-governance/',
    'Local MCP governance',
    'DBFlux documents local MCP governance and client configuration.',
    'mcp_ai_integration',
  ],
] as const;
export const ACQUISITION = records.map(([route, heading, description, docs]) => ({
  route,
  title: `${heading} | DBFlux`,
  description,
  heading,
  body: route.includes('local-mcp')
    ? [
        'A client launches the local dbflux mcp subprocess and communicates over stdio using newline-delimited JSON-RPC 2.0.',
        'Local trusted-client, connection, policy, approval, and audit governance applies to each request.',
      ]
    : ['DBFlux provides current installation and connection documentation for this workflow.'],
  docs: ['Read the current documentation', docs],
}));
export type AcquisitionRecord = (typeof ACQUISITION)[number];
if (ACQUISITION.length !== 5 || new Set(ACQUISITION.map(({ route }) => route)).size !== 5)
  throw new Error('Acquisition routes must contain exactly five unique records');
