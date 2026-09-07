# Privacy Policy

DBFlux is a local-first desktop application. It does not collect, transmit, or
store any information about you or your usage. The project website sets no
cookies and loads no third-party scripts. This document says what that means
in practice and names the two infrastructure providers that see traffic on the
way to you.

## Quick path

1. The application sends data only to the database servers you configure, and
   only what is needed to run the queries you ask for.
2. There is no telemetry, no crash reporting, no usage analytics, and no
   account. Nothing phones home.
3. The website and documentation are static pages. They set no cookies, run no
   analytics script, and keep no record of individual visitors.

## The application

| Question | Answer |
|----------|--------|
| Does DBFlux send usage data anywhere? | No. There is no telemetry and no crash reporter. |
| Does it check for updates? | No. Updates are found on the GitHub releases page. |
| What network connections does it open? | Only the ones you configure: database servers, SSH tunnels, proxies, and cloud APIs for the drivers you use. |
| Where does my data live? | On your machine, in one SQLite file plus the operating system keyring for secrets. |
| Does it keep a record of what I do? | Only the audit log, on your machine. It records connections, queries, and hook runs so you can review them, and it never leaves the computer it was written on. |
| Is anything shared with the project? | No. Bug reports and logs reach the project only when you attach them to an issue yourself. |

Connections to a database, an SSH host, a proxy, or a cloud provider go
directly from your machine to the server you named. The project operates none
of those servers and has no visibility into that traffic.

[Data & Privacy](docs/DATA_AND_PRIVACY.md) documents the files DBFlux writes,
what the audit log records, how secrets are stored, and how to back up or fully
reset the application.

## The website and documentation

`dbflux.dev` and `docs.dbflux.dev` are static sites built from this
repository. They:

- set no cookies, first-party or third-party;
- load no analytics, advertising, or tracking script;
- serve their fonts and assets from the same host, so a page view contacts no
  other domain;
- keep no server-side log the project can read per visitor.

The documentation search runs in your browser against an index file fetched
from the same host. The query never leaves the page.

## Infrastructure providers

Two services sit between the project and you. Neither is used to identify
individual visitors.

| Provider | Role | What it sees |
|----------|------|--------------|
| Cloudflare | Hosts the website, the documentation, and the documentation MCP endpoint at `mcp.dbflux.dev`. | Every HTTP request to those hosts, including the IP address and user agent, as any host does. Cloudflare exposes aggregate traffic counts to the project. It does not expose per-visitor records, and the project has enabled no feature that would. |
| Google Search Console | Reports how the site appears in Google search results. | Only what Google's crawler and search results already know. No script from Google is loaded on any page. |

Cloudflare's own handling of request data is described in the
[Cloudflare privacy policy](https://www.cloudflare.com/privacypolicy/).

## The documentation MCP endpoint

`mcp.dbflux.dev` lets an AI client search the documentation. A session lives
for the duration of one connection and holds only the messages exchanged in
it. There are no accounts, no persistent storage of queries, and no record
linking a session to a visitor after it ends.

## GitHub

Issues, pull requests, discussions, and release downloads are on GitHub.
Anything you post there is public and governed by
[GitHub's privacy statement](https://docs.github.com/site-policy/privacy-policies/github-general-privacy-statement).

## Changes to this policy

This file is versioned with the source code. The commit history of
`PRIVACY.md` is the change log. A change that makes the application or the
website collect anything it does not collect today will be announced in the
release notes of the version that introduces it.

## Contact

Questions go to an issue in this repository with the title prefix
`[privacy]`.
