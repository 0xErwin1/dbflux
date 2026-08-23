import type { Dictionary } from './index';

export const en = {
  nav: {
    features: 'Features',
    drivers: 'Drivers',
    docs: 'Docs',
    about: 'About',
    github: 'GitHub',
    download: 'Download',
    menu: 'Menu',
    language: 'Language',
  },
  footer: {
    product: 'Product',
    features: 'Features',
    drivers: 'Drivers',
    releases: 'Releases',
    docs: 'Docs',
    usage: 'Usage guide',
    connecting: 'Connecting',
    mcp: 'AI + MCP',
    project: 'Project',
    about: 'About',
    contributing: 'Contributing',
    source: 'Source',
    tagline: 'A fully open-source alternative to DBeaver, built in the open.',
    license: 'MIT or Apache-2.0, at your option.',
  },
  search: {
    placeholder: 'Search the documentation',
    move: 'move',
    open: 'open',
    close: 'close',
    no_results: 'No page matches “{query}”.',
    unavailable: 'Search is unavailable right now.',
    result_count_one: '{n} result',
    result_count_other: '{n} results',
  },
  versions: {
    label: 'Version',
    index_tag_title: 'This page does not exist in that version',
    index_tag: 'index',
    default_tag: 'default',
  },
  docs_tree: {
    search_cta: 'Search the docs',
    rail_toggle: 'Documentation menu',
    on_this_page: 'On this page',
    crumb_docs: 'Docs',
    crumb_overview: 'Overview',
    edit_page: 'Edit this page',
    report_issue: 'Report an issue',
    not_translated: 'This page has not been translated yet. Showing the English version.',
    view_in_english: 'View in English',
  },
  docs_index: {
    title: 'Documentation',
    intro:
      'Every page here is rendered from the markdown in the repository’s <code>docs/</code> directory, so a change in behaviour and the paragraph describing it ship in the same commit.',
    unfiled_title: 'Not yet filed',
    unfiled_body:
      'These pages exist in this version but have no place in the reading order declared in <code>src/data/nav.ts</code>.',
  },
  landing: {
    title: 'Every database you run, in one keyboard-driven window.',
    lede: 'An extensible, keyboard-first data platform. Twelve built-in drivers, a driver-neutral core, and anything else you need over the RPC driver protocol.',
    download_linux: 'Download for Linux',
    download_macos: 'Download for macOS',
    download_windows: 'Download for Windows',
    view_source: 'View source',
    platforms_meta: 'Linux · macOS · Windows — MIT or Apache-2.0',
    hero_caption: 'Main server — SELECT * FROM public.transactions (1.5s)',
    hero_alt:
      'DBFlux with a connection tree open, showing databases, schemas, routines and instance metrics for a PostgreSQL server.',
    drivers_eyebrow: 'Built-in drivers',
    drivers_link: 'Capability matrix →',
    drivers_note:
      'Relational, document, key-value, time-series and object stores share one result grid, one chart engine and one audit log. External drivers register over the RPC protocol without a fork.',
    features_eyebrow: 'What you get',
    feature: {
      editor: {
        title: 'Dialect-aware editor',
        body: 'Completion, validation and dangerous-statement detection come from the driver, not a shared guess. A DELETE without a WHERE is caught before it runs.',
      },
      grid: {
        title: 'Editable result grid',
        body: 'Edit cells in place when the result maps cleanly back to one table, page through millions of rows by keyset, and copy any selection as a native query.',
      },
      charts: {
        title: 'Charts and dashboards',
        body: 'Turn any result into a chart, save it, and pin it to a dashboard alongside instance metrics from the same connection.',
      },
      hooks: {
        title: 'Connection hooks',
        body: 'Run a command, a script or in-process Lua around connect and disconnect, with live output in the tasks panel and a failure policy you choose.',
      },
      reach: {
        title: 'Reach anything',
        body: 'SSH tunnels, HTTP proxies and AWS SSO are first-class. Secrets live in the OS keyring, never in the profile file.',
      },
      audit: {
        title: 'Auditable by default',
        body: 'Queries, hooks, scripts and MCP calls all write to the same event log, with redaction and retention you control.',
      },
    },
    keyboard_eyebrow: 'Keyboard-first',
    keyboard_title: 'The mouse is optional, not assumed.',
    keyboard_body:
      'Every surface has a binding and a command-palette entry: open a connection, run a statement, jump to a table, pivot a result into a chart. The empty state tells you the four you need on day one.',
    keyboard_link: 'Full keyboard reference →',
    shortcut: {
      new_query: 'new query',
      command_palette: 'command palette',
      open_script: 'open script from disk',
      new_connection: 'new connection',
    },
    governance_eyebrow: 'Governance',
    governance_title: 'Give an AI client a connection, not your database.',
    governance_body:
      'The MCP server classifies every operation — metadata, read, write, destructive, admin — and a policy engine decides per role and per connection. Write and destructive calls can sit behind human approval.',
    audit_eyebrow: 'Audit',
    audit_title: 'Every query, hook and tool call, on the record.',
    audit_body:
      'Events land in a local SQLite log with category, severity, actor and outcome. Query text is fingerprinted rather than stored, sensitive values are redacted, and the whole log exports to JSON or CSV.',
    docs_eyebrow: 'Documentation',
    docs_link: 'All guides →',
    doc_card: {
      usage: {
        title: 'Usage guide',
        body: 'First launch, creating a connection, running queries, browsing results, charting and exporting.',
      },
      connecting: {
        title: 'Connecting',
        body: 'SSH tunnels, proxies, AWS SSO and value sources for everything that is not a plain host and port.',
      },
      mcp: {
        title: 'AI + MCP',
        body: 'Wire an AI client to DBFlux, then set the roles, policies and approvals that keep it inside the lines.',
      },
    },
  },
  install: {
    all_downloads: 'All downloads →',
    copy: 'copy',
    copied: 'copied',
    copy_fallback: 'press ctrl+c',
    hint: {
      tarball:
        'Prefer no sudo? Append -s -- --prefix ~/.local to install under your home directory.',
      aur: 'Any AUR helper works. yay -S dbflux is equivalent.',
      deb: 'Swap amd64 for arm64 on ARM machines. The .rpm installs the same way with dnf.',
      appimage: 'Fully portable. Nothing is written outside your home directory.',
      nix: 'The default package is a prebuilt binary. Use #dbflux-source to build from source instead.',
      dmg: 'The build is not signed with an Apple developer certificate. To skip the dialog: xattr -cr /Applications/DBFlux.app. Requires macOS 11 Big Sur or later.',
      installer:
        'The executable is not signed with a Windows code signing certificate. Requires Windows 10 or later on x86_64; ARM64 is not supported yet.',
      portable: 'Nothing is installed and nothing is written outside the folder you extract into.',
    },
    steps: {
      dmg: [
        'Download dbflux-macos-arm64.dmg for Apple Silicon, or dbflux-macos-amd64.dmg for Intel.',
        'Open the DMG and drag DBFlux to Applications.',
        'On the "unidentified developer" warning, go to System Settings → Privacy & Security and click Open Anyway.',
      ],
      installer: [
        'Download dbflux-windows-amd64-setup.exe.',
        'Run it and follow the wizard.',
        'If SmartScreen warns, choose More info → Run anyway.',
      ],
      portable: ['Download dbflux-windows-amd64.zip.', 'Extract it anywhere.', 'Run dbflux.exe.'],
    },
  },
  about: {
    page_title: 'About DBFlux',
    page_description:
      'Why DBFlux exists, the principles behind it, and how the codebase is put together.',
    h1: 'Why DBFlux exists',
    intro_p1:
      'Every database client eventually asks you to pick a side: the fast native one that speaks a single engine, or the universal one that speaks all of them and makes you wait. DBFlux takes the third option — one driver-neutral core, drivers that plug into it, and a UI that never learns the name of any of them.',
    intro_p2:
      'That constraint is enforced in the code, not in a style guide. The interface adapts through capability flags and metadata, so a document store gets a document view and a time-series store gets a range picker without a single branch on a driver name. Adding a database is writing a driver, not patching the app.',
    intro_p3:
      'The long-term goal is stated plainly on the README: a fully open-source alternative to DBeaver. Rust and GPUI are how it stays fast enough to be worth switching to.',
    principles_eyebrow: 'Principles',
    principle: {
      p01: {
        title: 'Keyboard before pointer',
        body: 'If an action exists, it has a binding and a command-palette entry. The mouse is a fallback, and no workflow depends on it.',
      },
      p02: {
        title: 'The UI never knows a driver’s name',
        body: 'Category, query language and capability flags decide what renders. A driver that needs new behaviour adds a seam to the core rather than a special case to the interface.',
      },
      p03: {
        title: 'Dense over decorative',
        body: 'Square corners, hairline borders, one accent colour, monospace throughout. Screen space belongs to your data.',
      },
      p04: {
        title: 'Nothing runs unrecorded',
        body: 'Queries, hooks, scripts and AI tool calls all write to the same audit log, redacted by default and yours alone — it never leaves the machine.',
      },
    },
    layers_eyebrow: 'How it is put together',
    layer: {
      ui: {
        detail: 'Six crates, zero driver dependencies and zero per-driver feature flags.',
      },
      app: {
        detail: 'Registers drivers, resolves RPC services, owns connection state.',
      },
      core: {
        detail:
          'DbDriver, Connection, capabilities, metadata, language services, query generators.',
      },
      drivers: {
        detail: 'Twelve built in as Rust crates; anything else over the RPC driver protocol.',
      },
    },
    muted_links: {
      prefix: 'The full crate map and the cross-crate flows live in the ',
      architecture: 'architecture guide',
      middle: '. If you want to write a driver, start with ',
      driver_authoring: 'driver authoring',
      suffix: '.',
    },
    maintainer_title: 'Maintainer',
    maintainer_body:
      'Ignacio Perez, a backend and systems developer working in Rust and C. DBFlux is his, and the great majority of its commits are too.',
    contribute_title: 'Contribute',
    contribute_body:
      'Issues, drivers and docs are all welcome. The contributing guide covers the checks a pull request has to pass before review.',
    contribute_link: 'Read the contributing guide →',
  },
  notfound: {
    title: 'That page does not exist.',
    lede: 'It may have been renamed, or it may belong to a version of DBFlux other than the one you were reading.',
    docs_button: 'Documentation',
    home_button: 'Home',
    versions_label: 'Documentation by version:',
  },
  banner: {
    skip_link: 'Skip to content',
  },
} satisfies Dictionary;
