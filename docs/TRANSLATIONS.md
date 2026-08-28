# Contributing translations

DBFlux is translated in three places, and each one accepts contributions
differently: the application UI through [Hosted Weblate](https://hosted.weblate.org/engage/dbflux/),
the documentation through ordinary pull requests, and the website chrome through
a small code change. Partial translations are always welcome — every surface
falls back to English for anything not yet translated, so a language never has
to be finished before it ships.

## Quick path

| What you want to translate | Where it lives | How to contribute |
|---|---|---|
| Application UI (menus, dialogs, settings) | `crates/dbflux_i18n/locales/<code>.yml` | Translate on [Weblate](https://hosted.weblate.org/engage/dbflux/) — no account with us, no git needed |
| Documentation (this site's guides and driver pages) | `docs/<locale-dir>/` | Open a pull request adding markdown files |
| Website chrome (navigation, landing page, footer) | `web/src/i18n/<locale>.ts` | Open a pull request, or an issue if you don't write TypeScript |

<a href="https://hosted.weblate.org/engage/dbflux/"><img src="https://hosted.weblate.org/widget/dbflux/multi-auto.svg" alt="Translation status"></a>

## Application UI — Weblate

The in-app strings live in one YAML catalog per language under
`crates/dbflux_i18n/locales/`. [Hosted Weblate](https://hosted.weblate.org/engage/dbflux/)
edits those catalogs through a web interface and sends the result back to this
repository as pull requests, so translating there requires no development
setup at all.

Direct pull requests against the YAML files are also fine if you prefer git.
Either way:

- English (`en.yml`) is the source language and the fallback. A key missing
  from your language simply shows English at runtime.
- Every catalog must define a nonempty `language.native_name` — the language's
  own name in that language (for example `Español`). A contract test enforces
  it.
- A new language is a new `<code>.yml` file. The application discovers
  catalogs at build time, so no Rust changes are needed — the language appears
  in Settings automatically.

### Terminology

When in doubt, keep established database and product terms as the English
original rather than translating them: **Schema**, **MCP**, driver names, SQL
keywords. A translated term that users never see anywhere else is harder to
understand than the English one they already know.

## Documentation — pull requests

The site renders the repository's own markdown, so translating a page is
adding one file in the right place. Each language has a directory under
`docs/` (`docs/es/`, `docs/zh_Hans/`) that mirrors the English layout:

| English page | Translated file |
|---|---|
| `docs/USAGE.md` | `docs/<locale-dir>/USAGE.md` |
| `docs/SETTINGS.md` | `docs/<locale-dir>/SETTINGS.md` |
| Driver README (`crates/dbflux_driver_postgres/README.md`) | `docs/<locale-dir>/drivers/postgres.md` |
| `ARCHITECTURE.md`, `CONTRIBUTING.md`, `SECURITY.md` (repository root) | `docs/<locale-dir>/ARCHITECTURE.md`, etc. |

Rules that keep the site build happy:

- File names must match the English original exactly — the site pairs pages by
  name.
- Keep the markdown structure of the English page: same headings, same
  relative links, same ```mermaid fences. Links are rewritten to site routes
  at build time, so they must point at the same targets as the English page.
- Translate one file per pull request, or a few related ones. Small pull
  requests review faster.
- A page you haven't translated yet is not a problem: the site serves the
  English body under your language's URL with a "not translated yet" notice,
  never a 404.

When the English documentation changes, the translated page keeps rendering
its last state — updating translations after a behavior change is welcome as
its own pull request.

## Website chrome — a small code change

The navigation, landing page, footer, and search UI strings live in typed
TypeScript dictionaries under `web/src/i18n/`. Translating them for an
existing language is editing that language's dictionary; adding a **new**
language to the website is two changes:

1. Register the locale in `web/src/i18n/locale-registry.mjs`. The `id` is the
   public identity — it becomes the URL prefix (`/es/`, `/zh-Hans/`) and the
   HTML `lang`/`hreflang` value, so it must be a valid
   [BCP-47](https://en.wikipedia.org/wiki/IETF_language_tag) tag (hyphens,
   never underscores). `docsDirectory` names the folder under `docs/` and may
   differ from the id.
2. Add `web/src/i18n/<locale>.ts` exporting a `Dictionary` object. The type
   is exhaustive: a missing key is a compile error, caught by `pnpm check`.

If TypeScript is not your thing, open an issue with your language and we will
wire the code side — the catalog and documentation translations are where
native-speaker time actually matters.

## Checking your work

| Change | Check |
|---|---|
| App catalog YAML | `cargo test -p dbflux_i18n` |
| Documentation markdown | Read it on GitHub — if it renders correctly there, the site renders it |
| Website dictionaries | `cd web && pnpm check && pnpm build` |

Weblate contributions need none of this — its pull requests run the full CI
like any other.
