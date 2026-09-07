# DBFlux Trademark Policy

The DBFlux source code is free software under `MIT OR Apache-2.0`. The name
**DBFlux**, the DBFlux logo, and the `dbflux.dev` domain are not part of that
grant. This document says what you may do with them without asking.

## Quick path

1. You can use the name to say where your work comes from: "based on DBFlux",
   "a fork of DBFlux", "compatible with DBFlux".
2. You can redistribute **unmodified** official release artifacts under the
   DBFlux name, for example as a Homebrew, AUR, or nixpkgs package.
3. If you publish **modified** builds, use a different name and logo. Say that
   your project is derived from DBFlux and is not the official one.

If your case is not one of these three, open an issue in this repository and
ask.

## Marks covered

| Mark | What it means |
|------|---------------|
| DBFlux | The product name, in any capitalization, alone or in compounds such as "DBFlux Pro" or "DBFlux Nightly". |
| The logo | Every file under `resources/branding/` and `packaging/icons/`, and any derivative of them. |
| dbflux.dev | The domain and every page under it. |

The code license does not change this. The Apache 2.0 text says so in
section 6. MIT does not mention trademarks at all, so choosing MIT over Apache
does not grant a right to the name that Apache withholds.

## What you may do

| Use | Allowed without asking |
|-----|------------------------|
| Describe origin, compatibility, or comparison ("built on DBFlux", "works with DBFlux") | Yes |
| Keep the name in source files, commit history, and documentation of a fork | Yes |
| Package or mirror **unmodified** official releases under the DBFlux name | Yes |
| Write articles, talks, or tutorials about DBFlux, including the logo in a screenshot | Yes |
| Use the name and logo for a build that includes patches not in an official release | No |
| Publish releases, installers, or an update channel branded DBFlux from another repository | No |
| Register a domain, package name, app-store listing, or account whose main element is "DBFlux" | No |
| Use the logo, or an edited version of it, as the icon of a derived product | No |

"Unmodified" means the artifact was produced by the official release workflow
from a tag in this repository and its checksum matches the one published with
the release. A rebuild from the same tag counts as unmodified when the source
tree is byte-identical to that tag.

## Naming a fork

A derived product needs a name that a user cannot confuse with DBFlux. The
following work:

- A distinct name with an origin note: "Fluxbase, a fork of DBFlux".
- A qualifier that clearly marks it as unofficial: "DBFlux (community edition,
  unofficial)" is acceptable in a README, but the binary, window title, bundle
  identifier, and installer must not present themselves as plain "DBFlux".

The following do not work:

- Shipping a release named "DBFlux vX.Y.Z" from a fork.
- Pointing an in-app update check at a fork while the app still calls itself
  DBFlux.
- Reusing the DBFlux icon, or a recolored one, as the app icon.

The rename does not have to touch the source tree. Changing the display name,
bundle identifier, icon set, `repository` field, and release title is enough.

## Why this exists

A user who installs "DBFlux" expects the builds published by this project:
reviewed changes, the documented data-and-privacy behavior, and signed
artifacts where the platform supports them. A modified build under the same
name breaks that expectation, and the bug reports for it land here.

## Contact

Questions and permission requests go to an issue in this repository with the
title prefix `[trademark]`. There is no fee and no form. Most requests that
explain the use case are answered with a yes.
