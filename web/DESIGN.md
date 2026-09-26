---
version: beta
name: DBFlux Website
description: >-
  Design system for dbflux.dev and the documentation host: Bolt Byzantium, the
  same system the desktop client uses. Dark and light themes, Archivo for text,
  Archivo at full width for display type, JetBrains Mono for code and data.
  Tokens are the CSS custom properties in web/src/styles/tokens.css.
colors:
  background: '#09090B'
  background-sunken: '#050507'
  band: '#0D0C10'
  panel: '#100F13'
  raised: '#1A181E'
  raised-hover: '#26232B'
  text: '#C6C3CC'
  text-strong: '#F7F4F7'
  text-muted: '#8E8996'
  text-faint: '#37333D'
  border: '#232128'
  border-strong: '#37333D'
  row-line: '#18161B'
  accent: '#D48CC8'
  accent-fill: '#702963'
  accent-fill-deep: '#4A1B41'
  on-accent: '#FFFFFF'
  link: '#6EA8FF'
  link-hover: '#9CC3FF'
  success: '#7BE0A0'
  danger: '#FF6B5E'
  violet: '#B79CFF'
colors-light:
  background: '#F6F4F7'
  background-sunken: '#E6E1E9'
  band: '#EFEBF1'
  panel: '#FFFFFF'
  raised: '#EEEAF0'
  raised-hover: '#E4DFE7'
  text: '#3B3740'
  text-strong: '#141118'
  text-muted: '#6B6572'
  text-faint: '#CBC4D1'
  border: '#E3DEE6'
  border-strong: '#CBC4D1'
  row-line: '#EEEAF0'
  accent: '#702963'
  accent-fill: '#702963'
  link: '#1F5FD1'
  success: '#1C7F45'
  danger: '#C7362B'
  violet: '#6B4FD8'
typography:
  display-hero:
    fontFamily: Archivo
    fontStretch: 125%
    fontSize: 5.5rem
    fontWeight: 900
    lineHeight: 0.9
    textTransform: uppercase
  display-section:
    fontFamily: Archivo
    fontStretch: 125%
    fontSize: 3.5rem
    fontWeight: 900
    lineHeight: 0.95
    textTransform: uppercase
  label:
    fontFamily: Archivo
    fontStretch: 125%
    fontSize: 0.6875rem
    fontWeight: 800
    letterSpacing: 0.16em
    textTransform: uppercase
  body:
    fontFamily: Archivo
    fontSize: 1rem
    fontWeight: 400
    lineHeight: 1.65
  code:
    fontFamily: JetBrains Mono
    fontSize: 0.8125rem
spacing:
  gutter: 64px
  gutter-tablet: 40px
  gutter-phone: 16px
  nav-height: 72px
  content-max: 1440px
  rail-width: 320px
  tap-min: 44px
  breakpoint-phone: 640px
  breakpoint-drawer: 900px
  breakpoint-stack: 1180px
---

# DBFlux Website

## Overview

The site is set in the desktop client's own system, Bolt Byzantium, so a screenshot and the page around it read as one product. The reference boards are the `SiteByz*` and `DSWeb` design files; this document records the rules they follow.

Token names describe role, never appearance. Color tokens are the CSS custom properties in `web/src/styles/tokens.css`, with the dark values on `:root` and the light values under `html[data-theme='light']`. The theme picker in the footer offers Auto, Light and Dark; Auto follows the operating system.

## Colors

- **Byzantium (`--accent-fill`, #702963)** is the one fill: primary buttons, the selected chip, tab and rail entry, the marquee band, the keycap for the key that matters. It always carries white type.
- **Tint (`--accent`)** is the accent as text or outline: section labels, numbers, highlighted words. It is #D48CC8 on dark surfaces and byzantium itself on light ones.
- **Surfaces** step from the page (`--bg`) to `--panel` for cards and `--raised` for controls. `--band` is the full-width governance band; `--bg-sunken` frames the console mockup.
- **Semantic colors** carry meaning only: `--success` for allowed and included, `--danger` for denied and failed, `--link` for links and the MCP label, `--violet` for numbers in code.
- Verdicts use a colored edge, never a full colored fill: white type on green or red fails contrast.

## Typography

Two self-hosted families, served from `/fonts` and preloaded.

- **Archivo** is the text face at 400 to 700. The same variable file at `font-stretch: 125%` is the client's "Archivo Expanded", used through the `.x` class for display headings (900, capitals), buttons (800, capitals, 0.06em tracking) and section labels (800, capitals, 0.16em tracking).
- **JetBrains Mono** carries code, data, keycaps, version numbers and metadata.
- Chrome text keeps the browser's normal line height; running paragraphs set 1.6 to 1.7 themselves.

## Shape

Controls and cards are chamfered, not rounded: the `.c4` to `.c18` classes cut the top-left and bottom-right corners by that many pixels. Because a clip path would clip a real border along the diagonals, a 1px border on a chamfered box is an inset box shadow. Data surfaces (table rows, grid lines) keep hard edges. Focus outlines on chamfered controls are drawn inside the shape.

## Components

- **Section label**: a 14×8 byzantium tab skewed −30°, then the label. `.eyebrow`, with `--link` and `--ok` variants.
- **Buttons**: primary is byzantium with white expanded capitals; secondary is `--raised` with strong text. Links that lead somewhere are expanded blue capitals with a trailing chevron that nudges on hover (`.go`).
- **Cards**: `--panel` with an inset border, chamfer 14 or 18.
- **Tabs and pickers**: the active entry is `--raised` with a 3px byzantium underline; a selected option in a radio group inverts to strong-on-page.
- **Keycaps**: `--raised` with a 6px inset bottom edge; the key that matters is byzantium.
- **Docs rail**: sections with an icon and a count; the current page is a byzantium chamfered row. The table of contents marks the visible headings with a 2px byzantium edge.

## Motion

Entrances rise 14px over 700ms. The driver band scrolls continuously, the audit log tails, and the demos replay rows when their state changes. Everything stops under `prefers-reduced-motion`.

## Responsive

Above 1180px the layouts follow the 1440px boards. Between 640px and 1180px two-column sections stack and the nav collapses into a sheet. At 640px and below the home page follows the phone board: the keyboard, audit and rules sections are left out, the comparison becomes a five-row table against DBeaver Community, and install shows the one-line Linux command.
