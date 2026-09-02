---
version: alpha
name: DBFlux Website
description: >-
  Design system for dbflux.dev and the documentation host. A single fixed dark
  theme that mirrors the desktop client's Ayu Dark palette, set in monospace
  headings with a humanist sans for running text. Tokens are the CSS custom
  properties in web/src/styles/tokens.css.
colors:
  background: '#0a0e14'
  panel: '#0f1419'
  raised: '#151e2b'
  selection: '#273747'
  scrim: 'rgba(0, 0, 0, 0.55)'
  text: '#b3b1ad'
  text-strong: '#e6e1cf'
  text-muted: '#828d9d'
  text-faint: '#5c6773'
  border: '#1f2430'
  border-interactive: '#5c6773'
  primary: '#ffb454'
  primary-hover: '#e6a34c'
  primary-active: '#cc9143'
  on-primary: '#0a0e14'
  link: '#59c2ff'
  link-hover: '#8dd6ff'
  success: '#aad94c'
  warning: '#ffb454'
  danger: '#f07178'
  on-danger: '#ffffff'
  series-1: '#59c2ff'
  series-2: '#aad94c'
  series-3: '#ffb454'
  series-4: '#f07178'
  series-5: '#d2a6ff'
  series-6: '#95e6cb'
  scrollbar-thumb: 'rgba(179, 177, 173, 0.15)'
  scrollbar-thumb-hover: 'rgba(179, 177, 173, 0.25)'
typography:
  display:
    fontFamily: JetBrains Mono
    fontSize: 3.375rem
    fontWeight: 700
    lineHeight: 1.2
  headline-lg:
    fontFamily: JetBrains Mono
    fontSize: 2.375rem
    fontWeight: 700
    lineHeight: 1.2
  headline-md:
    fontFamily: JetBrains Mono
    fontSize: 1.5rem
    fontWeight: 700
    lineHeight: 1.2
  headline-sm:
    fontFamily: JetBrains Mono
    fontSize: 1.0625rem
    fontWeight: 500
    lineHeight: 1.2
  body-md:
    fontFamily: IBM Plex Sans
    fontSize: 0.9375rem
    fontWeight: 400
    lineHeight: 1.75
  body-sm:
    fontFamily: IBM Plex Sans
    fontSize: 0.8125rem
    fontWeight: 400
    lineHeight: 1.75
  label:
    fontFamily: JetBrains Mono
    fontSize: 0.6875rem
    fontWeight: 500
    lineHeight: 1.2
    letterSpacing: 0.18em
  code:
    fontFamily: JetBrains Mono
    fontSize: 0.8125rem
    fontWeight: 400
    lineHeight: 1.8
rounded:
  none: 0px
  full: 9999px
spacing:
  s-1: 4px
  s-2: 6px
  s-3: 8px
  s-4: 12px
  s-5: 16px
  s-6: 24px
  s-7: 40px
  s-8: 64px
  s-9: 96px
  gutter: 24px
  nav-height: 64px
  rail-width: 268px
  toc-width: 224px
  content-max: 1200px
  docs-max: 1440px
  control-height: 40px
  tap-min: 44px
  border-width: 1px
  border-width-emphasis: 2px
  breakpoint-sm: 640px
  breakpoint-md: 900px
  breakpoint-lg: 1024px
components:
  button-primary:
    backgroundColor: '{colors.primary}'
    textColor: '{colors.on-primary}'
    typography: '{typography.label}'
    rounded: '{rounded.none}'
    height: '{spacing.control-height}'
  button-primary-hover:
    backgroundColor: '{colors.primary-hover}'
  button-primary-active:
    backgroundColor: '{colors.primary-active}'
  button-secondary:
    backgroundColor: transparent
    textColor: '{colors.text}'
    typography: '{typography.label}'
    rounded: '{rounded.none}'
    height: '{spacing.control-height}'
  button-secondary-hover:
    textColor: '{colors.text-strong}'
  card:
    backgroundColor: '{colors.panel}'
    textColor: '{colors.text}'
    rounded: '{rounded.none}'
    padding: '{spacing.s-7}'
  card-hover:
    backgroundColor: '{colors.raised}'
  code-block:
    backgroundColor: '{colors.panel}'
    textColor: '{colors.text}'
    typography: '{typography.code}'
    rounded: '{rounded.none}'
    padding: '{spacing.s-5}'
  callout:
    backgroundColor: '{colors.panel}'
    textColor: '{colors.text}'
    rounded: '{rounded.none}'
    padding: '{spacing.s-5}'
  nav:
    backgroundColor: '{colors.background}'
    textColor: '{colors.text}'
    typography: '{typography.label}'
    height: '{spacing.nav-height}'
  sidebar-rail:
    backgroundColor: '{colors.background}'
    textColor: '{colors.text}'
    width: '{spacing.rail-width}'
  table-header:
    textColor: '{colors.text-muted}'
    typography: '{typography.label}'
  table-row-odd:
    backgroundColor: '{colors.panel}'
  search-dialog:
    backgroundColor: '{colors.raised}'
    textColor: '{colors.text}'
    rounded: '{rounded.none}'
  version-picker:
    backgroundColor: '{colors.panel}'
    textColor: '{colors.text}'
    rounded: '{rounded.none}'
  version-picker-menu:
    backgroundColor: '{colors.raised}'
---

# DBFlux Website

## Overview

The site is the public face of a keyboard-first database client, and it looks like the client. One fixed dark theme, no light mode, no toggle. Square corners everywhere. Borders delineate regions and shadows are reserved for the handful of surfaces that float. Headings and labels are set in the same monospace face the app uses, so the site reads as a terminal that learned typography. Running text uses a humanist sans so long documentation pages stay comfortable.

The audience is engineers evaluating or already using the client. The tone is precise and quiet. Nothing on the page competes with the content, and the single amber accent marks the one thing that matters in a given view.

Token names describe role, never appearance, so a value can change without a rename. Color tokens are the CSS custom properties in `web/src/styles/tokens.css`. Two of them intentionally diverge from the desktop client for measured contrast, and those divergences are documented below.

## Colors

The palette mirrors the desktop client's Ayu Dark theme. Surfaces step from the page background to a panel to a raised layer. Text has three working levels plus a decorative one. The accent is amber and is the only chromatic element outside links and semantic states.

- **Background (#0a0e14):** the page. Everything sits on this.
- **Panel (#0f1419):** cards, code blocks, callouts, table stripes, the mobile nav sheet. One step up from the page.
- **Raised (#151e2b):** floating chrome only. Dropdown menus, the search dialog, card hover.
- **Selection (#273747):** text selection and highlighted rows.
- **Text (#b3b1ad):** running copy.
- **Text strong (#e6e1cf):** headings and emphasized copy.
- **Text muted (#828d9d):** secondary copy, table headers, metadata. This value diverges from the app's muted `#5c6773`, which measures 3.35:1 on the background and fails the 4.5:1 text floor. The site value measures 5.51:1 on the panel.
- **Text faint (#5c6773):** decoration only, meaning icon strokes and tree guides. Never the sole carrier of meaning.
- **Border (#1f2430):** the hairline that separates regions.
- **Border interactive (#5c6773):** any control the reader has to find, such as inputs, pickers, secondary buttons. Measures 3.35:1 on the background, which clears the 3.0 floor for non-text boundaries. This is the second deliberate divergence from the app.
- **Primary (#ffb454):** the amber accent. Primary buttons, active indicators, inline code, the left bar on callouts and doc cards, highlighted search matches. Hover `#e6a34c`, active `#cc9143`, text on primary `#0a0e14`.
- **Link (#59c2ff):** in-prose links, hover `#8dd6ff`.
- **Success (#aad94c), Warning (#ffb454), Danger (#f07178):** status only. Text on danger is white.
- **Series 1 to 6:** chart colors in fixed order, blue, green, amber, red, purple, teal.

Code highlighting uses the Shiki `ayu-dark` theme so fenced blocks match the app's editor.

## Typography

Two self-hosted families. Fonts are bundled through `@fontsource` packages and preloaded rather than loaded from Google Fonts, to avoid a serial DNS and TLS chain on first paint.

- **JetBrains Mono** carries every heading, the nav, buttons, labels, table headers, footer column titles, inline code, and code blocks. Variable weight, 100 to 800 available, 700 used for headings and 500 for the h3 level inside documentation.
- **IBM Plex Sans** carries running text at 15px with a 1.75 line height. Weights 400, 400 italic, 500, and 600 are loaded.
- **Labels** are uppercase mono at 11px with 0.18em tracking. Eyebrows, table headers, footer columns, and nav links share this treatment.
- **Display and h1** are fluid: display clamps between 2rem and 3.375rem, h1 between 1.75rem and 2.375rem. The token values above record the desktop ceiling.
- **Code** runs at 13px with a 1.8 line height inside blocks.

Links inside prose and breadcrumbs are always underlined. Color alone is not enough for a reader who cannot distinguish hues. Block-level links such as nav items, cards, and buttons are exempt because their position identifies them.

## Layout

A centered column with a fixed maximum, and a three-column grid on documentation pages.

- **Content width:** landing pages max out at 1200px, documentation pages at 1440px. Prose inside documentation is capped at 72 characters.
- **Documentation grid:** a 268px sticky rail on the left, fluid content, and a 224px sticky table of contents on the right. Each rail has a 1px border on its inner edge.
- **Nav:** 64px tall, sticky, with a 1px bottom border.
- **Spacing scale:** 4, 6, 8, 12, 16, 24, 40, 64, 96px. The 6px step exists for tight label and pill padding and is the only non-doubling step.
- **Gutter:** 24px.
- **Controls:** 40px minimum height, 44px minimum tap target on touch.
- **Breakpoints:** 1024px drops the table of contents, 900px collapses the nav into a slide-down sheet and the rail into a drawer, 640px tightens gutters.
- **Motion:** 140ms for instant feedback, 200ms for quick transitions, 660ms for entrance animations. Easings are `cubic-bezier(.16,1,.3,1)` for exits and `cubic-bezier(.4,0,.2,1)` for standard moves.
- **Stacking:** nav 100, rail 90, dropdown 200, scrim 300, modal 310, tooltip 400.

## Elevation & Depth

Depth is conveyed by surface steps and 1px borders, not by shadows. A card on the page is a panel-colored box with a border. A hovered doc card moves to the raised surface and shifts 3px right. Active states in the rail, the table of contents, install tabs, and search results are an inset 2px accent bar on the left edge, which reads as emphasis rather than lift.

Shadows exist for exactly the surfaces that float above the document:

- **Shadow md** `0 4px 8px rgba(0,0,0,.24)` on the back-to-top button.
- **Shadow lg** `0 8px 24px rgba(0,0,0,.32)` on the version picker menu, the language picker menu, and the search dialog.

The sticky nav is translucent, the background mixed at 88% with transparent, and blurred at 8px so content scrolling under it stays legible. The search dialog backdrop is the scrim at 55% black.

## Shapes

Everything is square. The radius token is 0 and there is no scale. The only rounded element on the site is the live-status dot, which uses the full radius. Borders are 1px, with a 2px emphasis width for the accent bar on callouts, doc cards, and active indicators.

## Components

### Buttons

Mono label, 40px minimum height, square. Primary is amber with page-background text and bold weight. On hover the background moves to the hover amber and the button lifts 1px. Secondary is transparent with an interactive border and body text color, and on hover the border turns amber while the text turns strong.

### Cards

Panel background, 1px border, 40px internal padding, no shadow. Documentation cards add a 2px amber left border and move to the raised surface on hover.

### Code blocks and inline code

Blocks force the panel background over the Shiki theme, carry a 1px border, 16px padding, 13px mono at 1.8 line height. Inline code is amber text with no background.

### Callouts

A markdown blockquote is the only admonition. Panel background, 1px border, 2px amber left border.

### Tables

Rendered as a block-level scroll container so wide capability matrices neither crush the prose measure nor cause page-level horizontal scroll. Outer 1px border, collapsed inner borders. Headers are uppercase mono labels in muted text. Odd body rows carry the panel background as a stripe.

### Navigation

Sticky, translucent, blurred. Links are mono with an animated amber underline on hover. Below 900px it becomes a panel-colored sheet that slides down.

### Documentation rail and table of contents

The rail renders collapsible sections with numbered indices. The active entry shows a 2px inset amber bar. Count badges use the border color lifted to a readable value and are informational, not ornamental. The table of contents colors the active heading amber with a matching left border.

### Version and language pickers

The trigger is a panel-colored button with an interactive border that turns amber on hover. The menu is raised, bordered, and carries the large shadow. The active entry shows the inset amber bar.

### Search

A native dialog opened with Ctrl or Cmd plus K. Raised surface, interactive border, large shadow, scrim backdrop. Result rows move to the panel background on hover and show the inset bar. Matched text is amber with no highlight background.

### Scrollbars

Styled to match the desktop client: a 10px track with a thumb at 15% foreground, 25% on hover.

## Do's and Don'ts

- Do use the amber accent for one thing per view. If two elements are amber, one of them is wrong.
- Do underline links inside prose. Never rely on the link color alone.
- Do use the interactive border on any control the reader must locate. The hairline border is for region separation only.
- Do use the muted text token for secondary copy. Never demote text to the faint token, which is reserved for decoration.
- Do reach for a surface step and a border before a shadow. Shadows belong to menus, the search dialog, and the back-to-top button.
- Don't add a border radius. The site is square by design and the app is square by design.
- Don't introduce a light theme or a theme toggle. The site has one theme.
- Don't load fonts from a third-party host. Fonts are bundled.
- Don't add a second accent color. Blue is for links and chart series only.
- Don't let a table crush the prose column. Wide tables scroll inside their own container.
