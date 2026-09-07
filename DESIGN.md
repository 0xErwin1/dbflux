---
version: alpha
name: DBFlux Desktop
description: >-
  Design system for the DBFlux desktop client, a keyboard-first database tool
  built with GPUI. Three Ayu-derived palettes (Dark, Mirage, Light) and two
  density styles (Default, Compact). Tokens below record the Dark palette at
  Default density, which is what ships as the default. The other palettes are
  tabulated in the Colors section. Source of truth is
  crates/dbflux_components/src/{theme.rs,tokens.rs,density.rs,semantic.rs}.
colors:
  background: "#0A0E14"
  panel: "#0F1419"
  raised: "#151E2B"
  tiles: "#111823"
  foreground: "#B3B1AD"
  muted: "#5C6773"
  muted-foreground: "#828D9D"
  border: "#1F2430"
  ghost-border: "rgba(82, 68, 54, 0.15)"
  selection: "#273747"
  primary: "#FFB454"
  primary-hover: "#E6A34C"
  primary-active: "#CC9143"
  on-primary: "#0A0E14"
  hover-tint: "rgba(179, 177, 173, 0.05)"
  input-edge: "rgba(179, 177, 173, 0.14)"
  ring: "rgba(255, 180, 84, 0.75)"
  overlay: "rgba(0, 0, 0, 0.55)"
  danger: "#F07178"
  on-danger: "#FFFFFF"
  success: "#AAD94C"
  warning: "#FFB454"
  info: "#59C2FF"
  table-even: "rgba(179, 177, 173, 0.02)"
  table-hover: "rgba(179, 177, 173, 0.05)"
  table-active: "rgba(89, 194, 255, 0.15)"
  table-active-border: "rgba(89, 194, 255, 0.50)"
  row-insert: "rgba(170, 217, 76, 0.15)"
  row-dirty: "rgba(255, 180, 84, 0.20)"
  row-delete: "rgba(240, 113, 120, 0.10)"
  row-error: "rgba(240, 113, 120, 0.15)"
  row-saving: "rgba(255, 180, 84, 0.10)"
  syntax-table: "#4EC9B0"
  syntax-view: "#DCDCAA"
  syntax-column: "#9CDCFE"
  syntax-type: "#C586C0"
  syntax-database: "#CE9178"
  syntax-schema: "#569CD6"
  syntax-folder-dim: "#808080"
  chart-1: "#59C2FF"
  chart-2: "#AAD94C"
  chart-3: "#FFB454"
  chart-4: "#F07178"
  chart-5: "#D2A6FF"
  scrollbar-thumb: "rgba(179, 177, 173, 0.15)"
  scrollbar-thumb-hover: "rgba(179, 177, 173, 0.25)"
typography:
  title:
    fontFamily: JetBrains Mono
    fontSize: 20px
    fontWeight: 700
  heading:
    fontFamily: JetBrains Mono
    fontSize: 18px
    fontWeight: 700
  headline-sm:
    fontFamily: JetBrains Mono
    fontSize: 15px
    fontWeight: 700
  body:
    fontFamily: JetBrains Mono
    fontSize: 14px
    fontWeight: 500
  label:
    fontFamily: JetBrains Mono
    fontSize: 14px
    fontWeight: 500
  label-sm:
    fontFamily: JetBrains Mono
    fontSize: 13px
    fontWeight: 500
  caption:
    fontFamily: JetBrains Mono
    fontSize: 13px
    fontWeight: 500
  code:
    fontFamily: JetBrains Mono
    fontSize: 13px
    fontWeight: 500
  caption-xs:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: 500
  key-hint:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: 700
  sidebar-group-label:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: 700
  chart-label:
    fontFamily: JetBrains Mono
    fontSize: 11px
    fontWeight: 500
  chart-tiny:
    fontFamily: JetBrains Mono
    fontSize: 10px
    fontWeight: 500
rounded:
  sm: 0px
  md: 0px
  lg: 0px
  full: 9999px
spacing:
  xs: 4px
  xxs: 6px
  sm: 8px
  md: 12px
  lg: 16px
  xl: 24px
  border-thin: 1px
  border-medium: 2px
  row: 28px
  row-compact: 24px
  control: 28px
  button: 28px
  input: 32px
  toolbar: 32px
  tab: 36px
  header: 40px
  icon-sm: 16px
  icon-md: 20px
  icon-lg: 24px
  tab-stripe: 1px
  tree-indent: 14px
  results-panel: 220px
  inspector-width: 320px
  settings-list-panel: 300px
  settings-form-label: 220px
  form-dropdown: 240px
components:
  button-default:
    backgroundColor: "{colors.raised}"
    textColor: "{colors.foreground}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    height: "{spacing.button}"
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.on-primary}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    height: "{spacing.button}"
  button-primary-hover:
    backgroundColor: "{colors.primary-hover}"
  button-primary-active:
    backgroundColor: "{colors.primary-active}"
  button-ghost:
    backgroundColor: transparent
    textColor: "{colors.foreground}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    height: "{spacing.button}"
  button-ghost-hover:
    backgroundColor: "{colors.hover-tint}"
  button-danger:
    backgroundColor: "{colors.danger}"
    textColor: "{colors.on-danger}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    height: "{spacing.button}"
  input:
    backgroundColor: "{colors.background}"
    textColor: "{colors.foreground}"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    height: "{spacing.input}"
  focus-ring:
    backgroundColor: "{colors.ring}"
    size: "{spacing.border-thin}"
  tab-strip:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.muted-foreground}"
    typography: "{typography.label-sm}"
    height: "{spacing.tab}"
    padding: "{spacing.md}"
  tab-active:
    backgroundColor: "{colors.background}"
    textColor: "{colors.foreground}"
  sidebar-row:
    backgroundColor: transparent
    textColor: "{colors.foreground}"
    typography: "{typography.label-sm}"
    height: "{spacing.row}"
    padding: "{spacing.xs}"
  sidebar-row-hover:
    backgroundColor: "{colors.hover-tint}"
  sidebar-row-selected:
    backgroundColor: "{colors.selection}"
  table-row:
    backgroundColor: transparent
    textColor: "{colors.foreground}"
    typography: "{typography.label-sm}"
    height: "{spacing.row}"
  table-row-even:
    backgroundColor: "{colors.table-even}"
  table-row-hover:
    backgroundColor: "{colors.table-hover}"
  table-row-active:
    backgroundColor: "{colors.table-active}"
  table-header:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.muted-foreground}"
    typography: "{typography.label-sm}"
  panel:
    backgroundColor: "{colors.background}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.lg}"
  card:
    backgroundColor: "{colors.raised}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.lg}"
  popover:
    backgroundColor: "{colors.raised}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.md}"
  modal:
    backgroundColor: "{colors.background}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.lg}"
  modal-scrim:
    backgroundColor: "{colors.overlay}"
  badge-info:
    backgroundColor: "rgba(89, 194, 255, 0.15)"
    textColor: "{colors.info}"
    typography: "{typography.caption-xs}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs}"
  badge-success:
    backgroundColor: "rgba(170, 217, 76, 0.15)"
    textColor: "{colors.success}"
    typography: "{typography.caption-xs}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs}"
  badge-warning:
    backgroundColor: "rgba(255, 180, 84, 0.15)"
    textColor: "{colors.warning}"
    typography: "{typography.caption-xs}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs}"
  badge-danger:
    backgroundColor: "rgba(240, 113, 120, 0.15)"
    textColor: "{colors.danger}"
    typography: "{typography.caption-xs}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs}"
  badge-neutral:
    backgroundColor: "{colors.raised}"
    textColor: "{colors.muted-foreground}"
    typography: "{typography.caption-xs}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs}"
  status-bar:
    backgroundColor: "{colors.background}"
    textColor: "{colors.muted-foreground}"
    typography: "{typography.caption-xs}"
    height: "{spacing.toolbar}"
  status-bar-item-hover:
    backgroundColor: "{colors.raised}"
  key-hint:
    textColor: "{colors.muted-foreground}"
    typography: "{typography.key-hint}"
---

# DBFlux Desktop

## Overview

DBFlux is a database client for people who keep their hands on the keyboard. The interface is dense, flat, and monospaced. Every surface is a step in a short scale of near-black blues, every region is separated by a hairline or by nothing at all, and the single amber accent tells you where the cursor is, what is focused, or what will happen when you press Enter.

The emotional target is a well-configured terminal: calm, fast, and predictable. Nothing bounces, nothing glows, nothing is rounded unless the user opts into the Compact style. Hierarchy comes from tone and weight, not from decoration.

The system has two axes the user picks in Settings:

- **Theme:** Ayu Dark (default), Ayu Mirage, Ayu Light. Each is a hand-picked palette, not a derived one.
- **Style:** Default (square corners, 12 to 20px type scale) or Compact (2 to 3px radii, 11 to 18px type scale).

The tokens above record Dark at Default density. Everything else in this document is theme-invariant unless a table says otherwise.

Tokens live in code, not in a resource file. `theme.rs` holds the three palettes, `tokens.rs` the spacing, height, radius and shadow scales, `density.rs` the style-aware accessors, and `semantic.rs` the per-theme banner, row-state and chart colors. A guardrail test rejects bare pixel values of 4, 6, 8, 12, 16 or 24 and raw color literals anywhere else in the components crate, so the tables here are the only place a number gets to exist.

## Colors

Three palettes share one structure. Each defines the same twelve base slots and derives the rest from them. Hover and active variants of a semantic color are the base darkened by 10% and 20%. Tints for hover rows, table stripes and sidebar hover are the foreground at a low alpha, which is why they work on any palette without a rename.

### Base slots per palette

| Slot | Dark | Mirage | Light |
|---|---|---|---|
| background | `#0A0E14` | `#1F2430` | `#FAFAFA` |
| panel | `#0F1419` | `#232834` | `#F3F3F3` |
| raised (secondary, popover) | `#151E2B` | `#242936` | `#F7F8FA` |
| tiles | `#111823` | `#202734` | `#E8E8E8` |
| foreground | `#B3B1AD` | `#CBCCC6` | `#5C6166` |
| muted (decoration, switch, slider) | `#5C6773` | `#707A8C` | `#ABB0B6` |
| muted foreground (secondary text, icons) | `#828D9D` | `#8F98AA` | `#676E75` |
| border | `#1F2430` | `#3A4052` | `#D9DEE8` |
| selection | `#273747` | `#33415E` | `#D3E8F8` |
| primary (accent, caret, drag border) | `#FFB454` | `#FFCC66` | `#FF9940` |
| danger | `#F07178` | `#F28779` | `#E65050` |
| success | `#AAD94C` | `#AAD94C` | `#86B300` |
| warning | `#FFB454` | `#FFCC66` | `#F2AE49` |
| info (links, active row) | `#59C2FF` | `#73D0FF` | `#399EE6` |

### Alpha-derived slots

| Slot | Dark | Mirage | Light |
|---|---|---|---|
| hover tint (accent, sidebar accent, table hover) | fg 5% | fg 6% | fg 6% |
| table even stripe | fg 2% | fg 2% | fg 3% |
| table active | info 15% | info 12% | info 12% |
| table active border | info 50% | info 40% | info 40% |
| input edge | fg 14% | fg 9% | fg 6% |
| focus ring | primary 75% | primary 72% | primary 50% |
| modal overlay | black 55% | black 45% | black 30% |
| drop target | primary 10% | primary 10% | primary 10% |

Roles worth naming:

- **Primary** is amber. It is the caret, the focus ring, the primary button, the active tab stripe, the drag border, the busy status dot. Text on primary is the page background.
- **Info** is blue and is a second, quieter accent. Links, the active data row, and the first chart series use it. Do not use it for actions.
- **Danger text is always white**, even on Dark, because the palette's error red does not carry dark text at a readable contrast.
- **Ghost border** is `#524436` at 15% alpha in every theme. It is a felt-not-seen separator between major regions, for example the top edge of the status bar and the dividers between its sections. Use it where a solid border would be too loud.
- **Table row borders are transparent** in every theme. The even-row stripe does the separating.
- **Row state tints** are theme-invariant overlays for the data grid: insert green 15%, dirty amber 20%, delete red 10%, error red 15%, saving amber 10%. They layer on top of the stripe.
- **Syntax colors** for the object tree are theme-invariant and borrowed from the editor convention: tables teal, views yellow, columns light blue, types purple, databases orange, schemas blue, dimmed folders gray.
- **Chart series** run blue, green, amber, red, purple, in that order, with a Light variant that keeps the hue and lowers the brightness.
- **Banner colors** (info, success, warning, error) are hand-picked per theme at roughly 12 to 14% background alpha with a full-strength foreground. They are not computed at runtime, so legibility is guaranteed on all three palettes.

## Typography

One typeface. JetBrains Mono is the headline, body, code and shortcut face, bundled in eight weights and styles from `assets/fonts` and registered at startup. The fallback is the platform monospace. There is no sans-serif anywhere in the client.

The size scale has six steps and shifts down one notch under the Compact style:

| Step | Default | Compact | Role |
|---|---|---|---|
| xs | 12px | 11px | badges, captions, key hints, sidebar group labels |
| sm | 13px | 12px | labels, secondary metadata, code, table cells |
| base | 14px | 13px | body, inputs, field labels |
| lg | 15px | 14px | emphasized labels, headline level 1 |
| xl | 18px | 16px | headings, headline level 2 |
| title | 20px | 18px | window-level headings, headline level 3 |

Weights are Medium for everything that reads and Bold for everything that labels a region: titles, headings, sidebar group labels, key hints. Semi-bold appears only on the third headline level. Line height is left to GPUI's default.

Secondary text uses the muted foreground slot, a hand-picked readable value per palette. It has three levels: full alpha for captions, at 70% for secondary dims, at 50% for tertiary dims. Never invent a fourth.

## Layout

The window is a fixed frame of chrome around one flexible document area: sidebar on the left, tab strip on top, status bar at the bottom, optional inspector sliding in from the right.

- **Spacing scale:** 4, 8, 12, 16, 24px. A locked 6px half-step exists for form-row label padding and chart pills and is the only exception to the doubling rhythm.
- **Heights:** rows 28px (24px compact), inline controls and buttons 28px, inputs and toolbars 32px, tabs 36px, panel headers 40px. Anything packed into a toolbar uses the 28px control height so heterogeneous controls align.
- **Icons:** 16, 20, 24px.
- **Tree indent:** 14px per depth level in the sidebar.
- **Fixed regions:** the SQL results panel is 220px tall in split layout, the row inspector is 320px wide, the settings list panel is 300px wide with a 220px form-label column.
- **Borders:** 1px thin, 2px medium. Border tokens are widths only and are never reused as margins or radii.

## Elevation & Depth

Flat by default. Regions are separated by the input-edge tint (surfaces, separators, controls, modal separators) or by the solid border (popovers). Major regions use the ghost border. Rows are separated by nothing but the alternating stripe.

Surfaces map to a role, and the role decides the background slot, the edge color and the radius:

| Role | Background | Edge | Radius |
|---|---|---|---|
| Panel | background | input edge | lg |
| Card | raised | input edge | lg |
| Raised (popover, dropdown, tooltip) | raised | border | md |
| Modal container | background | border | lg |
| Scrim | overlay | none | lg |

The modal container deliberately uses the page background, not the popover slot, so child controls keep their contrast against it.

Shadows exist for floating chrome only:

- **md** `0 4px 8px rgba(0,0,0,.24)` on dropdowns, popovers and tooltips.
- **lg** `0 8px 24px rgba(0,0,0,.32)` on modals.
- **inspector left** `-6px 0 16px rgba(0,0,0,.28)` on the left edge of the slide-in inspector.

The modal scrim is black at 55%, 45% and 30% for Dark, Mirage and Light.

## Shapes

Square. Under the Default style every radius is 0px. Under the Compact style small and medium radii become 2px and the large radius becomes 3px. The full radius (9999px) is reserved for status dots, pills and avatars, and it does not change with style.

Buttons are the one place the rule is enforced by the component rather than by the token: the button primitive hardcodes a 0px radius regardless of style.

## Components

### Buttons

Five variants: Default, Primary, Ghost, Danger, Dropdown. Two sizes: Default and Small. 28px tall, square corners in every style. Primary is amber on page-background text. Danger is red on white text. Ghost is transparent and takes the hover tint. A focused button draws a 1px focus frame in the ring color as an absolute overlay, transparent when unfocused.

### Inputs

32px tall, page background, input-edge border, body text. Focus uses the same 1px ring frame as buttons. The caret is amber.

### Tabs

The tab strip is 36px tall on the panel background with 12px horizontal padding and 4px gaps. The active tab moves to the page background and carries a 1px amber stripe at its bottom edge. The strip's bottom edge is a separator, not a border.

### Sidebar tree

28px rows, 14px indent per level, 16px icons, 8px gap between icon and label, 4px padding. Hover takes the foreground tint, selection takes the selection color. Drag targets show the primary color at 10%, and drop indicators are 2px lines above or below the row.

### Data grid

28px rows, no row borders. Even rows carry the 2% stripe, hovered rows the 5% tint, the active row the info color at 15% with a 50% info border. Row-state tints for insert, dirty, delete, error and saving layer on top. Headers are muted small labels on the panel background.

### Badges

Info, Success, Warning, Danger, Neutral. Background is the semantic color at 15% alpha, text is the color at full strength. Neutral uses the raised surface with muted text. Pill mode pads 4px horizontally and 2px vertically at the xs size; dot mode is an 8px filled circle.

### Modals

A builder sets width and height as fixed, max or fraction, plus top offset or vertical centering. The frame is the modal-container surface over the scrim, with a separator under the header. Modals are top-anchored by default.

### Status bar

32px tall on the page background with a ghost-border top edge. Sections are separated by 1px by 16px ghost-border verticals. Items take the raised surface on hover. Status dots follow a fixed palette: idle muted, busy amber, success green, warning amber, danger red, neutral muted at 50%.

### Command palette

A raised overlay listing commands in mono labels with a mono caption for the shortcut. The selected row inverts to the selection color. Navigation is Up, Down, Ctrl-K, Ctrl-J, Escape, Enter.

### Key hints

Bold 12px mono in muted text. Wherever a shortcut is shown, this is the treatment.

### Charts

Chart chrome (panel background, border, label, value, muted, hover, pill, checkbox, stats accent) is hand-picked per theme in `semantic.rs`. Geometry is fixed: 10px tiny text, 11px labels, 1px hairlines, 2px accent stripe, 10px swatches, 11px legend rows.

## Do's and Don'ts

- Do read every color through the theme slots and every dimension through the token tables. The guardrail test will reject a raw literal.
- Do use the density accessors for font size and radius at render sites, so the Compact style is honored.
- Do use the ghost border between major regions and the input-edge tint between controls. Reserve the solid border for popovers.
- Do put white text on the danger color in every theme.
- Do use amber for the one focused or primary thing on screen and blue for links and the active data row. Never swap them.
- Don't add row dividers to a grid or a list. The stripe is the divider.
- Don't derive a semantic color at runtime from an opacity calculation. Pick it per theme in `semantic.rs`.
- Don't introduce a second typeface. Headings, body, code and shortcuts are all JetBrains Mono.
- Don't round a corner under the Default style, and don't exceed 3px under Compact.
- Don't branch on the theme name in a component. Read the slot, and the palette takes care of itself.
