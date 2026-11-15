# DBFlux design system

Captured from the website's design sources by the design sync. Values are copied from the sources named in each row; nothing here is invented. The site is set in the desktop client's own system, Bolt Byzantium, so the palette and type match the app.

Sources read: `web/src/styles/tokens.css` (all tokens), `web/src/styles/fonts.css` (typefaces), `web/src/styles/global.css` (component-level custom properties and base rules), `web/DESIGN.md` (component rules). Dark values are the `:root` block; light values are the `html[data-theme='light']` block.

## Colors: surfaces

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--bg` | `#09090b` | `#f6f4f7` | Page and window ground | `web/src/styles/tokens.css` |
| `--bg-sunken` | `#050507` | `#e6e1e9` | Frames the console mockup | `web/src/styles/tokens.css` |
| `--band` | `#0d0c10` | `#efebf1` | Full-width governance band | `web/src/styles/tokens.css` |
| `--panel` | `#100f13` | `#ffffff` | Cards and panes | `web/src/styles/tokens.css` |
| `--raised` | `#1a181e` | `#eeeaf0` | Controls and chips | `web/src/styles/tokens.css` |
| `--raised-hover` | `#26232b` | `#e4dfe7` | Hovered raised control | `web/src/styles/tokens.css` |
| `--selection` | `rgba(212, 140, 200, 0.28)` | `rgba(112, 41, 99, 0.18)` | Text selection fill | `web/src/styles/tokens.css` |
| `--scrim` | `rgba(0, 0, 0, 0.6)` | `rgba(20, 17, 24, 0.4)` | Backdrop behind modals | `web/src/styles/tokens.css` |
| `--console-edge` | `rgba(255, 255, 255, 0.05)` | `rgba(20, 17, 24, 0.06)` | Hairline on the console mockup | `web/src/styles/tokens.css` |
| `--wordmark` | `#17151b` | `#eeeaf0` | Large wordmark ground | `web/src/styles/tokens.css` |
| `--scrollbar-thumb` | `rgba(198, 195, 204, 0.15)` | `rgba(59, 55, 64, 0.22)` | Scrollbar thumb | `web/src/styles/tokens.css` |
| `--scrollbar-thumb-hover` | `rgba(198, 195, 204, 0.25)` | `rgba(59, 55, 64, 0.36)` | Scrollbar thumb on hover | `web/src/styles/tokens.css` |
| `--scrollbar-size` | `10px` | `10px` | Scrollbar width | `web/src/styles/tokens.css` |

## Colors: text and borders

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--text` | `#c6c3cc` | `#3b3740` | Body text | `web/src/styles/tokens.css` |
| `--text-strong` | `#f7f4f7` | `#141118` | Titles and data | `web/src/styles/tokens.css` |
| `--text-muted` | `#8e8996` | `#6b6572` | Meta and labels | `web/src/styles/tokens.css` |
| `--text-faint` | `#37333d` | `#cbc4d1` | Decoration only: line numbers, separators, inactive glyphs | `web/src/styles/tokens.css` |
| `--border` | `#232128` | `#e3dee6` | Separators | `web/src/styles/tokens.css` |
| `--border-strong` | `#37333d` | `#cbc4d1` | Control edges and headers | `web/src/styles/tokens.css` |
| `--row-line` | `#18161b` | `#eeeaf0` | Row separators | `web/src/styles/tokens.css` |
| `--border-interactive` | `#37333d` | `#cbc4d1` | Controls the reader has to find | `web/src/styles/tokens.css` |

## Colors: accent

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--accent` | `#d48cc8` | `#702963` | The accent as text or outline on this surface | `web/src/styles/tokens.css` |
| `--accent-fill` | `#702963` | `#702963` | The accent as a fill (primary buttons, selected state); always paired with --accent-on | `web/src/styles/tokens.css` |
| `--accent-fill-deep` | `#4a1b41` | `#4a1b41` | Pressed fill | `web/src/styles/tokens.css` |
| `--accent-on` | `#ffffff` | `#ffffff` | Text on the accent fill | `web/src/styles/tokens.css` |
| `--accent-hover` | `#d48cc8` | `#702963` | Accent on hover | `web/src/styles/tokens.css` |
| `--accent-active` | `#702963` | `#702963` | Accent when active | `web/src/styles/tokens.css` |
| `--bolt` | `#d48cc8` | `#e7b8dd` | The bolt mark | `web/src/styles/tokens.css` |

## Colors: semantic

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--link` | `#6ea8ff` | `#1f5fd1` | Links and the MCP label | `web/src/styles/tokens.css` |
| `--link-hover` | `#9cc3ff` | `#174aa6` | Link on hover | `web/src/styles/tokens.css` |
| `--success` | `#7be0a0` | `#1c7f45` | Allowed and included | `web/src/styles/tokens.css` |
| `--warning` | `#d48cc8` | `#702963` | Caution (the site maps it to the accent) | `web/src/styles/tokens.css` |
| `--danger` | `#ff6b5e` | `#c7362b` | Denied and failed | `web/src/styles/tokens.css` |
| `--danger-on` | `#ffffff` | `#ffffff` | Text on a danger fill | `web/src/styles/tokens.css` |
| `--violet` | `#b79cff` | `#6b4fd8` | Numbers in code | `web/src/styles/tokens.css` |
| `--series-1` | `#6ea8ff` | `#1f5fd1` | Chart series 1 | `web/src/styles/tokens.css` |
| `--series-2` | `#7be0a0` | `#1c7f45` | Chart series 2 | `web/src/styles/tokens.css` |
| `--series-3` | `#d48cc8` | `#702963` | Chart series 3 | `web/src/styles/tokens.css` |
| `--series-4` | `#ff6b5e` | `#c7362b` | Chart series 4 | `web/src/styles/tokens.css` |
| `--series-5` | `#b79cff` | `#6b4fd8` | Chart series 5 | `web/src/styles/tokens.css` |
| `--series-6` | `#c6c3cc` | `#3b3740` | Chart series 6 | `web/src/styles/tokens.css` |

## Typography

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--font-display` | `'Archivo', system-ui, -apple-system, sans-serif` | `'Archivo', system-ui, -apple-system, sans-serif` | Display headings (Archivo at full width) | `web/src/styles/tokens.css` |
| `--font-sans` | `'Archivo', system-ui, -apple-system, sans-serif` | `'Archivo', system-ui, -apple-system, sans-serif` | Text face | `web/src/styles/tokens.css` |
| `--font-mono` | `'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace` | `'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace` | Code, data, keycaps, metadata | `web/src/styles/tokens.css` |
| `--stretch-display` | `125%` | `125%` | font-stretch for display type | `web/src/styles/tokens.css` |
| `--fs-display` | `clamp(3.125rem, 1.5rem + 4.45vw, 5.5rem)` | `clamp(3.125rem, 1.5rem + 4.45vw, 5.5rem)` | Hero display size | `web/src/styles/tokens.css` |
| `--fs-h1` | `clamp(2.125rem, 1.4rem + 2.2vw, 3.5rem)` | `clamp(2.125rem, 1.4rem + 2.2vw, 3.5rem)` | Page title | `web/src/styles/tokens.css` |
| `--fs-h2` | `1.625rem` | `1.625rem` | Section title | `web/src/styles/tokens.css` |
| `--fs-h3` | `1.125rem` | `1.125rem` | Subsection title | `web/src/styles/tokens.css` |
| `--fs-body` | `0.9375rem` | `0.9375rem` | Body copy | `web/src/styles/tokens.css` |
| `--fs-small` | `0.8125rem` | `0.8125rem` | Small text | `web/src/styles/tokens.css` |
| `--fs-label` | `0.6875rem` | `0.6875rem` | Section label | `web/src/styles/tokens.css` |
| `--fs-code` | `0.8125rem` | `0.8125rem` | Code | `web/src/styles/tokens.css` |
| `--lh-body` | `1.65` | `1.65` | Running paragraphs | `web/src/styles/tokens.css` |
| `--lh-tight` | `0.95` | `0.95` | Display headings | `web/src/styles/tokens.css` |
| `--lh-code` | `1.8` | `1.8` | Code blocks | `web/src/styles/tokens.css` |
| `--ls-label` | `0.16em` | `0.16em` | Section label tracking | `web/src/styles/tokens.css` |

## Spacing

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--s-1` | `4px` | `4px` | 4px step | `web/src/styles/tokens.css` |
| `--s-2` | `6px` | `6px` | 6px step | `web/src/styles/tokens.css` |
| `--s-3` | `8px` | `8px` | 8px step | `web/src/styles/tokens.css` |
| `--s-4` | `12px` | `12px` | 12px step | `web/src/styles/tokens.css` |
| `--s-5` | `16px` | `16px` | 16px step | `web/src/styles/tokens.css` |
| `--s-6` | `24px` | `24px` | 24px step | `web/src/styles/tokens.css` |
| `--s-7` | `40px` | `40px` | 40px step | `web/src/styles/tokens.css` |
| `--s-8` | `64px` | `64px` | 64px step | `web/src/styles/tokens.css` |
| `--s-9` | `96px` | `96px` | 96px step | `web/src/styles/tokens.css` |

## Radius and borders

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--radius` | `0` | `0` | No rounding: controls are chamfered, not rounded | `web/src/styles/tokens.css` |
| `--radius-full` | `9999px` | `9999px` | Pills where a full radius is needed | `web/src/styles/tokens.css` |
| `--bw` | `1px` | `1px` | Default border width | `web/src/styles/tokens.css` |
| `--bw-emphasis` | `2px` | `2px` | Emphasis border width | `web/src/styles/tokens.css` |

## Shadows

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--shadow-md` | `0 4px 8px rgba(0, 0, 0, 0.3)` | `0 4px 8px rgba(40, 20, 40, 0.1)` | Small raised surface | `web/src/styles/tokens.css` |
| `--shadow-lg` | `0 18px 40px rgba(0, 0, 0, 0.5)` | `0 18px 40px rgba(40, 20, 40, 0.14)` | Popover and menu | `web/src/styles/tokens.css` |
| `--shadow-float` | `0 40px 80px rgba(0, 0, 0, 0.55)` | `0 40px 80px rgba(40, 20, 40, 0.12)` | Floating window | `web/src/styles/tokens.css` |

## Motion

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--t-instant` | `140ms` | `140ms` |  | `web/src/styles/tokens.css` |
| `--t-quick` | `200ms` | `200ms` |  | `web/src/styles/tokens.css` |
| `--t-entrance` | `700ms` | `700ms` |  | `web/src/styles/tokens.css` |
| `--ease-out` | `cubic-bezier(0.2, 0.8, 0.2, 1)` | `cubic-bezier(0.2, 0.8, 0.2, 1)` |  | `web/src/styles/tokens.css` |
| `--ease-std` | `cubic-bezier(0.4, 0, 0.2, 1)` | `cubic-bezier(0.4, 0, 0.2, 1)` |  | `web/src/styles/tokens.css` |
| `--reveal-distance` | `14px` | `14px` |  | `web/src/styles/tokens.css` |
| `--reveal-duration` | `520ms` | `520ms` |  | `web/src/styles/tokens.css` |
| `--reveal-stagger` | `60ms` | `60ms` |  | `web/src/styles/tokens.css` |
| `--reveal-stagger-max` | `360ms` | `360ms` |  | `web/src/styles/tokens.css` |
| `--glow-success` | `color-mix(in srgb, var(--success) 55%, transparent)` | `color-mix(in srgb, var(--success) 55%, transparent)` |  | `web/src/styles/tokens.css` |

## Layout, controls and focus

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--content-max` | `1440px` | `1440px` |  | `web/src/styles/tokens.css` |
| `--docs-max` | `1440px` | `1440px` |  | `web/src/styles/tokens.css` |
| `--prose-max` | `72ch` | `72ch` |  | `web/src/styles/tokens.css` |
| `--rail-w` | `320px` | `320px` |  | `web/src/styles/tokens.css` |
| `--toc-w` | `300px` | `300px` |  | `web/src/styles/tokens.css` |
| `--gutter` | `64px` | `64px` |  | `web/src/styles/tokens.css` |
| `--nav-h` | `72px` | `72px` |  | `web/src/styles/tokens.css` |
| `--control-h` | `40px` | `40px` | Default control height on the site | `web/src/styles/tokens.css` |
| `--tap-min` | `44px` | `44px` | Minimum touch target | `web/src/styles/tokens.css` |
| `--focus-ring` | `2px solid var(--accent)` | `2px solid var(--accent)` | Keyboard focus ring | `web/src/styles/tokens.css` |
| `--focus-offset` | `2px` | `2px` | Focus ring offset | `web/src/styles/tokens.css` |

## Stacking

| Token | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `--z-nav` | `100` | `100` |  | `web/src/styles/tokens.css` |
| `--z-rail` | `90` | `90` |  | `web/src/styles/tokens.css` |
| `--z-dropdown` | `200` | `200` |  | `web/src/styles/tokens.css` |
| `--z-scrim` | `300` | `300` |  | `web/src/styles/tokens.css` |
| `--z-modal` | `310` | `310` |  | `web/src/styles/tokens.css` |
| `--z-tooltip` | `400` | `400` |  | `web/src/styles/tokens.css` |

## Component-level properties

| Token | Value | Usage | Source |
|---|---|---|---|
| `--verdict-icon` | `var(--success)`, `var(--link)`, `var(--accent)`, `var(--text-muted)`, `var(--text-faint)` | Verdict icon colour per verdict kind | `web/src/styles/global.css` |
| `--verdict-text` | `var(--text-strong)`, `var(--text)`, `var(--accent)`, `var(--text-muted)` | Verdict text colour per verdict kind | `web/src/styles/global.css` |

## Typefaces

| Token | Value | Usage | Source |
|---|---|---|---|
| `Archivo` | `100-900, stretch 62%-125%` | One variable file: running text at normal width, display type ("Archivo Expanded") at `font-stretch: 125%` | `web/src/styles/fonts.css` |
| `JetBrains Mono` | `100-800` | Code, data, keycaps, version numbers, metadata | `web/src/styles/fonts.css` |

## Component rules

From `web/DESIGN.md`.

- Shape: controls and cards are chamfered, not rounded. The `.c4` to `.c18` classes cut the top-left and bottom-right corners by that many pixels; a 1px border on a chamfered box is an inset box shadow. Data surfaces (table rows, grid lines) keep hard edges. Focus outlines on chamfered controls are drawn inside the shape.
- Byzantium (`--accent-fill`) is the one fill: primary buttons, the selected chip, tab and rail entry. It always carries white type (`--accent-on`). Text that has to read as the accent uses `--accent` (tint on dark, byzantium on light).
- Surfaces step from the page (`--bg`) to `--panel` for cards and `--raised` for controls.
- Semantic colours carry meaning only: `--success` allowed and included, `--danger` denied and failed, `--link` links, `--violet` numbers in code. Verdicts use a coloured edge, never a full coloured fill.
- Buttons: primary is byzantium with white expanded capitals; secondary is `--raised` with strong text.
- Cards: `--panel` with an inset border, chamfer 14 or 18.
- Tabs and pickers: the active entry is `--raised` with a 3px byzantium underline.
- Keycaps: `--raised` with a 6px inset bottom edge; the key that matters is byzantium.
- Section label: Archivo Expanded 800, capitals, `--ls-label` tracking.
- Text: Archivo 400 to 700; chrome text keeps the browser's normal line height; running paragraphs set 1.6 to 1.7.
- Focus: `--focus-ring` (2px solid `--accent`) at `--focus-offset`, keyboard focus only.
- Motion: everything stops under `prefers-reduced-motion`.

## App artboards

Artboards of the desktop app (dialogs, wizards, documents, windows) follow the Bolt Byzantium app system in `design/redesign-design-system/`, not the website rules above: `04-ds-app.html` for the app components and `06-ds-states.html` for the interaction states, with the palette and type in `02-ds-foundations.html`. The two systems share one palette. Values below are found in those files.

| Role | Dark value | Light value | Usage | Source |
|---|---|---|---|---|
| `byzantine` | `#702963` | `#702963` | Fills: primary buttons, active states; accent text on light | `design/redesign-design-system/02-ds-foundations.html` |
| `tint` | `#D48CC8` | `-` | Accent text on dark | `design/redesign-design-system/02-ds-foundations.html` |
| `deep` | `#4A1B41` | `#4A1B41` | Pressed | `design/redesign-design-system/02-ds-foundations.html` |
| `ink` | `#FFFFFF` | `#FFFFFF` | Text on byzantine | `design/redesign-design-system/02-ds-foundations.html` |
| `bg` | `#09090B` | `#F6F4F7` | Window, page | `design/redesign-design-system/02-ds-foundations.html` |
| `panel` | `#100F13` | `#FFFFFF` | Panes, cards | `design/redesign-design-system/02-ds-foundations.html` |
| `raised` | `#1A181E` | `#EEEAF0` | Controls, chips | `design/redesign-design-system/02-ds-foundations.html` |
| `line` | `#232128` | `#E3DEE6` | Separators | `design/redesign-design-system/02-ds-foundations.html` |
| `line-2` | `#37333D` | `#CBC4D1` | Control edges, headers | `design/redesign-design-system/02-ds-foundations.html` |
| `strong` | `#F7F4F7` | `#141118` | Titles, data | `design/redesign-design-system/02-ds-foundations.html` |
| `text` | `#C6C3CC` | `#3B3740` | Body | `design/redesign-design-system/02-ds-foundations.html` |
| `muted` | `#8E8996` | `#6B6572` | Meta, labels | `design/redesign-design-system/02-ds-foundations.html` |
| `success` | `#7BE0A0` | `#1C7F45` | Included, connected | `design/redesign-design-system/02-ds-foundations.html` |
| `info` | `#6EA8FF` | `#1F5FD1` | Links, FK | `design/redesign-design-system/02-ds-foundations.html` |
| `warning` | `#FFC23D` | `#B7791F` | PK, caution | `design/redesign-design-system/02-ds-foundations.html` |
| `danger` | `#FF6B5E` | `#C7362B` | Delete, PROD | `design/redesign-design-system/02-ds-foundations.html` |
| `null` | `#B79CFF` | `#6B4FD8` | NULL, numbers | `design/redesign-design-system/02-ds-foundations.html` |

- App typography (`02-ds-foundations.html`): heading Archivo 700, 17 to 26 px; body and interface text Archivo 400 to 600, 13 px; section label Archivo Expanded 800, 10 to 11 px, +0.14em; display type is never used in the app.
- Control sizes (`06-ds-states.html`): 24 inline, 30 default; segmented control 30 tall with 26 px segments in a 2 px inset. Primary `#702963`, hover `#7F3171`, pressed `#4A1B41`; danger is red at 14%, 22%, 30%; disabled is 45% opacity with no hover.
- The cut lives on controls only; everything that holds data stays rectangular (`04-ds-app.html`).
- Focus ring (`06-ds-states.html`): keyboard focus only. Single controls get a tint 1.5 px ring following the cut. Composite controls (segmented, chips, tabs, lists) never ring the container: the focused item gets a tint underline or bar, or the row wash. Selected is fill only, no border.
- Hover and press change fill only, never size or position; 150 ms, `cubic-bezier(.2,.8,.2,1)`; reduced motion switches instantly.
- Dialogs, wizards and keycaps follow the existing Islands boards in `design/redesign-islands-overlays-and-flows-dark/` and `-light/` (stepper, footer, form rows).
