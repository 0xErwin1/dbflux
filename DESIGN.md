---
version: alpha
name: DBFlux Desktop
description: >-
  Bolt Byzantium, the design system of the DBFlux desktop client, a
  keyboard-first database tool built with GPUI. One byzantine accent on a
  near-black (or near-white) ground, three typefaces, and 45 degree cut corners
  on every control and surface. Two palettes (Dark, Light, plus Follow system)
  and two densities (Default, Compact). Tokens below record the Dark palette at
  Default density, which is what ships as the default. The Light palette is
  tabulated in the Colors section. Source of truth is
  crates/dbflux_components/src/{theme.rs,tokens.rs,density.rs,semantic.rs}.
colors:
  background: "#09090B"
  panel: "#100F13"
  raised: "#1A181E"
  line: "#232128"
  line-2: "#37333D"
  row-divider: "#18161B"
  strong: "#F7F4F7"
  foreground: "#C6C3CC"
  muted-foreground: "#8E8996"
  byzantine: "#702963"
  byzantine-hover: "#7F3171"
  deep: "#4A1B41"
  tint: "#D48CC8"
  on-primary: "#FFFFFF"
  primary: "{colors.byzantine}"
  primary-hover: "{colors.byzantine-hover}"
  primary-active: "{colors.deep}"
  secondary: "{colors.raised}"
  secondary-hover: "{colors.line}"
  secondary-active: "{colors.line-2}"
  border: "{colors.line}"
  input: "{colors.line-2}"
  ring: "{colors.tint}"
  success: "#7BE0A0"
  info: "#6EA8FF"
  warning: "#FFC23D"
  danger: "#FF6B5E"
  null: "#B79CFF"
  cyan: "#6FD3D8"
  on-semantic: "#09090B"
  hover-wash: "rgba(255, 255, 255, 0.04)"
  alternating-row: "rgba(255, 255, 255, 0.012)"
  selected-row: "rgba(212, 140, 200, 0.07)"
  selected-item: "rgba(212, 140, 200, 0.12)"
  text-selection: "rgba(212, 140, 200, 0.25)"
  drop-target: "rgba(212, 140, 200, 0.10)"
  overlay: "rgba(5, 5, 7, 0.62)"
  danger-soft: "rgba(255, 107, 94, 0.14)"
  danger-soft-hover: "rgba(255, 107, 94, 0.22)"
  danger-soft-pressed: "rgba(255, 107, 94, 0.30)"
  banner-info: "rgba(110, 168, 255, 0.12)"
  banner-success: "rgba(123, 224, 160, 0.12)"
  banner-warning: "rgba(255, 194, 61, 0.12)"
  banner-error: "rgba(255, 107, 94, 0.12)"
  row-insert: "rgba(123, 224, 160, 0.15)"
  row-delete: "rgba(255, 107, 94, 0.10)"
  row-error: "rgba(255, 107, 94, 0.15)"
  row-saving: "rgba(255, 194, 61, 0.10)"
  syntax-keyword: "#D48CC8"
  syntax-string: "#7BE0A0"
  syntax-number: "#B79CFF"
  syntax-comment: "#8E8996"
  syntax-type: "#6EA8FF"
  syntax-function: "#FFC23D"
  syntax-operator: "#C6C3CC"
  syntax-plain: "#F7F4F7"
  chart-1: "#D48CC8"
  chart-2: "#6EA8FF"
  chart-3: "#7BE0A0"
  chart-4: "#FFC23D"
  chart-5: "#FF6B5E"
  scrollbar-thumb: "rgba(142, 137, 150, 0.30)"
  scrollbar-thumb-hover: "rgba(142, 137, 150, 0.50)"
typography:
  title:
    fontFamily: Archivo
    fontSize: 20px
    fontWeight: 700
  heading:
    fontFamily: Archivo
    fontSize: 18px
    fontWeight: 700
  body:
    fontFamily: Archivo
    fontSize: 13px
    fontWeight: 500
  body-sm:
    fontFamily: Archivo
    fontSize: 12px
    fontWeight: 500
  label:
    fontFamily: Archivo Expanded
    fontSize: 11px
    fontWeight: 800
    letterSpacing: 0.14em
    textTransform: uppercase
  caption:
    fontFamily: Archivo
    fontSize: 12px
    fontWeight: 500
  code:
    fontFamily: JetBrains Mono
    fontSize: 13px
    fontWeight: 500
  key-hint:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: 500
  button:
    fontFamily: Archivo
    fontSize: 12.5px
    fontWeight: 600
  kbd:
    fontFamily: JetBrains Mono
    fontSize: 10.5px
    fontWeight: 500
rounded:
  sm: 0px
  md: 0px
  lg: 0px
  full: 9999px
cuts:
  keycap: 4px
  control: 6px
  input: 8px
  large-control: 10px
  overlay: 12px
  card: 14px
  modal: 18px
spacing:
  xs: 4px
  xxs: 6px
  sm: 8px
  md: 12px
  lg: 16px
  xl: 24px
  border-thin: 1px
  border-medium: 2px
  focus-ring: 1.5px
  control: 30px
  control-inline: 24px
  control-large: 44px
  icon-only-width: 32px
  input: 30px
  segment: 26px
  segment-track-padding: 2px
  filter-field: 34px
  checkbox: 16px
  menu-row: 30px
  tree-row: 26px
  tree-indent: 14px
  grid-row: 31px
  grid-header: 40px
  document-tab: 36px
  document-tab-bar: 42px
  panel-header: 40px
  title-bar: 43px
  activity-rail: 52px
  icon-sm: 16px
  icon-md: 20px
  icon-lg: 24px
motion:
  fast: 150ms
  easing: cubic-bezier(.2, .8, .2, 1)
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.on-primary}"
    typography: "{typography.button}"
    cut: "{cuts.control}"
    height: "{spacing.control}"
  button-primary-hover:
    backgroundColor: "{colors.primary-hover}"
  button-primary-active:
    backgroundColor: "{colors.primary-active}"
  button-secondary:
    backgroundColor: "{colors.secondary}"
    textColor: "{colors.foreground}"
    typography: "{typography.button}"
    cut: "{cuts.control}"
    height: "{spacing.control}"
  button-secondary-hover:
    backgroundColor: "{colors.secondary-hover}"
  button-secondary-active:
    backgroundColor: "{colors.secondary-active}"
  button-ghost:
    backgroundColor: transparent
    textColor: "{colors.foreground}"
    typography: "{typography.button}"
    cut: "{cuts.control}"
    height: "{spacing.control}"
  button-ghost-hover:
    backgroundColor: "{colors.raised}"
  button-ghost-active:
    backgroundColor: "{colors.line}"
  button-danger:
    backgroundColor: "{colors.danger-soft}"
    textColor: "{colors.danger}"
    typography: "{typography.button}"
    cut: "{cuts.control}"
    height: "{spacing.control}"
  button-danger-hover:
    backgroundColor: "{colors.danger-soft-hover}"
  button-danger-active:
    backgroundColor: "{colors.danger-soft-pressed}"
  input:
    backgroundColor: "{colors.background}"
    borderColor: "{colors.line}"
    textColor: "{colors.foreground}"
    cut: "{cuts.control}"
    height: "{spacing.input}"
  focus-ring:
    color: "{colors.ring}"
    size: "{spacing.focus-ring}"
  document-tab:
    backgroundColor: "{colors.background}"
    textColor: "{colors.muted-foreground}"
    height: "{spacing.document-tab}"
  document-tab-active:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.strong}"
    topEdge: "{colors.byzantine}"
    cut: "{cuts.input}"
  tree-row:
    height: "{spacing.tree-row}"
    textColor: "{colors.foreground}"
  tree-row-selected:
    backgroundColor: "{colors.selected-item}"
    leftBar: "{colors.tint}"
  grid-row:
    height: "{spacing.grid-row}"
    typography: "{typography.code}"
    borderColor: "{colors.row-divider}"
  grid-row-even:
    backgroundColor: "{colors.alternating-row}"
  grid-row-hover:
    backgroundColor: "{colors.hover-wash}"
  grid-row-selected:
    backgroundColor: "{colors.selected-row}"
  menu:
    backgroundColor: "{colors.raised}"
    borderColor: "{colors.line-2}"
    cut: "{cuts.overlay}"
  card:
    backgroundColor: "{colors.panel}"
    borderColor: "{colors.line-2}"
    cut: "{cuts.card}"
  modal:
    backgroundColor: "{colors.panel}"
    borderColor: "{colors.line-2}"
    cut: "{cuts.modal}"
  modal-scrim:
    backgroundColor: "{colors.overlay}"
  badge:
    height: 20px
    typography: "{typography.caption}"
    cut: "{cuts.keycap}"
  kbd:
    typography: "{typography.kbd}"
    cut: "{cuts.keycap}"
---

# DBFlux Desktop: Bolt Byzantium

## Overview

Bolt Byzantium is a dark, dense, keyboard-first interface with one accent: byzantine purple (`#702963`) as a fill and its light tint (`#D48CC8`) for accent text, icons and focus. Controls and surfaces have 45° cut corners; data (rows, cells, code, charts) keeps square edges.

The user picks two things in Settings:

- **Theme:** Dark (default), Light, or Follow system (`ThemeSetting::System`, resolved from the window's OS appearance).
- **Density:** Default or Compact. Compact lowers every text role by 1 px.

Where the tokens live:

| File | Holds |
|---|---|
| `crates/dbflux_components/src/theme.rs` | `Palette::dark()` / `Palette::light()` and their mapping onto gpui-component `Theme` fields |
| `crates/dbflux_components/src/tokens.rs` | spacing, heights, cuts (`ChamferCut`), per-component metrics (`ButtonMetrics`, `Fields`, `MenuMetrics`, `TreeMetrics`, `GridMetrics`, ...), `ChromeColors`, `SyntaxColors`, `Anim` |
| `crates/dbflux_components/src/density.rs` | density-aware font and radius accessors (`font_base(cx)`, ...) |
| `crates/dbflux_components/src/semantic.rs` | per-theme banner, row-state and chart chrome colors |
| `crates/dbflux_components/src/typography.rs` | `AppFonts` and the bundled font files |

A guardrail test (`style_guardrails.rs`) rejects raw color literals and bare `px(4/6/8/12/16/24)` anywhere in the components crate outside the token files. New numbers go into `tokens.rs` first.

The design canvas is the reference for intent: https://claude.ai/artifact/RrT5VLW14vaPQzV1ab71hT. When the canvas and this document disagree, the code on `main` wins and the gap is a bug to file.

## Colors

### Base roles

| Role | Dark | Light | Theme field / accessor | Use |
|---|---|---|---|---|
| bg | `#09090B` | `#F6F4F7` | `background` | window ground, sidebar, tab bar, input fill |
| panel | `#100F13` | `#FFFFFF` | `popover`, `tab_active`, `table` | panes, cards, modals, active tab |
| raised | `#1A181E` | `#EEEAF0` | `secondary` | secondary buttons, chips, menus, keycaps |
| line | `#232128` | `#E3DEE6` | `border` | separators, pane edges, input edges |
| line-2 | `#37333D` | `#CBC4D1` | `input`, `muted` | control edges on raised surfaces, card and modal edges |
| row divider | `#18161B` | `#F0ECF2` | `table_row_border` | grid row dividers |
| strong | `#F7F4F7` | `#141118` | `ChromeColors::strong` | titles, data values, active labels |
| body | `#C6C3CC` | `#3B3740` | `foreground` | body copy, button labels |
| muted | `#8E8996` | `#6B6572` | `muted_foreground` | metadata, section labels, inactive tabs |
| byzantine | `#702963` | `#702963` | `primary` | primary fills, checked boxes, active tab edge |
| byzantine hover | `#7F3171` | `#7F3171` | `primary_hover` | primary hover |
| deep | `#4A1B41` | `#4A1B41` | `primary_active` | primary pressed |
| tint | `#D48CC8` | `#702963` | `ring`, `ChromeColors::tint` | accent text and icons, focus ring, caret, keywords |
| ink | `#FFFFFF` | `#FFFFFF` | `primary_foreground` | text on byzantine |
| success | `#7BE0A0` | `#1C7F45` | `success` | connected, included, inserts |
| info | `#6EA8FF` | `#1F5FD1` | `info`, `link` | links, foreign keys |
| warning | `#FFC23D` | `#B7791F` | `warning` | caution, primary keys |
| danger | `#FF6B5E` | `#C7362B` | `danger` | delete, errors, production |
| null | `#B79CFF` | `#6B4FD8` | `SyntaxColors::number` | NULL cells, numbers |
| cyan | `#6FD3D8` | `#0F7C82` | `cyan` | chart stats accent |

Text on a solid success, info, warning or danger fill is `#09090B` on Dark and `#FFFFFF` on Light (`*_foreground`). Semantic hover and pressed fills are the base shifted +5 % and −6 % in lightness.

### Washes

| Wash | Dark | Light | Where |
|---|---|---|---|
| hover | white 4 % | strong 4 % | hovered rows, menu rows, ghost surfaces (`accent`, `table_hover`, `list_hover`) |
| alternating row | white 1.2 % | strong 1.5 % | even grid and list rows |
| selected row | tint 7 % | byzantine 7 % | selected grid row (`table_active`) |
| selected item | tint 12 % | byzantine 14 % | selected tree row, list item, sidebar item (`list_active`, `sidebar_accent`) |
| menu / grid cell highlight | tint 14 % / 12 % | same alphas | selected menu row (`MenuMetrics::SELECTED_ALPHA`), selected cell (`GridMetrics::CELL_SELECTED_ALPHA`) |
| selected secondary or ghost button | tint 14 / 22 / 30 % | same alphas | toggled toolbar buttons (`Button::selected`) |
| danger soft | danger 14 / 22 / 30 % | same alphas | danger buttons at rest, hover, pressed |
| success soft | success 14 % | same alpha | success badges (`Feedback::BADGE_FILL_ALPHA`) |
| banner field | semantic 12 % | semantic 14 % | `BannerColors` in `semantic.rs` |
| text selection | tint 25 % | byzantine 25 % | `selection` |
| drop target | tint 10 % | byzantine 10 % | drag and drop |
| scrim | `#050507` 62 % | `#1E1423` 28 % | behind modals (`overlay`) |

Row-state tints for the data grid (`RowStateColors`): insert success 15 %, delete danger 10 %, error danger 15 %, saving warning 10 % on Dark; 14 / 12 / 14 / 14 % on Light. Dirty rows get no row tint; the edited cell carries the marker.

### Two hard rules

1. **Byzantine is a fill, never text on the dark ground.** It reads at 2.1:1 on `#09090B`. Use it for primary buttons, checked boxes, active tab edges and bands.
2. **Accent text and icons use the tint.** Read it with `ChromeColors::tint(theme)` (or `Text::primary()`), never `theme.primary`. On Light the tint is byzantine itself, which is why the accessor exists.

## Typography

Three families, bundled from `crates/dbflux_components/assets/fonts` and exposed as `AppFonts`:

| Family | Constant | Use |
|---|---|---|
| Archivo | `AppFonts::INTERFACE` | everything a person reads as interface: buttons, menus, tabs, tree rows, forms, body copy |
| Archivo Expanded | `AppFonts::DISPLAY` | uppercase section labels and large titles, weight 800, 0.14 em tracking. Never body text |
| JetBrains Mono | `AppFonts::MONO` | data: grid cells, queries, identifiers, ids, keys, key hints, numeric readouts |

The eight text roles are the `Text` constructors in `primitives/text.rs`. Base size is 13 px. Compact lowers every role by 1 px (read through `TextVariant::density_size`).

| Role | Constructor | Family | Default | Compact | Weight | Default color |
|---|---|---|---|---|---|---|
| Title | `Text::title` | Archivo | 20 px | 18 px | 700 | strong |
| Heading | `Text::heading` | Archivo | 18 px | 16 px | 700 | strong |
| Body | `Text::body` | Archivo | 13 px | 12 px | 500 | body |
| Body small | `Text::body_sm` | Archivo | 12 px | 11 px | 500 | body |
| Label | `Text::label` | Archivo Expanded, uppercase, 0.14 em | 11 px | 10 px | 800 | muted |
| Caption | `Text::caption` | Archivo | 12 px | 11 px | 500 | muted |
| Code | `Text::code` | JetBrains Mono | 13 px | 12 px | 500 | body |
| Key hint | `Text::key_hint` | JetBrains Mono | 12 px | 11 px | 500 | muted |

`Text::label` capitalizes its content itself, so pass ordinary text. Color overrides go through `.primary()` (tint), `.danger()`, `.warning()`, `.success()`, `.link()` and `.muted_foreground()`. Buttons use Archivo 600 at 12.5 px (12 px inline, 13 px large); keycaps use JetBrains Mono at 10.5 px.

## Layout

- **Spacing scale:** 4, 8, 12, 16, 24 px (`Spacing`), plus a locked 6 px half-step (`Spacing::XXS`).
- **Borders:** 1 px thin, 2 px medium, 1.5 px focus ring (`Borders`).
- **Chrome:** title bar 43 px, activity rail 52 px wide with 38 px buttons, panel headers 40 px, document tab bar 42 px with 36 px tabs, result tab bar 40 px, result footer 36 px.
- **Rows:** tree 26 px with 14 px indent, grid 31 px with a 40 px header, menu 30 px, list rows in modals 40 px.

### Control sizes

| Control | Height | Width | Cut | Token |
|---|---|---|---|---|
| Button, default | 30 px | label + 12 px padding | 6 | `ButtonMetrics::HEIGHT` |
| Button, inline (rows, chips, cards) | 24 px | label + 10 px padding | 6 | `ButtonMetrics::HEIGHT_INLINE` |
| Button, large (rare call to action) | 44 px | label + 16 px padding | 10 | `ButtonMetrics::HEIGHT_LARGE` |
| Icon-only button | 30 px | 32 px (24 inline, 44 large) | 6 | `ButtonMetrics::ICON_ONLY_WIDTH` |
| Input, select | 30 px (24 small) | caller | 6 | `Fields::HEIGHT` |
| Segmented control | 30 px: 26 px segments + 2 px track inset | content | 6 track, 4 thumb | `Fields::SEGMENT_HEIGHT` |
| Filter field | 34 px | caller | 8 | `Fields::FILTER_HEIGHT` |
| Checkbox | 16 px box | — | 4 | `Fields::CHECKBOX_SIZE` |
| Badge | 20 px | content | 4 | `Feedback::BADGE_HEIGHT` |

Everything in one toolbar row is 30 px, so buttons, inputs, selects and segmented controls align without adjustments.

## Elevation & Depth

Flat. Regions are separated by a 1 px line, not by shadow. The five surface roles (`primitives::surface(SurfaceRole, cx)`) decide fill, edge and cut:

| Role | Fill | Edge | Cut | Use |
|---|---|---|---|---|
| `Panel` | panel | line | none | panes, document areas |
| `Card` | panel | line-2 | 14 | framed content, empty-state cards |
| `Raised` | raised | line | none | SQL previews, inset blocks |
| `Overlay` | panel | line-2 | 12 | popovers, floating panels |
| `Modal` | panel | line-2 | 18 | dialogs |

Menus (`menu_frame`) use the raised fill with a line-2 edge, cut 12 and the large shadow. Shadows exist for floating chrome only: menus, dropdown lists and popovers take the large shadow (GPUI `shadow_lg`, or `Shadows::lg()` where a `BoxShadow` is needed). The modal scrim is the `overlay` color.

## Shapes: the cut

The signature shape is a 45° cut on the **top-left and bottom-right** corners (`ChamferCorners::TopLeftBottomRight`, the default). Document and result tabs cut the **top-left only** (`ChamferCorners::TopLeft`); a split button cuts top-left on the main action and bottom-right on the menu segment. The cut depth grows with the size of the thing:

| Cut | Token | Applies to |
|---|---|---|
| 4 px | `ChamferCut::KEYCAP` | keycaps, badges, env tags, counters, checkboxes, segmented thumb |
| 6 px | `ChamferCut::CONTROL` | 24–30 px controls: buttons, inputs, selects, icon buttons, segmented track, command search |
| 8 px | `ChamferCut::INPUT` | document tabs (top-left only), banners, the 34 px filter field, modal code blocks |
| 10 px | `ChamferCut::LARGE_CONTROL` | 44 px buttons |
| 12 px | `ChamferCut::OVERLAY` | menus, popovers, dropdown lists, toasts |
| 14 px | `ChamferCut::CARD` | cards |
| 18 px | `ChamferCut::MODAL` | modals, hero frames |

Rules:

- **Controls and surfaces are cut; data never is.** Grid cells, rows, list items, code editors and charts keep square edges.
- **Borders run on the straight edges only.** `Chamfer::border` paints a 1 px inset line on the four straight sides; the diagonals stay borderless. A focus ring is the one stroke that follows the diagonals.
- Radii stay 0 in Default density. Compact keeps its 2–3 px radii for non-chamfered gpui-component widgets; the cut does not change with density.

### Using `Chamfer` in code

GPUI has no clip-path, so the shape is a canvas painted behind the content:

```rust
div()
    .relative()
    .h(ButtonMetrics::HEIGHT)
    .child(
        Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.secondary)
            .fill_hover(theme.secondary_hover)
            .fill_active(theme.secondary_active)
            .border(theme.input)
            .interactive("export-button"),
    )
    .child(label)
```

- `Chamfer` must be the **first child** of a `.relative()` container; it covers the box and does not affect layout.
- Do not set `.bg()` on that container. The chamfer paints the fill.
- `.interactive(id)` tracks hover and press and fades between fills; `.held(true)` shows the pressed fill while Enter or Space is held.
- `.ring(ChamferRing::focus_for(color, kind))` strokes the focus ring; `.top_edge`, `.bottom_edge` and `.left_edge` paint the active-tab edge, keycap edge and banner/toast stripe.
- Prefer a component that already does this (`Button`, `Input`, `surface`, `Kbd`, `Badge`) over a hand-built `Chamfer`.

## Components

### Interaction states

From the DSStates board, implemented in `controls::button::button_colors`:

| Variant | Rest | Hover | Pressed | Content | Focus ring |
|---|---|---|---|---|---|
| Primary | byzantine `#702963` | `#7F3171` | deep `#4A1B41` | white | outside, 2 px offset |
| Secondary | raised | line | line-2 | body | inset |
| Ghost / icon | transparent | raised | line | body; active icon in tint | inset |
| Danger (soft) | danger 14 % | danger 22 % | danger 30 % | danger (`#FF6B5E` / `#C7362B`) | outside, 2 px offset |
| Selected secondary or ghost | tint 14 % | tint 22 % | tint 30 % | tint | inset |
| Disabled | rest fill at 45 % opacity | none | none | 45 % opacity, no pointer | none |

Use one primary button per toolbar or dialog.

**Motion.** Fill changes fade over 150 ms (`Anim::FAST_MS`) on `cubic-bezier(.2, .8, .2, 1)` (`motion_ease`). The pressed state also shows while Enter or Space is held on a focused control. When the platform asks for reduced motion, fills switch instantly and the spinner draws fully charged without moving.

### Focus

- **Focus-visible only.** Focus indication shows only after keyboard input, stays while the pointer moves, and disappears on the next pointer press (`is_keyboard_modality`, `is_focus_visible`, `WhenFocusVisible` in `primitives/focus_ring.rs`, backed by the window's `keyboard_focus_visible`).
- **A focused control owns Enter and Space.** A focused button, checkbox or list row answers them itself, ahead of the shortcuts of the panel around it.
- **The ring:** 1.5 px (`Borders::FOCUS_RING`) in the tint, tracing the full cut outline, diagonals included. It sits **outside, 2 px from the edge** on filled controls (primary, danger, checked checkbox) and **inset** on everything else (`ChamferFillKind::Filled` vs `Surface`, `FocusShape::FilledChamfer` vs `Chamfer`). Square data surfaces use `FocusShape::Rect`.
- **Composite controls mark the focused item, never the container.** Segmented controls, chips and tabs draw a 2 px tint underline inside the item (`focus_underline`); lists and trees draw the tint wash plus a 2 px tint bar on the left (`ListRow`); menus use the row wash.
- **Selection is fill only.** A selected item takes a wash or a raised thumb, never a ring. Focus and selection can coexist on the same item.
- Controls with their own focus handle (`Button`, `Input`, `Dropdown`, `Checkbox`) draw the ring themselves. Use `focus_ring(...)` for controls whose focus lives in navigation state.

### Component map

Use these. Never hand-roll a rounded `div`, a color literal or a one-off control.

| Board component | Code | Module | When to use |
|---|---|---|---|
| Button | `Button` (`.primary/.secondary/.ghost/.danger`, `.inline/.large`, `.icon`, `.icon_only`, `.kbd`, `.selected`) | `dbflux_components::controls::button` | every clickable action, including icon buttons |
| Split | `SplitButton` | `composites::split_button` | an action with a menu of variants (Run, Refresh, Export) |
| Keycap | `Kbd` (`Kbd::new`, `Kbd::chord`) | `primitives::kbd` | any shortcut shown on screen |
| Badge / env tag | `Badge`, `EnvTag` | `primitives::badge` | short status labels; PROD/STAGING next to a connection |
| Status | `StatusIndicator` | `primitives::status` | connection or task state: 7 px diamond, label, latency |
| Banner | `BannerBlock` | `primitives::banner` | inline notice inside a pane or dialog |
| Toast | `Toast` (`::success/::info/::warning/::error`) | `dbflux_ui_base::toast` | transient feedback; errors go through `report_error` |
| Spinner | `Spinner` | `primitives::loading_state` | work in progress |
| Surface | `surface(SurfaceRole, cx)` | `primitives::surface` | any pane, card, raised block, overlay or modal frame |
| Modal | `Modal`, `modal_field`, `modal_code`, `modal_lead` | `modals` | dialogs; the `Modal` key context takes Escape, Enter and the scroll keys; handles focus |
| Headers | `panel_header`, `panel_header_with_actions`, `section_header`, `page_header`, `collapsible_bar` | `composites::header` | titles of panels, sections and pages |
| Tabs | `document_tab`, `result_tab`, `inline_tab` (+ `*_tab_bar`) | `composites::tabs` | document tabs, result tabs, in-pane tab strips |
| Focus | `focus_ring`, `focus_underline`, `WhenFocusVisible` | `primitives::focus_ring` | keyboard focus on anything that is not already a control |
| Text | `Text::title/heading/body/body_sm/label/caption/code/key_hint` | `primitives::text` | every piece of copy |
| Input | `Input` | `controls::input` | single-line text entry |
| Select | `Dropdown` (`.chevron_trigger`) | `controls::dropdown` | pick one value from a list |
| Checkbox | `Checkbox` | `controls::checkbox` | boolean options |
| Segmented | `SegmentedControl` | `primitives::segmented_control` | 2–5 mutually exclusive modes |
| Filter field | `FilterField` | `components::filter_bar` | WHERE / filter entry above a grid |
| Menu | `menu_row`, `menu_frame` | `composites::menu_item` | context menus, flyouts, dropdown lists |
| Tree | sidebar tree (`TreeMetrics`, `TreeNav`, `ListRow`) | `dbflux_ui_sidebar::render_tree`, `components::tree_nav` | connection and schema trees |
| Data grid | `DataTable` | `components::data_table` | tabular results |
| Result panel | `ResultPanel` + `ViewHandle` | `result_panel` | chrome around a result view (modes, toolbar segments) |
| Stepper | `render_wizard_rail`, `render_wizard_stepper` | `composites::wizard_rail` | multi-step wizards |
| List row | `ListRow` | `composites::list_row` | rows of lists, pickers, settings lists |
| Breadcrumb | `Breadcrumb` | `composites::breadcrumb` | object paths (bucket/prefix, schema/table) |
| Empty state | `EmptyState` (`.card`, `.danger`) | `composites::empty_state` | empty or failed regions |
| Divider | `divider(axis, DividerTone, cx)` | `primitives::divider` | 1 px rules between regions and control groups |
| Activity rail | `ActivityRail` | `composites::activity_rail` | the left rail of the main window |
| Command search | `CommandSearch` | `composites::shell_bar` | title-bar trigger for the command palette |
| Row inspector | `RowInspectorContent` | `dbflux_ui_document::data_grid_panel::row_inspector` | the inspector rail for one grid row |

## Board map

All boards live on the design canvas: https://claude.ai/artifact/RrT5VLW14vaPQzV1ab71hT. Each dark board has a `*Light*` twin (for example `AppByzLightTable`, `P1LightModals`).

| Board | Specifies |
|---|---|
| DSFoundations | colors, type, cut depths, motion, icons |
| DSStates | interaction states of buttons and controls |
| DSApp | app components |
| DSAppPlan | which code component each board component becomes |
| DSBrand | brand mark and app icons |
| AppByzTable | table view with the row inspector |
| AppByzEditor | query editor with results |
| AppByzMenu | cell context menu |
| P1Sidebar | sidebar states and menus |
| P1Empty | empty workspace and tasks panel |
| P1Palette | command palette |
| P1Modals | modals and toasts |
| P1Flows | sign-in, wizards and editors |
| P1Migrate | migrate wizard |
| P1ConnForm, P1DriverPicker | connection manager form and driver picker |
| P1SettingsGeneral, P1SettingsKeys, P1SettingsAuth, P1SettingsSsh, P1SettingsHooks, P1SettingsDrivers, P1SettingsMcp, P1SettingsUpdates | settings sections |
| P1Welcome, P1WhatsNew | first run, what's new and update notice |
| P1DocTable, P1DocSchema | document collections: table with inspector, schema |
| P1KvHash, P1KvString | key-value hash key, JSON string and large value |
| P1Builder | visual query builder |
| P1Chart, P1Dashboard | chart document, dashboard in edit mode |
| P1Schema | schema diagram |
| P1Audit, P1Approvals | audit viewer, MCP approvals |
| P1Buckets, P1Objects, P1ObjectEditor | object storage buckets, browser, editor |
| P2DocNested | nested column groups and commit conflicts |
| P2KvFilter, P2KvStream, P2KvZset | key-value search while scanning, streams, sorted sets |
| P2Series | time-series measurement |
| P2Findings | phase 2 review findings |

## Do's and Don'ts

- Do build screens from the components above. A new visual need is a new token or component in `dbflux_components`, not a local style.
- Do read colors through theme fields and `ChromeColors`, sizes through `tokens.rs`, and text through `Text` roles.
- Do cut controls and surfaces with the cut for their size, and keep data square.
- Do use the tint for accent text and icons, and byzantine only as a fill.
- Do keep one primary button per toolbar or dialog, and keep toolbar controls at 30 px.
- Do give interactive elements a stable `.id(...)` so UI automation can target them.
- Don't write byzantine text on the dark ground, or `theme.primary` as a text color.
- Don't put a border on a diagonal, a radius on a cut control, or a cut on a grid cell.
- Don't ring a composite container or show a ring after a mouse click.
- Don't mark selection with a ring. Selection is a fill.
- Don't use Archivo Expanded for body text or JetBrains Mono for interface copy.
- Don't branch on the theme name in a component. Read the role, and both palettes follow.
