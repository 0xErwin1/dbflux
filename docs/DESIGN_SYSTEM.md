# Design system

DBFlux's interface is built on **Bolt**, a design-system crate shared with
[ReqFlux](https://github.com/0xErwin1/reqflux). Bolt owns everything a product does
not: the palettes, the color and typography roles, the spacing and chrome
metrics, the cut corners, the theme mapping onto gpui-component, and the generic
GPUI components. A product supplies its accent, name and any token of its own.

The crate lives in `crates/bolt` while its API settles. It is designed to move
to its own repository without touching a screen: see
[Consuming Bolt from another repository](#consuming-bolt-from-another-repository).

## What Bolt owns

| Module | Contents |
|---|---|
| `bolt::tokens` | Spacing, heights, radii, cuts, chrome and control metrics, syntax and row colors |
| `bolt::brand` | `Brand` (accent, hover, pressed, tint, on-accent, name) and the washes derived from it |
| `bolt::palette` | `PaletteMode::{Dark, Light, System}` and the palette a product publishes |
| `bolt::theme` | `install`, `apply_theme`, the gpui-component mapping, editor chrome and syntax recoloring |
| `bolt::density` | `Density::{Default, Compact}` and the runtime font and radius accessors |
| `bolt::fonts` | `FontSettings`, `FontChoices`, the size bounds and the resolvers |
| `bolt::typography` | The bundled families and their files |
| `bolt::colors` | `Colors`: the neutral and semantic color roles per palette, read through `bolt::colors(cx)` |
| `bolt::semantic` | Banner, row-state and chart-chrome colors per palette |
| `bolt::icon` | `IconSource`: a named icon or an SVG path |
| `bolt::primitives` | Badge, banner, chamfer, divider, file picker, focus ring, keycap, label, loading state, segmented control, status, surface, text, type-to-confirm |
| `bolt::controls` | Button, checkbox, dropdown, select trigger, input, read-only text view |
| `bolt::composites` | Tabs (document, result, inline), menu item and popup menu, list row, empty state, split button, splitter, field row, island |
| `bolt::selector` | `id_selector`: the gpui debug selector every id-bearing component records for its element id |
| `bolt::modals` | The modal dialog, its focus helper and the shared body blocks (lead, statement, field, form row, value field) |

Bolt depends on `gpui`, `gpui-component`, `gpui-base`, `log` and `serde_json`.
It never depends on DBFlux's domain crates and never names a product, a driver,
a database or a design board: a source-scanning guardrail in
`crates/bolt/tests/guardrail.rs` enforces that.

## What a product supplies

- **A `Brand`.** `bolt::brand::install(brand, cx)` publishes the accent roles.
  Every accent-shaped color — the primary fill, the focus ring, the selection
  washes, syntax keywords, the checked checkbox — resolves through it. DBFlux's
  byzantine accent (`#702963`) is defined in
  `crates/dbflux_components/src/theme.rs`, not in Bolt.
- **A palette mode and a density.** Dark is unconditional; `Light` and
  `Compact` are opt-in. DBFlux maps its persisted `ThemeSetting` and `AppStyle`
  onto `PaletteMode` and `Density`.
- **Its font choices.** `FontChoices` carries the families and sizes the user
  picked; Bolt clamps them and drops the families the text system cannot load.
- **Its icon catalogue.** `IconSource` is a name or a path; DBFlux's 162-variant
  `AppIcon` and ReqFlux's Lucide set both plug in.
- **Its keymap and its copy.** The dropdown takes the action ids it answers and
  a translated placeholder through globals the product installs, so Bolt needs
  neither the keymap nor the i18n crate. The modal does the same through
  `ModalSeams`: the product registers its Cancel, Execute and scroll actions
  with `on_modal_key`, says which command a deferring modal runs, and supplies
  the Escape keycap label, the close icon and its translated name. A modal
  rendered before the seam is installed answers no keys.
- **Its title-bar icons.** `ShellIcons` (a global) holds the glyphs the
  command search field and the notification bell draw. Without it they use
  gpui-component's `Search` and `Bell`; DBFlux installs its own `AppIcon`s
  with `install_shell_icons`.

## Finding components in tests

Every Bolt component that takes an element id (button, checkbox, dropdown,
select trigger, input, file picker, tabs, menu row and overlay, list row,
split button, splitter, segmented control and its segments, modal backdrop)
records that id as its gpui debug selector, so a product's GPUI test finds it with
`cx.debug_bounds("<id>")`. A segment's id defaults to
`segmented-<group>-<id>`; `SegmentedItem::element_id` replaces it. An input
records its selector on its frame, so a bare input (`appearance(false)`) has
none. gpui records selectors only in debug builds with `test-support`;
nothing changes in what is drawn.

Some parts get an id derived from their owner's id:

| Part | Selector |
|---|---|
| A button's `.kbd(..)` keycap | `<button id>-key` |
| A menu row's shortcut | `<row id>-key` |
| A menu row's check mark | `<row id>-check` |
| The command search field's shortcut keycap | `<field id>-key` |
| An activity rail button | `rail-<entry id>` |
| A rail button's count badge | `rail-<entry id>-count` |
| The notification bell's count badge | `notifications-badge` (`NotificationBell::badge_id` replaces it) |

The rail root carries its own id as a selector too. The count badges carry
the count they draw as their accessible label (none while hidden), and the
command search keycap carries its shortcut text, so the accessibility tree and
UI automation read them. Rail buttons and the command search field have the
button role; a rail button reports itself selected while its entry is active.

`Badge`, `Kbd`, `BannerBlock` and `EmptyState` take an optional id with
`.id(..)`. Without one they render exactly as before; with one, the element
carries that id and its debug selector, so a product finds it without a
wrapper.

With an id, a `Badge` and a `Kbd` are also label nodes in the accessibility
tree, named by the text they draw: the badge label, or the keys joined by
`+` (`Ctrl+Shift+P`). `.aria_label(..)` replaces that name on both, for a
badge whose text needs context (`3` read as `3 headers`). gpui creates an
accessibility node only for an element with a role, so an id-less badge or
keycap has no node and its label has nothing to carry it.

An `EmptyState` with an id names its parts after it, each a label node named
by its text:

| Part | Id |
|---|---|
| The title | `<id>-title` |
| The sentence | `<id>-text` |
| The keycaps of the key-hint actions, numbered from 0 across all actions | `<id>-key-<n>` |

The shortcut keycap of a `menu_row` (`<row id>-key`) is a label node named by
the shortcut text it draws (`Ctrl+C`).

## Options for products

These options exist so a product composes its screens from Bolt alone. Each
one is off by default, so DBFlux draws the same pixels without them.

| Component | Option | What it does |
|---|---|---|
| `SegmentedControl` | `.tabs(true)` | A named control becomes a tab list and its segments tabs, selected when active, for a segmented control that switches views. The look and ids stay the same; the default is a radio group. |
| `SegmentedControl` | `.track_id(..)`, `.aria_label(..)` | Replace the track's element and accessibility id (`segmented-<group>` by default; the segment ids keep the group) and name the radio group or tab list. Both need `.group(..)`: an unnamed control has no accessibility node. |
| `SelectTrigger` | `.label_color(..)`, `.label_weight(..)`, `.label_font_family(..)` | Draw the label in a color, weight and font family of its own, such as an HTTP method chip (bold, mono, colored). A disabled trigger keeps the faded text color. Unset parts follow the field. |
| `SelectTrigger` | `SelectTrigger::new(id, label)` | The closed field of a select, drawn by the same code as a standard `Dropdown` trigger: cut-6 field, optional leading icon, truncated label, chevron. It is a button with the caller's click handler, `.disabled(..)`, `.focus_ring(..)` or `.focused(..)`, for a product that opens its own popup. The `Dropdown` entity itself is unchanged. |
| `Input` | `.padding(x, y)`, `.text_size(..)`, `.line_height(..)` | Replace the inner padding (the height then follows the line), the text size and the line height. With `appearance(false)`, set them to the values of the text the field replaces so nothing moves when the text turns into a field. Without them a bare input keeps gpui-component's size. |
| `Input` | `.font_weight(..)` | Replaces the text weight, normal in a framed field and medium in a bare one. Set a bare field to normal so it matches text drawn at the regular weight. |
| `EmptyState` | `.primary_action(Button)` | A real button under the sentence for the one thing the region asks for. The key-hint actions stay below it. |
| `MenuItem` | `.checked(bool)` | A checkable row: `true` draws a tint check mark (`MenuMetrics::CHECK_ICON`) after the label. The row reports itself as a checkable menu item, toggled or not. A row without it is a plain menu item. |
| `Splitter` | `Splitter::new(id, SplitAxis)` | The resize gutter between two panes (see below). |
| `RailEntry` | `.count(n, BellUrgency)` | A count badge over the rail button's top-right corner, sized and colored like the notification bell's. A zero count shows none. |
| `StatusChip` | `.tone(color)`, `.wash(true)` | A tone draws the chip's text in that color, semibold. With the wash the chip also sits on the tone at `ShellMetrics::STATUS_CHIP_ALPHA` instead of the island fill. |

### Shell chrome

The window shell is built from four composites:

- `ActivityRail::new(id, entries)`: the column of view buttons at the left
  edge, `ShellMetrics::RAIL_WIDTH` wide with no fill of its own. Each
  `RailEntry` has an id, an icon, a label (tooltip and accessible name), an
  active flag and a placement; `.bottom()` pins it to the bottom edge.
  `.on_select(..)` gets the entry id. The rail sets the
  `ACTIVITY_RAIL_KEY_CONTEXT` key context.
- `CommandSearch::new(id, placeholder)`: the title bar's search trigger. It
  is not an editor: a click, or Enter or Space while its `.focus_handle(..)`
  holds focus, runs `.on_open(..)`. `.shortcut(keys)` shows the chord as one
  keycap. It sets the `COMMAND_SEARCH_KEY_CONTEXT` key context.
- `NotificationBell::new(id, label, count)`: the title bar's bell, with a
  count badge (capped at "99+") colored by its `BellUrgency`, and a deeper
  wash while `.open(true)`.
- `StatusChip::new(id)`: a status bar segment. `.build(cx)` returns the row,
  so the caller adds its icon and text.

### Splitter

`Splitter` is a `SplitterMetrics::GUTTER` (6 px) strip across the split with a
three-dot muted grip in the middle. `SplitAxis::Horizontal` places the panes side
by side and shows the column-resize cursor; `SplitAxis::Vertical` stacks them
and shows the row-resize cursor. Hovering lays the accent wash at
`SplitterMetrics::HOVER_ALPHA` over the gutter, the same wash as the sidebar
and inspector grips. The tint shows while `.active(true)` or while keyboard
focus is visible.

The owner keeps the pane size, and the splitter reports what the user does:

- `.on_drag(|position, window, cx| ..)`: the pointer along the axis, in
  window coordinates, on every move of a drag the gutter started.
- `.on_step(|delta, window, cx| ..)`: Left and Right (side by side) or Up and
  Down (stacked) while the handle given to `.track_focus(..)` holds focus,
  `SplitterMetrics::KEY_STEP` (16 px) by default, set with `.step(..)`.
- `.on_toggle_collapse(|window, cx| ..)`: a double click, or Enter while
  focused.

Its accessibility role is a splitter, oriented across the axis.
`.aria_label(..)` names it (`Resize request and response`), and
`.value_percent(..)` reports the first pane's share of the split as its value,
numeric from 0 to 100 and as text (`62%`). DBFlux's sidebar and inspector keep
their own grips.

## Reading colors

A screen reads color roles from Bolt, never from a `gpui_component::Theme`
field:

- **Accent roles** come from `bolt::brand::brand(cx)` and the washes from
  `bolt::brand::colors(cx)`.
- **Neutral and semantic roles** come from `bolt::colors(cx)`, which returns a
  `Colors` for the palette the product published (dark until it publishes one,
  and for an unresolved `System`).
- **Banner, row-state and chart colors** come from `bolt::semantic`.

| Role | What it paints | Theme field it equals |
|---|---|---|
| `desk` | The window ground the islands sit on | `ChromeColors::desk` |
| `background` | Inset fills: title bar, sidebar, tab bar, editor | `background` |
| `panel` | Island, popover and table fill | `popover`, `table` |
| `raised` | Raised controls and secondary fills | `secondary` |
| `line` | Default border | `border` |
| `line_2` | Input and control edges | `input` |
| `row_divider` | Divider between data rows | `table_row_border` |
| `strong` | Titles, selected items, data values | `accent_foreground` |
| `foreground` | Body text | `foreground` |
| `muted` | Metadata, labels, counts | `muted_foreground` |
| `hover` | Hover wash | `list_hover`, `table_hover` |
| `alternating_row` | Every other data row | `table_even` |
| `island_edge` | Hairline along an island outline | `ChromeColors::island_edge` |
| `scrim` | Dimmed desk behind a modal | `overlay` |
| `success`, `info`, `warning`, `danger` | Semantic states | the field of the same name |
| `on_semantic` | Text on a solid semantic fill | `success_foreground` and its siblings |

Each role holds the value `bolt::theme::apply_theme` installs for that field,
and a test checks every pair in both palettes, so moving a screen from a theme
field to its role changes nothing on screen.

## Sizes

Every interface length goes through `tokens::ui(px)`, which expresses design
pixels in rems. Windows render under `gpui_component::Root`, which sets the rem
size from the interface font size, so a size built with `ui` follows the setting
without any call site reading it. Spacing, borders, radii and panel sizes stay
fixed pixels.

## How DBFlux consumes Bolt

DBFlux keeps its product layer in `crates/dbflux_components`, and that crate
re-exports every module Bolt now owns at the path it had before:

```rust
// crates/dbflux_components/src/lib.rs
pub use bolt::{fonts, icon, semantic, tokens, typography};
```

This is deliberate. About 1,800 references across 325 files read
`dbflux_components::tokens::…` and its siblings; re-exporting keeps them
compiling and keeps a move from turning into a search-and-replace with no
behavior change to show for it. Product-coupled helpers stay behind as
extensions over the generic primitive: `EnvTag` and `environment_badge_tone`
over `Badge`, the per-variant banner icon over `BannerBlock`, the translated
placeholder over `FilePicker`, `DropdownEnvironment` over `Dropdown`, and
`install_modal_seams` (keymap actions, keycap, close icon and name) over
`Modal`, and `install_shell_icons` (the search and bell glyphs) over
`CommandSearch` and `NotificationBell`.

## Consuming Bolt from another repository

ReqFlux carries its own copy of this design system today, with the same layering
and a smaller token set. The swap is a dependency change plus a brand:

1. Add the dependency, pinned to a tag so a build is reproducible:

   ```toml
   [dependencies]
   bolt = { git = "https://github.com/0xErwin1/dbflux", tag = "bolt-v0.1.0" }
   ```

2. Delete `crates/bolt` from ReqFlux and re-export the shared crate at the old
   path, so ReqFlux's screens keep compiling:

   ```rust
   // the module ReqFlux's screens already import
   pub use bolt::*;
   ```

3. Install the brand before the first window, and drop the copied theme code:

   ```rust
   bolt::brand::install(reqflux_brand(), cx);
   bolt::theme::apply_theme(bolt::palette::PaletteMode::Dark, bolt::density::Density::Compact, None, cx);
   ```

4. Keep ReqFlux's own tokens — HTTP method colors, the draft marker — where they
   are. They are product data, not design-system roles.

Two differences to expect when moving from the earlier copy: sizes follow the
interface font size through rems instead of fixed pixels, and the light palette
derives its selection washes from the accent while the dark one derives them
from the accent tint. Both are properties of the shared palette; a product that
wants different values changes its `Brand`, not Bolt.

## Guardrails

Two source-scanning tests keep the boundary honest:

- `crates/bolt/tests/guardrail.rs` rejects a product name, a raw color, a bare
  pixel length or a domain-crate reference inside Bolt, outside the files that
  legitimately define colors and geometry.
- `crates/dbflux_components/src/style_guardrails.rs` rejects bare spacing
  literals, raw color constructors, pixel text sizes and bundled font families
  in the product crate and the UI crates.

Both run with the test suite, so a violation fails the build rather than the
review.

## What Bolt does not own yet

Domain components stay in `dbflux_components` until their seams exist:
`data_table`, `document_tree`, the product dialogs built on `Modal`, the chart
engine, `result_panel`, the Vim layer and the composites that read product
state (rail navigation, notification center, wizard rail). They read `ColumnMeta`, `Value`, `QueryLanguage`
and `ContextId` from `dbflux_core`, so moving them means opening a generic seam
in the core first, one component at a time.
