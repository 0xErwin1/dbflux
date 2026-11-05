# Design system

DBFlux's interface is built on **Bolt**, a design-system crate shared with
[ReqFlux](https://github.com/0xErwin1/reqflux). Bolt owns everything a product does
not: the palettes, the color and typography roles, the spacing and chrome
metrics, the cut corners, the theme mapping onto gpui-component, and the generic
GPUI components. A product supplies its accent, name and any token of its own.

The crate lives in `crates/bolt` while its API settles. It is designed to move
to its own repository without touching a screen: see
[Consuming Bolt from another repository](#consuming-bolt-from-another-repository).

**The rule that decides every borderline case: Bolt carries no product logic and
no product components.** The test is not "does this compile without naming a
product" but "would an app in a completely different domain need this exact
thing". A chart renderer passes; a chart renderer that knows what a series means
in a SQL result does not. When the two are entangled the answer is a split — the
rendering in Bolt, the domain knowledge behind a seam the product fills.

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
| `bolt::primitives` | Badge, banner, chamfer, divider, file picker, focus ring, keycap, label, loading state, markdown view, segmented control, status, surface, text, timeline strip and histogram, type-to-confirm |
| `bolt::controls` | Button, checkbox, option group, dropdown, select trigger, input, search field with a match count, read-only text view, key capture |
| `bolt::composites` | Tabs (document, result, inline), menu item and popup menu, form popover, list row, empty state, split button, splitter, field row, island |
| `bolt::selector` | `id_selector`: the gpui debug selector every id-bearing component records for its element id |
| `bolt::modals` | The modal dialog, its focus helper and the shared body blocks (lead, statement, numbered statement, field, form row, value field) |
| `bolt::shell` | The activity rail: view buttons with one active entry, top or bottom pinned |
| `bolt::data` | The row-and-cell contract: `ColumnView`, `CellView`, `CellTone`, `CellContent`, `RowSource`, `RowVisualState`, `ColumnKind` |
| `bolt::components::grid` | The data-grid row list and cell painter over `RowSource`, the `GridHost` carrying the host facts, the column resize drag, and `GridOverflow` for a grid wider than its viewport |
| `bolt::components::tree_nav` | Tree navigation: flattening, cursor, expansion, the gutter, the navigation row with its chevron, count and trailing slot |
| `bolt::components::document_tree` | The document-tree row painter, its value-type colors and its row host |
| `bolt::chart` | Decimation, axis ticks, series stats, legend, axis bar, point inspector, the spec's value types and the readout formatters |
| `bolt::command_palette` | The palette engine: fuzzy query, scopes, grouping, cursor, highlighting, card and footer |
| `bolt::keymap` | The semantic command vocabulary, the installed `CommandSet` and the attach hook |
| `bolt::syntax` | The grammar seam over the shared highlighter registry, the bundled grammars behind opt-in `syntax-*` features |
| `bolt::strings` | The text seam: a key and a key with named arguments, answered by the product |
| `bolt::vim` | The modal-editing engine and its binding layer |
| `bolt::toast` | The toast model, its bounded stack, auto-dismiss timers, the presentation shell and a generic `TokenBucket` throttle |
| `bolt::keyboard_coverage` | The rule that every clickable element needs a keyboard path, as a registry a product fills |
| `bolt::forms` | The form-navigation contract: edit state, field, navigation result and the command trait |
| `bolt::controls::multi_select` | The multi-select control: state machine, menu, trigger, keyboard and pointer semantics |
| `bolt_keys` | The keyboard engine: chords and their storage format, layer resolution, override merging, conflict detection, the recorder. Re-exported as `bolt::keys` |

Bolt depends on `gpui`, `gpui-component`, `gpui-base`, `fuzzy-matcher`, `log`,
`chrono`, `serde` and `serde_json`, and on no product crate.
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
- **Its text.** `bolt::strings` holds two function pointers, one for a key and
  one for a key with named arguments (`%{name}`). The catalogue, the locale and
  the placeholder replacement all stay in the product's own functions.
- **Its commands.** `bolt::keymap::CommandSet` maps a small semantic vocabulary
  onto the product's action ids, both directions, together with the key context
  and the attach hook. The dropdown, the command palette and vim all dispatch
  through that one set.
- **Its editor bindings.** `VimSeams` supplies the leader action factory, the
  attach hook, the Vim key contexts and the mode labels, because action ids and
  context names are persisted in users' keybinding overrides and may not change.
- **Its rows.** Anything that draws data takes a `RowSource`; the host facts a
  painter cannot know arrive separately — `GridHost` for the grid, a row host
  for the document tree — so the model and the value types stay where the data
  lives.
- **Its palette items.** `PaletteEntry` describes one palette row and
  `PaletteHost` supplies the key context, the footer key labels, the attach hook
  and the closed event.
- **Its toast icons and its coverage registry.** `ToastIcons` mirrors `ShellIcons`
  for the severity glyphs, and `bolt::keyboard_coverage` takes the product's
  surfaces, element ids and bound commands as data — the live "is this bound in
  this context" answer comes from the product, which is where the user's
  overrides live.
- **Its chart chrome.** `ChartColumn { name, kind }` is what detection reads;
  `AxisBarChrome` and `PointInspectorText` carry the icons and translated labels
  those two surfaces need, since they translate at build time with no `cx`.

## Finding components in tests

Every Bolt component that takes an element id (button, checkbox, option group
and its options, dropdown, select trigger, input, file picker, tabs, menu row
and overlay, list row, split button, splitter, segmented control and its
segments, modal backdrop) records that id as its gpui debug selector, so a
product's GPUI test finds it with `cx.debug_bounds("<id>")`. An option group
names itself with `OptionGroup::new(group, ..)`; ids are
`option-group:<group>`, `option-group:<group>:<id>` and
`option-group:<group>:<id>:description`, with any `:` or `%` inside a group or
option name escaped, so no two groups, options or descriptions can share an id
(`option_group_id`, `option_group_item_id` and `option_group_description_id`
spell them out). A segment's id defaults to `segmented-<group>-<id>`;
`SegmentedItem::element_id` replaces it. An input
records its selector on its frame, so a bare input (`appearance(false)`) has
none. A form field row takes no id and records `field-row`. gpui records
selectors only in debug builds with `test-support`; nothing changes in what is
drawn.

Some parts get an id derived from their owner's id:

| Part | Selector |
|---|---|
| A field's error caption (`Input::error(..)`) | `<field id>-error` |
| An option group's option description | `option-group:<group>:<id>:description` |
| A button's `.kbd(..)` keycap | `<button id>-key` |
| A menu row's shortcut | `<row id>-key` |
| A menu row's check mark | `<row id>-check` |
| A popover's title, body and footer | `<id>-title`, `<id>-body`, `<id>-footer` |
| A popover's click-away layer | `<id>-away` |
| An open dropdown's menu, its scrolling column of rows, and its `n`th row numbered from 0 | `<id>-menu`, `<id>-menu-rows`, `<id>-item-<n>` |
| The command search field's shortcut keycap | `<field id>-key` |
| An activity rail button | `rail-<entry id>` |
| A rail button's count badge | `rail-<entry id>-count` |
| The notification bell's count badge | `notifications-badge` (`NotificationBell::badge_id` replaces it) |
| A markdown view's document | `<view id>-body` |
| A timeline histogram's bars | `<histogram id>-bar-<n>`, plus `<histogram id>-bar-<n>-rest` and `<histogram id>-bar-<n>-highlight` for the two parts of a bar and `<histogram id>-gap-<n>` for the gap after it |
| A timeline histogram's legend | `<histogram id>-legend`, and `<histogram id>-legend-<n>` and `<histogram id>-swatch-<n>` for its entries |

| A grid overflow's scrollbar strip | `<id>-scrollbar` |

| A closed search field's placeholder | `<trigger id>-placeholder` |
| A closed search field's shortcut keycap | `<trigger id>-key` |
| A value field's value | `<value field id>-value` |
| A code view's `n`th line, numbered from 1 | `<view id>-line-<n>` |
| A code view's scrolling code column | `<view id>-lines` |

The rail root carries its own id as a selector too. The count badges carry
the count they draw as their accessible label (none while hidden), and the
command search keycap carries its shortcut text, so the accessibility tree and
UI automation read them. Rail buttons, the command search field and the
notification bell have the button role, named by their label, so automation
finds and clicks them by id even when the bell has no click handler of its
own; a rail button reports itself selected while its entry is active.

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
| The label of the `n`th key-hint action, numbered from 0 | `<id>-action-<n>` |
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
| `SegmentedControl` | `SegmentedItem::disabled(..)`, `SegmentedItem::disabled_reason(..)`, `.disabled(true)` | A segment marked disabled, or every segment of a control marked disabled, draws at `Fields::DISABLED_OPACITY` (fill and content together, like a disabled button) with the not-allowed cursor and no hover wash, takes no click, and reports itself disabled with no click action in the accessibility tree. `disabled_reason(text)` also marks the segment disabled and shows `text` as its tooltip, for an option the product keeps visible but cannot offer right now. The arrow keys step over a disabled segment with `stepped_enabled_segment(&items, active, step)`, which counts only the enabled segments: from `A` with `B` disabled, a step of two lands on the second enabled segment after `A`, not on `B`. A segment keeps its reason while a locked control disables it. |
| `FieldRow` | `.disabled(true)` | Draws a form field row locked: dimmed at `Fields::DISABLED_OPACITY`, with the not-allowed cursor and a plate that hides the row's own content from the pointer, so a click on what the row draws does nothing. It blocks pointer input on the row, not the control inside it: the control keeps its own disabled state, and an overlay it opened (a `Dropdown`'s menu) is drawn outside the row and still takes clicks, so a host locking such a row disables the control or closes the overlay. The builder also covers the shapes the free functions draw (`.desc(..)`, `.label_width(..)`, `.vertical()`, then `.build(cx)`); every row records the `field-row` selector. |
| `SelectTrigger` | `.label_color(..)`, `.label_weight(..)`, `.label_font_family(..)` | Draw the label in a color, weight and font family of its own, such as an HTTP method chip (bold, mono, colored). A disabled trigger keeps the faded text color. Unset parts follow the field. |
| `SelectTrigger` | `SelectTrigger::new(id, label)` | The closed field of a select, drawn by the same code as a standard `Dropdown` trigger: cut-6 field, optional leading icon, truncated label, chevron. It is a button with the caller's click handler, `.disabled(..)`, `.focus_ring(..)` or `.focused(..)`, for a product that opens its own popup. The `Dropdown` entity itself is unchanged. |
| `Input` | `.padding(x, y)`, `.text_size(..)`, `.line_height(..)` | Replace the inner padding (the height then follows the line), the text size and the line height. With `appearance(false)`, set them to the values of the text the field replaces so nothing moves when the text turns into a field. Without them a bare input keeps gpui-component's size. |
| `Input` | `.font_weight(..)` | Replaces the text weight, normal in a framed field and medium in a bare one. Set a bare field to normal so it matches text drawn at the regular weight. |
| `Input` | `.error(Option<impl Into<SharedString>>)` | Reports a validation error: the frame's line becomes the danger role and the message is drawn as a 12 px danger caption one `Spacing::XXS` (6 px) under the field, with the element id `<field id>-error`. The caption is an alert node named by the message, so assistive technology announces it when it appears; the input's own node is untouched, because gpui has no description or invalid state for a node another element builds. The caption sits outside the frame, so the field keeps its size, and a field that fills its row still fills it. `None` draws the field exactly as without the option. |
| `InputState` | `decorate_input(&state, styled_ranges(ranges, style), cx)` | Styles UTF-8 byte ranges of a single-line field with a `HighlightStyle` (text color, background wash, underline) while it is edited as well as at rest. The ranges follow edits, do not grow when typing at an edge, and are not drawn while the field is masked. The returned `TextDecorationCollection` replaces them with `.set(..)` or removes them with `.clear(..)`; `set_value` removes ranges over the old text, so set them again after it. The product computes the ranges; Bolt never parses the text. A field without decorations renders as before. |
| `CommandSearch` | `.shrinkable(true)` | The field shrinks below its 420 px width in a narrow title bar, down to `ShellMetrics::COMMAND_SEARCH_MIN_WIDTH`, and the placeholder truncates with an ellipsis. By default it keeps its width and never shrinks. |
| `EmptyState` | `.primary_action(Button)` | A real button under the sentence for the one thing the region asks for. The key-hint actions stay below it. |
| `MenuItem` | `.checked(bool)` | A checkable row: `true` draws a tint check mark (`MenuMetrics::CHECK_ICON`) after the label. The row reports itself as a checkable menu item, toggled or not. A row without it is a plain menu item. |
| `Splitter` | `Splitter::new(id, SplitAxis)` | The resize gutter between two panes (see below). |
| `RailEntry` | `.count(n, BellUrgency)` | A count badge over the rail button's top-right corner, sized and colored like the notification bell's. A zero count shows none. |
| `StatusChip` | `.tone(color)`, `.wash(true)` | A tone draws the chip's text in that color, semibold. With the wash the chip also sits on the tone at `ShellMetrics::STATUS_CHIP_ALPHA` instead of the island fill. |
| `SearchField` | `SearchTrigger::new(id, placeholder)` | The field's closed state: the same frame, leading icon and metrics as the open field, drawing the placeholder and an optional `.shortcut("Ctrl F")` keycap instead of an input. `.on_open(..)` runs when it is clicked and the host opens the field; no `InputState` exists while it is closed. `.icon(..)` and `.width(..)` behave as on the field. |
| `ValueField` | `ValueField::new(value)` | A value drawn in the frame of a text field — cut 6, ground fill, line, `Fields::HEIGHT`, `Fields::PADDING_X` — without an `InputState` per value, for a page that shows many effective values. It is always one line: a `\r` or `\n` in the value reads as a space, so nothing draws past the field. `.mono(true)` draws it in the editor face, `.text_color(..)` overrides the strong value color, and `.id(..)` names the value node `<id>-value`. |
| `CodeView` | `CodeView::new(id, code)` | Read-only code with the rows, gutter and syntax seam of `ModalCode` (see below), drawable outside a dialog. It brings no frame or padding of its own, so the host places it in a `FieldWell` or any container for its font family and color; its size is its own — `CodeMetrics::FONT` (12.5 px), so a line always fits a `CodeMetrics::LINE_HEIGHT` (20 px) row whatever size the host sets around it. The caller's highlight ranges are sorted and merged before they reach the text, so an unordered or overlapping set colors the bytes it names. |
| `FieldWell` | `FieldWell::new()` | The framed, filled area a read-only block sits in: the ground fill cut 6 behind a line, with `.child(..)` content above it. `.id(..)` records the well, `.cut(..)` draws another cut, and it is a `Styled`/`ParentElement`, so layout chains as on a `div`. |

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

## Documents and timelines

Two primitives read a document's own content instead of drawing interface
chrome around it.

### Markdown documents

`MarkdownView::new(id, source)` renders a markdown document — headings,
paragraphs, lists, inline code, fenced code blocks, links and tables — in
Bolt's type and colors:

- The prose is the interface face at `FontSizes::BASE`; a heading takes a step
  of the scale per level, `FontSizes::TITLE` for a `#` down to
  `FontSizes::LABEL`; a fenced code block is the mono face at
  `MarkdownMetrics::CODE_FONT` on the raised fill, and inline code, table
  headers and the block itself sit on the raised fill; links use the link role
  and the blocks sit `MarkdownMetrics::PARAGRAPH_GAP` apart.
- `.selectable(true)` is the default, so dragging over the document copies the
  text it draws rather than the markdown source. `.selectable(false)` draws the
  same document without the selection.
- `.well(true)` puts the document on the inset ground inside the line border,
  `Spacing::MD` of side padding and `Spacing::SM` above and below, for a
  read-only preview that sits in a field. Without it the document sits on the
  pane's own ground.
- `.aria_label(..)` names the view, which is a group node.
- `MarkdownView::typography(cx)` returns the `MarkdownTypography` the view
  draws with — `body`, `code`, `headings` by level and `paragraph_gap` —
  resolved from the tokens and the active interface size, so a test checks the
  scale without a render.

The renderer and the parser are gpui-component's `TextView`; Bolt styles it and
never parses markdown itself. Links open with the platform's opener.

### Count histograms

`TimelineHistogram::new(id)` is the counting half of the timeline used above a
log or audit table: one bar per time bucket, sized by its share of the busiest
bucket, with a highlighted series stacked over the rest of the bucket.

- `.buckets(..)` or `.bucket(..)` take `TimelineBucket::new(total, highlighted)`.
  A negative count is no events, the highlighted part is cut at its bucket's
  total, and a bucket with any event draws at least a hairline.
- `.primary_tone(..)` (accent by default) and `.highlight_tone(..)` (danger by
  default) color the two parts; `.legend([..])` adds the swatch legend, each
  `TimelineLegend::new(label, tone)` a square and the caller's own word.
- The bars are `DocumentMetrics::TIMELINE_BAR_HEIGHT` tall and
  `DocumentMetrics::TIMELINE_BAR_GAP` apart, `TIMELINE_PADDING_Y` above and
  below, with the legend `TIMELINE_LEGEND_GAP` to their right, its entries
  `TIMELINE_LEGEND_ROW_GAP` apart and `TIMELINE_LEGEND_FONT` in size. The
  legend never grows past `TIMELINE_LEGEND_MAX_WIDTH`: a label too long for it
  truncates with an ellipsis instead of squeezing the bars.
- A strip holding so many buckets that the gaps no longer fit drops the gaps
  before it drops the bars: every bucket with events keeps at least a hairline,
  and the gap shrinks from `TIMELINE_BAR_GAP` down to nothing as the buckets
  crowd. A bucket with no events takes no width of its own.
- It paints no ground and no border: it is the strip inside a region the
  caller frames. It draws no interaction either — a document that lets the
  reader drag across the bars to zoom owns that gesture itself.

### Grid overflow

`GridOverflow` puts the grid's header row and its row list in one viewport
that scrolls sideways, for a pane narrower than the columns add up to. The
host keeps the column widths and the scroll offset, exactly as it does for
`render_header` and `render_rows`:

```rust
GridOverflow::new("events-grid", &horizontal, total_width)
    .header(render_header(..))
    .rows(uniform_list(..).track_scroll(&rows))
    .flex_1()
    .min_h_0()
```

- The header and the rows scroll on the one handle passed to `new`, so a
  column stays over its own cells at every offset, and the wheel's horizontal
  delta scrolls them together.
- A horizontal scrollbar draws in a `GridMetrics::SCROLLBAR_WIDTH` (12 px)
  strip along the bottom of the viewport, the way the data table draws it.
  While the columns are wider than the viewport the row list ends one strip
  above the edge, so the last row stays reachable at the bottom of the
  vertical scroll instead of sitting under the bar. A grid that fits reserves
  nothing: no bar, full height. The strip follows the width the grid is given,
  measured during layout, so narrowing or widening the pane needs no extra
  frame.
- Only the horizontal axis belongs to the viewport: the row list keeps its own
  vertical scroll, and a vertical wheel never turns into a sideways one.
- Column resize keeps working while the grid is scrolled. The header's handles
  move with the header, and `track_column_resize` resizes by pointer delta, so
  the scroll offset never enters the width. The host mounts it on its own root,
  around the overflow.

A grid that fits its viewport draws exactly as it does without the wrapper,
which is why the component is opt-in: a product wraps the grid only where the
pane can be narrower than the columns.

The sizing above is for a pane with a definite height. A page that lays out to
its content's height — a settings page, a form section, a document that
scrolls as a whole — has no such height, so a viewport that fills its pane
collapses to 0 px and its rows are clipped away. Those pages take
`.content_height()` and chain no height at all:

```rust
GridOverflow::new("tables-grid", &horizontal, total_width)
    .header(render_header(..))
    .rows(rows)
    .content_height()
```

- The viewport takes the height of its header and its rows instead of the
  height of a pane, and the rows are not asked to scroll vertically: a
  vertical wheel over the grid goes on to the page around it, because the
  component keeps `restrict_scroll_to_axis` on the viewport.
- It still scrolls sideways on the same handle, header and rows together, with
  the bar and its reserved strip only while the columns overflow. A grid that
  fits draws exactly as it does without the wrapper, at the height of its own
  rows.
- Without a pane there is no definite height for a container query to be given,
  so the width is read from the handle the viewport tracks, which carries the
  frame just laid out, and a frame drawn with a width that has since changed
  asks for one more. The strip settles in the frame after the page's width
  first reaches the grid — the same frame gpui's own scrollbar settles in — and
  a grid that fits draws as it does unwrapped on every frame.

### Option group

`OptionGroup::new(group, items, selected, on_change)` is the vertical list of
mutually-exclusive options a form uses where a segmented control is too narrow:
each `OptionGroupItem` is a label, an optional description drawn under it as one
line (12 px, muted, clipped when it is longer) and a per-option `.disabled(..)`. The group draws its own name
as the radio group, so `group` must be unique in the window; `.aria_label(..)`
names it for assistive technology, since a radio group has no text of its own.

The indicator reuses the checkbox's geometry: a `Fields::CHECKBOX_SIZE` (16 px)
box with `ChamferCut::KEYCAP`, the accent fill with a `Fields::RADIO_DOT` (6 px)
accent-foreground dot when chosen, the inset control line otherwise, and the
same tint focus ring, drawn outside the accent fill of the chosen option while
the group holds keyboard focus. Rows take
`Fields::CHECK_ROW_PADDING_Y` above and below, `Fields::CHECKBOX_GAP` between the
indicator and the text, and `Fields::CHECK_ROW_LINE_GAP` between the label and
its description, so an option lines up with a `Checkbox` row above it.

The group is one focusable stop, a radio group whose options are radio buttons
named by their labels, described by their descriptions, and toggled when chosen;
while the group holds keyboard focus, the chosen option is the focused node of
the radio group, so assistive technology follows the choice as it moves. Up and
Down (and Left and Right) move the choice through the enabled options, reporting
each one through `on_change`; Space and Enter report the current choice again,
so an owner that only acts on the reported id keeps working, unless that choice
is disabled; while nothing is chosen yet (`selected` is an id that matches no
option, such as an empty string) the first arrow key chooses the first enabled
option in that direction, so the keyboard can make the first choice; a pointer
press or the accessibility click action chooses the option it lands on; an arrow
key that would move past either end is left to the host, so a form can move to
the next field from there. Disabled options are
dimmed with `Fields::DISABLED_OPACITY`, are skipped by the arrow keys and answer
neither the pointer nor an accessibility click.

```rust
OptionGroup::new(
    "secret-mode",
    vec![
        OptionGroupItem::new("omit", "Leave secrets out")
            .description("The bundle lists them by name, empty."),
        OptionGroupItem::new("encrypt", "Encrypt secrets with a passphrase")
            .description("Values are sealed with a passphrase."),
    ],
    mode.clone(),
    move |id, _, cx| set_mode(id.clone(), cx),
)
.aria_label("Secrets in a bundle")
```

### Form popovers

`Popover` is the floating surface a screen opens over one thing it is about: a
title, a body of fields and a footer of buttons, on the cut-12 overlay surface
(`SurfaceRole::Overlay`) with the panel fill and the line-2 border. It is two
pieces, and a host owns both:

- `PopoverState` is the entity that holds the open flag, the anchor, the focus
  and the hover-intent timers. `open_at(bounds, window, cx)` opens the surface
  against a rectangle and `open(window, cx)` opens it where the host last
  pointed it; `close(window, cx)` closes it; `set_anchor(bounds)` moves what it
  points at; `set_initial_focus(handle)` names the field focus moves into;
  `on_dismiss(handler)` runs once it has closed and focus has come back.
- `Popover::new(id, state)` is the frame. It draws nothing while the state is
  closed, so a host renders it every frame. `.title(..)`, `.icon(..)`,
  `.body(..)` and `.footer(buttons)` build the parts, `.width(..)` replaces
  `PopoverMetrics::WIDTH`, and `.aria_label(..)` names the dialog, which
  otherwise takes its name from the title.

**Anchoring.** The anchor is a `Bounds<Pixels>` in window coordinates, so a
host anchors the surface to an element's laid-out bounds or to a range of text
it measured inside a field (a mark in an editable string). The surface lands
`PopoverMetrics::ANCHOR_GAP` under the rectangle, flips above it when there is
no room below, and is snapped inside the window keeping
`PopoverMetrics::WINDOW_MARGIN`; `place_popover` is that placement as a
function. With no anchor it stays where the host renders it, as `anchored()`
does for a trigger.

**Dismissal.** Escape closes the surface, and so does a click away from it.
Escape is caught twice, so a host's own key bindings cannot swallow it: the
frame answers the product's cancel action (the command a `CommandSet` maps
onto `Command::Cancel`) the way a `Dropdown` does, from where it sits on the
dispatch path — inside every shell that would answer that action, and outside
anything the body opened for itself, which gets the cancel first and leaves the
popover open. A raw Escape that no binding claimed is caught on the way down,
before anything inside the body sees it. The click-away layer is drawn under the
surface rather than around it, so a click in anything drawn above it — a
`Dropdown` list opened from the body, for one — is a click inside and leaves the
popover open; the same click still selects the dropdown row.

**Focus.** Opening moves focus to the handle given to
`set_initial_focus` (or to the surface itself, with nothing to focus) and
remembers the element that had it; closing gives that element focus back while
focus is still anywhere inside the surface, so a field the body focused instead
of the initial one still hands focus back, and Escape returns the user to the
trigger.

**Hover intent.** Built with `PopoverState::hover_intent(true)`, the surface
opens `HOVER_DELAY` (400 ms) after the pointer comes to rest on the anchor,
stays open while the pointer is over the anchor or the surface, and closes
`HOVER_GRACE` (120 ms) after it leaves both. The host reports the anchor's
hover with `hover_anchor(true/false, window, cx)`; once the surface is open it
follows the pointer itself. A surface dismissed while the pointer still rests
on its anchor waits for the pointer to leave and come back. Both timings are
exported constants, so a test advances the gpui clock by them.

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

## Syntax roles

`bolt::tokens::SyntaxColors` holds the code colors of each palette variant, and
`bolt::theme` maps every tree-sitter capture onto one of its roles:

| Captures | Role |
|---|---|
| `keyword` (bold) | `keyword`, the brand accent |
| `string`, `string.escape`, `string.regex`, `string.special`, `string.special.symbol` | `string` |
| `number`, `boolean`, `constant` | `number` |
| `comment`, `comment.doc` | `comment` |
| `type`, `constructor`, `enum`, `variable.special`, `tag` | `type_name` |
| `function` (bold) | `function` |
| `operator`, `punctuation` and its variants | `operator` |
| `variable`, `primary` | `plain` |
| `namespace` | `namespace` |
| `field` | `field` |
| `property`, `attribute` | `property` (`#A9A4B2` dark, `#5C5766` light) |

A dotted capture without a style of its own takes its prefix's
(`tag.doctype` reads as `tag`). The highlighter keeps no `string.special.key`
slot, so a grammar whose query captures object keys that way paints them as
strings; register it with keys captured as `property` instead (the bundled
`json` grammar does).

## Grammars

The code editor and every highlighted view look a language name up in one
shared registry. A product adds a grammar with `bolt::syntax::Grammar`, never
with gpui-component's highlighter types:

```rust
use bolt::syntax::Grammar;

Grammar::new("lua", tree_sitter_lua::LANGUAGE, tree_sitter_lua::HIGHLIGHTS_QUERY)
    .register();

let plaintext = Grammar::new("plaintext", tree_sitter_json::LANGUAGE, "");
plaintext.register();
plaintext.register_as("text");
```

`Grammar::new(name, language, highlights)` takes the grammar's
`tree_sitter::Language` (or its `LANGUAGE` constant) and its highlights query.
`.injections(query)` and `.locals(query)` add the other two queries. An
injection is highlighted with the grammar registered under the language name
it sets, so HTML's script elements take the `javascript` grammar once it is
registered; an unregistered injected language stays plain.
`bolt::syntax::is_registered(name)` answers whether a name resolves. Grammar
crates must target tree-sitter 0.26 (ABI 13 to 15); bolt re-exports the crate as
`bolt::syntax::tree_sitter`.

Bolt bundles six grammars, each behind its own feature so a product links only
what it asks for. `bolt::syntax::register_bundled()` registers every enabled
one; `bolt::syntax::bundled::json()` and its siblings return one grammar to
register on its own.

| Feature | Name | Crate |
|---|---|---|
| `syntax-json` | `json` (keys captured as `property`) | `tree-sitter-json` 0.24.8 |
| `syntax-javascript` | `javascript` | `tree-sitter-javascript` 0.23.1 |
| `syntax-bash` | `bash` | `tree-sitter-bash` 0.23.3 |
| `syntax-python` | `python` | `tree-sitter-python` 0.23.6 |
| `syntax-xml` | `xml` | `tree-sitter-xml` 0.7.0 |
| `syntax-html` | `html`, injecting `javascript` and `css` | `tree-sitter-html` 0.23.2 |

### Highlighting text a host draws

`bolt::syntax::highlight(language, text, cx)` returns the syntax runs of
read-only text as `Vec<(Range<usize>, HighlightStyle)>`: byte ranges, sorted and
non-overlapping, styled by the current theme. A language with no registered
grammar, or whose query does not compile, returns no runs. Recent results are
kept, so a view rebuilt every frame does not parse again.
`bolt::syntax::overlay(base, top)` lays a host's own runs over them; a top
run replaces the syntax style over its range:

```rust
let runs = bolt::syntax::overlay(
    bolt::syntax::highlight("json", &body, cx),
    search_matches.iter().map(|range| (range.clone(), match_style)),
);
StyledText::new(body).with_highlights(runs)
```

`CodeView::language(name)` colors a read-only code view the same way, line
numbers included, with its `.highlights(..)` drawn over the syntax;
`ModalCode` is a `CodeView` in a `FieldWell`, so a block inside a dialog and a
code view inside a page draw the same rows, gutter and colors. A code view
that carries an id records `<id>-line-<n>` for each numbered line and
`<id>-lines` for the column that scrolls sideways beneath a fixed gutter,
exactly as the modal block does.
`ModalCode::highlights_with_tone(ranges, tone)` adds ranges drawn bold in a
`BadgeTone`'s text color instead of the tint (for example `Warning` for a
reference that does not resolve); they draw over the tint highlights and the
syntax where they overlap:

```rust
ModalCode::new("codegen", code)
    .language("bash")
    .highlights_with_tone(resolved, BadgeTone::Accent)
    .highlights_with_tone(unresolved, BadgeTone::Warning)
    .line_numbers(true)
```

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

## What stays product-side, and why

The migration moved what a second app would need unchanged and left what only a
database client would. Each of these stays in `dbflux_components` for a reason,
not by omission:

| Stays | Why |
|---|---|
| `Value`, its formatting, and every extractor that reads it | The value model is the product's data contract. `extract_f64`, `group_key`, `time_axis_epoch_ms` and cell text all interpret a value, and formatting belongs with the owner of the data. |
| The data grid's header chrome | Titles, type labels, primary and foreign key icons, sort arrows, annotations and the document group row are host state. The cell painter moved; the header did not. |
| `ChartView` (the chart entity) and its data sources | `build` takes a `QueryResult`, and the orphan rule makes a Bolt-side impl for a product type impossible. A second pass could split the resolved-series renderer from the extraction. |
| The persisted `ChartSpec` composite and the saved-chart sources | They bind `&[ColumnMeta]` and `&QueryResult`; the spec's *value* types moved, the product's composite did not. |
| The document tree's state machine and events | Its public API is `Value`-typed and consumed untouched from the UI crates. |
| The semantic vocabulary's product half | The `Command` enum, `RunCommand`, the leader action, the keybinding registration. Bolt owns the table; the product owns the actions. |
| The icon catalogue (`AppIcon`), the accents, the adapters onto `ThemeSetting`/`AppStyle`, i18n, the SQL keyword table, `rail_nav` | Product identity, product settings, product language, product language *again*, and a controller that depends on the product's `Command` and menu composite. |

Two things follow from this that are easy to get wrong later:

- **A seam is not a place to smuggle a product concept.** When the point
  inspector's quick action was called `show-in-tree`, it moved as data
  (`quick_action_slug`) instead of keeping a name only a database tree explains.
  When the modal grew a SQL keyword list, it went back to the product and Bolt
  kept a statement block that highlights whatever ranges it is given.
- **When the type is the product's, the seam is data or a product-owned
  adapter — never a trait over the foreign type.** The orphan rule has forced
  this boundary four times now: the environment badge, the keymap vocabulary, the
  keyboard-coverage vocabulary and the form-navigation mapping. Expect it, and
  design the seam in that shape from the start.
- **A contract that cannot carry a rendering decision gets the missing fact, not
  a winner.** The grid's `dirty` flag exists because one `CellTone` could not say
  both what a value is and whether it holds an uncommitted edit, and picking one
  silently changed a pixel.

## Verifying a move

Every slice of this work followed the same shape, and a future one should:

1. Measure the coupling, not the line count. `vim` was 5,750 lines with one
   domain reference; the chart was 10,299 with forty-three, concentrated in two
   files.
2. Move or split according to that map, never by size.
3. Keep the product's public paths alive by re-exporting what moved.
4. Pin the surviving behaviour with a test that asserts the values the old code
   produced, then delete the losing side of any duplicate pair.
5. Close with the full gate: `cargo check --workspace --all-targets`,
   `scripts/lint.py fmt --check`, `clippy`, `mouse-down`, the crate suites and
   `crates/bolt/tests/guardrail.rs`.

What that gate does not cover is pixels. The tests assert the decisions a painter
makes — tone, colour role, italic, strike, mark, wash, chevron — and element-tree
selectors let them read the real render, but no test compares a rendered frame
against a reference. A visual pass over the running app is still the check for a
change to the design system, and it is worth doing before a release.
