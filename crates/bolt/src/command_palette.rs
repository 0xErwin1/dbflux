//! The command palette: a searchable overlay of everything a surface can
//! open or run.
//!
//! The engine owns the generic half of a palette: the fuzzy query with its
//! `>` commands and `@` tables scopes, the section grouping, the key cursor
//! and its scroll, the match highlight, the footer hints and the card on
//! the shared scrim.
//!
//! What a product owns stays with the product: the items it offers — their
//! names, kinds, icons and the selection each one yields — and the keymap
//! its keys run. It supplies them through two seams, both traits the
//! product implements:
//!
//! - [`PaletteEntry`], one impl per item, carries an item's searchable
//!   text, display label, qualifier, icon, section, keycaps and selection.
//! - [`PaletteHost`], one impl per app, names the key context the product's
//!   keymap binds the palette keys in, answers the keycap label of a
//!   semantic command, and registers the product's run-command action on
//!   the palette's element.
//!
//! The words the engine draws come from [`crate::strings`]. A palette
//! rendered without an installed translator draws no words, and without an
//! installed [`crate::keymap::CommandSet`] answers no keys.

use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder as _;
use gpui::*;
use gpui_component::ActiveTheme as _;
use gpui_component::IconName;

use crate::controls::{GpuiInput as Input, InputEvent, InputState};
use crate::fonts::editor_family;
use crate::icon::IconSource;
use crate::keymap::{Command as KeymapCommand, CommandSet};
use crate::primitives::{Chamfer, Icon, Kbd, SurfaceRole, Text, inspect_surface_role, overlay_bg};
use crate::strings;
use crate::tokens::{ChamferCut, ChromeColors, PaletteMetrics};

/// The keycaps of a shortcut string like `"ctrl-shift-k"`, one per keycap.
///
/// Recognizes the canonical modifier tokens used in `KeyBinding` strings
/// (`ctrl`, `shift`, `alt`, `cmd`) and capitalizes them for display. The
/// final segment is the key name: uppercased after a modifier (`Ctrl E`),
/// kept as typed on its own (`x`).
pub fn palette_shortcut_parts(shortcut: &str) -> Vec<SharedString> {
    let tokens: Vec<&str> = shortcut.split('-').collect();

    if tokens.is_empty() {
        return Vec::new();
    }

    let mut parts: Vec<SharedString> = Vec::with_capacity(tokens.len());
    let last_idx = tokens.len() - 1;

    for (idx, token) in tokens.iter().enumerate() {
        let display = if idx == last_idx && last_idx == 0 {
            token.to_string()
        } else if idx == last_idx {
            token.to_uppercase()
        } else {
            match token.to_lowercase().as_str() {
                "ctrl" => "Ctrl".to_string(),
                "shift" => "Shift".to_string(),
                "alt" => "Alt".to_string(),
                "cmd" | "command" | "super" | "platform" => "Cmd".to_string(),
                other => {
                    let mut chars = other.chars();
                    match chars.next() {
                        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                        None => String::new(),
                    }
                }
            }
        };

        parts.push(SharedString::from(display));
    }

    parts
}

/// The handler the product's run-command registration invokes with the
/// action id the dispatched action carries. It answers whether the palette
/// handled the command, so the registration propagates the ones it did not.
pub type RunCommandHandler = Arc<dyn Fn(&str, &mut Window, &mut App) -> bool>;

/// The product-installed facts a palette reads, one impl per app.
///
/// Implement it once next to the product's palette items and name it in the
/// palette's type, the way [`CommandPalette`] is constructed at the old
/// path. Every method mirrors what the palette used to read from the
/// product's keymap directly.
pub trait PaletteHost: 'static {
    /// The event the palette emits when it closes without a selection: the
    /// type the product's workspace subscribes to.
    type Closed: Default + 'static;

    /// The key context the product's keymap binds the palette's keys in.
    fn key_context() -> &'static str;

    /// The keycap label of the key that runs `command` while the palette is
    /// open, or `None` when nothing is bound for it there.
    fn key_label(command: KeymapCommand) -> Option<SharedString>;

    /// Registers on `element` the listener for the product's run-command
    /// action: it invokes `on_command` with the action's command id and
    /// propagates when `on_command` reports the palette did not answer it.
    fn attach_run_command(element: Stateful<Div>, on_command: RunCommandHandler) -> Stateful<Div>;
}

/// One item of the product's palette, as the engine sees it: the facts the
/// query, the grouping and the row render read, and the selection the item
/// yields when it is confirmed.
///
/// The product keeps its own item vocabulary and implements this trait on
/// it; every method mirrors what the palette used to compute inline.
pub trait PaletteEntry: 'static {
    /// What the palette emits when the user confirms this item.
    type Selection: 'static;

    /// Text searched by the fuzzy matcher. Embed the product's kind words
    /// so an item matches on its kind as well as its name.
    fn search_text(&self) -> String;
    /// The row's `(category_label, display_name)`.
    fn display_label(&self) -> (String, String);
    /// Type priority for tiebreaking (lower = higher priority).
    fn type_priority(&self) -> u8;
    /// Right-aligned mono qualifier: where a resource lives, a script's
    /// folder, a chart's connection.
    fn qualifier(&self) -> Option<String>;
    /// Muted qualifier drawn right after a command's name: its category.
    fn inline_qualifier(&self) -> Option<SharedString> {
        None
    }
    /// Leading icon and its color, when the item carries a tone of its
    /// own; toneless rows draw the icon muted, or tinted while selected.
    fn icon(&self, cx: &App) -> (IconSource, Option<Hsla>);
    /// The section the item groups under.
    fn section(&self) -> PaletteSection;
    /// The keycaps the row shows, one per keycap. Commands show the keys
    /// their product command is bound to; other kinds show none.
    fn keycaps(&self) -> Vec<SharedString> {
        Vec::new()
    }
    /// The selection the item yields when confirmed. `new_tab` is `true`
    /// when the user asked for another tab even if one already shows it.
    fn select(&self, new_tab: bool) -> Self::Selection;
}

/// Section grouping for the rendered palette list.
///
/// The order here is the visual order in the palette. Sections render only
/// when at least one matching item exists for that section. Section headers
/// themselves are not selectable — they are a render-only concern.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaletteSection {
    Commands,
    Connections,
    Charts,
    Tables,
    Scripts,
}

impl PaletteSection {
    const ORDER: [PaletteSection; 5] = [
        Self::Commands,
        Self::Connections,
        Self::Charts,
        Self::Tables,
        Self::Scripts,
    ];

    /// The section's header text, through the text seam.
    fn label(self, cx: &App) -> SharedString {
        strings::text(
            cx,
            match self {
                Self::Connections => "palette.section.connections",
                Self::Commands => "palette.section.commands",
                Self::Charts => "palette.section.charts",
                Self::Tables => "palette.section.tables",
                Self::Scripts => "palette.section.scripts",
            },
        )
    }

    /// Visual ordering key, the position in [`Self::ORDER`], so keyboard
    /// navigation walks the list in the same order the user sees it.
    fn sort_order(self) -> usize {
        Self::ORDER
            .iter()
            .position(|section| *section == self)
            .unwrap_or(Self::ORDER.len())
    }
}

/// What a query searches, chosen by its first character: `>` keeps only
/// commands, `@` only tables and collections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaletteScope {
    All,
    Commands,
    Tables,
}

impl PaletteScope {
    /// Splits a typed query into its scope and the text to match.
    fn parse(query: &str) -> (Self, &str) {
        if let Some(rest) = query.strip_prefix('>') {
            (Self::Commands, rest.trim_start())
        } else if let Some(rest) = query.strip_prefix('@') {
            (Self::Tables, rest.trim_start())
        } else {
            (Self::All, query)
        }
    }

    fn includes(self, section: PaletteSection) -> bool {
        match self {
            Self::All => true,
            Self::Commands => section == PaletteSection::Commands,
            Self::Tables => section == PaletteSection::Tables,
        }
    }
}

/// The items a typed query keeps, with their fuzzy-match scores. A leading
/// `>` or `@` narrows the search to commands or to tables and collections.
fn filter_items<T: PaletteEntry>(
    items: &[T],
    matcher: &SkimMatcherV2,
    query: &str,
) -> Vec<FilteredItem> {
    let (scope, text) = PaletteScope::parse(query);

    items
        .iter()
        .enumerate()
        .filter(|(_, item)| scope.includes(item.section()))
        .filter_map(|(index, item)| {
            if text.is_empty() {
                return Some(FilteredItem { index, score: 0 });
            }

            matcher
                .fuzzy_match(&item.search_text(), text)
                .map(|score| FilteredItem { index, score })
        })
        .collect()
}

/// Orders `filtered` the way the list shows it: by section, then by score,
/// then by kind. A mixed query keeps only its best
/// [`MIXED_QUERY_COMMAND_LIMIT`] commands so every kind it matches shows.
fn arrange_results<T: PaletteEntry>(
    items: &[T],
    mut filtered: Vec<FilteredItem>,
    query: &str,
) -> Vec<FilteredItem> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the three construction sites of `FilteredItem` (`filter_items`, \
                  `open_with_items` and `toggle`) each pair the index with \
                  `.enumerate()` over the same `items` slice, so `a.index` and \
                  `b.index` stay below `items.len()`"
    )]
    filtered.sort_by(|a, b| {
        let item_a = &items[a.index];
        let item_b = &items[b.index];
        let section_a = item_a.section().sort_order();
        let section_b = item_b.section().sort_order();

        section_a
            .cmp(&section_b)
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| item_a.type_priority().cmp(&item_b.type_priority()))
    });

    let (scope, text) = PaletteScope::parse(query);

    if scope == PaletteScope::All && !text.trim().is_empty() {
        let mut commands_kept = 0;

        #[expect(
            clippy::indexing_slicing,
            reason = "the three construction sites of `FilteredItem` (`filter_items`, \
                      `open_with_items` and `toggle`) each pair the index with \
                      `.enumerate()` over the same `items` slice, so \
                      `filtered_item.index` stays below `items.len()`"
        )]
        filtered.retain(|filtered_item| {
            if items[filtered_item.index].section() != PaletteSection::Commands {
                return true;
            }

            commands_kept += 1;
            commands_kept <= MIXED_QUERY_COMMAND_LIMIT
        });
    }

    filtered
}

struct FilteredItem {
    index: usize,
    score: i64,
}

const VISIBLE_ITEMS: usize = 8;

/// Most commands a mixed query (no `>` or `@` prefix) keeps, so the
/// connections and tables it also matches stay on screen.
const MIXED_QUERY_COMMAND_LIMIT: usize = 3;

/// Byte ranges of `name` that fuzzy-match `query`, merged into runs, for the
/// tinted match highlight. Empty when the query is empty or only matched
/// other fields of the item.
fn match_ranges(matcher: &SkimMatcherV2, name: &str, query: &str) -> Vec<Range<usize>> {
    if query.is_empty() {
        return Vec::new();
    }

    let Some((_, char_indices)) = matcher.fuzzy_indices(name, query) else {
        return Vec::new();
    };

    let mut ranges: Vec<Range<usize>> = Vec::new();

    for (char_index, (byte_index, character)) in name.char_indices().enumerate() {
        if !char_indices.contains(&char_index) {
            continue;
        }

        let byte_end = byte_index + character.len_utf8();

        match ranges.last_mut() {
            Some(last) if last.end == byte_index => last.end = byte_end,
            _ => ranges.push(byte_index..byte_end),
        }
    }

    ranges
}

/// Where the chosen item opens: `Enter` reuses the tab already showing it,
/// the new-tab chord (`Ctrl ↵`) always opens another one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenPlacement {
    ReuseTab,
    NewTab,
}

/// Render row produced by section grouping.
///
/// `Item.display_idx` is the position in the filtered list (i.e. the value
/// `selected_index` compares against). `palette_idx` is the index into
/// `items`. `SectionHeader` rows are not selectable.
enum PaletteRow {
    SectionHeader(SharedString),
    Item {
        display_idx: usize,
        palette_idx: usize,
    },
}

/// The palette engine over the product's items (`T`) and keymap facts
/// (`H`).
pub struct CommandPalette<T: PaletteEntry, H: PaletteHost> {
    visible: bool,
    items: Vec<T>,
    filtered: Vec<FilteredItem>,
    selected_index: usize,
    scroll_offset: usize,
    input_state: Entity<InputState>,
    matcher: SkimMatcherV2,
    /// The typed query without its scope prefix, for the match highlight.
    match_query: String,
    host: PhantomData<fn() -> H>,
}

impl<T: PaletteEntry, H: PaletteHost> CommandPalette<T, H>
where
    Self: EventEmitter<T::Selection> + EventEmitter<H::Closed>,
{
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx).placeholder(strings::text(cx, "palette.search.placeholder"))
        });

        cx.subscribe_in(
            &input_state,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    let query = this.input_state.read(cx).value().to_string();
                    this.update_filter(&query, cx);
                }
                // The search input owns Enter and the primary-modifier Enter
                // while it has focus, so the new-tab chord arrives here as a
                // secondary Enter rather than through the palette keymap.
                InputEvent::PressEnter { secondary, .. } => {
                    let placement = if *secondary {
                        OpenPlacement::NewTab
                    } else {
                        OpenPlacement::ReuseTab
                    };

                    this.execute_selected(placement, window, cx);
                }
                _ => {}
            },
        )
        .detach();

        Self {
            visible: false,
            items: Vec::new(),
            filtered: Vec::new(),
            selected_index: 0,
            scroll_offset: 0,
            input_state,
            matcher: SkimMatcherV2::default(),
            match_query: String::new(),
            host: PhantomData,
        }
    }

    /// Set items and reset filter state. Called on each toggle.
    pub fn open_with_items(&mut self, items: Vec<T>, window: &mut Window, cx: &mut Context<Self>) {
        self.items = items;
        self.filtered = self
            .items
            .iter()
            .enumerate()
            .map(|(index, _)| FilteredItem { index, score: 0 })
            .collect();

        self.visible = true;
        self.selected_index = 0;
        self.scroll_offset = 0;
        self.match_query.clear();
        self.filtered = arrange_results(&self.items, std::mem::take(&mut self.filtered), "");

        self.input_state.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        cx.notify();
    }

    pub fn toggle(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = !self.visible;

        if self.visible {
            self.input_state.update(cx, |state, cx| {
                state.set_value("", window, cx);
                state.focus(window, cx);
            });
            self.selected_index = 0;
            self.scroll_offset = 0;
            self.match_query.clear();
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
            self.filtered = arrange_results(&self.items, std::mem::take(&mut self.filtered), "");
        }

        cx.notify();
    }

    pub fn hide(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.emit(H::Closed::default());
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    fn update_filter(&mut self, query: &str, cx: &mut Context<Self>) {
        let filtered = filter_items(&self.items, &self.matcher, query);
        self.filtered = arrange_results(&self.items, filtered, query);
        self.match_query = PaletteScope::parse(query).1.to_string();

        self.selected_index = 0;
        self.scroll_offset = 0;
        cx.notify();
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if !self.filtered.is_empty() {
            self.selected_index = (self.selected_index + 1) % self.filtered.len();
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if !self.filtered.is_empty() {
            self.selected_index = if self.selected_index == 0 {
                self.filtered.len() - 1
            } else {
                self.selected_index - 1
            };
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn ensure_selected_visible(&mut self) {
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        }
        if self.selected_index >= self.scroll_offset + VISIBLE_ITEMS {
            self.scroll_offset = self.selected_index - VISIBLE_ITEMS + 1;
        }
    }

    fn scroll_down(&mut self, cx: &mut Context<Self>) {
        if self.selected_index < self.filtered.len().saturating_sub(1) {
            self.selected_index += 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn scroll_up(&mut self, cx: &mut Context<Self>) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    /// Answers one run-command action id through the installed
    /// [`CommandSet`], the way the dropdown does. Returns whether the
    /// palette handled it; the host registration propagates when it did
    /// not.
    fn handle_run_command(
        &mut self,
        command_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(command) = cx
            .try_global::<CommandSet>()
            .and_then(|keys| keys.command(command_id))
        else {
            return false;
        };

        match command {
            KeymapCommand::SelectPrev => self.select_prev(cx),
            KeymapCommand::SelectNext => self.select_next(cx),
            KeymapCommand::Cancel => self.hide(cx),
            KeymapCommand::Execute => self.execute_selected(OpenPlacement::ReuseTab, window, cx),
            KeymapCommand::OpenInNewTab => self.execute_selected(OpenPlacement::NewTab, window, cx),
            KeymapCommand::ExpandCollapse | KeymapCommand::FocusSearch | KeymapCommand::Save => {
                return false;
            }
        }

        true
    }

    fn execute_selected(
        &mut self,
        placement: OpenPlacement,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let new_tab = placement == OpenPlacement::NewTab;

        if let Some(filtered) = self.filtered.get(self.selected_index)
            && let Some(item) = self.items.get(filtered.index)
        {
            let selection = item.select(new_tab);

            self.visible = false;
            cx.emit(selection);
            cx.notify();
        }
    }

    fn render_palette_item(
        &self,
        display_idx: usize,
        item: &T,
        is_selected: bool,
        cx: &App,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let (_, name) = item.display_label();

        let (icon, icon_tone) = item.icon(cx);
        let icon_color = match icon_tone {
            Some(color) => color,
            None if is_selected => tint,
            None => theme.muted_foreground,
        };

        let name_color = if is_selected {
            ChromeColors::strong(theme)
        } else {
            theme.foreground
        };

        let highlights: Vec<(Range<usize>, HighlightStyle)> =
            match_ranges(&self.matcher, &name, &self.match_query)
                .into_iter()
                .map(|range| {
                    (
                        range,
                        HighlightStyle {
                            color: Some(tint),
                            font_weight: Some(FontWeight::BOLD),
                            ..Default::default()
                        },
                    )
                })
                .collect();

        let keycaps: Vec<AnyElement> = item
            .keycaps()
            .into_iter()
            .map(|part| Kbd::new(part).into_any_element())
            .collect();

        let wash = tint.opacity(PaletteMetrics::SELECTED_ALPHA);

        div()
            .id(("cmd", display_idx))
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::ROW_GAP)
            .h(PaletteMetrics::ROW_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .cursor_pointer()
            .when(is_selected, |row| {
                row.bg(wash).child(
                    div()
                        .absolute()
                        .left_0()
                        .top_0()
                        .bottom_0()
                        .w(PaletteMetrics::SELECTION_BAR)
                        .bg(tint),
                )
            })
            .when(!is_selected, |row| {
                row.hover(|row| row.bg(theme.list_hover))
            })
            .child(
                Icon::new(icon)
                    .size(PaletteMetrics::ROW_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(PaletteMetrics::ROW_FONT)
                    .text_color(name_color)
                    .child(StyledText::new(name).with_highlights(highlights)),
            )
            .when_some(item.inline_qualifier(), |row, qualifier| {
                row.child(
                    div()
                        .flex_shrink_0()
                        .text_size(PaletteMetrics::QUALIFIER_FONT)
                        .text_color(theme.muted_foreground)
                        .child(qualifier),
                )
            })
            .child(div().flex_1())
            .when_some(item.qualifier(), |row, qualifier| {
                row.child(
                    div()
                        .flex_shrink_0()
                        .font_family(editor_family(cx))
                        .text_size(PaletteMetrics::QUALIFIER_FONT)
                        .text_color(theme.muted_foreground)
                        .child(qualifier),
                )
            })
            .children(keycaps)
    }

    fn render_search_row(&self, cx: &App) -> Div {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::SEARCH_GAP)
            .h(PaletteMetrics::SEARCH_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(IconSource::from(IconName::Search))
                    .size(PaletteMetrics::SEARCH_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Input::new(&self.input_state)
                        .appearance(false)
                        .text_size(PaletteMetrics::QUERY_FONT)
                        .text_color(ChromeColors::strong(theme))
                        .px_0(),
                ),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .font_family(editor_family(cx))
                    .text_size(PaletteMetrics::COUNT_FONT)
                    .text_color(theme.muted_foreground)
                    .child(format!("{} / {}", self.filtered.len(), self.items.len())),
            )
            .when_some(H::key_label(KeymapCommand::Cancel), |header, label| {
                header.child(Kbd::new(label))
            })
    }

    fn render_footer(cx: &App) -> Div {
        let theme = cx.theme();
        let navigate_keys = palette_navigate_label(
            H::key_label(KeymapCommand::SelectPrev),
            H::key_label(KeymapCommand::SelectNext),
        );

        let hint = |label: String| div().flex_shrink_0().child(label);

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PaletteMetrics::FOOTER_GAP)
            .h(PaletteMetrics::FOOTER_HEIGHT)
            .px(PaletteMetrics::PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .text_size(PaletteMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .when_some(navigate_keys, |footer, keys| {
                footer.child(Kbd::new(keys)).child(hint(
                    strings::text(cx, "palette.footer.navigate").to_string(),
                ))
            })
            .when_some(H::key_label(KeymapCommand::Execute), |footer, label| {
                footer
                    .child(Kbd::new(footer_key_label(&label)))
                    .child(hint(strings::text(cx, "palette.footer.run").to_string()))
            })
            .when_some(
                H::key_label(KeymapCommand::OpenInNewTab),
                |footer, label| {
                    footer.child(Kbd::new(label)).child(hint(
                        strings::text(cx, "palette.footer.open_in_new_tab").to_string(),
                    ))
                },
            )
            .child(div().flex_1())
            .child(Kbd::new(">"))
            .child(hint(
                strings::text(cx, "palette.footer.commands_only").to_string(),
            ))
            .child(Kbd::new("@"))
            .child(hint(
                strings::text(cx, "palette.footer.tables_only").to_string(),
            ))
    }
}

/// The footer's move keycap: the previous and next keys side by side in one
/// keycap (`↑↓`), or whichever of them is bound.
fn palette_navigate_label(
    previous: Option<SharedString>,
    next: Option<SharedString>,
) -> Option<SharedString> {
    match (previous, next) {
        (None, None) => None,
        (previous, next) => Some(
            previous
                .into_iter()
                .chain(next)
                .map(|label| label.to_string())
                .collect::<String>()
                .into(),
        ),
    }
}

/// A lone Enter reads `↵` in the footer, like the new-tab chord next to it
/// (`Ctrl ↵`).
fn footer_key_label(label: &str) -> SharedString {
    if label == "Enter" {
        "\u{21b5}".into()
    } else {
        label.to_string().into()
    }
}

// The product implements `EventEmitter` for its concrete palette at the old
// path, for its own selection and its own closed event (the host's
// [`PaletteHost::Closed`]): one generic impl over `T::Selection` would
// conflict with the closed event, the way two blanket impls of one event
// trait always do.

impl<T: PaletteEntry, H: PaletteHost> Render for CommandPalette<T, H>
where
    Self: EventEmitter<T::Selection> + EventEmitter<H::Closed>,
{
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();

        // Build the windowed list (scroll_offset..+VISIBLE_ITEMS) then group
        // the resulting items by section. Section headers are interleaved
        // before the first item of each section but are NOT counted in the
        // `display_idx` that compares against `selected_index`.
        let windowed: Vec<(usize, usize)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(display_idx, filtered)| (display_idx, filtered.index))
            .collect();

        let mut rows: Vec<PaletteRow> =
            Vec::with_capacity(windowed.len() + PaletteSection::ORDER.len());

        for section in PaletteSection::ORDER {
            let mut header_pushed = false;

            for &(display_idx, palette_idx) in &windowed {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`palette_idx` is a `FilteredItem` index; the three \
                              construction sites of `FilteredItem` (`filter_items`, \
                              `open_with_items` and `toggle`) each pair the index \
                              with `.enumerate()` over `self.items`, so \
                              `palette_idx` stays below `self.items.len()`"
                )]
                if self.items[palette_idx].section() != section {
                    continue;
                }

                if !header_pushed {
                    rows.push(PaletteRow::SectionHeader(section.label(cx)));
                    header_pushed = true;
                }

                rows.push(PaletteRow::Item {
                    display_idx,
                    palette_idx,
                });
            }
        }

        let list_rows: Vec<AnyElement> = rows
            .into_iter()
            .map(|row| match row {
                PaletteRow::SectionHeader(label) => div()
                    .pt(PaletteMetrics::SECTION_PADDING_TOP)
                    .pb(PaletteMetrics::SECTION_PADDING_BOTTOM)
                    .px(PaletteMetrics::PADDING_X)
                    .child(Text::label(label).font_size(PaletteMetrics::SECTION_FONT))
                    .into_any_element(),
                PaletteRow::Item {
                    display_idx,
                    palette_idx,
                } => {
                    let is_selected = display_idx == self.selected_index;

                    #[expect(
                        clippy::indexing_slicing,
                        reason = "`palette_idx` is a `FilteredItem` index; the three \
                                  construction sites of `FilteredItem` (`filter_items`, \
                                  `open_with_items` and `toggle`) each pair the index \
                                  with `.enumerate()` over `self.items`, so \
                                  `palette_idx` stays below `self.items.len()`"
                    )]
                    self.render_palette_item(display_idx, &self.items[palette_idx], is_selected, cx)
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.selected_index = display_idx;
                            this.execute_selected(OpenPlacement::ReuseTab, window, cx);
                        }))
                        .into_any_element()
                }
            })
            .collect();

        let surface = inspect_surface_role(SurfaceRole::Modal);
        let card_shape = Chamfer::new(ChamferCut::CARD)
            .fill(surface.fill.resolve(theme))
            .border(surface.border.resolve(theme));

        let max_height = window.viewport_size().height - PaletteMetrics::TOP_OFFSET * 2.0;

        let handle = cx.entity().downgrade();
        let overlay = div()
            .id("command-palette-overlay")
            .absolute()
            .flex()
            .justify_center()
            .items_start()
            .pt(PaletteMetrics::TOP_OFFSET)
            .bg(overlay_bg(theme))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                }),
            );

        // The product's keymap binds its CommandPalette layer to its
        // run-command action; the host registers the listener, and anything
        // the palette does not answer goes on to the workspace.
        let overlay = H::attach_run_command(
            overlay,
            Arc::new(move |command_id, window, cx| {
                let Some(entity) = handle.upgrade() else {
                    return false;
                };

                entity.update(cx, |this, cx| {
                    this.handle_run_command(command_id, window, cx)
                })
            }),
        );

        overlay
            .key_context(H::key_context())
            .child(
                div()
                    .id("command-palette-container")
                    .relative()
                    .w(PaletteMetrics::WIDTH)
                    .max_w_full()
                    .max_h(max_height)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(card_shape)
                    .child(self.render_search_row(cx))
                    .child(
                        div()
                            .id("command-palette-list")
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h_0()
                            .overflow_y_hidden()
                            .pb(PaletteMetrics::LIST_PADDING_BOTTOM)
                            .on_scroll_wheel(cx.listener(
                                |this, event: &ScrollWheelEvent, _window, cx| {
                                    let delta = event.delta.pixel_delta(px(1.0));
                                    if delta.y < px(0.0) {
                                        this.scroll_down(cx);
                                    } else if delta.y > px(0.0) {
                                        this.scroll_up(cx);
                                    }
                                },
                            ))
                            .children(list_rows)
                            .when(self.filtered.is_empty(), |list| {
                                list.child(
                                    div()
                                        .flex()
                                        .justify_center()
                                        .h(PaletteMetrics::SEARCH_HEIGHT)
                                        .items_center()
                                        .child(
                                            Text::body(
                                                strings::text(cx, "palette.empty").to_string(),
                                            )
                                            .muted_foreground(),
                                        ),
                                )
                            }),
                    )
                    .child(Self::render_footer(cx)),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use fuzzy_matcher::skim::SkimMatcherV2;
    use gpui::{
        AppContext as _, Entity, EventEmitter, ParentElement as _, Styled as _, TestAppContext, div,
    };
    use gpui_component::IconName;

    use super::{
        CommandPalette, MIXED_QUERY_COMMAND_LIMIT, OpenPlacement, PaletteEntry, PaletteHost,
        PaletteScope, PaletteSection, arrange_results, filter_items, footer_key_label,
        match_ranges, palette_navigate_label, palette_shortcut_parts,
    };
    use crate::icon::IconSource;
    use crate::keymap::{Command as KeymapCommand, CommandSet};

    fn command_palette_source() -> String {
        let source = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/command_palette.rs"
        ))
        .unwrap_or_else(|error| panic!("failed to read command_palette.rs: {error}"));

        // Extract only the `impl Render` body. The marker is anchored on the
        // ` {` brace so it matches the real implementation and not the string
        // literals inside this helper, and the slice stops at the test module
        // so the assertions never inspect their own source text.
        let impl_start = source
            .find("impl<T: PaletteEntry, H: PaletteHost> Render for CommandPalette<T, H>")
            .expect("command_palette.rs should contain a render implementation");
        let after_impl = &source[impl_start..];
        let render_end = after_impl.find("#[cfg(test)]").unwrap_or(after_impl.len());

        after_impl[..render_end].to_string()
    }

    /// A stand-in item with the four kinds the scope and grouping tests
    /// exercise, mirroring the product's command/connection/resource/script
    /// priorities.
    #[derive(Clone)]
    struct TestItem {
        kind: TestKind,
        name: &'static str,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TestKind {
        Command,
        Connection,
        Resource,
        Script,
    }

    impl TestItem {
        fn command(name: &'static str) -> Self {
            Self {
                kind: TestKind::Command,
                name,
            }
        }

        fn connection(name: &'static str) -> Self {
            Self {
                kind: TestKind::Connection,
                name,
            }
        }

        fn resource(name: &'static str) -> Self {
            Self {
                kind: TestKind::Resource,
                name,
            }
        }

        fn script(name: &'static str) -> Self {
            Self {
                kind: TestKind::Script,
                name,
            }
        }
    }

    impl PaletteEntry for TestItem {
        type Selection = (TestKind, bool);

        fn search_text(&self) -> String {
            self.name.to_string()
        }

        fn qualifier(&self) -> Option<String> {
            None
        }

        fn display_label(&self) -> (String, String) {
            (String::new(), self.name.to_string())
        }

        fn type_priority(&self) -> u8 {
            match self.kind {
                TestKind::Command => 0,
                TestKind::Connection => 1,
                TestKind::Resource => 3,
                TestKind::Script => 4,
            }
        }

        fn icon(&self, _cx: &gpui::App) -> (IconSource, Option<gpui::Hsla>) {
            (IconSource::from(IconName::Map), None)
        }

        fn section(&self) -> PaletteSection {
            match self.kind {
                TestKind::Command => PaletteSection::Commands,
                TestKind::Connection => PaletteSection::Connections,
                TestKind::Resource => PaletteSection::Tables,
                TestKind::Script => PaletteSection::Scripts,
            }
        }

        fn select(&self, new_tab: bool) -> (TestKind, bool) {
            (self.kind, new_tab)
        }
    }

    /// The test palette's closed event, as a product would own one.
    #[derive(Default)]
    struct TestClosed;

    // The events a test palette emits, as a product would implement them.
    impl EventEmitter<(TestKind, bool)> for CommandPalette<TestItem, TestHost> {}
    impl EventEmitter<TestClosed> for CommandPalette<TestItem, TestHost> {}

    /// A stand-in host: the keycap and dispatch seams answer nothing, so
    /// the lookups under test exercise the no-facts paths.
    struct TestHost;

    impl PaletteHost for TestHost {
        type Closed = TestClosed;

        fn key_context() -> &'static str {
            "CommandPalette"
        }

        fn key_label(_command: KeymapCommand) -> Option<gpui::SharedString> {
            None
        }

        fn attach_run_command(
            element: gpui::Stateful<gpui::Div>,
            _on_command: super::RunCommandHandler,
        ) -> gpui::Stateful<gpui::Div> {
            element
        }
    }

    /// A no-op stand-in for a product's attach hook: the set under test is
    /// never attached to an element.
    fn test_attach(
        element: gpui::Stateful<gpui::Div>,
        _handler: crate::keymap::KeyCommandHandler,
    ) -> gpui::Stateful<gpui::Div> {
        element
    }

    #[test]
    fn shortcut_parts_give_one_keycap_per_key() {
        assert_eq!(
            palette_shortcut_parts("ctrl-shift-2"),
            vec!["Ctrl", "Shift", "2"]
        );
        assert_eq!(palette_shortcut_parts("ctrl-e"), vec!["Ctrl", "E"]);
        assert_eq!(palette_shortcut_parts("x"), vec!["x"]);
    }

    #[test]
    fn commands_come_first_then_connections_then_tables() {
        assert!(PaletteSection::Commands.sort_order() < PaletteSection::Connections.sort_order());
        assert!(PaletteSection::Connections.sort_order() < PaletteSection::Tables.sort_order());
        assert!(PaletteSection::Tables.sort_order() < PaletteSection::Scripts.sort_order());
    }

    #[test]
    fn scope_prefix_is_split_from_the_query() {
        assert_eq!(PaletteScope::parse("orders"), (PaletteScope::All, "orders"));
        assert_eq!(
            PaletteScope::parse("> export"),
            (PaletteScope::Commands, "export")
        );
        assert_eq!(PaletteScope::parse("@ord"), (PaletteScope::Tables, "ord"));
    }

    #[test]
    fn command_scope_keeps_only_commands_and_table_scope_only_tables() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            TestItem::command("Export results"),
            TestItem::connection("orders-db"),
            TestItem::resource("orders"),
            TestItem::script("orders.sql"),
        ];

        let commands = filter_items(&items, &matcher, ">");
        assert_eq!(
            commands.iter().map(|item| item.index).collect::<Vec<_>>(),
            vec![0]
        );

        let tables = filter_items(&items, &matcher, "@or");
        assert_eq!(
            tables.iter().map(|item| item.index).collect::<Vec<_>>(),
            vec![2]
        );

        let everything = filter_items(&items, &matcher, "");
        assert_eq!(everything.len(), 4);
    }

    #[test]
    fn a_mixed_query_caps_commands_so_connections_and_tables_show() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            TestItem::command("Export orders"),
            TestItem::command("Focus orders editor"),
            TestItem::command("Open orders history"),
            TestItem::command("Close orders tab"),
            TestItem::command("Next orders tab"),
            TestItem::connection("orders-db"),
            TestItem::resource("orders"),
            TestItem::resource("order_items"),
        ];

        let arranged = arrange_results(&items, filter_items(&items, &matcher, "or"), "or");
        let kinds: Vec<PaletteSection> = arranged
            .iter()
            .map(|filtered| items[filtered.index].section())
            .collect();

        let command_count = kinds
            .iter()
            .filter(|section| **section == PaletteSection::Commands)
            .count();

        assert_eq!(command_count, MIXED_QUERY_COMMAND_LIMIT);
        assert!(kinds.contains(&PaletteSection::Connections));
        assert_eq!(
            kinds
                .iter()
                .filter(|section| **section == PaletteSection::Tables)
                .count(),
            2
        );
    }

    #[test]
    fn prefixed_and_empty_queries_keep_every_command() {
        let matcher = SkimMatcherV2::default();
        let items = vec![
            TestItem::command("Export orders"),
            TestItem::command("Focus orders editor"),
            TestItem::command("Open orders history"),
            TestItem::command("Close orders tab"),
            TestItem::resource("orders"),
        ];

        let commands_only = arrange_results(&items, filter_items(&items, &matcher, "> or"), "> or");
        assert_eq!(commands_only.len(), 4);

        let everything = arrange_results(&items, filter_items(&items, &matcher, ""), "");
        assert_eq!(everything.len(), 5);
    }

    #[test]
    fn footer_shows_arrows_in_one_keycap_and_enter_as_a_glyph() {
        assert_eq!(
            palette_navigate_label(Some("↑".into()), Some("↓".into())).as_deref(),
            Some("↑↓")
        );
        assert_eq!(palette_navigate_label(None, None), None);
        assert_eq!(footer_key_label("Enter").as_ref(), "\u{21b5}");
        assert_eq!(footer_key_label("Ctrl ↵").as_ref(), "Ctrl ↵");
    }

    #[test]
    fn match_ranges_cover_the_matched_characters_of_the_name() {
        let matcher = SkimMatcherV2::default();

        assert_eq!(match_ranges(&matcher, "orders", "or"), vec![0..2]);
        assert!(match_ranges(&matcher, "orders", "").is_empty());
        assert!(match_ranges(&matcher, "orders", "xyz").is_empty());
    }

    #[test]
    fn command_palette_card_uses_the_modal_surface_on_the_shared_scrim() {
        let source = command_palette_source();

        assert!(source.contains(".bg(overlay_bg(theme))"));
        assert!(source.contains("inspect_surface_role(SurfaceRole::Modal)"));
        assert!(source.contains("Chamfer::new(ChamferCut::CARD)"));
        assert!(source.contains(".id(\"command-palette-container\")"));
    }

    #[test]
    fn command_palette_render_keeps_overlay_identity_and_close_behavior() {
        let source = command_palette_source();

        assert!(source.contains(".id(\"command-palette-overlay\")"));
        assert!(source.contains("H::key_context()"));
        assert!(source.contains("this.hide(cx);"));
    }

    /// A host view standing in for the workspace: it renders the palette
    /// and records the selections it emits.
    struct Host {
        palette: Entity<CommandPalette<TestItem, TestHost>>,
        selections: Vec<(TestKind, bool)>,
        _subscription: gpui::Subscription,
    }

    impl gpui::Render for Host {
        fn render(
            &mut self,
            _: &mut gpui::Window,
            _: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            div().size_full().child(self.palette.clone())
        }
    }

    /// Confirming an item emits the selection the item's `select` yields,
    /// and the new-tab placement is passed through to it.
    #[gpui::test]
    fn confirming_an_item_emits_the_product_selection(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let (host, window) = cx.add_window_view(|window, cx| {
            let palette = cx.new(|cx| CommandPalette::<TestItem, TestHost>::new(window, cx));
            let subscription = cx.subscribe(
                &palette,
                |this: &mut Host, _, event: &(TestKind, bool), _| {
                    this.selections.push(*event);
                },
            );

            Host {
                palette,
                selections: Vec::new(),
                _subscription: subscription,
            }
        });
        window.run_until_parked();

        window.update(|window, cx| {
            host.update(cx, |host, cx| {
                host.palette.update(cx, |palette, cx| {
                    palette.open_with_items(vec![TestItem::command("Only")], window, cx);
                });
            });
        });
        window.run_until_parked();

        window.update(|window, cx| {
            host.update(cx, |host, cx| {
                host.palette.update(cx, |palette, cx| {
                    palette.execute_selected(OpenPlacement::NewTab, window, cx);
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let host = host.read(cx);
            assert_eq!(host.selections, vec![(TestKind::Command, true)]);
            assert!(!host.palette.read(cx).is_visible());
        });
    }

    /// The palette answers its keys through the installed
    /// [`CommandSet`], and propagates the commands it does not own.
    #[gpui::test]
    fn the_palette_answers_its_commands_through_the_installed_command_set(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            cx.set_global(
                CommandSet::new("CommandPalette", test_attach)
                    .map(KeymapCommand::SelectNext, "test_select_next")
                    .map(KeymapCommand::Cancel, "test_cancel"),
            );
        });

        let (palette, window) = cx.add_window_view(CommandPalette::<TestItem, TestHost>::new);
        window.run_until_parked();

        window.update(|window, cx| {
            palette.update(cx, |palette, cx| {
                palette.open_with_items(
                    vec![TestItem::command("Only"), TestItem::resource("Also")],
                    window,
                    cx,
                );
            });
            assert!(palette.read(cx).is_visible());

            palette.update(cx, |palette, cx| {
                assert!(
                    palette.handle_run_command("test_select_next", window, cx),
                    "the palette answers a mapped command"
                );
            });
            assert_eq!(palette.read(cx).selected_index, 1);

            palette.update(cx, |palette, cx| {
                assert!(
                    palette.handle_run_command("test_cancel", window, cx),
                    "the palette answers its cancel"
                );
            });
            assert!(!palette.read(cx).is_visible(), "cancel closes the palette");

            palette.update(cx, |palette, cx| {
                assert!(
                    !palette.handle_run_command("test_unknown", window, cx),
                    "an unmapped command is left to the workspace"
                );
            });
        });
        window.run_until_parked();
    }

    /// Without an installed [`CommandSet`] the palette answers no keys at
    /// all, the way a component without the keymap seam answers nothing.
    #[gpui::test]
    fn a_palette_without_an_installed_command_set_answers_no_keys(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let (palette, window) = cx.add_window_view(CommandPalette::<TestItem, TestHost>::new);
        window.run_until_parked();

        window.update(|window, cx| {
            palette.update(cx, |palette, cx| {
                palette.open_with_items(vec![TestItem::command("Only")], window, cx);
                assert!(!palette.handle_run_command("test_cancel", window, cx));
            });
            assert!(palette.read(cx).is_visible());
        });
    }
}
