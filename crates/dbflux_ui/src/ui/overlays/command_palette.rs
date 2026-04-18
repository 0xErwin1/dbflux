use crate::keymap::ContextId;
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::helpers::text_color_for_selected;
use dbflux_components::primitives::{Text, surface_panel};
use dbflux_core::{CollectionRef, TableRef};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, Sizable};
use std::path::PathBuf;
use uuid::Uuid;

actions!(command_palette, [SelectNext, SelectPrev, Close, Execute]);

pub fn command_palette_keybindings() -> Vec<KeyBinding> {
    let ctx = Some(ContextId::CommandPalette.as_gpui_context());
    vec![
        KeyBinding::new("up", SelectPrev, ctx),
        KeyBinding::new("down", SelectNext, ctx),
        KeyBinding::new("ctrl-k", SelectPrev, ctx),
        KeyBinding::new("ctrl-j", SelectNext, ctx),
        KeyBinding::new("escape", Close, ctx),
        KeyBinding::new("enter", Execute, ctx),
    ]
}

/// A searchable item in the command palette.
#[derive(Clone)]
pub enum PaletteItem {
    Action {
        id: &'static str,
        name: &'static str,
        category: &'static str,
        shortcut: Option<&'static str>,
    },
    Connection {
        profile_id: Uuid,
        name: String,
        is_connected: bool,
    },
    Resource(ResourceItem),
    Script {
        /// Absolute filesystem path (used to open the script).
        path: PathBuf,
        /// File name (e.g., "health-check.sql").
        name: String,
        /// Path relative to the scripts root directory (for display/search).
        relative_path: String,
    },
}

/// Schema resource variants surfaced by connected profiles.
#[derive(Clone)]
pub enum ResourceItem {
    Table {
        profile_id: Uuid,
        profile_name: String,
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    Collection {
        profile_id: Uuid,
        profile_name: String,
        database: String,
        name: String,
    },
    View {
        profile_id: Uuid,
        profile_name: String,
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    KeyValueDb {
        profile_id: Uuid,
        profile_name: String,
        database: String,
    },
}

impl PaletteItem {
    /// Text searched by `SkimMatcherV2`.
    pub fn search_text(&self) -> String {
        match self {
            Self::Action { category, name, .. } => format!("{} {}", category, name),
            Self::Connection { name, .. } => format!("Connection {}", name),
            Self::Resource(r) => match r {
                ResourceItem::Table {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!("Table {} {}", profile_name, name);
                    if let Some(db) = database {
                        parts.push_str(&format!(" {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" {}", s));
                    }
                    parts
                }
                ResourceItem::Collection {
                    profile_name,
                    database,
                    name,
                    ..
                } => format!("Collection {} {} {}", profile_name, name, database),
                ResourceItem::View {
                    profile_name,
                    database,
                    schema,
                    name,
                    ..
                } => {
                    let mut parts = format!("View {} {}", profile_name, name);
                    if let Some(db) = database {
                        parts.push_str(&format!(" {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" {}", s));
                    }
                    parts
                }
                ResourceItem::KeyValueDb {
                    profile_name,
                    database,
                    ..
                } => format!("Keyspace {} {}", profile_name, database),
            },
            Self::Script {
                name,
                relative_path,
                ..
            } => {
                format!("Script {} {}", name, relative_path)
            }
        }
    }

    /// Returns `(category_label, display_name)`.
    pub fn display_label(&self) -> (String, String) {
        match self {
            Self::Action { category, name, .. } => (category.to_string(), name.to_string()),
            Self::Connection { name, .. } => ("Connection".to_string(), name.clone()),
            Self::Resource(r) => match r {
                ResourceItem::Table { name, .. } => ("Table".to_string(), name.clone()),
                ResourceItem::Collection { name, .. } => ("Collection".to_string(), name.clone()),
                ResourceItem::View { name, .. } => ("View".to_string(), name.clone()),
                ResourceItem::KeyValueDb { database, .. } => {
                    ("Keyspace".to_string(), database.clone())
                }
            },
            Self::Script { name, .. } => ("Script".to_string(), name.clone()),
        }
    }

    /// Type priority for tiebreaking (lower = higher priority).
    pub fn type_priority(&self) -> u8 {
        match self {
            Self::Action { .. } => 0,
            Self::Connection { .. } => 1,
            Self::Resource(_) => 2,
            Self::Script { .. } => 3,
        }
    }

    /// Optional qualifier text shown after the item name.
    pub fn qualifier(&self) -> Option<String> {
        match self {
            Self::Action { shortcut, .. } => shortcut.map(|s| s.to_string()),
            Self::Resource(r) => match r {
                ResourceItem::Table {
                    profile_name,
                    database,
                    schema,
                    ..
                }
                | ResourceItem::View {
                    profile_name,
                    database,
                    schema,
                    ..
                } => {
                    let mut parts = profile_name.clone();
                    if let Some(db) = database {
                        parts.push_str(&format!(" / {}", db));
                    }
                    if let Some(s) = schema {
                        parts.push_str(&format!(" / {}", s));
                    }
                    Some(parts)
                }
                ResourceItem::Collection {
                    profile_name,
                    database,
                    ..
                } => Some(format!("{} / {}", profile_name, database)),
                ResourceItem::KeyValueDb { profile_name, .. } => Some(profile_name.clone()),
            },
            Self::Script { relative_path, .. } => {
                if relative_path.contains('/') {
                    Some(relative_path.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Legacy static command descriptor kept for `default_commands()` backwards compat.
#[derive(Clone)]
pub struct PaletteCommand {
    pub id: &'static str,
    pub name: &'static str,
    pub category: &'static str,
    pub shortcut: Option<&'static str>,
}

impl PaletteCommand {
    pub const fn new(id: &'static str, name: &'static str, category: &'static str) -> Self {
        Self {
            id,
            name,
            category,
            shortcut: None,
        }
    }

    pub const fn with_shortcut(mut self, shortcut: &'static str) -> Self {
        self.shortcut = Some(shortcut);
        self
    }
}

impl From<PaletteCommand> for PaletteItem {
    fn from(cmd: PaletteCommand) -> Self {
        PaletteItem::Action {
            id: cmd.id,
            name: cmd.name,
            category: cmd.category,
            shortcut: cmd.shortcut,
        }
    }
}

struct FilteredItem {
    index: usize,
    score: i64,
}

const VISIBLE_ITEMS: usize = 8;

fn palette_item_name(
    name: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> Text {
    Text::label_sm(name).color(if is_selected {
        theme.primary_foreground
    } else {
        theme.foreground
    })
}

fn palette_category_text(
    label: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> Text {
    Text::caption(label).color(if is_selected {
        theme.primary_foreground.opacity(0.7)
    } else {
        theme.muted_foreground
    })
}

fn palette_qualifier_text(
    label: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> Text {
    Text::caption(label).color(if is_selected {
        theme.primary_foreground.opacity(0.6)
    } else {
        theme.muted_foreground
    })
}

fn palette_shortcut_text(
    shortcut: impl Into<SharedString>,
    is_selected: bool,
    theme: &gpui_component::theme::Theme,
) -> Text {
    Text::caption(shortcut)
        .font_size(FontSizes::XS)
        .color(text_color_for_selected(is_selected, theme))
}

pub struct CommandPalette {
    visible: bool,
    items: Vec<PaletteItem>,
    filtered: Vec<FilteredItem>,
    selected_index: usize,
    scroll_offset: usize,
    input_state: Entity<InputState>,
    matcher: SkimMatcherV2,
}

/// Event emitted when the user selects a palette item.
pub enum PaletteSelection {
    Command {
        id: &'static str,
    },
    Connect {
        profile_id: Uuid,
    },
    OpenTable {
        profile_id: Uuid,
        table: TableRef,
        database: Option<String>,
    },
    OpenCollection {
        profile_id: Uuid,
        collection: CollectionRef,
    },
    OpenKeyValue {
        profile_id: Uuid,
        database: String,
    },
    FocusConnection {
        profile_id: Uuid,
    },
    OpenScript {
        path: PathBuf,
    },
}

pub struct CommandPaletteClosed;

impl CommandPalette {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search commands, connections, tables, scripts...")
        });

        cx.subscribe_in(
            &input_state,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    let query = this.input_state.read(cx).value().to_string();
                    this.update_filter(&query, cx);
                }
                InputEvent::PressEnter { .. } => {
                    this.execute_selected(window, cx);
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
        }
    }

    /// Set items and reset filter state. Called by Workspace on each toggle.
    pub fn open_with_items(
        &mut self,
        items: Vec<PaletteItem>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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

        self.input_state.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        cx.notify();
    }

    pub fn register_commands(&mut self, _commands: Vec<PaletteCommand>) {
        // No-op; items are now set via open_with_items.
        // Kept to avoid breaking the call site during migration.
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
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
        }

        cx.notify();
    }

    pub fn hide(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.emit(CommandPaletteClosed);
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    fn update_filter(&mut self, query: &str, cx: &mut Context<Self>) {
        if query.is_empty() {
            self.filtered = self
                .items
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredItem { index, score: 0 })
                .collect();
        } else {
            let mut scored: Vec<FilteredItem> = self
                .items
                .iter()
                .enumerate()
                .filter_map(|(index, item)| {
                    let search_text = item.search_text();
                    self.matcher
                        .fuzzy_match(&search_text, query)
                        .map(|score| FilteredItem { index, score })
                })
                .collect();

            scored.sort_by(|a, b| {
                b.score.cmp(&a.score).then_with(|| {
                    self.items[a.index]
                        .type_priority()
                        .cmp(&self.items[b.index].type_priority())
                })
            });

            self.filtered = scored;
        }

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

    fn execute_selected(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(filtered) = self.filtered.get(self.selected_index)
            && let Some(item) = self.items.get(filtered.index)
        {
            let selection = match item {
                PaletteItem::Action { id, .. } => PaletteSelection::Command { id },
                PaletteItem::Connection {
                    profile_id,
                    is_connected,
                    ..
                } => {
                    if *is_connected {
                        PaletteSelection::FocusConnection {
                            profile_id: *profile_id,
                        }
                    } else {
                        PaletteSelection::Connect {
                            profile_id: *profile_id,
                        }
                    }
                }
                PaletteItem::Resource(r) => match r {
                    ResourceItem::Table {
                        profile_id,
                        schema,
                        name,
                        database,
                        ..
                    }
                    | ResourceItem::View {
                        profile_id,
                        schema,
                        name,
                        database,
                        ..
                    } => PaletteSelection::OpenTable {
                        profile_id: *profile_id,
                        table: TableRef {
                            schema: schema.clone(),
                            name: name.clone(),
                        },
                        database: database.clone(),
                    },
                    ResourceItem::Collection {
                        profile_id,
                        database,
                        name,
                        ..
                    } => PaletteSelection::OpenCollection {
                        profile_id: *profile_id,
                        collection: CollectionRef {
                            database: database.clone(),
                            name: name.clone(),
                        },
                    },
                    ResourceItem::KeyValueDb {
                        profile_id,
                        database,
                        ..
                    } => PaletteSelection::OpenKeyValue {
                        profile_id: *profile_id,
                        database: database.clone(),
                    },
                },
                PaletteItem::Script { path, .. } => {
                    PaletteSelection::OpenScript { path: path.clone() }
                }
            };

            self.visible = false;
            cx.emit(selection);
            cx.notify();
        }
    }

    fn render_palette_item(
        idx: usize,
        item: &PaletteItem,
        is_selected: bool,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let (category, name) = item.display_label();

        let right_el = match item {
            PaletteItem::Action { shortcut, .. } => shortcut.map(|s| {
                div()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .font_family("monospace")
                    .when(is_selected, |d| d.bg(theme.primary_foreground.opacity(0.2)))
                    .when(!is_selected, |d| d.bg(theme.secondary))
                    .child(palette_shortcut_text(s, is_selected, theme))
                    .into_any_element()
            }),
            PaletteItem::Connection { is_connected, .. } => {
                let indicator = if *is_connected {
                    div()
                        .size(px(8.0))
                        .rounded_full()
                        .bg(gpui::green())
                        .when(is_selected, |d| {
                            d.border_1()
                                .border_color(theme.primary_foreground.opacity(0.5))
                        })
                } else {
                    div()
                        .size(px(8.0))
                        .rounded_full()
                        .bg(theme.muted_foreground.opacity(0.4))
                };
                Some(indicator.into_any_element())
            }
            PaletteItem::Resource(_) => item
                .qualifier()
                .map(|q| palette_qualifier_text(q, is_selected, theme).into_any_element()),
            PaletteItem::Script { .. } => item
                .qualifier()
                .map(|q| palette_qualifier_text(q, is_selected, theme).into_any_element()),
        };

        div()
            .id(("cmd", idx))
            .w_full()
            .px(Spacing::MD)
            .py(Spacing::SM)
            .flex()
            .items_center()
            .justify_between()
            .rounded(Radii::SM)
            .cursor_pointer()
            .when(is_selected, |d| d.bg(theme.primary))
            .when(!is_selected, |d| {
                d.bg(theme.background).hover(|d| d.bg(theme.secondary))
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(palette_category_text(category, is_selected, theme))
                    .child(palette_item_name(name, is_selected, theme)),
            )
            .when_some(right_el, |d, el| d.child(el))
    }
}

impl EventEmitter<PaletteSelection> for CommandPalette {}
impl EventEmitter<CommandPaletteClosed> for CommandPalette {}

impl Render for CommandPalette {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let input_state = self.input_state.clone();

        let items_to_render: Vec<(usize, PaletteItem, bool)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(idx, filtered)| {
                let item = self.items[filtered.index].clone();
                let is_selected = idx == self.selected_index;
                (idx, item, is_selected)
            })
            .collect();

        div()
            .id("command-palette-overlay")
            .key_context(ContextId::CommandPalette.as_gpui_context())
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .pt(px(80.0))
            .bg(gpui::black().opacity(0.5))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &SelectPrev, _window, cx| {
                this.select_prev(cx);
            }))
            .on_action(cx.listener(|this, _: &SelectNext, _window, cx| {
                this.select_next(cx);
            }))
            .on_action(cx.listener(|this, _: &Close, _window, cx| {
                this.hide(cx);
            }))
            .on_action(cx.listener(|this, _: &Execute, window, cx| {
                this.execute_selected(window, cx);
            }))
            .child(
                surface_panel(cx)
                    .id("command-palette-container")
                    .w(px(500.0))
                    .max_h(px(400.0))
                    .shadow_lg()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(
                        div()
                            .p(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .child(Input::new(&input_state).small().cleanable(true)),
                    )
                    .child(
                        div()
                            .id("command-palette-list")
                            .flex_1()
                            .overflow_y_hidden()
                            .p(Spacing::XS)
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
                            .children(items_to_render.into_iter().map(
                                |(idx, item, is_selected)| {
                                    Self::render_palette_item(idx, &item, is_selected, theme)
                                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                            cx.stop_propagation();
                                        })
                                        .on_click(cx.listener(move |this, _, window, cx| {
                                            this.selected_index = idx;
                                            this.execute_selected(window, cx);
                                        }))
                                },
                            ))
                            .when(self.filtered.is_empty(), |d| {
                                d.child(
                                    div()
                                        .w_full()
                                        .py(Spacing::LG)
                                        .flex()
                                        .justify_center()
                                        .child(Text::muted("No matching items")),
                                )
                            }),
                    )
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::MD)
                                    .child(Text::caption("↑↓/C-jk Navigate"))
                                    .child(Text::caption("↵ Execute"))
                                    .child(Text::caption("Esc Close")),
                            )
                            .child(Text::caption(format!("{} items", self.filtered.len()))),
                    ),
            )
            .into_any_element()
    }
}
