use crate::keymap::ContextId;
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{ActiveTheme, Sizable};

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

struct FilteredCommand {
    index: usize,
    score: i64,
}

const VISIBLE_ITEMS: usize = 8;

pub struct CommandPalette {
    visible: bool,
    commands: Vec<PaletteCommand>,
    filtered: Vec<FilteredCommand>,
    selected_index: usize,
    scroll_offset: usize,
    input_state: Entity<InputState>,
    matcher: SkimMatcherV2,
}

pub struct CommandExecuted {
    pub command_id: &'static str,
}

pub struct CommandPaletteClosed;

impl CommandPalette {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| InputState::new(window, cx).placeholder("Type a command..."));

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
            commands: Vec::new(),
            filtered: Vec::new(),
            selected_index: 0,
            scroll_offset: 0,
            input_state,
            matcher: SkimMatcherV2::default(),
        }
    }

    pub fn register_commands(&mut self, commands: Vec<PaletteCommand>) {
        self.commands = commands;
        self.filtered = self
            .commands
            .iter()
            .enumerate()
            .map(|(index, _)| FilteredCommand { index, score: 0 })
            .collect();
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
                .commands
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredCommand { index, score: 0 })
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
                .commands
                .iter()
                .enumerate()
                .map(|(index, _)| FilteredCommand { index, score: 0 })
                .collect();
        } else {
            let mut scored: Vec<FilteredCommand> = self
                .commands
                .iter()
                .enumerate()
                .filter_map(|(index, cmd)| {
                    let search_text = format!("{} {}", cmd.category, cmd.name);
                    self.matcher
                        .fuzzy_match(&search_text, query)
                        .map(|score| FilteredCommand { index, score })
                })
                .collect();

            scored.sort_by(|a, b| b.score.cmp(&a.score));
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
        // If selected is above visible window, scroll up
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        }
        // If selected is below visible window, scroll down
        if self.selected_index >= self.scroll_offset + VISIBLE_ITEMS {
            self.scroll_offset = self.selected_index - VISIBLE_ITEMS + 1;
        }
    }

    fn scroll_down(&mut self, cx: &mut Context<Self>) {
        // Move selection down (same as select_next but doesn't wrap)
        if self.selected_index < self.filtered.len().saturating_sub(1) {
            self.selected_index += 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn scroll_up(&mut self, cx: &mut Context<Self>) {
        // Move selection up (same as select_prev but doesn't wrap)
        if self.selected_index > 0 {
            self.selected_index -= 1;
            self.ensure_selected_visible();
            cx.notify();
        }
    }

    fn execute_selected(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(filtered) = self.filtered.get(self.selected_index)
            && let Some(cmd) = self.commands.get(filtered.index)
        {
            let command_id = cmd.id;
            self.visible = false;
            cx.emit(CommandExecuted { command_id });
            cx.notify();
        }
    }

    fn render_command_item(
        idx: usize,
        cmd: PaletteCommand,
        is_selected: bool,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let shortcut_el = cmd.shortcut.map(|shortcut| {
            div()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .rounded(Radii::SM)
                .text_size(FontSizes::XS)
                .font_family("monospace")
                .when(is_selected, |d| {
                    d.bg(theme.primary_foreground.opacity(0.2))
                        .text_color(theme.primary_foreground)
                })
                .when(!is_selected, |d| {
                    d.bg(theme.secondary).text_color(theme.muted_foreground)
                })
                .child(shortcut)
        });

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
            .when(is_selected, |d| {
                d.bg(theme.primary).text_color(theme.primary_foreground)
            })
            .when(!is_selected, |d| {
                d.bg(theme.background)
                    .text_color(theme.foreground)
                    .hover(|d| d.bg(theme.secondary))
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .when(is_selected, |d| {
                                d.text_color(theme.primary_foreground.opacity(0.7))
                            })
                            .when(!is_selected, |d| d.text_color(theme.muted_foreground))
                            .child(cmd.category),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_weight(FontWeight::MEDIUM)
                            .child(cmd.name),
                    ),
            )
            .when_some(shortcut_el, |d, el| d.child(el))
    }
}

impl EventEmitter<CommandExecuted> for CommandPalette {}
impl EventEmitter<CommandPaletteClosed> for CommandPalette {}

impl Render for CommandPalette {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let input_state = self.input_state.clone();

        let commands_to_render: Vec<(usize, PaletteCommand, bool)> = self
            .filtered
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(VISIBLE_ITEMS)
            .map(|(idx, filtered)| {
                let cmd = self.commands[filtered.index].clone();
                let is_selected = idx == self.selected_index;
                (idx, cmd, is_selected)
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
                div()
                    .id("command-palette-container")
                    .w(px(500.0))
                    .max_h(px(400.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::LG)
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
                            .children(commands_to_render.into_iter().map(
                                |(idx, cmd, is_selected)| {
                                    Self::render_command_item(idx, cmd, is_selected, theme)
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
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .child("No matching commands"),
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
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::MD)
                                    .child("↑↓/C-jk Navigate")
                                    .child("↵ Execute")
                                    .child("Esc Close"),
                            )
                            .child(format!("{} commands", self.filtered.len())),
                    ),
            )
            .into_any_element()
    }
}
