use crate::keymap::{ContextId, KeyChord, default_keymap};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::input::Input;
use gpui_component::{Icon, IconName};

use super::{KeybindingsListItem, KeybindingsSelection, SettingsFocus, SettingsWindow};

impl SettingsWindow {
    pub(super) fn render_keybindings_section(
        &mut self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let keymap = default_keymap();
        let filter_text = self.keybindings_filter.read(cx).value().to_lowercase();
        let has_filter = !filter_text.is_empty();

        // Validate selection when filter is active
        if has_filter {
            self.validate_selection_for_filter(cx);
        }

        // Extract theme colors before closures to avoid borrow issues
        let border = theme.border;
        let muted_foreground = theme.muted_foreground;
        let secondary = theme.secondary;

        let current_selection = self.keybindings_selection;
        let is_content_focused =
            self.focus_area == SettingsFocus::Content && !self.keybindings_editing_filter;

        // Flat list required for scroll_to_item to work correctly
        let mut flat_items: Vec<KeybindingsListItem> = Vec::new();

        for (idx, context) in ContextId::all_variants().iter().enumerate() {
            let is_expanded = has_filter || self.keybindings_expanded.contains(context);
            let bindings = keymap.bindings_for_context(*context);

            let filtered_bindings: Vec<_> = if has_filter {
                bindings
                    .iter()
                    .filter(|(chord, cmd, _)| {
                        let chord_str = chord.to_string().to_lowercase();
                        let cmd_name = cmd.display_name().to_lowercase();
                        chord_str.contains(&filter_text) || cmd_name.contains(&filter_text)
                    })
                    .cloned()
                    .collect()
            } else {
                bindings
            };

            // Skip contexts with no matching bindings when filtering
            if has_filter && filtered_bindings.is_empty() {
                continue;
            }

            let is_context_selected = is_content_focused
                && matches!(current_selection, KeybindingsSelection::Context(i) if i == idx);

            // Add context header
            flat_items.push(KeybindingsListItem::ContextHeader {
                context: *context,
                ctx_idx: idx,
                is_expanded,
                is_selected: is_context_selected,
                binding_count: filtered_bindings.len(),
            });

            // Add bindings if expanded
            if is_expanded {
                for (binding_idx, (chord, cmd, source_ctx)) in filtered_bindings.iter().enumerate()
                {
                    let is_inherited = *source_ctx != *context;
                    let is_binding_selected = is_content_focused
                        && matches!(
                            current_selection,
                            KeybindingsSelection::Binding(ci, bi) if ci == idx && bi == binding_idx
                        );

                    flat_items.push(KeybindingsListItem::Binding {
                        chord: chord.clone(),
                        cmd_name: cmd.display_name().to_string(),
                        is_inherited,
                        is_selected: is_binding_selected,
                        ctx_idx: idx,
                        binding_idx,
                    });
                }
            }
        }

        if let Some(scroll_idx) = self.keybindings_pending_scroll.take() {
            self.keybindings_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Keyboard Shortcuts"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(muted_foreground)
                            .child("View all keyboard shortcuts by context"),
                    ),
            )
            .child(
                div().p_4().border_b_1().border_color(border).child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            Icon::new(IconName::Search)
                                .size(px(16.0))
                                .text_color(muted_foreground),
                        )
                        .child(
                            div()
                                .flex_1()
                                .child(Input::new(&self.keybindings_filter).small()),
                        ),
                ),
            )
            .child(
                div()
                    .id("keybindings-scroll-container")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.keybindings_scroll_handle)
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_0()
                    .children(flat_items.into_iter().map(|item| match item {
                        KeybindingsListItem::ContextHeader {
                            context,
                            ctx_idx,
                            is_expanded,
                            is_selected,
                            binding_count,
                        } => {
                            let has_parent = context.parent().is_some();
                            let parent_name =
                                context.parent().map(|p| p.display_name()).unwrap_or("");

                            div()
                                .id(SharedString::from(format!(
                                    "context-{}",
                                    context.as_gpui_context()
                                )))
                                .flex()
                                .items_center()
                                .gap_2()
                                .px_3()
                                .py_2()
                                .mt_1()
                                .rounded(px(4.0))
                                .cursor_pointer()
                                .bg(if is_selected {
                                    secondary
                                } else {
                                    gpui::transparent_black()
                                })
                                .hover(|d| d.bg(secondary))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.keybindings_selection =
                                        KeybindingsSelection::Context(ctx_idx);
                                    this.focus_area = SettingsFocus::Content;

                                    if this.keybindings_expanded.contains(&context) {
                                        this.keybindings_expanded.remove(&context);
                                    } else {
                                        this.keybindings_expanded.insert(context);
                                    }
                                    cx.notify();
                                }))
                                // Chevron icon
                                .child(
                                    div()
                                        .w(px(16.0))
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .child(
                                            Icon::new(if is_expanded {
                                                IconName::ChevronDown
                                            } else {
                                                IconName::ChevronRight
                                            })
                                            .size(px(16.0))
                                            .text_color(muted_foreground),
                                        ),
                                )
                                // Context name and bindings count
                                .child(
                                    div()
                                        .flex_1()
                                        .flex()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            div()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child(context.display_name()),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(muted_foreground)
                                                .child(format!("({} bindings)", binding_count)),
                                        ),
                                )
                                // Inherits info
                                .when(has_parent, |d| {
                                    d.child(
                                        div()
                                            .text_xs()
                                            .text_color(muted_foreground)
                                            .child(format!("inherits from {}", parent_name)),
                                    )
                                })
                        }

                        KeybindingsListItem::Binding {
                            chord,
                            cmd_name,
                            is_inherited,
                            is_selected,
                            ctx_idx,
                            binding_idx,
                        } => self.render_binding_row(
                            &chord,
                            &cmd_name,
                            is_inherited,
                            is_selected,
                            ctx_idx,
                            binding_idx,
                            muted_foreground,
                            secondary,
                            border,
                            cx,
                        ),
                    })),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_binding_row(
        &self,
        chord: &KeyChord,
        cmd_name: &str,
        is_inherited: bool,
        is_selected: bool,
        ctx_idx: usize,
        binding_idx: usize,
        muted_foreground: Hsla,
        secondary: Hsla,
        border: Hsla,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        div()
            .id(SharedString::from(format!(
                "binding-{}-{}",
                ctx_idx, binding_idx
            )))
            .ml(px(28.0))
            .pl_4()
            .border_l_2()
            .border_color(border)
            .flex()
            .items_center()
            .py_1()
            .px_2()
            .rounded_r(px(4.0))
            .gap_4()
            .cursor_pointer()
            .bg(if is_selected {
                secondary
            } else {
                gpui::transparent_black()
            })
            .hover(|d| d.bg(secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.keybindings_selection = KeybindingsSelection::Binding(ctx_idx, binding_idx);
                this.focus_area = SettingsFocus::Content;
                cx.notify();
            }))
            .child(div().w(px(140.0)).child(self.render_key_badge(
                chord,
                muted_foreground,
                secondary,
            )))
            .child(
                div()
                    .flex_1()
                    .text_sm()
                    .when(is_inherited, |d| d.text_color(muted_foreground))
                    .child(cmd_name.to_string()),
            )
            .when(is_inherited, |d| {
                d.child(
                    div()
                        .text_xs()
                        .text_color(muted_foreground)
                        .px_2()
                        .py(px(2.0))
                        .rounded(px(4.0))
                        .bg(secondary)
                        .child("inherited"),
                )
            })
    }

    fn render_key_badge(
        &self,
        chord: &KeyChord,
        muted_foreground: Hsla,
        secondary: Hsla,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_1()
            .children(self.chord_to_badges(chord, muted_foreground, secondary))
    }

    fn chord_to_badges(
        &self,
        chord: &KeyChord,
        muted_foreground: Hsla,
        secondary: Hsla,
    ) -> Vec<Div> {
        let mut badges = Vec::new();

        if chord.modifiers.ctrl {
            badges.push(self.render_single_key_badge("Ctrl", muted_foreground, secondary));
        }
        if chord.modifiers.alt {
            badges.push(self.render_single_key_badge("Alt", muted_foreground, secondary));
        }
        if chord.modifiers.shift {
            badges.push(self.render_single_key_badge("Shift", muted_foreground, secondary));
        }
        if chord.modifiers.platform {
            badges.push(self.render_single_key_badge("Cmd", muted_foreground, secondary));
        }

        let key_display = self.format_key(&chord.key);
        badges.push(self.render_single_key_badge(&key_display, muted_foreground, secondary));

        badges
    }

    fn render_single_key_badge(&self, key: &str, muted_foreground: Hsla, secondary: Hsla) -> Div {
        div()
            .px_2()
            .py(px(2.0))
            .rounded(px(4.0))
            .bg(secondary)
            .border_1()
            .border_color(muted_foreground.opacity(0.3))
            .text_xs()
            .font_weight(FontWeight::MEDIUM)
            .child(key.to_string())
    }

    fn format_key(&self, key: &str) -> String {
        match key {
            "down" => "↓".to_string(),
            "up" => "↑".to_string(),
            "left" => "←".to_string(),
            "right" => "→".to_string(),
            "enter" => "Enter".to_string(),
            "escape" => "Esc".to_string(),
            "backspace" => "⌫".to_string(),
            "delete" => "Del".to_string(),
            "tab" => "Tab".to_string(),
            "space" => "Space".to_string(),
            "home" => "Home".to_string(),
            "end" => "End".to_string(),
            "pageup" => "PgUp".to_string(),
            "pagedown" => "PgDn".to_string(),
            _ => key.to_uppercase(),
        }
    }

    fn get_filter_text(&self, cx: &Context<Self>) -> String {
        self.keybindings_filter.read(cx).value().to_lowercase()
    }

    fn binding_matches_filter(chord: &KeyChord, cmd_name: &str, filter: &str) -> bool {
        if filter.is_empty() {
            return true;
        }
        let chord_str = chord.to_string().to_lowercase();
        let cmd_lower = cmd_name.to_lowercase();
        chord_str.contains(filter) || cmd_lower.contains(filter)
    }

    fn get_filtered_bindings(
        &self,
        context: ContextId,
        filter: &str,
    ) -> Vec<(KeyChord, crate::keymap::Command, ContextId)> {
        let keymap = default_keymap();
        let bindings = keymap.bindings_for_context(context);

        if filter.is_empty() {
            bindings
        } else {
            bindings
                .into_iter()
                .filter(|(chord, cmd, _)| {
                    Self::binding_matches_filter(chord, cmd.display_name(), filter)
                })
                .collect()
        }
    }

    fn is_context_visible(&self, ctx_idx: usize, filter: &str) -> bool {
        if filter.is_empty() {
            return true;
        }
        if let Some(context) = ContextId::all_variants().get(ctx_idx) {
            !self.get_filtered_bindings(*context, filter).is_empty()
        } else {
            false
        }
    }

    fn is_context_expanded(&self, context: &ContextId, has_filter: bool) -> bool {
        has_filter || self.keybindings_expanded.contains(context)
    }

    pub(super) fn get_visible_binding_count(&self, ctx_idx: usize, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        let has_filter = !filter.is_empty();

        if let Some(context) = ContextId::all_variants().get(ctx_idx) {
            if !self.is_context_expanded(context, has_filter) {
                return 0;
            }
            self.get_filtered_bindings(*context, &filter).len()
        } else {
            0
        }
    }

    pub(super) fn first_visible_context(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        (0..ContextId::all_variants().len())
            .find(|&idx| self.is_context_visible(idx, &filter))
            .unwrap_or(0)
    }

    pub(super) fn last_visible_context(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        (0..ContextId::all_variants().len())
            .rev()
            .find(|&idx| self.is_context_visible(idx, &filter))
            .unwrap_or(0)
    }

    fn next_visible_context(&self, after_idx: usize, cx: &Context<Self>) -> Option<usize> {
        let filter = self.get_filter_text(cx);
        ((after_idx + 1)..ContextId::all_variants().len())
            .find(|&idx| self.is_context_visible(idx, &filter))
    }

    fn prev_visible_context(&self, before_idx: usize, cx: &Context<Self>) -> Option<usize> {
        let filter = self.get_filter_text(cx);
        (0..before_idx)
            .rev()
            .find(|&idx| self.is_context_visible(idx, &filter))
    }

    fn validate_selection_for_filter(&mut self, cx: &Context<Self>) {
        let filter = self.get_filter_text(cx);
        if filter.is_empty() {
            return;
        }

        let ctx_idx = self.keybindings_selection.context_idx();

        if !self.is_context_visible(ctx_idx, &filter) {
            self.keybindings_selection =
                KeybindingsSelection::Context(self.first_visible_context(cx));
            return;
        }

        if let KeybindingsSelection::Binding(_, binding_idx) = self.keybindings_selection {
            let visible_count = self.get_visible_binding_count(ctx_idx, cx);
            if binding_idx >= visible_count {
                if visible_count > 0 {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, visible_count - 1);
                } else {
                    self.keybindings_selection = KeybindingsSelection::Context(ctx_idx);
                }
            }
        }
    }

    pub(super) fn keybindings_move_next(&mut self, cx: &Context<Self>) {
        let binding_count =
            self.get_visible_binding_count(self.keybindings_selection.context_idx(), cx);

        match self.keybindings_selection {
            KeybindingsSelection::Context(ctx_idx) => {
                if binding_count > 0 {
                    self.keybindings_selection = KeybindingsSelection::Binding(ctx_idx, 0);
                } else if let Some(next) = self.next_visible_context(ctx_idx, cx) {
                    self.keybindings_selection = KeybindingsSelection::Context(next);
                }
            }
            KeybindingsSelection::Binding(ctx_idx, binding_idx) => {
                if binding_idx + 1 < binding_count {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, binding_idx + 1);
                } else if let Some(next) = self.next_visible_context(ctx_idx, cx) {
                    self.keybindings_selection = KeybindingsSelection::Context(next);
                }
            }
        }
    }

    pub(super) fn keybindings_move_prev(&mut self, cx: &Context<Self>) {
        match self.keybindings_selection {
            KeybindingsSelection::Context(ctx_idx) => {
                if let Some(prev) = self.prev_visible_context(ctx_idx, cx) {
                    let prev_count = self.get_visible_binding_count(prev, cx);
                    if prev_count > 0 {
                        self.keybindings_selection =
                            KeybindingsSelection::Binding(prev, prev_count - 1);
                    } else {
                        self.keybindings_selection = KeybindingsSelection::Context(prev);
                    }
                }
            }
            KeybindingsSelection::Binding(ctx_idx, binding_idx) => {
                if binding_idx > 0 {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, binding_idx - 1);
                } else {
                    self.keybindings_selection = KeybindingsSelection::Context(ctx_idx);
                }
            }
        }
    }

    pub(super) fn keybindings_flat_index(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        let has_filter = !filter.is_empty();
        let mut flat_idx = 0;

        for (ctx_idx, context) in ContextId::all_variants().iter().enumerate() {
            if !self.is_context_visible(ctx_idx, &filter) {
                continue;
            }

            match self.keybindings_selection {
                KeybindingsSelection::Context(sel) if sel == ctx_idx => return flat_idx,
                KeybindingsSelection::Binding(sel, bi) if sel == ctx_idx => {
                    return flat_idx + 1 + bi;
                }
                _ => {}
            }

            flat_idx += 1;
            if self.is_context_expanded(context, has_filter) {
                flat_idx += self.get_filtered_bindings(*context, &filter).len();
            }
        }
        flat_idx
    }
}
