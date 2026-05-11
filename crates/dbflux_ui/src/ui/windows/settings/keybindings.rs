use crate::keymap::{ContextId, KeyChord, default_keymap};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::Radii;
use dbflux_components::controls::Input;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Chord, Icon as FluxIcon};
use dbflux_components::typography::{Body, FieldLabel, MonoCaption};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::keybindings_section::{KeybindingsListItem, KeybindingsSection, KeybindingsSelection};

impl KeybindingsSection {
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
        let is_content_focused = self.content_focused && !self.keybindings_editing_filter;

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
                // Pre-compute conflict map: chord → list of command names that share it.
                // Used to render an inline `BannerBlock::Warning` above the first
                // occurrence of each conflicting chord within this context.
                let mut chord_groups: std::collections::HashMap<KeyChord, Vec<String>> =
                    std::collections::HashMap::new();
                for (chord, cmd, _) in filtered_bindings.iter() {
                    chord_groups
                        .entry(chord.clone())
                        .or_default()
                        .push(cmd.display_name().to_string());
                }

                let mut emitted_conflict_for: std::collections::HashSet<KeyChord> =
                    std::collections::HashSet::new();

                for (binding_idx, (chord, cmd, source_ctx)) in filtered_bindings.iter().enumerate()
                {
                    let is_inherited = *source_ctx != *context;
                    let is_binding_selected = is_content_focused
                        && matches!(
                            current_selection,
                            KeybindingsSelection::Binding(ci, bi) if ci == idx && bi == binding_idx
                        );

                    if let Some(group) = chord_groups.get(chord)
                        && group.len() > 1
                        && !emitted_conflict_for.contains(chord)
                    {
                        let current_name = cmd.display_name().to_string();
                        let others: Vec<String> = group
                            .iter()
                            .filter(|name| **name != current_name)
                            .cloned()
                            .collect();
                        flat_items.push(KeybindingsListItem::ConflictWarning {
                            chord: chord.clone(),
                            other_cmd_names: others,
                        });
                        emitted_conflict_for.insert(chord.clone());
                    }

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
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(dbflux_components::composites::section_header(
                "Keyboard Shortcuts",
                "View all keyboard shortcuts by context",
                cx,
            ))
            .child(
                div().p_4().border_b_1().border_color(border).child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            FluxIcon::new(AppIcon::Search)
                                .size(px(16.0))
                                .color(muted_foreground),
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
                    .children(flat_items.into_iter().map(|item| {
                        match item {
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
                                    .rounded(Radii::SM)
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
                                                FluxIcon::new(if is_expanded {
                                                    AppIcon::ChevronDown
                                                } else {
                                                    AppIcon::ChevronRight
                                                })
                                                .size(px(16.0))
                                                .color(muted_foreground),
                                            ),
                                    )
                                    // Context name and bindings count
                                    .child(
                                        div()
                                            .flex_1()
                                            .flex()
                                            .items_center()
                                            .gap_2()
                                            .child(FieldLabel::new(context.display_name()))
                                            .child(
                                                Body::new(format!("({} bindings)", binding_count))
                                                    .color(muted_foreground),
                                            ),
                                    )
                                    // Inherits info
                                    .when(has_parent, |d| {
                                        d.child(MonoCaption::new(format!(
                                            "inherits from {}",
                                            parent_name
                                        )))
                                    })
                                    .into_any_element()
                            }

                            KeybindingsListItem::Binding {
                                chord,
                                cmd_name,
                                is_inherited,
                                is_selected,
                                ctx_idx,
                                binding_idx,
                            } => self
                                .render_binding_row(
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
                                )
                                .into_any_element(),

                            KeybindingsListItem::ConflictWarning {
                                chord,
                                other_cmd_names,
                            } => Self::render_conflict_warning(&chord, &other_cmd_names)
                                .into_any_element(),
                        }
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
                cx.notify();
            }))
            .child(
                div()
                    .w(px(140.0))
                    .child(Chord::new(self.chord_to_parts(chord))),
            )
            .child(div().flex_1().child(if is_inherited {
                Body::new(cmd_name.to_string()).color(muted_foreground)
            } else {
                Body::new(cmd_name.to_string())
            }))
            .when(is_inherited, |d| {
                d.child(
                    div()
                        .px_2()
                        .py(px(2.0))
                        .rounded(Radii::SM)
                        .bg(secondary)
                        .child(MonoCaption::new("inherited")),
                )
            })
    }

    fn chord_to_parts(&self, chord: &KeyChord) -> Vec<SharedString> {
        let mut parts: Vec<SharedString> = Vec::new();

        if chord.modifiers.ctrl {
            parts.push("Ctrl".into());
        }
        if chord.modifiers.alt {
            parts.push("Alt".into());
        }
        if chord.modifiers.shift {
            parts.push("Shift".into());
        }
        if chord.modifiers.platform {
            parts.push("Cmd".into());
        }

        parts.push(SharedString::from(self.format_key(&chord.key)));
        parts
    }

    fn render_conflict_warning(chord: &KeyChord, others: &[String]) -> impl IntoElement {
        let other_list = if others.is_empty() {
            "another binding".to_string()
        } else {
            others.join(", ")
        };

        div().ml(px(28.0)).py_1().child(
            BannerBlock::new(
                BannerVariant::Warning,
                format!("Conflict: {} also bound to {}", chord, other_list),
            )
            .with_body("Two or more commands share this chord in this context."),
        )
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
