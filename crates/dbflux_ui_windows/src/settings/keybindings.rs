use dbflux_app::keymap::{ContextId, KeyChord};
use dbflux_components::controls::Input;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Chord, Icon as FluxIcon};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_components::typography::{Body, FieldLabel, MonoCaption};
use dbflux_ui_base::keymap::default_keymap;
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
                        let cmd_name = crate::labels::keybinding_command_name(cmd).to_lowercase();
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
                        .push(crate::labels::keybinding_command_name(cmd));
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
                        let current_name = crate::labels::keybinding_command_name(cmd);
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
                        cmd_name: crate::labels::keybinding_command_name(cmd),
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

        // Hoisted once per render: shared across every row in the list below,
        // rather than re-resolving the same static translation per row.
        let inherited_label = dbflux_i18n::t!("settings.keybindings.inherited");
        let conflict_body = dbflux_i18n::t!("settings.keybindings.conflict.body");
        let conflict_unknown_other = dbflux_i18n::t!("settings.keybindings.conflict.unknown_other");

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.keybindings.title"),
                dbflux_i18n::t!("settings.keybindings.subtitle"),
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
                                .size(Heights::ICON_SM)
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
                                let parent_name = context
                                    .parent()
                                    .map(|p| crate::labels::keybinding_context_name(&p))
                                    .unwrap_or_default();

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
                                            .w(Heights::ICON_SM)
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .child(
                                                FluxIcon::new(if is_expanded {
                                                    AppIcon::ChevronDown
                                                } else {
                                                    AppIcon::ChevronRight
                                                })
                                                .size(Heights::ICON_SM)
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
                                            .child(FieldLabel::new(
                                                crate::labels::keybinding_context_name(&context),
                                            ))
                                            .child(
                                                Body::new(
                                                    crate::labels::keybindings_binding_count(
                                                        binding_count,
                                                    ),
                                                )
                                                .color(muted_foreground),
                                            ),
                                    )
                                    // Inherits info
                                    .when(has_parent, |d| {
                                        d.child(MonoCaption::new(
                                            crate::labels::keybindings_inherits_from(&parent_name),
                                        ))
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
                                    &inherited_label,
                                    cx,
                                )
                                .into_any_element(),

                            KeybindingsListItem::ConflictWarning {
                                chord,
                                other_cmd_names,
                            } => Self::render_conflict_warning(
                                &chord,
                                &other_cmd_names,
                                &conflict_body,
                                &conflict_unknown_other,
                            )
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
        inherited_label: &str,
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
            .rounded_r(Spacing::XS)
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
                        .child(MonoCaption::new(inherited_label.to_string())),
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

    fn render_conflict_warning(
        chord: &KeyChord,
        others: &[String],
        conflict_body: &str,
        conflict_unknown_other: &str,
    ) -> impl IntoElement {
        let other_list = if others.is_empty() {
            conflict_unknown_other.to_string()
        } else {
            others.join(", ")
        };

        div().ml(px(28.0)).py_1().child(
            BannerBlock::new(
                BannerVariant::Warning,
                crate::labels::keybindings_conflict_title(&chord.to_string(), &other_list),
            )
            .with_body(conflict_body.to_string()),
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
    ) -> Vec<(KeyChord, dbflux_app::keymap::Command, ContextId)> {
        let keymap = default_keymap();
        let bindings = keymap.bindings_for_context(context);

        if filter.is_empty() {
            bindings
        } else {
            bindings
                .into_iter()
                .filter(|(chord, cmd, _)| {
                    let cmd_name = crate::labels::keybinding_command_name(cmd);
                    Self::binding_matches_filter(chord, &cmd_name, filter)
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

#[cfg(test)]
mod tests {
    use crate::labels::{
        keybindings_binding_count, keybindings_conflict_title, keybindings_inherits_from,
    };

    const KEYBINDINGS_CATALOG_KEYS: &[&str] = &[
        "settings.keybindings.title",
        "settings.keybindings.subtitle",
        "settings.keybindings.filter_placeholder",
        "settings.keybindings.binding_count.one",
        "settings.keybindings.binding_count.many",
        "settings.keybindings.inherits_from",
        "settings.keybindings.inherited",
        "settings.keybindings.conflict.title",
        "settings.keybindings.conflict.body",
        "settings.keybindings.conflict.unknown_other",
    ];

    #[test]
    fn settings_keybindings_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in KEYBINDINGS_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn settings_keybindings_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.keybindings.title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.keybindings.title", locale = "es");

        assert_eq!(english, "Keyboard Shortcuts");
        assert_eq!(spanish, "Atajos de teclado");
        assert_ne!(english, spanish);
    }

    #[test]
    fn keybindings_binding_count_uses_singular_and_plural_forms() {
        assert!(keybindings_binding_count(1).contains('1'));
        assert!(keybindings_binding_count(3).contains('3'));
        assert_ne!(keybindings_binding_count(1), keybindings_binding_count(3));
    }

    #[test]
    fn keybindings_inherits_from_embeds_parent_context_name() {
        let label = keybindings_inherits_from("Global");

        assert!(label.contains("Global"));
    }

    #[test]
    fn keybindings_conflict_title_embeds_chord_and_other_commands() {
        let title = keybindings_conflict_title("ctrl-k", "Run Query");

        assert!(title.contains("ctrl-k"));
        assert!(title.contains("Run Query"));
    }
}
