use super::layout;
use crate::tokens::SettingsMetrics;
use dbflux_app::keymap::{
    BindingSlot, ContextId, KeyChord, KeymapOverrides, RecordingState, default_slots,
};
use dbflux_components::composites::ListRow;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, ChamferRing, Icon as FluxIcon, Kbd,
};
use dbflux_components::tokens::{ChamferCut, ChromeColors, Fields, Spacing};
use dbflux_ui_base::keymap::{chord_display_parts, default_keymap};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use super::keybindings_section::{
    BindingEntry, KeybindingsListItem, KeybindingsSection, KeybindingsSelection,
};

/// Height of a binding row. (36 px plus its 1 px divider)
const KEYBINDING_ROW_HEIGHT: Pixels = px(37.0);

/// Left padding of a binding row, which indents it under the context
/// header's chevron. (22 px)
const KEYBINDING_ROW_INDENT: Pixels = px(22.0);

/// Width of the trailing action column: the pencil, and the reset arrow of
/// an overridden binding before it. (32 px each)
const KEYBINDING_ACTION_WIDTH: Pixels = px(32.0);

/// Height of a context header. (38 px plus its 1 px line)
const KEYBINDING_HEADER_HEIGHT: Pixels = px(39.0);

/// Chevron of a context header. (14 px)
const KEYBINDING_CHEVRON: Pixels = px(14.0);

/// Width of the context filter next to the text filter. (170 px)
const CONTEXT_FILTER_WIDTH: Pixels = px(170.0);

/// Gap between the text filter and the context filter. (10 px)
const FILTER_ROW_GAP: Pixels = px(10.0);

/// Every binding of the default keymap, computed once.
static DEFAULT_SLOTS: LazyLock<Vec<BindingSlot>> =
    LazyLock::new(|| default_slots(default_keymap()));

/// Rows of `context`: its own bindings, unbound ones included so they can be
/// rebound or reset, then the bindings it inherits from its parent contexts
/// that are still bound and not shadowed by a nearer binding of the same
/// chord.
pub(super) fn entries_for_context(
    slots: &[BindingSlot],
    overrides: &KeymapOverrides,
    context: ContextId,
) -> Vec<BindingEntry> {
    let mut entries: Vec<BindingEntry> = slots
        .iter()
        .filter(|slot| slot.context == context)
        .map(|slot| BindingEntry {
            slot: slot.clone(),
            chord: overrides.effective_chord(slot),
            is_inherited: false,
        })
        .collect();

    let mut taken: HashSet<KeyChord> = entries
        .iter()
        .filter_map(|entry| entry.chord.clone())
        .collect();

    let mut ancestor = context.parent();

    while let Some(parent) = ancestor {
        for slot in slots.iter().filter(|slot| slot.context == parent) {
            let Some(chord) = overrides.effective_chord(slot) else {
                continue;
            };

            if taken.insert(chord.clone()) {
                entries.push(BindingEntry {
                    slot: slot.clone(),
                    chord: Some(chord),
                    is_inherited: true,
                });
            }
        }

        ancestor = parent.parent();
    }

    entries
}

fn entry_matches_filter(entry: &BindingEntry, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }

    let chord_matches = entry
        .chord
        .as_ref()
        .is_some_and(|chord| chord.to_string().to_lowercase().contains(filter));

    chord_matches
        || crate::labels::keybinding_command_name(&entry.slot.command)
            .to_lowercase()
            .contains(filter)
}

impl KeybindingsSection {
    pub(super) fn render_keybindings_section(
        &mut self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let filter_text = self.keybindings_filter.read(cx).value().to_lowercase();
        let has_filter = !filter_text.is_empty();
        let filter_is_empty = self.keybindings_filter.read(cx).value().is_empty();
        let filter_focused = self.keybindings_editing_filter;

        if has_filter || self.context_filter_value.is_some() {
            self.validate_selection_for_filter(cx);
        }

        let border = theme.border;
        let group_line = theme.input;
        let muted_foreground = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);

        let current_selection = self.keybindings_selection;
        let is_content_focused = self.content_focused && !self.keybindings_editing_filter;

        let conflicts_by_slot: HashMap<BindingSlot, Vec<BindingSlot>> = self
            .overrides
            .existing_conflicts(default_keymap())
            .into_iter()
            .collect();
        let recording_target = self.recorder.target().cloned();

        // Flat list required for scroll_to_item to work correctly
        let mut flat_items: Vec<KeybindingsListItem> = Vec::new();

        for (idx, context) in ContextId::all_variants().iter().enumerate() {
            if !self.is_context_visible(idx, &filter_text) {
                continue;
            }

            let is_expanded = self.is_context_expanded(context, has_filter);
            let entries = self.get_filtered_bindings(*context, &filter_text);

            let is_context_selected = is_content_focused
                && matches!(current_selection, KeybindingsSelection::Context(i) if i == idx);

            flat_items.push(KeybindingsListItem::ContextHeader {
                context: *context,
                ctx_idx: idx,
                is_expanded,
                is_selected: is_context_selected,
                binding_count: entries.len(),
            });

            if !is_expanded {
                continue;
            }

            for (binding_idx, entry) in entries.into_iter().enumerate() {
                let is_binding_selected = is_content_focused
                    && matches!(
                        current_selection,
                        KeybindingsSelection::Binding(ci, bi) if ci == idx && bi == binding_idx
                    );

                let is_recording = recording_target.as_ref() == Some(&entry.slot)
                    && self.recording_group == Some(*context);
                let is_overridden = self.overrides.is_overridden(&entry.slot);
                let conflict_context = conflicts_by_slot
                    .get(&entry.slot)
                    .and_then(|others| others.first())
                    .map(|other| other.context);

                let pending_conflict = if is_recording {
                    match self.recorder.state() {
                        RecordingState::Conflict {
                            slot,
                            chord,
                            conflicts,
                        } => Some(KeybindingsListItem::PendingConflict {
                            chord: chord.clone(),
                            target: slot.clone(),
                            conflicts: conflicts.clone(),
                        }),
                        _ => None,
                    }
                } else {
                    None
                };

                flat_items.push(KeybindingsListItem::Binding {
                    entry,
                    is_selected: is_binding_selected,
                    is_recording,
                    is_overridden,
                    conflict_context,
                    ctx_idx: idx,
                    binding_idx,
                });

                if let Some(item) = pending_conflict {
                    flat_items.push(item);
                }
            }
        }

        if let Some(scroll_idx) = self.keybindings_pending_scroll.take() {
            self.keybindings_scroll_handle.scroll_to_item(scroll_idx);
        }

        let inherited_label = dbflux_i18n::t!("settings.keybindings.inherited");

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .flex_shrink_0()
                    .pb(SettingsMetrics::PAGE_HEAD_PADDING_BOTTOM - Spacing::SM)
                    .border_b_1()
                    .border_color(border)
                    .child(dbflux_components::composites::page_header(
                        dbflux_i18n::t!("settings.keybindings.title"),
                        dbflux_i18n::t!("settings.keybindings.subtitle"),
                        cx,
                    )),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .flex()
                    .items_center()
                    .gap(FILTER_ROW_GAP)
                    .px(SettingsMetrics::BODY_PADDING_X)
                    .pt(SettingsMetrics::PAGE_HEAD_PADDING_BOTTOM)
                    .pb(Spacing::SM)
                    .child(div().flex_1().min_w_0().child(self.render_filter_field(
                        filter_is_empty,
                        filter_focused,
                        cx,
                    )))
                    .child(
                        div()
                            .flex_shrink_0()
                            .w(CONTEXT_FILTER_WIDTH)
                            .child(self.context_filter.clone()),
                    ),
            )
            .child(
                div()
                    .id("keybindings-scroll-container")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.keybindings_scroll_handle)
                    .px(SettingsMetrics::BODY_PADDING_X)
                    .pb(Spacing::XL)
                    .flex()
                    .flex_col()
                    .children(flat_items.into_iter().map(|item| {
                        match item {
                            KeybindingsListItem::ContextHeader {
                                context,
                                ctx_idx,
                                is_expanded,
                                is_selected,
                                binding_count,
                            } => Self::render_context_header(
                                context,
                                ctx_idx,
                                is_expanded,
                                is_selected,
                                binding_count,
                                group_line,
                                muted_foreground,
                                strong,
                                cx,
                            )
                            .into_any_element(),

                            KeybindingsListItem::Binding {
                                entry,
                                is_selected,
                                is_recording,
                                is_overridden,
                                conflict_context,
                                ctx_idx,
                                binding_idx,
                            } => self
                                .render_binding_row(
                                    entry,
                                    BindingRowState {
                                        is_selected,
                                        is_recording,
                                        is_overridden,
                                        conflict_context,
                                    },
                                    ctx_idx,
                                    binding_idx,
                                    &inherited_label,
                                    cx,
                                )
                                .into_any_element(),

                            KeybindingsListItem::PendingConflict {
                                chord,
                                target,
                                conflicts,
                            } => Self::render_pending_conflict(&chord, &target, &conflicts, cx)
                                .into_any_element(),
                        }
                    })),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_context_header(
        context: ContextId,
        ctx_idx: usize,
        is_expanded: bool,
        is_selected: bool,
        binding_count: usize,
        group_line: Hsla,
        muted_foreground: Hsla,
        strong: Hsla,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let parent_name = context
            .parent()
            .map(|parent| crate::labels::keybinding_context_name(&parent));

        ListRow::new(SharedString::from(format!(
            "context-{}",
            context.as_gpui_context()
        )))
        .focused(is_selected)
        .build(cx)
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .h(KEYBINDING_HEADER_HEIGHT)
        .mt(Spacing::SM)
        .border_b_1()
        .border_color(group_line)
        .on_click(cx.listener(move |this, _, _, cx| {
            this.keybindings_selection = KeybindingsSelection::Context(ctx_idx);

            if this.keybindings_expanded.contains(&context) {
                this.keybindings_expanded.remove(&context);
            } else {
                this.keybindings_expanded.insert(context);
            }
            cx.notify();
        }))
        .child(
            FluxIcon::new(if is_expanded {
                AppIcon::ChevronDown
            } else {
                AppIcon::ChevronRight
            })
            .size(KEYBINDING_CHEVRON)
            .color(muted_foreground),
        )
        .child(
            div()
                .flex_1()
                .flex()
                .items_center()
                .gap(FILTER_ROW_GAP)
                .child(
                    Text::body(crate::labels::keybinding_context_name(&context))
                        .font_weight(FontWeight::BOLD)
                        .text_color(strong),
                )
                .child(layout::help_text(crate::labels::keybindings_binding_count(
                    binding_count,
                ))),
        )
        .when_some(parent_name, |header, parent_name| {
            header.child(layout::help_text(crate::labels::keybindings_inherits_from(
                &parent_name,
            )))
        })
    }

    fn render_binding_row(
        &self,
        entry: BindingEntry,
        state: BindingRowState,
        ctx_idx: usize,
        binding_idx: usize,
        inherited_label: &str,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let row_divider = theme.table_row_border;
        let muted_foreground = theme.muted_foreground;
        let tint = ChromeColors::tint(theme);

        let command_name = crate::labels::keybinding_command_name(&entry.slot.command);
        let row_id = format!("keybinding-{ctx_idx}-{binding_idx}");

        let keycaps: AnyElement = if state.is_recording {
            Text::body(dbflux_i18n::t!("settings.keybindings.recording.prompt"))
                .text_color(tint)
                .into_any_element()
        } else {
            match &entry.chord {
                Some(chord) => Kbd::chord(chord_display_parts(chord)).into_any_element(),
                None => Text::body(dbflux_i18n::t!("settings.keybindings.no_shortcut"))
                    .muted_foreground()
                    .into_any_element(),
            }
        };

        let command_text = if entry.is_inherited {
            Text::body(command_name).color(muted_foreground)
        } else {
            Text::body(command_name)
        };

        let badge: Option<AnyElement> = if let Some(other_context) = state.conflict_context {
            Some(
                Badge::new(
                    crate::labels::keybindings_conflict_badge(
                        &crate::labels::keybinding_context_name(&other_context),
                    ),
                    BadgeTone::Warning,
                )
                .icon(AppIcon::TriangleAlert)
                .into_any_element(),
            )
        } else if entry.is_inherited {
            Some(Badge::new(inherited_label.to_string(), BadgeTone::Neutral).into_any_element())
        } else {
            None
        };

        let select_slot = entry.slot.clone();

        ListRow::new(SharedString::from(row_id.clone()))
            .selected(state.is_selected || state.is_recording)
            .build(cx)
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .h(KEYBINDING_ROW_HEIGHT)
            .pl(KEYBINDING_ROW_INDENT)
            .border_b_1()
            .border_color(row_divider)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.keybindings_selection = KeybindingsSelection::Binding(ctx_idx, binding_idx);
                if this
                    .recorder
                    .target()
                    .is_some_and(|target| *target != select_slot)
                {
                    this.cancel_recording(cx);
                }
                cx.notify();
            }))
            .child(
                div()
                    .w(SettingsMetrics::FORM_LABEL_WIDTH)
                    .flex_shrink_0()
                    .child(keycaps),
            )
            .child(div().flex_1().min_w_0().child(command_text))
            .when(state.is_recording, |row| {
                row.child(self.render_recording_controls(&entry, &row_id, cx))
            })
            .when(!state.is_recording, |row| {
                row.when_some(badge, |row, badge| row.child(badge))
                    .child(self.render_row_actions(
                        &entry,
                        state.is_overridden,
                        (ctx_idx, binding_idx),
                        &row_id,
                        cx,
                    ))
            })
    }

    /// Reset arrow (overridden bindings only) and the edit pencil.
    fn render_row_actions(
        &self,
        entry: &BindingEntry,
        is_overridden: bool,
        (ctx_idx, binding_idx): (usize, usize),
        row_id: &str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let edit_slot = entry.slot.clone();
        let reset_slot = entry.slot.clone();
        let group = ContextId::all_variants().get(ctx_idx).copied();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .when(is_overridden, |actions| {
                actions.child(
                    div().w(KEYBINDING_ACTION_WIDTH).child(
                        Button::new(
                            SharedString::from(format!("{row_id}-reset")),
                            dbflux_i18n::t!("settings.keybindings.action.reset"),
                        )
                        .ghost()
                        .inline()
                        .icon(AppIcon::RotateCcw)
                        .icon_only()
                        .tab_stop(false)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.reset_binding(&reset_slot, cx);
                        })),
                    ),
                )
            })
            .child(
                div().w(KEYBINDING_ACTION_WIDTH).child(
                    Button::new(
                        SharedString::from(format!("{row_id}-edit")),
                        dbflux_i18n::t!("settings.keybindings.action.edit"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::Pencil)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.keybindings_selection =
                            KeybindingsSelection::Binding(ctx_idx, binding_idx);
                        if let Some(group) = group {
                            this.start_recording(edit_slot.clone(), group, cx);
                        }
                    })),
                ),
            )
    }

    /// While a row records: the Esc hint, "Remove shortcut" when the binding
    /// has one, and a cancel button.
    fn render_recording_controls(
        &self,
        entry: &BindingEntry,
        row_id: &str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let remove_slot = entry.slot.clone();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::SM)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(Kbd::new("Esc"))
                    .child(Text::caption(dbflux_i18n::t!(
                        "settings.keybindings.recording.cancel_hint"
                    ))),
            )
            .when(entry.chord.is_some(), |controls| {
                controls.child(
                    Button::new(
                        SharedString::from(format!("{row_id}-remove")),
                        dbflux_i18n::t!("settings.keybindings.action.remove"),
                    )
                    .ghost()
                    .inline()
                    .tab_stop(false)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.remove_shortcut(remove_slot.clone(), cx);
                    })),
                )
            })
            .child(
                div().w(KEYBINDING_ACTION_WIDTH).child(
                    Button::new(
                        SharedString::from(format!("{row_id}-cancel")),
                        dbflux_i18n::t!("settings.keybindings.action.cancel"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::X)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.cancel_recording(cx);
                    })),
                ),
            )
    }

    /// Warning under the recorded row when its new chord is taken: names
    /// every binding that holds it and offers Cancel or Replace.
    fn render_pending_conflict(
        chord: &KeyChord,
        target: &BindingSlot,
        conflicts: &[BindingSlot],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let holders = conflicts
            .iter()
            .map(|slot| {
                crate::labels::keybindings_conflict_holder(
                    &crate::labels::keybinding_command_name(&slot.command),
                    &crate::labels::keybinding_context_name(&slot.context),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let chord_label = chord_display_parts(chord)
            .iter()
            .map(|part| part.to_string())
            .collect::<Vec<_>>()
            .join("+");

        let actions = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "keybindings-conflict-cancel",
                    dbflux_i18n::t!("settings.keybindings.action.cancel"),
                )
                .secondary()
                .inline()
                .kbd("Esc")
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cancel_recording(cx);
                })),
            )
            .child(
                Button::new(
                    "keybindings-conflict-replace",
                    dbflux_i18n::t!("settings.keybindings.action.replace"),
                )
                .primary()
                .inline()
                .kbd("Enter")
                .on_click(cx.listener(|this, _, _, cx| {
                    this.replace_conflicting(cx);
                })),
            );

        div().pl(KEYBINDING_ROW_INDENT).py(Spacing::XS).child(
            BannerBlock::new(
                BannerVariant::Warning,
                crate::labels::keybindings_conflict_title(&chord_label, &holders),
            )
            .with_body(crate::labels::keybindings_conflict_body(
                &crate::labels::keybinding_command_name(&target.command),
            ))
            .with_actions(actions),
        )
    }

    /// Left side of the footer: the conflict count in the warning color and
    /// the number of overridden bindings. Nothing while both are zero.
    pub(super) fn render_footer_summary(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let theme = cx.theme();
        let conflict_count = self
            .overrides
            .existing_conflicts(default_keymap())
            .iter()
            .map(|(_, others)| others.len())
            .sum::<usize>()
            / 2;
        let overridden_count = self.overrides.len();

        if conflict_count == 0 && overridden_count == 0 {
            return None;
        }

        Some(
            div()
                .id("keybindings-footer-summary")
                .flex()
                .items_center()
                .gap(SettingsMetrics::FOOTER_GAP + Spacing::SM)
                .text_size(SettingsMetrics::DIRTY_FONT)
                .when(conflict_count > 0, |summary| {
                    summary.child(
                        div()
                            .flex()
                            .items_center()
                            .gap(SettingsMetrics::FOOTER_GAP)
                            .text_color(theme.warning)
                            .child(
                                FluxIcon::new(AppIcon::TriangleAlert)
                                    .size(SettingsMetrics::NAV_SEARCH_ICON)
                                    .color(theme.warning),
                            )
                            .child(crate::labels::keybindings_conflict_count(conflict_count)),
                    )
                })
                .when(overridden_count > 0, |summary| {
                    summary.child(div().text_color(theme.muted_foreground).child(
                        crate::labels::keybindings_overridden_count(overridden_count),
                    ))
                })
                .into_any_element(),
        )
    }

    /// "Reset to defaults": drops every override. Disabled while the keymap
    /// is already the default one.
    pub(super) fn render_reset_all_button(&self, cx: &mut Context<Self>) -> AnyElement {
        Button::new(
            "keybindings-reset-all",
            dbflux_i18n::t!("settings.keybindings.action.reset_all"),
        )
        .secondary()
        .icon(AppIcon::RotateCcw)
        .disabled(self.overrides.is_empty())
        .on_click(cx.listener(|this, _, _, cx| {
            this.reset_all(cx);
        }))
        .into_any_element()
    }

    /// Filter field of the page: a 30 px chamfered field with the search
    /// icon, the frameless input and the `/` keycap while it is empty.
    fn render_filter_field(
        &self,
        filter_is_empty: bool,
        focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let mut shape = Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.background)
            .border(theme.border);

        if focused {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        div()
            .relative()
            .flex()
            .items_center()
            .gap(Fields::GAP)
            .h(Fields::HEIGHT)
            .px(Fields::PADDING_X)
            .text_size(Fields::TEXT)
            .child(shape)
            .child(
                FluxIcon::new(AppIcon::Search)
                    .size(SettingsMetrics::NAV_SEARCH_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Input::new(&self.keybindings_filter)
                        .id("keybindings-filter")
                        .aria_label(dbflux_i18n::t!("settings.keybindings.filter_label"))
                        .small()
                        .appearance(false),
                ),
            )
            .when(filter_is_empty, |field| field.child(Kbd::new("/")))
    }

    fn get_filter_text(&self, cx: &Context<Self>) -> String {
        self.keybindings_filter.read(cx).value().to_lowercase()
    }

    /// Rows of `context` that match `filter`, in display order.
    pub(super) fn filtered_entries(&self, context: ContextId, filter: &str) -> Vec<BindingEntry> {
        entries_for_context(&DEFAULT_SLOTS, &self.overrides, context)
            .into_iter()
            .filter(|entry| entry_matches_filter(entry, filter))
            .collect()
    }

    fn get_filtered_bindings(&self, context: ContextId, filter: &str) -> Vec<BindingEntry> {
        self.filtered_entries(context, filter)
    }

    fn is_context_visible(&self, ctx_idx: usize, filter: &str) -> bool {
        let Some(context) = ContextId::all_variants().get(ctx_idx) else {
            return false;
        };

        if self
            .context_filter_value
            .is_some_and(|chosen| chosen != *context)
        {
            return false;
        }

        filter.is_empty() || !self.get_filtered_bindings(*context, filter).is_empty()
    }

    fn is_context_expanded(&self, context: &ContextId, has_filter: bool) -> bool {
        has_filter
            || self.context_filter_value.is_some()
            || self.keybindings_expanded.contains(context)
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

    /// Position of the selection in the rendered flat list, for scrolling.
    /// A pending-conflict banner counts as one row after its binding.
    pub(super) fn keybindings_flat_index(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        let has_filter = !filter.is_empty();
        let mut flat_idx = 0;

        for (ctx_idx, context) in ContextId::all_variants().iter().enumerate() {
            if !self.is_context_visible(ctx_idx, &filter) {
                continue;
            }

            let entries = if self.is_context_expanded(context, has_filter) {
                self.get_filtered_bindings(*context, &filter)
            } else {
                Vec::new()
            };
            let banner_after = self.pending_conflict_position(&entries);

            match self.keybindings_selection {
                KeybindingsSelection::Context(sel) if sel == ctx_idx => return flat_idx,
                KeybindingsSelection::Binding(sel, bi) if sel == ctx_idx => {
                    let banner_rows = usize::from(banner_after.is_some_and(|row| row < bi));
                    return flat_idx + 1 + bi + banner_rows;
                }
                _ => {}
            }

            flat_idx += 1 + entries.len() + usize::from(banner_after.is_some());
        }
        flat_idx
    }

    /// Index in `entries` of the row whose pending conflict banner is shown
    /// right after it, if any.
    fn pending_conflict_position(&self, entries: &[BindingEntry]) -> Option<usize> {
        let RecordingState::Conflict { slot, .. } = self.recorder.state() else {
            return None;
        };

        entries.iter().position(|entry| entry.slot == *slot)
    }
}

/// Per-row flags of a binding row.
struct BindingRowState {
    is_selected: bool,
    is_recording: bool,
    is_overridden: bool,
    conflict_context: Option<ContextId>,
}

#[cfg(test)]
mod tests {
    use super::{BindingEntry, entries_for_context, entry_matches_filter};
    use crate::labels::{
        keybindings_binding_count, keybindings_conflict_badge, keybindings_conflict_count,
        keybindings_conflict_title, keybindings_inherits_from, keybindings_overridden_count,
    };
    use dbflux_app::keymap::{
        BindingSlot, Command, ContextId, KeyChord, KeymapOverrides, Modifiers,
    };

    const KEYBINDINGS_CATALOG_KEYS: &[&str] = &[
        "settings.keybindings.title",
        "settings.keybindings.subtitle",
        "settings.keybindings.filter_placeholder",
        "settings.keybindings.binding_count.one",
        "settings.keybindings.binding_count.many",
        "settings.keybindings.inherits_from",
        "settings.keybindings.inherited",
        "settings.keybindings.context_filter.all",
        "settings.keybindings.no_shortcut",
        "settings.keybindings.recording.prompt",
        "settings.keybindings.recording.cancel_hint",
        "settings.keybindings.action.edit",
        "settings.keybindings.action.reset",
        "settings.keybindings.action.remove",
        "settings.keybindings.action.cancel",
        "settings.keybindings.action.replace",
        "settings.keybindings.action.reset_all",
        "settings.keybindings.conflict.title",
        "settings.keybindings.conflict.body",
        "settings.keybindings.conflict.holder",
        "settings.keybindings.conflict.badge",
        "settings.keybindings.footer.conflicts.one",
        "settings.keybindings.footer.conflicts.many",
        "settings.keybindings.footer.overridden.one",
        "settings.keybindings.footer.overridden.many",
        "settings.keybindings.save_error",
    ];

    #[test]
    fn settings_keybindings_keys_resolve_in_every_locale() {
        for locale in ["en", "es", "ko", "zh_Hans"] {
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

        assert_eq!(english, "Keyboard shortcuts");
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
    fn keybindings_footer_counts_use_singular_and_plural_forms() {
        assert_ne!(keybindings_conflict_count(1), keybindings_conflict_count(2));
        assert!(keybindings_conflict_count(2).contains('2'));
        assert_ne!(
            keybindings_overridden_count(1),
            keybindings_overridden_count(4)
        );
        assert!(keybindings_overridden_count(4).contains('4'));
    }

    #[test]
    fn keybindings_inherits_from_embeds_parent_context_name() {
        let label = keybindings_inherits_from("Global");

        assert!(label.contains("Global"));
    }

    #[test]
    fn keybindings_conflict_labels_embed_their_arguments() {
        let title = keybindings_conflict_title("Ctrl+K", "Run Query (Editor)");

        assert!(title.contains("Ctrl+K"));
        assert!(title.contains("Run Query (Editor)"));
        assert!(keybindings_conflict_badge("Global").contains("Global"));
    }

    fn slot(context: ContextId, command: Command, key: &str, modifiers: Modifiers) -> BindingSlot {
        BindingSlot::new(context, command, KeyChord::new(key, modifiers))
    }

    fn test_slots() -> Vec<BindingSlot> {
        vec![
            slot(
                ContextId::Global,
                Command::NewQueryTab,
                "n",
                Modifiers::ctrl(),
            ),
            slot(
                ContextId::Global,
                Command::CloseCurrentTab,
                "w",
                Modifiers::ctrl(),
            ),
            slot(
                ContextId::Editor,
                Command::SaveQuery,
                "s",
                Modifiers::ctrl(),
            ),
            slot(
                ContextId::Editor,
                Command::OpenSavedQueries,
                "w",
                Modifiers::ctrl(),
            ),
        ]
    }

    #[test]
    fn entries_list_own_bindings_then_unshadowed_inherited_ones() {
        let slots = test_slots();
        let entries = entries_for_context(&slots, &KeymapOverrides::new(), ContextId::Editor);

        let commands: Vec<(Command, bool)> = entries
            .iter()
            .map(|entry| (entry.slot.command, entry.is_inherited))
            .collect();

        assert_eq!(
            commands,
            vec![
                (Command::SaveQuery, false),
                (Command::OpenSavedQueries, false),
                (Command::NewQueryTab, true),
            ],
            "Ctrl+W of the editor shadows the global Close Tab"
        );
    }

    #[test]
    fn entries_keep_unbound_own_bindings_and_drop_unbound_inherited_ones() {
        let slots = test_slots();
        let mut overrides = KeymapOverrides::new();
        overrides.set(slots[0].clone(), None);
        overrides.set(slots[2].clone(), None);

        let editor = entries_for_context(&slots, &overrides, ContextId::Editor);
        assert_eq!(editor[0].slot.command, Command::SaveQuery);
        assert_eq!(editor[0].chord, None);
        assert!(
            editor
                .iter()
                .all(|entry| entry.slot.command != Command::NewQueryTab),
            "an unbound global binding is not inherited"
        );

        let global = entries_for_context(&slots, &overrides, ContextId::Global);
        assert_eq!(global.len(), 2);
        assert_eq!(global[0].chord, None);
    }

    #[test]
    fn filter_matches_chord_or_command_name() {
        let entry = BindingEntry {
            slot: test_slots()[0].clone(),
            chord: Some(KeyChord::new("n", Modifiers::ctrl())),
            is_inherited: false,
        };

        assert!(entry_matches_filter(&entry, ""));
        assert!(entry_matches_filter(&entry, "ctrl+n"));
        assert!(!entry_matches_filter(&entry, "ctrl+q"));

        let unbound = BindingEntry {
            chord: None,
            ..entry
        };
        assert!(!entry_matches_filter(&unbound, "ctrl+n"));
    }
}
