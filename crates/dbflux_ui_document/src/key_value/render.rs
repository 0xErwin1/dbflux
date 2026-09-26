//! Layout of the key-value document: the document toolbar, the pattern and
//! type filter row, the key list beside the value pane, the command console
//! and the overlays (bulk actions menu, confirmations, context menu).

use super::KeyValueDocument;
use super::bulk_delete::{
    BULK_DELETE_BATCH_SIZE, BULK_DELETE_PREVIEW_KEYS, BulkDeleteStage, BulkDeleteState,
    BulkScanState,
};
use super::key_tree::{KeyListLayout, group_thousands};
use super::metadata::{format_ttl, ttl_tone};
use super::parsing::{database_label, key_type_label, type_badge};
use super::view::{render_delete_confirm_modal, render_kv_context_menu};
use crate::handle::DocumentEvent;
use dbflux_components::composites::{
    Breadcrumb, BreadcrumbSegment, MenuItem, refresh_split_button, render_menu_items,
    render_menu_overlay,
};
use dbflux_components::controls::{Button, ButtonVariant, Input};
use dbflux_components::icons::{AppIcon, DriverIconTone};
use dbflux_components::modals::Modal;
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, Icon, Kbd, SegmentedControl, SegmentedItem, Text, vdivider,
};
use dbflux_components::tokens::{ChamferCut, ChromeColors, FontSizes, KeyValueMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::{KeyType, KeyValueFeatures};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Types offered by the server-side type filter, in toolbar order.
const TYPE_FILTERS: [KeyType; 7] = [
    KeyType::String,
    KeyType::Hash,
    KeyType::List,
    KeyType::Set,
    KeyType::SortedSet,
    KeyType::Stream,
    KeyType::Json,
];

pub(super) fn type_filter_id(type_filter: Option<KeyType>) -> &'static str {
    match type_filter {
        None => "all",
        Some(KeyType::String) | Some(KeyType::Bytes) => "string",
        Some(KeyType::Hash) => "hash",
        Some(KeyType::List) => "list",
        Some(KeyType::Set) => "set",
        Some(KeyType::SortedSet) => "zset",
        Some(KeyType::Stream) => "stream",
        Some(KeyType::Json) => "json",
        Some(KeyType::Unknown) => "all",
    }
}

pub(super) fn type_filter_from_id(id: &str) -> Option<KeyType> {
    TYPE_FILTERS
        .into_iter()
        .find(|key_type| type_filter_id(Some(*key_type)) == id)
}

fn layout_id(layout: KeyListLayout) -> &'static str {
    match layout {
        KeyListLayout::List => "list",
        KeyListLayout::Tree => "tree",
    }
}

/// Runs `update` on the document if it is still alive; a closed document
/// simply ignores the late UI callback.
pub(super) fn update_document(
    entity: &WeakEntity<KeyValueDocument>,
    cx: &mut App,
    update: impl FnOnce(&mut KeyValueDocument, &mut Context<KeyValueDocument>),
) {
    if let Err(error) = entity.update(cx, update) {
        log::debug!("key-value document closed before a UI callback: {error}");
    }
}

/// Key count chip of the breadcrumb: `1,474 keys`.
pub(super) fn key_total_label(total: u64) -> String {
    if total == 1 {
        dbflux_i18n::t!("document.key_value.toolbar.key_total.one")
    } else {
        dbflux_i18n::t!(
            "document.key_value.toolbar.key_total.many",
            count = group_thousands(total)
        )
    }
}

/// Type badge used in the key list, the value header and the bulk-delete
/// matches: short name on a 4 px chamfer, 26 x 18 px.
pub(super) fn type_badge_element(key_type: Option<KeyType>, cx: &App) -> impl IntoElement {
    let (label, tone) = type_badge(key_type);
    let color = tone.text_color(cx.theme());

    div()
        .relative()
        .flex()
        .flex_none()
        .items_center()
        .justify_center()
        .w(KeyValueMetrics::TYPE_BADGE_WIDTH)
        .h(KeyValueMetrics::TYPE_BADGE_HEIGHT)
        .child(
            Chamfer::new(ChamferCut::KEYCAP)
                .fill(color.opacity(KeyValueMetrics::TYPE_BADGE_FILL_ALPHA)),
        )
        .child(
            div()
                .font_family(AppFonts::MONO)
                .text_size(KeyValueMetrics::TYPE_BADGE_FONT)
                .font_weight(FontWeight::BOLD)
                .text_color(color)
                .child(label),
        )
}

impl Render for KeyValueDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_open_new_key_modal {
            self.pending_open_new_key_modal = false;
            self.new_key_modal
                .update(cx, |modal, cx| modal.open(window, cx));
        }

        if let Some(key_type) = self.pending_open_add_member_modal.take() {
            self.add_member_modal
                .update(cx, |modal, cx| modal.open(key_type, window, cx));
        }

        if let Some((action, target)) = self.pending_menu_action.take() {
            self.execute_menu_action(action, target, window, cx);
        }

        self.flush_pending_stream_pane(window, cx);
        self.ensure_bulk_delete_input(window, cx);

        let theme = cx.theme().clone();

        let has_pending_delete =
            self.pending_key_delete.is_some() || self.pending_member_delete.is_some();
        let (delete_title, delete_message) = if let Some(pending) = &self.pending_key_delete {
            (
                dbflux_i18n::t!("document.key_value.render.delete_key.title"),
                dbflux_i18n::t!(
                    "document.key_value.render.delete_key.message",
                    key = pending.key
                ),
            )
        } else if let Some(pending) = &self.pending_member_delete {
            (
                dbflux_i18n::t!("document.key_value.render.delete_member.title"),
                dbflux_i18n::t!(
                    "document.key_value.render.delete_member.message",
                    member = pending.member_display
                ),
            )
        } else {
            (String::new(), String::new())
        };

        let toolbar = self.render_document_toolbar(cx);
        let filter_row = self.render_filter_row(cx);
        let key_list = self.render_key_list_section(cx);
        let value_pane = self.render_value_section(cx);
        let console = self.render_console(cx);
        let bulk_modal = self
            .bulk_delete
            .as_ref()
            .map(|state| self.render_bulk_delete_modal(state, cx));
        let bulk_menu = self
            .bulk_actions_open
            .then(|| self.render_bulk_actions_menu(cx));

        let this_entity = cx.entity().clone();

        div()
            .id("kv-document")
            .size_full()
            .relative()
            .flex()
            .flex_col()
            .bg(theme.table)
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                if this.handle_document_key(event, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = super::KeyValueFocusMode::List;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .child(
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
            .child(toolbar)
            .child(filter_row)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .child(key_list)
                    .child(value_pane),
            )
            .child(console)
            .when_some(bulk_menu, |root, menu| root.child(menu))
            .when(self.new_key_modal.read(cx).is_visible(), |root| {
                root.child(self.new_key_modal.clone())
            })
            .when(self.add_member_modal.read(cx).is_visible(), |root| {
                root.child(self.add_member_modal.clone())
            })
            .when_some(bulk_modal, |root, modal| root.child(modal))
            .when(has_pending_delete, |root| {
                root.child(render_delete_confirm_modal(
                    &delete_title,
                    &delete_message,
                    cx,
                ))
            })
            .when_some(self.context_menu.as_ref(), |root, menu| {
                root.child(render_kv_context_menu(
                    menu,
                    &self.context_menu_focus,
                    self.panel_origin,
                    cx,
                ))
            })
    }
}

impl KeyValueDocument {
    /// Document-level shortcuts that are not in the shared keymap: `t` for
    /// the expiry editor, `Ctrl+J` to load more keys and `Ctrl+\`` for the
    /// console. Keys typed into a field are left alone.
    fn handle_document_key(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let keystroke = &event.keystroke;
        let modifiers = &keystroke.modifiers;
        let only_control =
            modifiers.control && !modifiers.alt && !modifiers.shift && !modifiers.platform;

        if only_control && keystroke.key == "`" {
            self.toggle_console(window, cx);
            return true;
        }

        if !self.focus_handle.is_focused(window) {
            return false;
        }

        if only_control && keystroke.key == "j" {
            self.load_more_keys(cx);
            return true;
        }

        let no_modifiers = !modifiers.control
            && !modifiers.alt
            && !modifiers.shift
            && !modifiers.platform
            && !modifiers.function;

        if no_modifiers && keystroke.key == "t" && self.selected_value.is_some() {
            self.open_expiry_editor(window, cx);
            return true;
        }

        false
    }

    fn render_document_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let state = self.app_state.read(cx);

        let mut segments = Vec::new();

        if let Some(profile) = state
            .profiles()
            .iter()
            .find(|profile| profile.id == self.profile_id)
        {
            let mut segment = BreadcrumbSegment::new(profile.name.clone());

            if let Some(driver) = state.drivers().get(&profile.driver_id()) {
                let metadata = driver.metadata();
                segment = segment.icon(
                    AppIcon::for_driver(metadata.icon, metadata.category),
                    Some(DriverIconTone::for_driver(metadata.icon, metadata.category).resolve(cx)),
                );
            }

            segments.push(segment);
        }

        segments.push(BreadcrumbSegment::new(database_label(&self.database)));

        let mut breadcrumb = Breadcrumb::new(segments);
        if let Some(total) = self.key_total {
            breadcrumb = breadcrumb.meta(key_total_label(total));
        }

        let refresh_entity = cx.entity().downgrade();
        let refresh = refresh_split_button(
            "kv-refresh-control",
            self.refresh_policy,
            false,
            false,
            self.refresh_dropdown.clone(),
            move |_, cx| {
                update_document(&refresh_entity, cx, |this, cx| {
                    if this.runner.is_primary_active() {
                        this.runner.cancel_primary(cx);
                        this.last_error = None;
                        cx.notify();
                    } else {
                        this.reload_keys(cx);
                    }
                });
            },
        )
        .variant(ButtonVariant::Primary);

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(KeyValueMetrics::TOOLBAR_GAP)
            .h(KeyValueMetrics::TOOLBAR_HEIGHT)
            .px(KeyValueMetrics::TOOLBAR_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(breadcrumb)
            .child(div().flex_1())
            .when(self.supports_bulk_delete(cx), |toolbar| {
                toolbar.child(
                    Button::new(
                        "kv-bulk-actions",
                        dbflux_i18n::t!("document.key_value.toolbar.bulk_actions"),
                    )
                    .icon(AppIcon::Layers)
                    .selected(self.bulk_actions_open)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.bulk_actions_open = !this.bulk_actions_open;
                        cx.notify();
                    })),
                )
            })
            .child(
                Button::new(
                    "kv-new-key",
                    dbflux_i18n::t!("document.key_value.toolbar.new_key"),
                )
                .icon(AppIcon::Plus)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.pending_open_new_key_modal = true;
                    cx.notify();
                })),
            )
            .child(refresh)
    }

    fn render_filter_row(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let entity = cx.entity().downgrade();

        let pattern_field = div()
            .flex_1()
            .min_w_0()
            .font_family(AppFonts::MONO)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = super::KeyValueFocusMode::TextInput;
                    cx.stop_propagation();
                    cx.notify();
                }),
            )
            .child(
                Input::new(&self.filter_input)
                    .id("kv-pattern")
                    .w_full()
                    .prefix(
                        Icon::new(AppIcon::Search)
                            .size(KeyValueMetrics::FOLDER_ICON)
                            .color(theme.muted_foreground),
                    )
                    .suffix(Kbd::new("/")),
            );

        let type_control = self
            .key_features
            .contains(KeyValueFeatures::SCAN_TYPE_FILTER)
            .then(|| {
                let items = std::iter::once(SegmentedItem::new(
                    "all",
                    dbflux_i18n::t!("document.key_value.toolbar.type_all"),
                ))
                .chain(TYPE_FILTERS.into_iter().map(|key_type| {
                    SegmentedItem::new(type_filter_id(Some(key_type)), key_type_label(key_type))
                }))
                .collect();

                let entity = entity.clone();
                SegmentedControl::new(items, type_filter_id(self.type_filter), move |id, _, cx| {
                    let type_filter = type_filter_from_id(id.as_ref());
                    update_document(&entity, cx, |this, cx| {
                        this.set_type_filter(type_filter, cx)
                    });
                })
            });

        let layout_control = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    layout_id(KeyListLayout::List),
                    dbflux_i18n::t!("document.key_value.toolbar.layout_list"),
                )
                .icon(AppIcon::Rows3),
                SegmentedItem::new(
                    layout_id(KeyListLayout::Tree),
                    dbflux_i18n::t!("document.key_value.toolbar.layout_tree"),
                )
                .icon(AppIcon::Layers),
            ],
            layout_id(self.list_layout),
            move |id, _, cx| {
                let layout = if id.as_ref() == layout_id(KeyListLayout::List) {
                    KeyListLayout::List
                } else {
                    KeyListLayout::Tree
                };
                update_document(&entity, cx, |this, cx| this.set_list_layout(layout, cx));
            },
        );

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(KeyValueMetrics::TOOLBAR_GAP)
            .h(KeyValueMetrics::TOOLBAR_HEIGHT)
            .px(KeyValueMetrics::TOOLBAR_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(pattern_field)
            .when_some(type_control, |row, control| row.child(control))
            .child(
                vdivider(cx)
                    .h(KeyValueMetrics::TOOLBAR_DIVIDER_HEIGHT)
                    .mx(KeyValueMetrics::TOOLBAR_DIVIDER_MARGIN_X),
            )
            .child(layout_control)
    }

    fn render_bulk_actions_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().downgrade();
        let dismiss_entity = entity.clone();

        let items = vec![
            MenuItem::new(dbflux_i18n::t!("document.key_value.bulk_delete.menu_item"))
                .icon(AppIcon::Delete)
                .danger(),
        ];

        let menu = render_menu_items(
            "kv-bulk-actions-menu",
            &items,
            None,
            move |_, cx| update_document(&entity, cx, |this, cx| this.open_bulk_delete(cx)),
            |_, _| {},
            cx,
        );

        deferred(
            div()
                .absolute()
                .size_full()
                .child(render_menu_overlay(
                    "kv-bulk-actions-overlay",
                    move |_, cx| {
                        update_document(&dismiss_entity, cx, |this, cx| {
                            this.bulk_actions_open = false;
                            cx.notify();
                        });
                    },
                ))
                .child(
                    div()
                        .absolute()
                        .top(KeyValueMetrics::TOOLBAR_HEIGHT)
                        .right(KeyValueMetrics::TOOLBAR_PADDING_X)
                        .occlude()
                        .child(menu),
                ),
        )
        .with_priority(1)
        .into_any_element()
    }

    fn render_bulk_delete_modal(
        &self,
        state: &BulkDeleteState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let muted = theme.muted_foreground;
        let entity = cx.entity().downgrade();

        let close = {
            let entity = entity.clone();
            move |window: &mut Window, cx: &mut App| {
                update_document(&entity, cx, |this, cx| this.close_bulk_delete(window, cx));
            }
        };

        let match_count = state.matches.len() as u64;
        let scanning = state.scan_state == BulkScanState::Scanning;
        let typed = self.bulk_delete_typed_text(cx);
        let can_delete = state.can_delete(&typed);

        let title = if scanning {
            dbflux_i18n::t!(
                "document.key_value.bulk_delete.title_scanning",
                count = group_thousands(match_count)
            )
        } else {
            dbflux_i18n::t!(
                "document.key_value.bulk_delete.title",
                count = group_thousands(match_count)
            )
        };

        let scope = match state.type_filter {
            Some(key_type) => dbflux_i18n::t!(
                "document.key_value.bulk_delete.scope_typed",
                database = database_label(&self.database),
                pattern = state.pattern.as_str(),
                key_type = key_type_label(key_type)
            ),
            None => dbflux_i18n::t!(
                "document.key_value.bulk_delete.scope",
                database = database_label(&self.database),
                pattern = state.pattern.as_str()
            ),
        };

        let preview_rows = state
            .matches
            .iter()
            .take(BULK_DELETE_PREVIEW_KEYS)
            .enumerate()
            .map(|(index, entry)| {
                let ttl = state
                    .preview_ttls
                    .get(&entry.key)
                    .copied()
                    .flatten()
                    .map(|seconds| format_ttl(Some(seconds.max(0) as u64)));

                div()
                    .id(("kv-bulk-match", index))
                    .flex()
                    .items_center()
                    .gap(KeyValueMetrics::FOOTER_GAP)
                    .h(KeyValueMetrics::BULK_ROW_HEIGHT)
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_color(theme.table_row_border)
                    .font_family(AppFonts::MONO)
                    .text_size(KeyValueMetrics::LIST_ROW_FONT)
                    .child(type_badge_element(entry.key_type, cx))
                    .child(div().text_color(strong).child(entry.key.clone()))
                    .child(div().flex_1())
                    .when_some(ttl, |row, ttl| {
                        row.child(
                            div()
                                .text_size(KeyValueMetrics::LIST_META_FONT)
                                .text_color(theme.danger)
                                .child(ttl),
                        )
                    })
            })
            .collect::<Vec<_>>();

        let remaining = match_count.saturating_sub(BULK_DELETE_PREVIEW_KEYS as u64);

        let matches_block = div()
            .flex()
            .flex_col()
            .gap(Spacing::XXS)
            .child(Text::label(dbflux_i18n::t!(
                "document.key_value.bulk_delete.matches_label"
            )))
            .child(
                div()
                    .relative()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(Chamfer::new(ChamferCut::INPUT).border(theme.border))
                    .children(preview_rows)
                    .when(remaining > 0, |list| {
                        list.child(
                            div()
                                .flex()
                                .items_center()
                                .gap(KeyValueMetrics::FOOTER_GAP)
                                .h(KeyValueMetrics::BULK_ROW_HEIGHT)
                                .px(Spacing::MD)
                                .font_family(AppFonts::MONO)
                                .text_size(KeyValueMetrics::LIST_ROW_FONT)
                                .text_color(muted)
                                .child(div().w(KeyValueMetrics::TYPE_BADGE_WIDTH))
                                .child(dbflux_i18n::t!(
                                    "document.key_value.bulk_delete.more",
                                    count = group_thousands(remaining)
                                )),
                        )
                    })
                    .when(match_count == 0 && !scanning, |list| {
                        list.child(
                            div()
                                .flex()
                                .items_center()
                                .h(KeyValueMetrics::BULK_ROW_HEIGHT)
                                .px(Spacing::MD)
                                .text_color(muted)
                                .child(dbflux_i18n::t!(
                                    "document.key_value.bulk_delete.no_matches"
                                )),
                        )
                    })
                    .when(scanning, |list| {
                        list.child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(KeyValueMetrics::BULK_ROW_HEIGHT)
                                .px(Spacing::MD)
                                .text_color(muted)
                                .child(
                                    Icon::new(AppIcon::Loader)
                                        .size(KeyValueMetrics::META_ICON)
                                        .color(muted),
                                )
                                .child(dbflux_i18n::t!(
                                    "document.key_value.bulk_delete.scanning",
                                    count = group_thousands(match_count)
                                ))
                                .child(div().flex_1())
                                .child(
                                    Button::new(
                                        "kv-bulk-stop-scan",
                                        dbflux_i18n::t!("document.key_value.bulk_delete.stop"),
                                    )
                                    .ghost()
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.stop_bulk_delete_scan(cx);
                                        },
                                    )),
                                ),
                        )
                    }),
            );

        let scan_note = match &state.scan_state {
            BulkScanState::Cancelled => Some(dbflux_i18n::t!(
                "document.key_value.bulk_delete.scan_stopped"
            )),
            BulkScanState::Failed(message) => Some(dbflux_i18n::t!(
                "document.key_value.bulk_delete.scan_failed",
                error = message.as_str()
            )),
            _ => None,
        };

        let confirm_block = div()
            .flex()
            .flex_col()
            .gap(Spacing::XXS)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .text_size(FontSizes::XS)
                    .text_color(muted)
                    .child(dbflux_i18n::t!(
                        "document.key_value.bulk_delete.type_prefix"
                    ))
                    .child(
                        div()
                            .font_family(AppFonts::MONO)
                            .text_color(strong)
                            .child(state.pattern.clone()),
                    )
                    .child(dbflux_i18n::t!(
                        "document.key_value.bulk_delete.type_suffix"
                    )),
            )
            .when_some(state.confirm_input.clone(), |block, input| {
                block.child(div().font_family(AppFonts::MONO).child(input))
            });

        let note = div()
            .flex()
            .items_center()
            .gap(Spacing::XXS)
            .text_size(FontSizes::XS)
            .text_color(muted)
            .child(
                Icon::new(AppIcon::FingerprintPattern)
                    .size(KeyValueMetrics::META_ICON)
                    .color(muted),
            )
            .child(dbflux_i18n::t!(
                "document.key_value.bulk_delete.note",
                count = BULK_DELETE_BATCH_SIZE
            ));

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body(scope))
            .child(matches_block)
            .when_some(scan_note, |body, note| {
                body.child(Text::caption(note).warning())
            })
            .child(confirm_block)
            .child(note);

        let exporting = state.stage == BulkDeleteStage::Exporting;
        let deleting = state.stage == BulkDeleteStage::Deleting;
        let exported = state.exported_to.is_some();

        let footer = div()
            .flex()
            .items_center()
            .justify_end()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "kv-bulk-export",
                    if exported {
                        dbflux_i18n::t!("document.key_value.bulk_delete.exported")
                    } else {
                        dbflux_i18n::t!("document.key_value.bulk_delete.export")
                    },
                )
                .icon(if exporting {
                    AppIcon::Loader
                } else if exported {
                    AppIcon::Check
                } else {
                    AppIcon::Download
                })
                .disabled(scanning || exporting || deleting || match_count == 0)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.export_bulk_delete_matches(cx);
                })),
            )
            .child(
                Button::new(
                    "kv-bulk-cancel",
                    dbflux_i18n::t!("document.key_value.bulk_delete.cancel"),
                )
                .disabled(deleting)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.close_bulk_delete(window, cx);
                })),
            )
            .child(
                Button::new(
                    "kv-bulk-confirm",
                    dbflux_i18n::t!(
                        "document.key_value.bulk_delete.confirm",
                        count = group_thousands(match_count)
                    ),
                )
                .danger()
                .icon(if deleting {
                    AppIcon::Loader
                } else {
                    AppIcon::Delete
                })
                .disabled(!can_delete)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.confirm_bulk_delete(cx);
                })),
            );

        Modal::new(title)
            .id("kv-bulk-delete-modal")
            .danger()
            .icon(AppIcon::Delete)
            .width(KeyValueMetrics::BULK_MODAL_WIDTH)
            .header_extra(Kbd::new("Esc"))
            .on_close(close)
            .body(body)
            .footer(footer)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{TYPE_FILTERS, key_total_label, type_filter_from_id, type_filter_id};
    use dbflux_core::KeyType;

    #[test]
    fn type_filter_ids_round_trip() {
        for key_type in TYPE_FILTERS {
            assert_eq!(
                type_filter_from_id(type_filter_id(Some(key_type))),
                Some(key_type)
            );
        }

        assert_eq!(type_filter_id(None), "all");
        assert_eq!(type_filter_from_id("all"), None);
    }

    #[test]
    fn bytes_keys_filter_as_strings() {
        assert_eq!(type_filter_id(Some(KeyType::Bytes)), "string");
    }

    #[test]
    fn key_total_labels_group_digits() {
        assert_eq!(key_total_label(1_474), "1,474 keys");
        assert_eq!(key_total_label(1), "1 key");
    }
}
