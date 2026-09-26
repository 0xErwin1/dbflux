//! Rendering for `ObjectBrowserDocument`.
//!
//! Layout, top to bottom: breadcrumb path bar, toolbar (per-prefix filter,
//! tree-mode toggle, upload / new folder / refresh), column header, listing
//! rows, and a footer summary + keyboard hint bar. The optional preview pane
//! splits off to the right of the listing. Every row carries a single
//! row-level mouse handler; cells are pure presentation.

use super::metadata::is_archived_storage_class;
use super::tree::{ObjectTreeEntry, ObjectTreeNodeId, PrefixLoadState};
use super::{ListingRow, ObjectBrowserDocument, ObjectBrowserFocusMode, VisibleRow};
use crate::buckets_table::format_bytes;
use crate::chrome::{document_bar, document_footer, footer_item, footer_key_hint, search_field};
use crate::handle::DocumentEvent;
use crate::labels::object_browser_status_summary;
use crate::types::DocumentState;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{EmptyState, ListRow};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, Icon, SegmentedControl, SegmentedItem,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, DocumentMetrics, Fields, ObjectStoreMetrics, SyntaxColors,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::chrono::{DateTime, Utc};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Column widths after the key (P1Objects: 1fr, 100, 130, 140).
const SIZE_WIDTH: Pixels = px(100.0);
const CLASS_WIDTH: Pixels = px(130.0);
const MODIFIED_WIDTH: Pixels = px(140.0);
/// Width of the filter field in the header.
const FILTER_WIDTH: Pixels = px(240.0);
/// Height of the "Load more" row.
const LOAD_MORE_HEIGHT: Pixels = px(38.0);

/// Indentation applied per tree-mode depth level. Matches the connections
/// sidebar so both trees read at the same rhythm.
const TREE_INDENT: Pixels = px(14.0);

/// Width of the disclosure-chevron slot, reserved on every tree-mode row —
/// including object rows, which have nothing to disclose — so names stay
/// aligned within a level. Same slot the sidebar reserves.
const CHEVRON_SLOT: Pixels = px(14.0);

const UNKNOWN: &str = "—";

/// Opacity of an archived object's row: it cannot be read without a restore.
const ARCHIVED_ROW_OPACITY: f32 = 0.55;

/// How a storage class is presented in the listing. `Archived` also dims the
/// whole row: those objects cannot be read without a restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StorageClassStyle {
    Standard,
    Infrequent,
    Archived,
}

/// Classifies the raw storage-class string reported by the driver. Unknown
/// vendor-specific classes fall back to the plain presentation rather than
/// implying a tier the UI does not understand.
pub(super) fn storage_class_style(storage_class: Option<&str>) -> StorageClassStyle {
    if is_archived_storage_class(storage_class) {
        return StorageClassStyle::Archived;
    }

    match storage_class.unwrap_or("STANDARD").to_uppercase().as_str() {
        "STANDARD_IA" | "ONEZONE_IA" | "INTELLIGENT_TIERING" | "GLACIER_IR" => {
            StorageClassStyle::Infrequent
        }
        _ => StorageClassStyle::Standard,
    }
}

pub(super) fn storage_class_label(storage_class: Option<&str>) -> String {
    storage_class.unwrap_or("STANDARD").to_uppercase()
}

/// Icon for an object, chosen from its file extension. Prefixes always use the
/// folder icon and never reach here.
pub(crate) fn object_icon(display_name: &str) -> AppIcon {
    let extension = display_name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_lowercase())
        .unwrap_or_default();

    match extension.as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "svg" | "ico" | "avif" => AppIcon::Image,
        "json" | "yaml" | "yml" | "toml" | "xml" | "ndjson" => AppIcon::Braces,
        "csv" | "tsv" | "parquet" | "xlsx" | "avro" | "orc" => AppIcon::FileSpreadsheet,
        "txt" | "md" | "log" | "text" => AppIcon::ScrollText,
        "rs" | "py" | "js" | "ts" | "tsx" | "jsx" | "go" | "java" | "rb" | "php" | "c" | "cpp"
        | "h" | "sh" | "sql" | "html" | "css" => AppIcon::FileCode,
        "zip" | "gz" | "tar" | "tgz" | "bz2" | "zst" | "7z" => AppIcon::Layers,
        _ => AppIcon::File,
    }
}

/// Palette color of an object's icon: folders in amber, tables in green,
/// structured documents in blue, images in the NULL violet, the rest muted.
pub(crate) fn object_icon_color(icon: AppIcon, cx: &App) -> Hsla {
    let theme = cx.theme();

    match icon {
        AppIcon::Folder => theme.warning,
        AppIcon::FileSpreadsheet => theme.success,
        AppIcon::Braces | AppIcon::FileCode => theme.info,
        AppIcon::Image => SyntaxColors::for_current(cx).number,
        _ => theme.muted_foreground,
    }
}

/// Keystroke bound to `command` in the listing, from the live keymap.
fn listing_shortcut(command: Command) -> Option<String> {
    dbflux_ui_base::effective_keymap().shortcut_for_command(ContextId::Results, command)
}

pub(super) fn format_modified(modified: Option<DateTime<Utc>>) -> String {
    modified
        .map(|value| value.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| UNKNOWN.to_string())
}

/// Footer summary: how many folders and objects the listing shows, and the
/// total size of those objects.
pub(super) fn summary_line(rows: &[VisibleRow]) -> String {
    let folders = rows.iter().filter(|row| row.entry.is_prefix()).count();
    let objects = rows.len() - folders;

    let total_bytes: u64 = rows
        .iter()
        .filter_map(|row| match &row.entry {
            ObjectTreeEntry::Object(summary) => Some(summary.size_bytes),
            ObjectTreeEntry::Prefix(_) => None,
        })
        .sum();

    object_browser_status_summary(folders, objects, total_bytes)
}

impl ObjectBrowserDocument {
    /// The path field: the URI scheme, the bucket in the tint, one
    /// clickable segment per prefix level with the current one in the strong
    /// color, and a copy button for the current prefix's URI.
    fn render_path_field(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let segments = self.tree.breadcrumb_segments();
        let segment_count = segments.len();
        let entity = cx.entity();

        let mut path = div()
            .relative()
            .flex()
            .min_w_0()
            .items_center()
            .gap(ObjectStoreMetrics::PATH_GAP)
            .h(Fields::HEIGHT)
            .px(Fields::PADDING_X)
            .overflow_hidden()
            .whitespace_nowrap()
            .font_family(AppFonts::MONO)
            .text_size(Fields::TEXT)
            .text_color(muted)
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(div().child("s3://"))
            .child(
                div()
                    .id("object-path-bucket")
                    .cursor_pointer()
                    .font_weight(FontWeight::BOLD)
                    .text_color(tint)
                    .on_click({
                        let entity = entity.clone();
                        move |_, window, cx| {
                            entity.update(cx, |this, cx| {
                                this.navigate_to_prefix(String::new(), window, cx);
                            });
                        }
                    })
                    .child(self.bucket.clone()),
            )
            .child(div().child("/"));

        let mut walked = String::new();

        for (index, segment) in segments.iter().enumerate() {
            walked.push_str(segment);
            walked.push('/');

            let target = walked.clone();
            let entity = entity.clone();
            let is_current = index + 1 == segment_count;

            path = path
                .child(
                    div()
                        .id(SharedString::from(format!("object-path-segment-{index}")))
                        .cursor_pointer()
                        .when(is_current, |segment| segment.text_color(strong))
                        .when(!is_current, |segment| {
                            segment.hover(move |segment| segment.text_color(strong))
                        })
                        .on_click(move |_, window, cx| {
                            entity.update(cx, |this, cx| {
                                this.navigate_to_prefix(target.clone(), window, cx);
                            });
                        })
                        .child(segment.clone()),
                )
                .child(div().child("/"));
        }

        let current_prefix = self.tree.current_prefix.clone();

        path.child(
            div()
                .id("object-path-copy")
                .flex()
                .items_center()
                .cursor_pointer()
                .hover(move |copy| copy.text_color(strong))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.copy_object_uri(&current_prefix, cx);
                }))
                .child(
                    Icon::new(AppIcon::Copy)
                        .size(ObjectStoreMetrics::PATH_COPY_ICON)
                        .color(muted),
                ),
        )
    }

    /// The header row: up a level, the path, the prefix filter, the List and
    /// Tree switch, Upload, New folder and the primary Refresh.
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let is_loading = self.state == DocumentState::Loading;
        let tree_mode_on = self.tree.is_tree_mode();
        let at_root = self.tree.current_prefix.is_empty();

        let filter = div()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = ObjectBrowserFocusMode::Filter;
                    cx.stop_propagation();
                    cx.notify();
                }),
            )
            .child(search_field(
                &self.filter_input,
                Some(FILTER_WIDTH),
                self.focus_mode == ObjectBrowserFocusMode::Filter,
                cx,
            ));

        let weak_self = cx.weak_entity();
        let mode_switch = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "object-browser-mode-list",
                    dbflux_i18n::t!("document.object_browser.toolbar.list"),
                )
                .icon(AppIcon::Rows3),
                SegmentedItem::new(
                    "object-browser-mode-tree",
                    dbflux_i18n::t!("document.object_browser.toolbar.tree"),
                )
                .icon(AppIcon::Layers),
            ],
            if tree_mode_on {
                "object-browser-mode-tree"
            } else {
                "object-browser-mode-list"
            },
            move |id, _, cx| {
                let wants_tree = id.as_ref() == "object-browser-mode-tree";

                if let Some(doc) = weak_self.upgrade() {
                    doc.update(cx, |this, cx| {
                        if this.tree.is_tree_mode() != wants_tree {
                            this.toggle_tree_mode(cx);
                        }
                    });
                }
            },
        );

        document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
            .child(
                Button::new("object-browser-up", "")
                    .icon(AppIcon::ChevronUp)
                    .icon_only()
                    .disabled(at_root)
                    .tooltip(dbflux_i18n::t!("document.object_browser.toolbar.up"))
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.navigate_up(window, cx);
                    })),
            )
            .child(self.render_path_field(cx))
            .child(div().flex_1())
            .child(filter)
            .child(mode_switch)
            .child(
                Button::new(
                    "object-browser-upload",
                    dbflux_i18n::t!("document.object_browser.toolbar.upload"),
                )
                .icon(AppIcon::ArrowUp)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.request_upload(cx);
                })),
            )
            .child(
                Button::new(
                    "object-browser-new-folder",
                    dbflux_i18n::t!("document.object_browser.toolbar.new_folder"),
                )
                .icon(AppIcon::Folder)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.request_new_folder(cx);
                })),
            )
            .child(
                Button::new(
                    "object-browser-refresh",
                    dbflux_i18n::t!("document.object_browser.toolbar.refresh"),
                )
                .primary()
                .icon(if is_loading {
                    AppIcon::Loader
                } else {
                    AppIcon::RefreshCcw
                })
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.reload_current_prefix(cx);
                })),
            )
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(DocumentMetrics::TABLE_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .child(dbflux_i18n::t!("document.object_browser.columns.key")),
            )
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex_shrink_0()
                    .child(dbflux_i18n::t!("document.object_browser.columns.size")),
            )
            .child(
                div()
                    .w(CLASS_WIDTH)
                    .flex_shrink_0()
                    .child(dbflux_i18n::t!("document.object_browser.columns.class")),
            )
            .child(
                div()
                    .w(MODIFIED_WIDTH)
                    .flex_shrink_0()
                    .child(dbflux_i18n::t!(
                        "document.object_browser.columns.last_modified"
                    )),
            )
    }

    pub(super) fn render_storage_class(
        &self,
        storage_class: Option<&str>,
        _cx: &Context<Self>,
    ) -> AnyElement {
        let label = storage_class_label(storage_class);

        match storage_class_style(storage_class) {
            StorageClassStyle::Standard => Badge::new(label, BadgeTone::Neutral).into_any_element(),
            StorageClassStyle::Infrequent => {
                Badge::new(label, BadgeTone::Accent).into_any_element()
            }
            StorageClassStyle::Archived => Badge::new(label, BadgeTone::Info)
                .icon(AppIcon::Lock)
                .into_any_element(),
        }
    }

    /// Disclosure chevron for a tree-mode row. Prefix rows get a clickable
    /// chevron-right / chevron-down, exactly like the connections sidebar;
    /// object rows get the empty slot so names stay aligned within a level.
    fn render_tree_chevron(&self, row: &VisibleRow, cx: &mut Context<Self>) -> AnyElement {
        let slot = div()
            .id(SharedString::from(format!(
                "object-row-chevron-{}",
                row.entry.full_key()
            )))
            .w(CHEVRON_SLOT)
            .flex()
            .justify_center();

        let ObjectTreeEntry::Prefix(prefix) = &row.entry else {
            return slot.into_any_element();
        };

        let icon = if self.tree.is_expanded(prefix) {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };
        let prefix = prefix.clone();

        slot.cursor_pointer()
            // The row-level mouse-down would select the row underneath;
            // the chevron only ever discloses, same as in the sidebar.
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .on_click(cx.listener(move |this, _, _, cx| {
                cx.stop_propagation();
                this.toggle_tree_node(prefix.clone(), cx);
            }))
            .child(Icon::new(icon).size(CHEVRON_SLOT).muted())
            .into_any_element()
    }

    fn render_row(&self, row: &VisibleRow, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        // Built before the theme borrow: the chevron installs a listener and
        // so needs `cx` mutably.
        let tree_mode = self.tree.is_tree_mode();
        let chevron = tree_mode.then(|| self.render_tree_chevron(row, cx));
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);
        let display_name = row.entry.display_name(&row.parent_prefix);
        let node_id = row.entry.node_id();
        let row_id = SharedString::from(format!("object-row-{}", row.entry.full_key()));

        let (icon, name_label, size_label, class_element, modified_label, archived) =
            match &row.entry {
                ObjectTreeEntry::Prefix(prefix) => {
                    let child_count = self
                        .tree
                        .level(prefix)
                        .filter(|level| level.state == PrefixLoadState::Loaded)
                        .map(|level| level.entries.len());
                    let label = match child_count {
                        Some(count) => format!("{display_name}/  ({count})"),
                        None => format!("{display_name}/"),
                    };

                    (
                        AppIcon::Folder,
                        label,
                        UNKNOWN.to_string(),
                        div().into_any_element(),
                        UNKNOWN.to_string(),
                        false,
                    )
                }
                ObjectTreeEntry::Object(summary) => (
                    object_icon(&display_name),
                    display_name.clone(),
                    format_bytes(summary.size_bytes),
                    self.render_storage_class(summary.storage_class.as_deref(), cx),
                    format_modified(summary.last_modified),
                    storage_class_style(summary.storage_class.as_deref())
                        == StorageClassStyle::Archived,
                ),
            };

        let icon_color = object_icon_color(icon, cx);
        let activate_id = node_id.clone();
        let select_id = node_id.clone();
        let menu_id = node_id.clone();

        ListRow::new(row_id)
            .selected(selected)
            .selection_bar(true)
            .build(cx)
            // `uniform_list` sizes each item from its own content instead of
            // stretching it like a flex column child, so the row needs an
            // explicit full width to line up with the header columns.
            .w_full()
            .flex()
            .items_center()
            .h(ObjectStoreMetrics::OBJECT_ROW_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .when(archived, |d| d.opacity(ARCHIVED_ROW_OPACITY))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.select_node(select_id.clone(), cx);
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, _, cx| {
                    cx.stop_propagation();
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(menu_id.clone(), event.position, cx);
                }),
            )
            // In tree mode a folder row behaves like a sidebar folder: one
            // click discloses it. Everywhere else activation stays on the
            // double click, so a single click only ever selects.
            .on_click(cx.listener(move |this, event: &ClickEvent, window, cx| {
                let single_click = event.click_count() == 1;

                match &activate_id {
                    ObjectTreeNodeId::Prefix(prefix) if tree_mode => {
                        if single_click {
                            this.toggle_tree_node(prefix.clone(), cx);
                        }
                    }
                    ObjectTreeNodeId::Prefix(prefix) => {
                        if !single_click {
                            this.navigate_to_prefix(prefix.clone(), window, cx);
                        }
                    }
                    ObjectTreeNodeId::Object(key) => {
                        if !single_click {
                            this.open_preview(key.clone(), cx);
                        }
                    }
                }
            }))
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap(ObjectStoreMetrics::NAME_GAP)
                    .overflow_hidden()
                    .pl(TREE_INDENT * row.depth as f32)
                    .when_some(chevron, |d, chevron| d.child(chevron))
                    .child(
                        Icon::new(icon)
                            .size(ObjectStoreMetrics::NAME_ICON)
                            .color(icon_color),
                    )
                    .child(
                        div()
                            .truncate()
                            .font_family(AppFonts::MONO)
                            .text_size(ObjectStoreMetrics::OBJECT_NAME_FONT)
                            .text_color(strong)
                            .child(name_label),
                    ),
            )
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex_shrink_0()
                    .font_family(AppFonts::MONO)
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(muted)
                    .child(size_label),
            )
            .child(div().w(CLASS_WIDTH).flex_shrink_0().child(class_element))
            .child(
                div()
                    .w(MODIFIED_WIDTH)
                    .flex_shrink_0()
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(muted)
                    .child(modified_label),
            )
            .into_any_element()
    }

    /// Continuation row for one prefix node, shown while `ListObjectsV2`
    /// still reports a continuation token for it. Each node paginates
    /// independently — in tree mode every expanded node can show its own
    /// "load more" row, not just the current listing level.
    fn render_load_more(
        &self,
        depth: usize,
        prefix: &str,
        loading: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let row_id = SharedString::from(format!("object-browser-load-more-{prefix}"));
        let prefix = prefix.to_string();
        let strong = ChromeColors::strong(theme);

        div()
            .id(row_id)
            .w_full()
            .flex()
            .items_center()
            .justify_center()
            .gap(DocumentMetrics::GAP)
            .h(LOAD_MORE_HEIGHT)
            .pl(TREE_INDENT * depth as f32)
            .text_size(Fields::TEXT)
            .text_color(theme.muted_foreground)
            .when(loading, |d| d.opacity(Fields::DISABLED_OPACITY))
            .when(!loading, |d| {
                d.cursor_pointer()
                    .hover(move |d| d.text_color(strong))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.load_more(prefix.clone(), cx);
                    }))
            })
            .child(
                Icon::new(if loading {
                    AppIcon::Loader
                } else {
                    AppIcon::ChevronDown
                })
                .size(Fields::CHEVRON)
                .color(theme.muted_foreground),
            )
            .child(if loading {
                dbflux_i18n::t!("document.object_browser.status.loading_more")
            } else {
                dbflux_i18n::t!("document.object_browser.status.load_more")
            })
    }

    /// Per-level error strip: the failure stays attached to the level that
    /// failed instead of replacing the whole document with an error state.
    fn render_level_error(&self, message: &str, cx: &mut Context<Self>) -> impl IntoElement {
        BannerBlock::new(BannerVariant::Danger, message.to_string()).with_actions(
            Button::new(
                "object-browser-retry",
                dbflux_i18n::t!("document.object_browser.status.retry"),
            )
            .icon(AppIcon::RefreshCcw)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| {
                this.reload_current_prefix(cx);
            })),
        )
    }

    fn render_empty_state(&self, loading: bool) -> AnyElement {
        let message = if loading {
            dbflux_i18n::t!("document.object_browser.empty.loading")
        } else if self
            .tree
            .level(&self.tree.current_prefix)
            .is_some_and(|level| !level.filter.trim().is_empty())
        {
            dbflux_i18n::t!("document.object_browser.empty.filtered")
        } else if self.tree.current_prefix.is_empty() {
            dbflux_i18n::t!("document.object_browser.empty.bucket")
        } else {
            dbflux_i18n::t!("document.object_browser.empty.prefix")
        };

        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .p(DocumentMetrics::PADDING_X)
            .child(EmptyState::new(AppIcon::Folder, message))
            .into_any_element()
    }

    fn render_footer(&self, rows: &[VisibleRow], cx: &Context<Self>) -> impl IntoElement {
        let tree_mode_on = self.tree.is_tree_mode();

        let hints = [
            (
                Command::Execute,
                "document.object_browser.status.key_hint.open",
            ),
            (
                Command::ExpandCollapse,
                "document.object_browser.status.key_hint.preview",
            ),
            (
                Command::ColumnLeft,
                "document.object_browser.status.key_hint.up",
            ),
            (
                Command::Rename,
                "document.object_browser.status.key_hint.rename",
            ),
            (
                Command::Delete,
                "document.object_browser.status.key_hint.delete",
            ),
        ]
        .into_iter()
        .filter_map(|(command, key)| {
            listing_shortcut(command)
                .map(|shortcut| footer_key_hint(shortcut, dbflux_i18n::t!(key)))
        });

        document_footer(cx)
            .h(ObjectStoreMetrics::FOOTER_HEIGHT)
            .child(footer_item(AppIcon::Folder, summary_line(rows), cx))
            .when(tree_mode_on, |footer| {
                footer.child(dbflux_i18n::t!("document.object_browser.status.tree_mode"))
            })
            .child(div().flex_1())
            .children(hints)
    }
}

impl Render for ObjectBrowserDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Both of these need a `Window` that the background continuations that
        // queued them never had: building the editor's buffer, and resuming a
        // navigation the user chose to save before.
        if let Some(pending) = self.pending_text_body.take() {
            self.install_text_editor(pending, window, cx);
        }

        if let Some(navigation) = self.resume_navigation.take() {
            self.run_navigation(navigation, window, cx);
        }

        // Neither needs a `Window`, but draining them here keeps every
        // toolbar/action-bar intent flowing through the same
        // `pending_* + take()` convention as the two continuations above.
        self.drain_pending_upload(cx);
        self.drain_pending_new_folder(window, cx);
        self.drain_pending_object_action(window, cx);

        // The recursive-delete modal's type-to-confirm widget owns an
        // `InputState`, which is another thing only a render pass can build.
        self.ensure_delete_prefix_input(window, cx);

        let rows = self.visible_rows();
        let entry_rows: Vec<VisibleRow> = rows
            .iter()
            .filter_map(|row| match row {
                ListingRow::Entry(visible) => Some(visible.clone()),
                ListingRow::LoadMore { .. } => None,
            })
            .collect();
        let selected = self.tree.selected.clone();

        let level_state = self
            .tree
            .level(&self.tree.current_prefix)
            .map(|level| level.state.clone())
            .unwrap_or_default();

        let is_loading = matches!(level_state, PrefixLoadState::Loading);
        let level_error = match &level_state {
            PrefixLoadState::Error(message) => Some(message.clone()),
            _ => None,
        };

        // The listing is virtualized: a large bucket page (up to 1000 keys per
        // ListObjectsV2 call, more with expanded tree nodes) built as plain
        // `.children(...)` lays out every row on every frame and makes the
        // document lag. `uniform_list` only builds the rows in the viewport;
        // `min_h_0` is still required so the flex child shrinks instead of
        // pushing the footer off-screen.
        let listing = if entry_rows.is_empty() {
            self.render_empty_state(is_loading)
        } else {
            let entity = cx.entity().clone();
            let list_rows = rows.clone();
            let list_selected = selected.clone();

            div()
                .flex_1()
                .min_h_0()
                .overflow_hidden()
                .child(
                    uniform_list(
                        "object-browser-listing",
                        list_rows.len(),
                        move |range, _window, cx| {
                            entity.update(cx, |this, cx| {
                                range
                                    .filter_map(|index| list_rows.get(index))
                                    .map(|row| match row {
                                        ListingRow::Entry(visible) => {
                                            let is_selected = list_selected.as_ref()
                                                == Some(&visible.entry.node_id());
                                            this.render_row(visible, is_selected, cx)
                                        }
                                        ListingRow::LoadMore {
                                            depth,
                                            prefix,
                                            loading,
                                        } => this
                                            .render_load_more(*depth, prefix, *loading, cx)
                                            .into_any_element(),
                                    })
                                    .collect()
                            })
                        },
                    )
                    .size_full()
                    .track_scroll(&self.listing_scroll),
                )
                .into_any_element()
        };

        // The context menu positions itself at the click, which arrives in
        // window coordinates; this canvas keeps the document's own origin so
        // the popup can be placed inside it.
        let origin_probe = cx.entity().clone();
        let origin_canvas = canvas(
            move |bounds, _, cx| {
                origin_probe.update(cx, |this, _| {
                    this.panel_origin = bounds.origin;
                });
            },
            |_, _, _, _| {},
        )
        .absolute()
        .size_full();

        let pending_navigation = self.pending_navigation.clone();
        let pending_object_delete = self.pending_object_delete.clone();
        let delete_prefix_confirm = self.delete_prefix_confirm().cloned();
        let presign = self.presign().cloned();
        let has_new_folder = self.new_folder().is_some();
        let has_rename = self.rename_object().is_some();

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().popover)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = ObjectBrowserFocusMode::Listing;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .child(origin_canvas)
            .child(self.render_toolbar(cx))
            .when_some(level_error, |this, message| {
                this.child(self.render_level_error(&message, cx))
            })
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_row()
                    .overflow_hidden()
                    .child(
                        div()
                            .flex_1()
                            .min_h_0()
                            // Without `min_w_0` the listing's own content sets
                            // the column's floor, so a long key would push the
                            // preview pane instead of ellipsizing.
                            .min_w_0()
                            .flex()
                            .flex_col()
                            .overflow_hidden()
                            .child(self.render_header(cx))
                            .child(listing),
                    ),
            )
            .child(self.render_footer(&entry_rows, cx))
            .when_some(pending_navigation, |this, navigation| {
                this.child(self.render_unsaved_edits_confirm(&navigation, cx))
            })
            .when_some(pending_object_delete, |this, pending| {
                this.child(self.render_object_delete_confirm(&pending, cx))
            })
            .when_some(delete_prefix_confirm, |this, confirm| {
                this.child(self.render_delete_prefix_confirm(&confirm, cx))
            })
            .when_some(presign, |this, presign| {
                this.child(self.render_presign_modal(&presign, cx))
            })
            .when(has_new_folder, |this| {
                this.child(self.render_new_folder_overlay(cx))
            })
            .when(has_rename, |this| {
                this.child(self.render_rename_overlay(cx))
            })
            .when_some(self.context_menu.as_ref(), |this, menu| {
                this.child(self.render_context_menu(menu, cx))
            })
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::{
        StorageClassStyle, format_modified, object_icon, storage_class_label, storage_class_style,
        summary_line,
    };
    use crate::object_browser::VisibleRow;
    use crate::object_browser::tree::ObjectTreeEntry;
    use dbflux_components::icons::AppIcon;
    use dbflux_core::ObjectSummary;

    fn object_row(key: &str, size_bytes: u64) -> VisibleRow {
        VisibleRow {
            depth: 0,
            parent_prefix: String::new(),
            entry: ObjectTreeEntry::Object(ObjectSummary {
                key: key.to_string(),
                size_bytes,
                storage_class: None,
                last_modified: None,
            }),
        }
    }

    fn prefix_row(prefix: &str) -> VisibleRow {
        VisibleRow {
            depth: 0,
            parent_prefix: String::new(),
            entry: ObjectTreeEntry::Prefix(prefix.to_string()),
        }
    }

    /// T24: the footer counts folders and objects separately and sums only
    /// the object sizes.
    #[test]
    fn summary_line_counts_folders_objects_and_total_size() {
        let rows = [
            prefix_row("logs/"),
            object_row("a.txt", 1024),
            object_row("b.txt", 1024),
        ];

        assert_eq!(summary_line(&rows), "1 folder · 2 objects · 2.0 KiB");
    }

    /// T24: singular wording and a zero total for an empty listing.
    #[test]
    fn summary_line_handles_the_empty_listing() {
        assert_eq!(summary_line(&[]), "0 folders · 0 objects · 0 B");
    }

    /// T24: only GLACIER and DEEP_ARCHIVE are treated as archived (those are
    /// the tiers that cannot be previewed without a restore).
    #[test]
    fn storage_class_style_marks_only_the_archived_tiers() {
        assert_eq!(
            storage_class_style(Some("GLACIER")),
            StorageClassStyle::Archived
        );
        assert_eq!(
            storage_class_style(Some("deep_archive")),
            StorageClassStyle::Archived
        );
        assert_eq!(
            storage_class_style(Some("STANDARD_IA")),
            StorageClassStyle::Infrequent
        );
        assert_eq!(storage_class_style(None), StorageClassStyle::Standard);
        assert_eq!(
            storage_class_style(Some("VENDOR_SPECIFIC")),
            StorageClassStyle::Standard
        );
    }

    /// T24: an object without a reported storage class still shows the S3
    /// default rather than a placeholder.
    #[test]
    fn storage_class_label_defaults_to_standard() {
        assert_eq!(storage_class_label(None), "STANDARD");
        assert_eq!(storage_class_label(Some("glacier")), "GLACIER");
    }

    /// T24: the row icon follows the key's extension, with a generic file
    /// icon for anything unrecognized.
    #[test]
    fn object_icon_follows_the_extension() {
        assert_eq!(object_icon("photo.PNG"), AppIcon::Image);
        assert_eq!(object_icon("config.yaml"), AppIcon::Braces);
        assert_eq!(object_icon("export.csv"), AppIcon::FileSpreadsheet);
        assert_eq!(object_icon("notes.md"), AppIcon::ScrollText);
        assert_eq!(object_icon("main.rs"), AppIcon::FileCode);
        assert_eq!(object_icon("backup"), AppIcon::File);
    }

    /// T24: a missing modification date renders as the em-dash placeholder.
    #[test]
    fn format_modified_falls_back_to_the_placeholder() {
        assert_eq!(format_modified(None), "—");
    }
}
