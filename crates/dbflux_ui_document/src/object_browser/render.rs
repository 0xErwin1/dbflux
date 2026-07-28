//! Rendering for `ObjectBrowserDocument`.
//!
//! Layout, top to bottom: breadcrumb path bar, toolbar (per-prefix filter,
//! tree-mode toggle, upload / new folder / refresh), column header, listing
//! rows, and a footer summary + keyboard hint bar. The optional preview pane
//! splits off to the right of the listing. Every row carries a single
//! row-level mouse handler; cells are pure presentation.

use super::metadata::is_archived_storage_class;
use super::tree::{ObjectTreeEntry, ObjectTreeNodeId, PrefixLoadState, TreeModeStatus};
use super::{ObjectBrowserDocument, ObjectBrowserFocusMode, VisibleRow};
use crate::buckets_table::format_bytes;
use crate::handle::DocumentEvent;
use crate::types::DocumentState;
use dbflux_components::controls::Input;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::chrono::{DateTime, Utc};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Column widths. `Key` takes the remaining space; the rest are fixed so the
/// size column stays right-aligned against a stable edge.
const SIZE_WIDTH: Pixels = px(96.0);
const CLASS_WIDTH: Pixels = px(132.0);
const MODIFIED_WIDTH: Pixels = px(150.0);

/// Indentation applied per tree-mode depth level.
const TREE_INDENT: Pixels = px(14.0);

const UNKNOWN: &str = "—";

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
pub(super) fn object_icon(display_name: &str) -> AppIcon {
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

    let folder_word = if folders == 1 { "folder" } else { "folders" };
    let object_word = if objects == 1 { "object" } else { "objects" };

    format!(
        "{folders} {folder_word} · {objects} {object_word} · {}",
        format_bytes(total_bytes)
    )
}

/// Status line for a tree-mode walk, or `None` when tree mode is off.
pub(super) fn tree_mode_status_line(status: &TreeModeStatus, pages_walked: u32) -> Option<String> {
    match status {
        TreeModeStatus::Off => None,
        TreeModeStatus::Running => Some(format!("tree mode · walking ({pages_walked} pages)")),
        TreeModeStatus::Done => Some(format!("tree mode · {pages_walked} pages")),
        TreeModeStatus::Cancelled => Some("tree mode · cancelled".to_string()),
        TreeModeStatus::Capped => Some(format!("tree mode · stopped at {pages_walked} pages")),
        TreeModeStatus::Error(message) => Some(format!("tree mode · {message}")),
    }
}

impl ObjectBrowserDocument {
    /// Breadcrumb path bar: `s3:/` root, the bucket, then one clickable
    /// segment per prefix level. Clicking a segment navigates to that level.
    fn render_breadcrumb(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let segments = self.tree.breadcrumb_segments();
        let at_root = self.tree.current_prefix.is_empty();

        let separator = |cx: &Context<Self>| {
            div()
                .px(Spacing::XXS)
                .child(Text::caption("/").color(cx.theme().muted_foreground))
        };

        let mut trail = div().flex().items_center().overflow_hidden();
        let mut walked = String::new();

        for (index, segment) in segments.iter().enumerate() {
            walked.push_str(segment);
            walked.push('/');

            let target = walked.clone();
            let is_last = index + 1 == segments.len();

            trail = trail.child(separator(cx)).child(
                div()
                    .id(SharedString::from(format!("breadcrumb-{index}")))
                    .px(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.navigate_to_prefix(target.clone(), window, cx);
                    }))
                    .child(if is_last {
                        Text::code(segment.clone())
                    } else {
                        Text::code(segment.clone()).muted_foreground()
                    }),
            );
        }

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(
                div()
                    .id("object-browser-up")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size(Heights::CONTROL)
                    .rounded(Radii::SM)
                    .when(at_root, |d| d.opacity(0.4))
                    .when(!at_root, |d| {
                        d.cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.navigate_up(window, cx);
                            }))
                    })
                    .child(Icon::new(AppIcon::ChevronUp).small().muted()),
            )
            .child(Text::caption("s3:/").color(theme.muted_foreground))
            .child(
                div()
                    .id("breadcrumb-bucket")
                    .px(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.navigate_to_prefix(String::new(), window, cx);
                    }))
                    .child(Text::code(self.bucket.clone()).primary()),
            )
            .child(trail)
    }

    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_loading = self.state == DocumentState::Loading;
        let tree_mode_on = self.tree.tree_mode.status == TreeModeStatus::Running;

        let action_button =
            |id: &'static str, icon: AppIcon, label: &'static str, cx: &Context<Self>| {
                div()
                    .id(id)
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .h(Heights::CONTROL)
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(cx.theme().secondary))
                    .child(Icon::new(icon).small().muted())
                    .child(Text::caption(label))
            };

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap(Spacing::SM)
                    .max_w(px(360.0))
                    .child(Icon::new(AppIcon::ListFilter).small().muted())
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.focus_mode = ObjectBrowserFocusMode::Filter;
                                    cx.stop_propagation();
                                    cx.notify();
                                }),
                            )
                            .child(
                                Input::new(&self.filter_input)
                                    .small()
                                    .cleanable(true)
                                    .w_full(),
                            ),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .id("object-browser-tree-mode")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .border_1()
                            .border_color(if tree_mode_on {
                                theme.primary
                            } else {
                                theme.border
                            })
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_tree_mode(cx);
                            }))
                            .child(if tree_mode_on {
                                Icon::new(AppIcon::Layers).small().primary()
                            } else {
                                Icon::new(AppIcon::Layers).small().muted()
                            })
                            .child(if tree_mode_on {
                                Text::caption("Tree").primary()
                            } else {
                                Text::caption("Tree")
                            }),
                    )
                    .child(
                        action_button("object-browser-upload", AppIcon::ArrowUp, "Upload", cx)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.request_upload(cx);
                            })),
                    )
                    .child(
                        action_button(
                            "object-browser-new-folder",
                            AppIcon::Folder,
                            "New folder",
                            cx,
                        )
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.request_new_folder(cx);
                        })),
                    )
                    .child(
                        action_button(
                            "object-browser-refresh",
                            if is_loading {
                                AppIcon::Loader
                            } else {
                                AppIcon::RefreshCcw
                            },
                            "Refresh",
                            cx,
                        )
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.reload_current_prefix(cx);
                        })),
                    ),
            )
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(div().flex_1().child(Text::caption("Key")))
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::caption("Size")),
            )
            .child(div().w(CLASS_WIDTH).child(Text::caption("Class")))
            .child(
                div()
                    .w(MODIFIED_WIDTH)
                    .child(Text::caption("Last modified")),
            )
    }

    pub(super) fn render_storage_class(
        &self,
        storage_class: Option<&str>,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let label = storage_class_label(storage_class);

        match storage_class_style(storage_class) {
            StorageClassStyle::Standard => Text::code(label).muted_foreground().into_any_element(),
            StorageClassStyle::Infrequent => div()
                .px(Spacing::XS)
                .rounded(Radii::SM)
                .border_1()
                .border_color(theme.border)
                .bg(theme.secondary)
                .child(Text::caption(label))
                .into_any_element(),
            StorageClassStyle::Archived => div()
                .flex()
                .items_center()
                .gap(Spacing::XXS)
                .px(Spacing::XS)
                .rounded(Radii::SM)
                .border_1()
                .border_color(theme.warning)
                .child(Icon::new(AppIcon::Lock).small().warning())
                .child(Text::caption(label).warning())
                .into_any_element(),
        }
    }

    fn render_row(&self, row: &VisibleRow, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let display_name = row.entry.display_name(&row.parent_prefix);
        let node_id = row.entry.node_id();
        let row_id = SharedString::from(format!("object-row-{}", row.entry.full_key()));

        let (icon, name_element, size_label, class_element, modified_label, archived) =
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
                        Text::code(label).primary(),
                        UNKNOWN.to_string(),
                        div().into_any_element(),
                        UNKNOWN.to_string(),
                        false,
                    )
                }
                ObjectTreeEntry::Object(summary) => (
                    object_icon(&display_name),
                    Text::code(display_name.clone()),
                    format_bytes(summary.size_bytes),
                    self.render_storage_class(summary.storage_class.as_deref(), cx),
                    format_modified(summary.last_modified),
                    storage_class_style(summary.storage_class.as_deref())
                        == StorageClassStyle::Archived,
                ),
            };

        let activate_id = node_id.clone();
        let select_id = node_id.clone();

        div()
            .id(row_id)
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .h(Heights::ROW)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .when(archived, |d| d.opacity(0.55))
            .when(selected, |d| d.bg(theme.list_active))
            .when(!selected, |d| d.hover(|d| d.bg(theme.list_active)))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.select_node(select_id.clone(), cx);
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .on_click(cx.listener(move |this, event: &ClickEvent, window, cx| {
                if event.click_count() < 2 {
                    return;
                }

                match &activate_id {
                    ObjectTreeNodeId::Prefix(prefix) => {
                        this.navigate_to_prefix(prefix.clone(), window, cx)
                    }
                    ObjectTreeNodeId::Object(key) => this.open_preview(key.clone(), cx),
                }
            }))
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap(Spacing::SM)
                    .overflow_hidden()
                    .pl(TREE_INDENT * row.depth as f32)
                    .child(if row.entry.is_prefix() {
                        Icon::new(icon).small().primary()
                    } else {
                        Icon::new(icon).small().muted()
                    })
                    .child(
                        div()
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(name_element),
                    ),
            )
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::code(size_label).muted_foreground()),
            )
            .child(div().w(CLASS_WIDTH).child(class_element))
            .child(
                div()
                    .w(MODIFIED_WIDTH)
                    .child(Text::code(modified_label).muted_foreground()),
            )
            .into_any_element()
    }

    /// Continuation row for the current level, shown while `ListObjectsV2`
    /// still reports a continuation token.
    fn render_load_more(&self, loading: bool, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let prefix = self.tree.current_prefix.clone();

        div()
            .id("object-browser-load-more")
            .flex()
            .items_center()
            .justify_center()
            .gap(Spacing::XS)
            .h(Heights::ROW)
            .border_b_1()
            .border_color(theme.border)
            .when(loading, |d| d.opacity(0.6))
            .when(!loading, |d| {
                d.cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
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
                .small()
                .muted(),
            )
            .child(Text::caption(if loading {
                "Loading more…"
            } else {
                "Load more"
            }))
    }

    /// Per-level error strip: the failure stays attached to the level that
    /// failed instead of replacing the whole document with an error state.
    fn render_level_error(&self, message: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .overflow_hidden()
                    .child(Icon::new(AppIcon::TriangleAlert).small().danger())
                    .child(Text::caption(message.to_string()).danger()),
            )
            .child(
                div()
                    .id("object-browser-retry")
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .h(Heights::CONTROL)
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.muted))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.reload_current_prefix(cx);
                    }))
                    .child(Icon::new(AppIcon::RefreshCcw).small().muted())
                    .child(Text::caption("Retry")),
            )
    }

    fn render_empty_state(&self, loading: bool) -> AnyElement {
        let message = if loading {
            "Loading objects…".to_string()
        } else if self
            .tree
            .level(&self.tree.current_prefix)
            .is_some_and(|level| !level.filter.trim().is_empty())
        {
            "No keys match this filter".to_string()
        } else if self.tree.current_prefix.is_empty() {
            "This bucket is empty".to_string()
        } else {
            "This prefix is empty".to_string()
        };

        div()
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .child(Icon::new(AppIcon::Folder).size(Heights::ICON_LG).muted())
            .child(Text::muted(message))
            .into_any_element()
    }

    fn render_footer(&self, rows: &[VisibleRow], cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tree_mode_line = tree_mode_status_line(
            &self.tree.tree_mode.status,
            self.tree.tree_mode.pages_walked,
        );

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::MD)
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(Icon::new(AppIcon::Folder).small().muted())
                            .child(Text::caption(summary_line(rows))),
                    )
                    .when_some(tree_mode_line, |this, line| {
                        this.child(Text::caption(line).muted_foreground())
                    }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::MD)
                    .child(Text::key_hint("Enter open"))
                    .child(Text::key_hint("Space preview"))
                    .child(Text::key_hint("← up a level"))
                    .child(Text::key_hint("/ filter")),
            )
    }
}

impl Render for ObjectBrowserDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let rows = self.visible_rows();
        let selected = self.tree.selected.clone();

        let level_state = self
            .tree
            .level(&self.tree.current_prefix)
            .map(|level| level.state.clone())
            .unwrap_or_default();
        let has_more = self
            .tree
            .level(&self.tree.current_prefix)
            .is_some_and(|level| level.has_more());

        let is_loading = matches!(level_state, PrefixLoadState::Loading);
        let level_error = match &level_state {
            PrefixLoadState::Error(message) => Some(message.clone()),
            _ => None,
        };

        let listing = if rows.is_empty() {
            self.render_empty_state(is_loading)
        } else {
            div()
                .flex_1()
                .overflow_hidden()
                .children(rows.iter().map(|row| {
                    let is_selected = selected.as_ref() == Some(&row.entry.node_id());
                    self.render_row(row, is_selected, cx)
                }))
                .when(has_more, |this| {
                    this.child(
                        self.render_load_more(level_state == PrefixLoadState::LoadingMore, cx),
                    )
                })
                .into_any_element()
        };

        let preview_key = self.preview_key.clone();

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().background)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = ObjectBrowserFocusMode::Listing;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .child(self.render_breadcrumb(cx))
            .child(self.render_toolbar(cx))
            .when_some(level_error, |this, message| {
                this.child(self.render_level_error(&message, cx))
            })
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_row()
                    .overflow_hidden()
                    .child(
                        div()
                            .flex_1()
                            .flex()
                            .flex_col()
                            .overflow_hidden()
                            .child(self.render_header(cx))
                            .child(listing),
                    )
                    .when_some(preview_key, |this, key| {
                        this.child(self.render_preview_pane(&key, cx))
                    }),
            )
            .child(self.render_footer(&rows, cx))
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::{
        StorageClassStyle, format_modified, object_icon, storage_class_label, storage_class_style,
        summary_line, tree_mode_status_line,
    };
    use crate::object_browser::VisibleRow;
    use crate::object_browser::tree::{ObjectTreeEntry, TreeModeStatus};
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

    /// T24: the footer only carries a tree-mode line while tree mode is in
    /// use, and reports the cap explicitly when the walk stopped short.
    #[test]
    fn tree_mode_status_line_reports_only_active_walks() {
        assert_eq!(tree_mode_status_line(&TreeModeStatus::Off, 0), None);
        assert_eq!(
            tree_mode_status_line(&TreeModeStatus::Running, 3),
            Some("tree mode · walking (3 pages)".to_string())
        );
        assert_eq!(
            tree_mode_status_line(&TreeModeStatus::Capped, 500),
            Some("tree mode · stopped at 500 pages".to_string())
        );
    }
}
