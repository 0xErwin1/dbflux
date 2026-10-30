//! Rendering of the listing of `SecretStoreDocument`: the toolbar with the
//! path and the store-side search, the rows, and the footer. The details pane
//! lives in `details.rs`.

use super::listing::{PathMode, child_path_label, path_segments, relative_name};
use super::{RegionListState, SecretFocusMode, SecretRowKey, SecretStoreDocument};
use crate::chrome::{
    connection_segment, document_bar, document_footer, footer_item, footer_key_hint, search_field,
};
use crate::handle::DocumentEvent;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{
    Breadcrumb, BreadcrumbSegment, EmptyState, EmptyStateAction, ListRow,
};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, SegmentedControl, SegmentedItem};
use dbflux_components::tokens::{
    ChromeColors, DocumentMetrics, NavigationMetrics, ObjectStoreMetrics, SecretStoreMetrics,
};
use dbflux_core::chrono::{DateTime, Local, Utc};
use dbflux_core::{SecretRotation, SecretStoreCapabilities, SecretSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

/// Placeholder for a field the store did not report.
pub(super) const UNKNOWN: &str = "—";

/// Keystroke bound to `command` while the listing has the keyboard.
pub(super) fn listing_shortcut(command: Command) -> Option<String> {
    dbflux_ui_base::effective_keymap().shortcut_for_command(ContextId::Results, command)
}

/// A timestamp in local time, as the listing and details show it.
pub(super) fn format_time(time: DateTime<Utc>) -> String {
    time.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

pub(super) fn format_optional_time(time: Option<DateTime<Utc>>) -> String {
    time.map(format_time).unwrap_or_else(|| UNKNOWN.to_string())
}

/// The rotation column: the schedule in words ("Every 30 days"), the raw
/// schedule expression when it is not a plain rate, or Off.
pub(super) fn rotation_label(rotation: Option<&SecretRotation>) -> String {
    let Some(rotation) = rotation.filter(|rotation| rotation.enabled) else {
        return dbflux_i18n::t!("document.secret_store.rotation.off");
    };

    let Some(schedule) = rotation.schedule.as_deref() else {
        return dbflux_i18n::t!("document.secret_store.rotation.enabled");
    };

    rate_label(schedule).unwrap_or_else(|| schedule.to_string())
}

/// `rate(30 days)` as "Every 30 days", `rate(12 hours)` as "Every 12 hours".
fn rate_label(schedule: &str) -> Option<String> {
    let inner = schedule.trim().strip_prefix("rate(")?.strip_suffix(')')?;
    let (count, unit) = inner.trim().split_once(' ')?;
    let count: u32 = count.parse().ok()?;

    let key = match unit.trim() {
        "day" | "days" => "document.secret_store.rotation.every_days",
        "hour" | "hours" => "document.secret_store.rotation.every_hours",
        _ => return None,
    };

    Some(dbflux_i18n::t!(key, count = count))
}

/// "1 secret loaded" or "N secrets loaded".
pub(super) fn loaded_label(count: usize) -> String {
    let key = if count == 1 {
        "document.secret_store.footer.loaded.one"
    } else {
        "document.secret_store.footer.loaded.many"
    };

    dbflux_i18n::t!(key, count = count)
}

/// The status bar text of a store call.
pub(super) fn call_timing_label(timing: super::CallTiming) -> String {
    let millis = timing.millis.to_string();

    match timing.call {
        super::StoreCall::List { count } => dbflux_i18n::t!(
            if count == 1 {
                "document.secret_store.status.list_timing.one"
            } else {
                "document.secret_store.status.list_timing.many"
            },
            count = count,
            millis = millis.as_str()
        ),
        super::StoreCall::Describe => dbflux_i18n::t!(
            "document.secret_store.status.describe_timing",
            millis = millis.as_str()
        ),
        super::StoreCall::Retrieve => dbflux_i18n::t!(
            "document.secret_store.status.retrieve_timing",
            millis = millis.as_str()
        ),
    }
}

/// A fixed-width listing cell in the meta font.
fn meta_cell(width: Rems, text: String, color: Hsla) -> Div {
    div()
        .w(width)
        .flex_shrink_1()
        .min_w_0()
        .pr(DocumentMetrics::GAP)
        .truncate()
        .text_size(DocumentMetrics::TABLE_META_FONT)
        .text_color(color)
        .child(text)
}

impl SecretStoreDocument {
    /// The connection, the store and, on a hierarchical store, every segment
    /// of the current path. Each segment before the last opens its level.
    /// The store segment of the breadcrumb row: the connection and the store.
    fn identity_segments(&self, cx: &mut Context<Self>) -> Vec<BreadcrumbSegment> {
        let mut segments: Vec<BreadcrumbSegment> =
            connection_segment(&self.app_state, self.profile_id, cx)
                .into_iter()
                .collect();

        let entity = cx.entity().downgrade();
        let navigate = move |path: Option<String>| {
            let entity = entity.clone();
            move |_: &ClickEvent, _: &mut Window, cx: &mut App| {
                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.navigate_to(path.clone(), cx));
                }
            }
        };

        let mut store_segment =
            BreadcrumbSegment::new(self.store.label.clone()).icon(AppIcon::Lock, None);

        if self.hierarchical() && self.path.is_some() {
            store_segment = store_segment.on_click("secret-path-crumb-root", navigate(None));
        }

        segments.push(store_segment);
        segments
    }

    /// On a hierarchical store, every segment of the current path. Each one
    /// before the last opens its level.
    fn store_path_segments(&self, cx: &mut Context<Self>) -> Vec<BreadcrumbSegment> {
        if !self.hierarchical() {
            return Vec::new();
        }

        let entity = cx.entity().downgrade();
        let navigate = move |path: Option<String>| {
            let entity = entity.clone();
            move |_: &ClickEvent, _: &mut Window, cx: &mut App| {
                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.navigate_to(path.clone(), cx));
                }
            }
        };

        let segments = path_segments(self.path.as_deref().unwrap_or_default());
        let last = segments.len().saturating_sub(1);

        segments
            .into_iter()
            .enumerate()
            .map(|(index, (label, path))| {
                let segment = BreadcrumbSegment::new(label);

                if index == last {
                    segment
                } else {
                    segment.on_click(format!("secret-path-crumb-{index}"), navigate(Some(path)))
                }
            })
            .collect()
    }

    /// Separator between two breadcrumb groups, the one glyph the
    /// `Breadcrumb` component draws between its own segments.
    fn breadcrumb_separator(cx: &Context<Self>) -> Icon {
        Icon::new(AppIcon::ChevronRight)
            .size(NavigationMetrics::BREADCRUMB_CHEVRON)
            .color(cx.theme().input)
    }

    /// The connection, the store, the region when the connection can move
    /// between them, and the path of a hierarchical store.
    ///
    /// The region belongs with the identity the breadcrumb already spells
    /// out: "which region am I reading" is the same question as "which store
    /// is this", so it sits between the store and the path rather than in the
    /// toolbar among the controls.
    ///
    /// While the region list is loading or failed, the picker cannot be
    /// offered; the region then stays visible as a plain segment, so the
    /// listing never loses the answer to which region it was loaded under.
    fn render_breadcrumb_row(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let mut row = div()
            .flex()
            .min_w_0()
            .items_center()
            .gap(NavigationMetrics::BREADCRUMB_GAP)
            .child(Breadcrumb::new(self.identity_segments(cx)).mono());

        if let Some(picker) = self.render_region_picker() {
            row = row.child(Self::breadcrumb_separator(cx)).child(picker);
        } else if self.region_switchable
            && let Some(region) = self.region.clone()
        {
            row = row.child(Self::breadcrumb_separator(cx)).child(
                // The same wrapper id the picker carries, so the plain
                // segment reaches the accessibility tree too.
                div()
                    .id("secret-region-plain")
                    .flex_shrink_0()
                    .child(Breadcrumb::new(vec![BreadcrumbSegment::new(region)])),
            );
        }

        let path = self.store_path_segments(cx);

        if !path.is_empty() {
            row = row
                .child(Self::breadcrumb_separator(cx))
                .child(Breadcrumb::new(path).mono());
        }

        row
    }

    /// The region picker, once the connection listed its regions. It is the
    /// last segment of the breadcrumb row, and it is the only dropdown of the
    /// document, so its trigger carries the picker's own element id.
    fn render_region_picker(&self) -> Option<AnyElement> {
        if !matches!(self.regions, RegionListState::Loaded(_)) {
            return None;
        }

        self.region_dropdown.clone().map(|dropdown| {
            div()
                .id("secret-region")
                .flex_shrink_0()
                .child(dropdown.into_any_element())
                .into_any_element()
        })
    }

    /// The failure of the region listing: the store's error and the retry,
    /// where the toolbar keeps transient notices. A failed listing never
    /// renders as an empty picker.
    fn render_region_failure(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let RegionListState::Failed(error) = &self.regions else {
            return Vec::new();
        };

        vec![
            div()
                .id("secret-region-error")
                .max_w(px(240.0))
                .truncate()
                .text_size(DocumentMetrics::TABLE_META_FONT)
                .text_color(cx.theme().muted_foreground)
                .child(error.clone())
                .into_any_element(),
            Button::new(
                "secret-region-retry",
                dbflux_i18n::t!("document.secret_store.region.retry"),
            )
            .on_click(cx.listener(|this, _, _, cx| this.load_regions(cx)))
            .into_any_element(),
        ]
    }

    fn render_path_mode(&self, cx: &mut Context<Self>) -> SegmentedControl {
        let entity = cx.entity().downgrade();
        let active = match self.path_mode {
            PathMode::Tree => "tree",
            PathMode::Flat => "flat",
        };

        SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "tree",
                    dbflux_i18n::t!("document.secret_store.path_mode.tree"),
                ),
                SegmentedItem::new(
                    "flat",
                    dbflux_i18n::t!("document.secret_store.path_mode.flat"),
                ),
            ],
            active,
            move |id, _window, cx| {
                let mode = if id.as_ref() == "flat" {
                    PathMode::Flat
                } else {
                    PathMode::Tree
                };

                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.set_path_mode(mode, cx));
                }
            },
        )
        .group("secret-path-mode")
    }

    /// Breadcrumb row, the Tree | Flat switch of a hierarchical store, the
    /// store-side search and Refresh, plus the notice of a failed region
    /// listing.
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let mut refresh = Button::new(
            "secret-refresh",
            dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
        )
        .primary()
        .icon(if self.listing.loading {
            AppIcon::Loader
        } else {
            AppIcon::RefreshCcw
        })
        .tab_stop(false)
        .on_click(cx.listener(|this, _, _, cx| this.refresh(cx)));

        if let Some(key) = listing_shortcut(Command::RefreshSchema) {
            refresh = refresh.kbd(key);
        }

        let hierarchical = self.hierarchical();

        // The breadcrumb and the search narrow before Refresh does, so a
        // narrow listing keeps its toolbar actions on screen.
        document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
            .child(
                div()
                    .min_w_0()
                    .overflow_hidden()
                    .child(self.render_breadcrumb_row(cx)),
            )
            .child(div().flex_1())
            .when(hierarchical, |bar| bar.child(self.render_path_mode(cx)))
            .when(self.region_switchable, |bar| {
                bar.children(self.render_region_failure(cx))
            })
            .child(
                div()
                    .flex()
                    .w(ObjectStoreMetrics::SEARCH_WIDTH)
                    .min_w(SecretStoreMetrics::SEARCH_MIN_WIDTH)
                    .flex_shrink_1()
                    .child(search_field(
                        &self.search_input,
                        None,
                        self.focus_mode == SecretFocusMode::Search,
                        cx,
                    )),
            )
            .child(div().flex_shrink_0().child(refresh))
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let column = |width: Option<Rems>, key: &str| {
            let label = dbflux_i18n::t!(key);
            match width {
                Some(width) => div()
                    .w(width)
                    .flex_shrink_1()
                    .min_w_0()
                    .pr(DocumentMetrics::GAP)
                    .truncate()
                    .child(label),
                None => div()
                    .flex_1()
                    .min_w(SecretStoreMetrics::NAME_MIN_WIDTH)
                    .truncate()
                    .child(label),
            }
        };

        let header = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(ObjectStoreMetrics::TABLE_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(DocumentMetrics::TABLE_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(column(None, "document.secret_store.columns.name"));

        if self.store.supports(SecretStoreCapabilities::TYPED_VALUES) {
            header
                .child(column(
                    Some(SecretStoreMetrics::TYPE_WIDTH),
                    "document.secret_store.columns.type",
                ))
                .child(column(
                    Some(SecretStoreMetrics::TIER_WIDTH),
                    "document.secret_store.columns.tier",
                ))
                .child(column(
                    Some(SecretStoreMetrics::VERSION_WIDTH),
                    "document.secret_store.columns.version",
                ))
                .child(column(
                    Some(SecretStoreMetrics::LAST_CHANGED_WIDTH),
                    "document.secret_store.columns.last_modified",
                ))
        } else {
            header
                .child(column(
                    Some(SecretStoreMetrics::DESCRIPTION_WIDTH),
                    "document.secret_store.columns.description",
                ))
                .when(
                    self.store.supports(SecretStoreCapabilities::ROTATION),
                    |header| {
                        header.child(column(
                            Some(SecretStoreMetrics::ROTATION_WIDTH),
                            "document.secret_store.columns.rotation",
                        ))
                    },
                )
                .child(column(
                    Some(SecretStoreMetrics::LAST_CHANGED_WIDTH),
                    "document.secret_store.columns.last_changed",
                ))
        }
    }

    /// The badges after a secret's name: the service that manages it and a
    /// pending deletion.
    fn summary_badges(summary: &SecretSummary) -> Vec<Badge> {
        let mut badges = Vec::new();

        if let Some(service) = summary.owning_service.as_deref() {
            badges.push(Badge::new(
                dbflux_i18n::t!("document.secret_store.badge.managed", service = service),
                BadgeTone::Info,
            ));
        }

        if let Some(date) = summary.deletion_scheduled_for {
            badges.push(Badge::new(
                dbflux_i18n::t!(
                    "document.secret_store.badge.deletes",
                    date = date.with_timezone(&Local).format("%Y-%m-%d").to_string()
                ),
                BadgeTone::Danger,
            ));
        }

        badges
    }

    fn row_frame(&self, id: SharedString, selected: bool, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();

        ListRow::new(id)
            .selected(selected)
            .selection_bar(true)
            .build(cx)
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(ObjectStoreMetrics::TABLE_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .text_size(ObjectStoreMetrics::OBJECT_NAME_FONT)
    }

    fn row_name(icon: AppIcon, icon_color: Hsla, name: String, cx: &App) -> Div {
        div()
            .flex()
            .flex_1()
            .min_w(SecretStoreMetrics::NAME_MIN_WIDTH)
            .items_center()
            .gap(ObjectStoreMetrics::NAME_GAP)
            .overflow_hidden()
            .child(
                Icon::new(icon)
                    .size(ObjectStoreMetrics::NAME_ICON)
                    .color(icon_color),
            )
            .child(
                div()
                    .truncate()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_color(ChromeColors::strong(cx.theme()))
                    .child(name),
            )
    }

    fn render_secret_row(
        &self,
        summary: &SecretSummary,
        selected: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let foreground = theme.foreground;
        let lock_color = theme.warning;
        let row = SecretRowKey::Secret(summary.name.clone());

        let display_name = if self.path_mode == PathMode::Tree {
            relative_name(&summary.name, self.path.as_deref()).to_string()
        } else {
            summary.name.clone()
        };

        let name = Self::row_name(AppIcon::KeyRound, lock_color, display_name, cx)
            .children(Self::summary_badges(summary));

        let frame = self
            .row_frame(format!("secret-row-{}", summary.name).into(), selected, cx)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_row(row.clone(), cx);
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .child(name);

        let last_changed = summary
            .last_changed
            .map(format_time)
            .unwrap_or_else(|| UNKNOWN.to_string());

        if self.store.supports(SecretStoreCapabilities::TYPED_VALUES) {
            let or_unknown =
                |value: &Option<String>| value.clone().unwrap_or_else(|| UNKNOWN.to_string());

            return frame
                .child(meta_cell(
                    SecretStoreMetrics::TYPE_WIDTH,
                    or_unknown(&summary.value_type),
                    foreground,
                ))
                .child(meta_cell(
                    SecretStoreMetrics::TIER_WIDTH,
                    or_unknown(&summary.tier),
                    muted,
                ))
                .child(meta_cell(
                    SecretStoreMetrics::VERSION_WIDTH,
                    or_unknown(&summary.version),
                    muted,
                ))
                .child(meta_cell(
                    SecretStoreMetrics::LAST_CHANGED_WIDTH,
                    last_changed,
                    muted,
                ))
                .into_any_element();
        }

        frame
            .child(meta_cell(
                SecretStoreMetrics::DESCRIPTION_WIDTH,
                summary
                    .description
                    .clone()
                    .unwrap_or_else(|| UNKNOWN.to_string()),
                muted,
            ))
            .when(
                self.store.supports(SecretStoreCapabilities::ROTATION),
                |row| {
                    row.child(meta_cell(
                        SecretStoreMetrics::ROTATION_WIDTH,
                        rotation_label(summary.rotation.as_ref()),
                        foreground,
                    ))
                },
            )
            .child(meta_cell(
                SecretStoreMetrics::LAST_CHANGED_WIDTH,
                last_changed,
                muted,
            ))
            .into_any_element()
    }

    fn render_path_row(&self, path: &str, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        let folder_color = cx.theme().muted_foreground;
        let row = SecretRowKey::Path(path.to_string());
        let open = path.to_string();

        self.row_frame(format!("secret-path-row-{path}").into(), selected, cx)
            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                if event.click_count() >= 2 {
                    this.navigate_to(Some(open.clone()), cx);
                } else {
                    this.select_row(row.clone(), cx);
                }
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .child(Self::row_name(
                AppIcon::Folder,
                folder_color,
                child_path_label(path),
                cx,
            ))
            .into_any_element()
    }

    fn render_load_more_row(&self, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        let tint = ChromeColors::tint(cx.theme());
        let label = if self.listing.loading {
            dbflux_i18n::t!("document.secret_store.loading_more")
        } else {
            dbflux_i18n::t!("document.secret_store.load_more")
        };

        self.row_frame("secret-load-more".into(), selected, cx)
            .on_click(cx.listener(|this, _, _, cx| {
                this.load_more(cx);
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .text_color(tint)
            .child(label)
            .into_any_element()
    }

    fn render_rows(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.rows();

        if rows.is_empty() {
            return self.render_empty_state();
        }

        let selected = self.selected.clone();
        let elements: Vec<AnyElement> = rows
            .iter()
            .map(|row| {
                let is_selected = selected.as_ref() == Some(row);
                match row {
                    SecretRowKey::Path(path) => self.render_path_row(path, is_selected, cx),
                    SecretRowKey::Secret(name) => match self.listing.find(name).cloned() {
                        Some(summary) => self.render_secret_row(&summary, is_selected, cx),
                        None => div().into_any_element(),
                    },
                    SecretRowKey::LoadMore => self.render_load_more_row(is_selected, cx),
                }
            })
            .collect();

        div()
            .id("secret-rows")
            .flex_1()
            .min_h_0()
            .overflow_y_scrollbar()
            .children(elements)
            .into_any_element()
    }

    fn render_empty_state(&self) -> AnyElement {
        let message = match (&self.listing.error, self.listing.loading) {
            (_, true) => dbflux_i18n::t!("document.secret_store.empty.loading"),
            (Some(error), false) => {
                dbflux_i18n::t!("document.secret_store.empty.error", error = error.as_str())
            }
            (None, false) if !self.name_filter.is_empty() => dbflux_i18n::t!(
                "document.secret_store.empty.no_match",
                query = self.name_filter.as_str()
            ),
            (None, false) => dbflux_i18n::t!("document.secret_store.empty.no_secrets"),
        };

        let is_error = self.listing.error.is_some() && !self.listing.loading;
        let mut empty = EmptyState::new(
            if is_error {
                AppIcon::TriangleAlert
            } else {
                AppIcon::Lock
            },
            message,
        );

        if is_error {
            empty = empty.danger();
        }

        if let Some(key) = listing_shortcut(Command::RefreshSchema) {
            empty = empty.action(EmptyStateAction::new(
                AppIcon::RefreshCcw,
                dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
                [key],
            ));
        }

        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .p(DocumentMetrics::PADDING_X)
            .child(empty)
            .into_any_element()
    }

    fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let mut hints = vec![
            (Command::Execute, "document.secret_store.footer.hint.open"),
            (
                Command::ToggleValuePanel,
                "document.secret_store.footer.hint.retrieve",
            ),
            (
                Command::ResultsCopyCell,
                "document.secret_store.footer.hint.copy_id",
            ),
            (
                Command::FocusSearch,
                "document.secret_store.footer.hint.filter",
            ),
        ];

        if self.hierarchical() {
            hints.push((Command::ColumnLeft, "document.secret_store.footer.hint.up"));
        }

        let hints = hints.into_iter().filter_map(|(command, key)| {
            listing_shortcut(command)
                .map(|shortcut| footer_key_hint(shortcut, dbflux_i18n::t!(key)))
        });

        document_footer(cx)
            .h(ObjectStoreMetrics::FOOTER_HEIGHT)
            .child(footer_item(
                AppIcon::KeyRound,
                loaded_label(self.listing.entries.len()),
                cx,
            ))
            .child(div().flex_1())
            .children(hints)
    }

    /// Builds the inputs that draw a retrieved value, and keeps their masked
    /// flag on the reveal state. Each one is read-only and marked secret, so
    /// the value never reaches the accessibility tree or UI automation,
    /// whether it is masked or revealed on screen.
    fn sync_value_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use dbflux_core::secrecy::ExposeSecret as _;

        let super::ValueState::Loaded(value) = &self.value else {
            return;
        };

        let field_count = value.field_count();

        if self.value_inputs_view != Some(value.view) || self.value_inputs.len() != field_count {
            let texts: Vec<String> = value
                .fields()
                .iter()
                .map(|(_, text)| text.expose_secret().to_string())
                .collect();

            self.value_inputs = texts
                .into_iter()
                .map(|text| cx.new(|cx| masked_value_state(text, window, cx)))
                .collect();
            self.value_input_masks = vec![true; field_count];
            self.value_inputs_view = Some(value.view);
        }

        let masks: Vec<bool> = (0..field_count)
            .map(|index| !value.is_revealed(index))
            .collect();

        for (index, masked) in masks.into_iter().enumerate() {
            if self.value_input_masks.get(index).copied() == Some(masked) {
                continue;
            }

            if let Some(input) = self.value_inputs.get(index) {
                input.update(cx, |input, cx| input.set_masked(masked, window, cx));
            }

            if let Some(mask) = self.value_input_masks.get_mut(index) {
                *mask = masked;
            }
        }
    }
}

/// The state behind one value field: masked until revealed.
fn masked_value_state(
    text: String,
    window: &mut Window,
    cx: &mut Context<dbflux_components::controls::InputState>,
) -> dbflux_components::controls::InputState {
    dbflux_components::controls::InputState::new(window, cx)
        .masked(true)
        .default_value(text)
}

impl Render for SecretStoreDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.sync_value_inputs(window, cx);

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .bg(cx.theme().popover)
            .child(
                div()
                    .flex_1()
                    .min_w(SecretStoreMetrics::LISTING_MIN_WIDTH)
                    .overflow_hidden()
                    .h_full()
                    .flex()
                    .flex_col()
                    .child(self.render_toolbar(cx))
                    .child(self.render_header(cx))
                    .child(self.render_rows(cx))
                    .child(self.render_footer(cx)),
            )
            .child(self.render_details_pane(cx))
    }
}
