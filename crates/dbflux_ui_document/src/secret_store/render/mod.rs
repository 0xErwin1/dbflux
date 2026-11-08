//! Rendering of the listing of `SecretStoreDocument`: the toolbar with the
//! path and the store-side search, the rows, and the footer. The details pane
//! lives in `view/details`.

mod footer;

pub(in crate::secret_store) use footer::FooterFit;

use super::list::{format_short_time, rotation_chip};
use super::listing::{PathMode, child_path_label, path_segments, relative_name};
use super::{RegionListState, SecretFocusMode, SecretRowKey, SecretStoreDocument, context_menu};
use crate::chrome::{connection_segment, document_bar, search_field};
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

/// Flex shrink factor of the search field. Shrink is proportional to this
/// factor, so a weight far above the breadcrumb's makes the search absorb the
/// narrowing until it reaches its minimum width.
const SEARCH_SHRINK_WEIGHT: f32 = 1000.0;

/// The narrowest the identity gets: the store icon and a few characters
/// before the ellipsis, so it stays visible and hoverable for its tooltip.
const IDENTITY_MIN_WIDTH: Rems = dbflux_components::tokens::ui(72.0);

/// The listing width from which the toolbar's New button shows its label, and
/// the narrower one from which it shows its icon alone. Below that the button
/// is left out, and New stays in the pane actions menu: the toolbar's other
/// controls keep their room at the document's minimum width.
const NEW_LABEL_MIN_WIDTH: Rems = dbflux_components::tokens::ui(900.0);
const NEW_ICON_MIN_WIDTH: Rems = dbflux_components::tokens::ui(640.0);

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
        super::StoreCall::List {
            count,
            hierarchical,
        } => dbflux_i18n::t!(
            match (hierarchical, count == 1) {
                (false, true) => "document.secret_store.status.list_timing.one",
                (false, false) => "document.secret_store.status.list_timing.many",
                (true, true) => "document.secret_store.status.list_timing_parameters.one",
                (true, false) => "document.secret_store.status.list_timing_parameters.many",
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

/// A version that is a plain number reads "v3"; an opaque version id (a UUID)
/// is shown as the store reports it.
pub(super) fn version_label(version: Option<&str>) -> String {
    match version {
        Some(version) if version.parse::<u64>().is_ok() => format!("v{version}"),
        Some(version) => version.to_string(),
        None => UNKNOWN.to_string(),
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
    /// While the region list is loading, or when the store cannot list
    /// regions, the picker cannot be offered; the region then stays visible
    /// as a plain chip, so the listing never loses the answer to which region
    /// it was loaded under.
    fn breadcrumb_parts(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let connection_name = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| format!("{} › ", connected.profile.name))
            .unwrap_or_default();
        let identity_text = format!("{connection_name}{}", self.store.label);

        // The parts are direct children of the header bar, so the region chip
        // is never inside a clipping container: only the identity and the
        // path give way.
        let mut parts: Vec<AnyElement> = vec![
            // The identity gives way before the region chip does: it
            // clips, and the full "connection › store" stays in the
            // tooltip and the accessible name.
            div()
                .id("secret-identity")
                .flex()
                .flex_shrink_1()
                .min_w(IDENTITY_MIN_WIDTH)
                .overflow_hidden()
                .role(gpui::accesskit::Role::Label)
                .aria_label(identity_text.clone())
                .tooltip(move |window, cx| {
                    gpui_component::tooltip::Tooltip::new(identity_text.clone()).build(window, cx)
                })
                .child(Breadcrumb::new(self.identity_segments(cx)).mono())
                .into_any_element(),
        ];

        if let Some(picker) = self.render_region_picker() {
            parts.push(picker);
        } else if self.region_switchable
            && let Some(region) = self.region.clone()
        {
            // The configured region, drawn like the picker's label but with
            // no chevron: there is nothing to pick from.
            parts.push(
                div()
                    .id("secret-region-plain")
                    .flex_shrink_0()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(NavigationMetrics::BREADCRUMB_FONT)
                    .text_color(cx.theme().muted_foreground)
                    .child(region)
                    .into_any_element(),
            );
        }

        let path = self.store_path_segments(cx);

        if !path.is_empty() {
            let full_path = self.path.clone().unwrap_or_default();
            let tooltip_path = full_path.clone();

            // The path is the one part of the header that gives way: it
            // clips with an ellipsis, and its full text stays in the tooltip
            // and the accessible name.
            parts.push(
                div()
                    .id("secret-path")
                    .flex()
                    .items_center()
                    .gap(NavigationMetrics::BREADCRUMB_GAP)
                    .flex_shrink_1()
                    .min_w_0()
                    .overflow_hidden()
                    .role(gpui::accesskit::Role::Label)
                    .aria_label(full_path)
                    .tooltip(move |window, cx| {
                        gpui_component::tooltip::Tooltip::new(tooltip_path.clone())
                            .build(window, cx)
                    })
                    .child(Self::breadcrumb_separator(cx))
                    .child(Breadcrumb::new(path).mono())
                    .into_any_element(),
            );
        }

        parts
    }

    /// The region picker, once the connection listed its regions. It is the
    /// last segment of the breadcrumb row, and it is the only dropdown of the
    /// document, so its trigger carries the picker's own element id.
    fn render_region_picker(&self) -> Option<AnyElement> {
        if !matches!(&self.regions, RegionListState::Loaded(regions) if !regions.is_empty()) {
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
    /// store-side search and Refresh.
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

        // As the header narrows the search gives way first, down to its
        // minimum, then the path of the breadcrumb; Refresh, the region and
        // the store identity keep their width.
        let mut search = div()
            .id("secret-search")
            .flex()
            .w(ObjectStoreMetrics::SEARCH_WIDTH)
            .min_w(SecretStoreMetrics::SEARCH_MIN_WIDTH)
            .flex_shrink_1();
        search.style().flex_shrink = Some(SEARCH_SHRINK_WEIGHT);

        document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
            .children(self.breadcrumb_parts(cx))
            .child(div().flex_1())
            .when(hierarchical, |bar| bar.child(self.render_path_mode(cx)))
            .child(search.child(search_field(
                &self.search_input,
                None,
                self.focus_mode == SecretFocusMode::Search,
                cx,
            )))
            .children(
                self.new_button(cx)
                    .map(|button| div().flex_shrink_0().child(button)),
            )
            .child(div().flex_shrink_0().child(refresh))
    }

    /// New secret or New parameter, by what the store calls its entries. The
    /// button gives way to its icon, and then to the pane actions menu, as
    /// the listing narrows; it waits for the first measurement of the
    /// listing's width.
    fn new_button(&self, cx: &mut Context<Self>) -> Option<Button> {
        if !self.supports_create() {
            return None;
        }

        let width = self.footer_fit.width.get();
        let label_min = dbflux_components::fonts::ui_px(cx, NEW_LABEL_MIN_WIDTH);
        let icon_min = dbflux_components::fonts::ui_px(cx, NEW_ICON_MIN_WIDTH);

        if width < icon_min {
            return None;
        }

        let label = if self.hierarchical() {
            dbflux_i18n::t!("document.secret_store.create.button_parameter")
        } else {
            dbflux_i18n::t!("document.secret_store.create.button_secret")
        };

        let button = Button::new("secret-new", label)
            .icon(AppIcon::Plus)
            .tab_stop(false)
            .disabled(!self.can_open_create(cx))
            .on_click(cx.listener(|this, _, window, cx| this.open_create(window, cx)));

        Some(if width < label_min {
            button.icon_only()
        } else {
            button
        })
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
    fn summary_badges(
        summary: &SecretSummary,
        deletion: Option<Option<DateTime<Utc>>>,
    ) -> Vec<Badge> {
        let mut badges = Vec::new();

        if let Some(service) = summary.owning_service.as_deref() {
            badges.push(Badge::new(
                dbflux_i18n::t!("document.secret_store.badge.managed", service = service),
                BadgeTone::Info,
            ));
        }

        if let Some(date) = deletion {
            let label = match date {
                Some(date) => dbflux_i18n::t!(
                    "document.secret_store.badge.deletes",
                    date = date.with_timezone(&Local).format("%Y-%m-%d").to_string()
                ),
                None => dbflux_i18n::t!("document.secret_store.value.deletion_scheduled_unknown"),
            };

            badges.push(Badge::new(label, BadgeTone::Danger));
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

    /// The rotation column: a chip, success-toned while rotation is enabled.
    fn rotation_cell(summary: &SecretSummary) -> Stateful<Div> {
        let chip = rotation_chip(summary.rotation.as_ref());
        let chip_label = chip.label.clone();
        let mut badge = Badge::new(chip.label, chip.tone)
            .id(format!("secret-rotation-{}", summary.name))
            .aria_label(chip_label.clone());

        if chip.tone == BadgeTone::Success {
            badge = badge.icon(AppIcon::RefreshCcw);
        }

        let tooltip = chip_label.clone();

        // The chip clips inside its cell, so a narrow listing never lets it
        // run over Last changed; the full label stays in the tooltip.
        div()
            .id(format!("secret-rotation-cell-{}", summary.name))
            .flex()
            .items_center()
            .w(SecretStoreMetrics::ROTATION_WIDTH)
            .flex_shrink_1()
            .min_w_0()
            .overflow_hidden()
            .pr(DocumentMetrics::GAP)
            .tooltip(move |window, cx| {
                gpui_component::tooltip::Tooltip::new(tooltip.clone()).build(window, cx)
            })
            .child(
                div()
                    .id(format!("secret-rotation-chip-{}", summary.name))
                    .flex()
                    .min_w_0()
                    .overflow_hidden()
                    .child(badge),
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
        let menu_row = row.clone();

        let display_name = if self.path_mode == PathMode::Tree {
            relative_name(&summary.name, self.path.as_deref()).to_string()
        } else {
            summary.name.clone()
        };

        let deletion = self.view.read(cx).deletion_of(summary);
        let name = Self::row_name(AppIcon::KeyRound, lock_color, display_name, cx)
            .children(Self::summary_badges(summary, deletion));

        let frame = self
            .row_frame(format!("secret-row-{}", summary.name).into(), selected, cx)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_row(row.clone(), cx);
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(menu_row.clone(), event.position, window, cx);
                }),
            )
            .child(name);

        let last_changed = format_short_time(summary.last_changed);

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
                    version_label(summary.version.as_deref()),
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
                |row| row.child(Self::rotation_cell(summary)),
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
        let menu_row = row.clone();

        self.row_frame(format!("secret-path-row-{path}").into(), selected, cx)
            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                if event.click_count() >= 2 {
                    this.navigate_to(Some(open.clone()), cx);
                } else {
                    this.select_row(row.clone(), cx);
                }
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(menu_row.clone(), event.position, window, cx);
                }),
            )
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
}

impl Render for SecretStoreDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.view
            .update(cx, |view, cx| view.prepare_frame(window, cx));

        if std::mem::take(&mut self.clear_search_input) {
            self.search_input
                .update(cx, |input, cx| input.set_value("", window, cx));
        }

        // A menu entry chosen by click is queued and runs here, which has
        // the window the actions' focus changes need.
        if let Some(action) = self.pending_menu_action.take() {
            self.run_menu_action(action, window, cx);
        }

        let listing = div()
            .id("secret-listing")
            .flex_1()
            .min_w(SecretStoreMetrics::LISTING_MIN_WIDTH)
            .overflow_hidden()
            .h_full()
            .flex()
            .flex_col()
            .child(self.render_toolbar(cx))
            .child(self.render_header(cx))
            .child(self.render_rows(cx))
            .child(self.render_footer(cx));

        let context = self.active_context(cx);
        let mut root = div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .bg(cx.theme().popover)
            .child(listing)
            .child(
                self.view
                    .update(cx, |view, cx| view.render_pane(context, cx)),
            );

        // The menu draws above the document and rebuilds its entries each
        // frame, so a disabled Copy ARN re-enables once the describe that
        // the right-click itself triggered lands.
        let rebuild = self.context_menu.as_ref().map(|menu| menu.target.clone());

        if let Some(target) = rebuild {
            let items = self.build_menu_items(&target, cx);

            if let Some(menu) = self.context_menu.as_mut() {
                menu.items = items;
                // The entries can shrink under the highlight (a restore that
                // landed); keep it on a row that exists.
                menu.selected_index = menu.selected_index.min(menu.items.len().saturating_sub(1));
            }

            if self
                .context_menu
                .as_ref()
                .is_some_and(|menu| menu.items.is_empty())
            {
                self.context_menu = None;
            }
        }

        if let Some(menu) = self.context_menu.as_ref() {
            let element = context_menu::render_context_menu(menu, &self.context_menu_focus, cx);

            root = root.child(element);
        }

        if let Some(dialog) = self.render_dialog(cx) {
            root = root.child(dialog);
        }

        root
    }
}
