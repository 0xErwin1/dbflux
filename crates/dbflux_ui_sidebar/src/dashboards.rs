//! The Dashboards view of the sidebar: every saved dashboard, grouped by the
//! connection profile it belongs to, whether or not that connection is open
//! (AppByzTable rail, "Dashboards").
//!
//! The rows live in their own `TreeState` so the sidebar's keyboard
//! navigation, selection and context menus work on them unchanged. Each
//! dashboard row reuses the `DashboardItem` node id of the connections tree,
//! so opening it and its Open / Rename / Duplicate / Delete menu go through
//! the same events; a profile group heading reuses that profile's
//! `DashboardsFolder` id, whose menu creates or imports a dashboard for it.
//! The rows are flat: a heading and its dashboards are siblings, so nothing
//! here writes the expansion state the connections tree keeps for its own
//! Dashboards folders.

use super::*;
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::Button;
use dbflux_components::icons::DriverIconTone;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{ChromeColors, TreeMetrics};
use gpui_component::tree::TreeEntry;
use std::rc::Rc;

/// Row id of the heading that gathers dashboards with no connection profile,
/// or whose profile no longer exists. It parses as no schema node, so the
/// heading has no context menu and does nothing when executed.
pub(crate) const UNASSIGNED_GROUP_ID: &str = "dashboards-unassigned";

/// The facts the Dashboards view shows about one dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DashboardSummary {
    pub id: Uuid,
    pub name: String,
    pub profile_id: Option<Uuid>,
    pub panel_count: usize,
}

/// One heading of the Dashboards view and the dashboards under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DashboardGroup {
    /// `None` for the unassigned group.
    pub profile_id: Option<Uuid>,
    pub name: String,
    pub dashboards: Vec<DashboardSummary>,
}

/// Group `dashboards` under `profiles`, in the profiles' order, with the
/// unassigned group last.
///
/// A dashboard is unassigned when it has no profile or its profile is not in
/// `profiles`. Dashboards are sorted by name within a group. A non-empty
/// `query` keeps a whole group whose name contains it and, in the other
/// groups, the dashboards whose name contains it; groups left empty are
/// dropped. Matching ignores case.
pub(crate) fn group_dashboards(
    dashboards: &[DashboardSummary],
    profiles: &[(Uuid, String)],
    unassigned_label: &str,
    query: &str,
) -> Vec<DashboardGroup> {
    let query = query.trim().to_lowercase();

    let matching = |group_name: &str, members: Vec<&DashboardSummary>| {
        let group_matches = query.is_empty() || group_name.to_lowercase().contains(&query);

        let mut members: Vec<DashboardSummary> = members
            .into_iter()
            .filter(|dashboard| group_matches || dashboard.name.to_lowercase().contains(&query))
            .cloned()
            .collect();

        members.sort_by(|left, right| {
            left.name
                .to_lowercase()
                .cmp(&right.name.to_lowercase())
                .then(left.id.cmp(&right.id))
        });

        members
    };

    let mut groups: Vec<DashboardGroup> = profiles
        .iter()
        .filter_map(|(profile_id, profile_name)| {
            let members = dashboards
                .iter()
                .filter(|dashboard| dashboard.profile_id == Some(*profile_id))
                .collect();
            let members = matching(profile_name, members);

            (!members.is_empty()).then(|| DashboardGroup {
                profile_id: Some(*profile_id),
                name: profile_name.clone(),
                dashboards: members,
            })
        })
        .collect();

    let unassigned = dashboards
        .iter()
        .filter(|dashboard| {
            dashboard
                .profile_id
                .is_none_or(|profile_id| !profiles.iter().any(|(id, _)| *id == profile_id))
        })
        .collect();
    let unassigned = matching(unassigned_label, unassigned);

    if !unassigned.is_empty() {
        groups.push(DashboardGroup {
            profile_id: None,
            name: unassigned_label.to_string(),
            dashboards: unassigned,
        });
    }

    groups
}

/// Row id of a group heading.
pub(crate) fn group_item_id(profile_id: Option<Uuid>) -> String {
    match profile_id {
        Some(profile_id) => SchemaNodeId::DashboardsFolder { profile_id }.to_string(),
        None => UNASSIGNED_GROUP_ID.to_string(),
    }
}

/// Row id of a dashboard: the connections tree's `DashboardItem` id. A
/// dashboard without a profile carries the nil id, which no profile has.
pub(crate) fn dashboard_item_id(dashboard: &DashboardSummary) -> String {
    SchemaNodeId::DashboardItem {
        profile_id: dashboard.profile_id.unwrap_or(Uuid::nil()),
        dashboard_id: dashboard.id,
    }
    .to_string()
}

/// The flat row list for `groups`: each heading followed by its dashboards.
pub(crate) fn dashboard_tree_items(groups: &[DashboardGroup]) -> Vec<TreeItem> {
    groups
        .iter()
        .flat_map(|group| {
            std::iter::once(TreeItem::new(
                group_item_id(group.profile_id),
                group.name.clone(),
            ))
            .chain(group.dashboards.iter().map(|dashboard| {
                TreeItem::new(dashboard_item_id(dashboard), dashboard.name.clone())
            }))
        })
        .collect()
}

/// What a row of the Dashboards view draws besides its label.
#[derive(Clone)]
enum DashboardRow {
    Group {
        icon: AppIcon,
        icon_color: Option<Hsla>,
        count: usize,
    },
    Dashboard {
        panel_count: usize,
    },
}

impl Sidebar {
    /// The dashboards grouped for the view, filtered by its search field.
    pub(crate) fn dashboard_groups(&self, cx: &App) -> Vec<DashboardGroup> {
        let state = self.app_state.read(cx);

        let dashboards: Vec<DashboardSummary> = state
            .dashboards
            .all_dashboards()
            .iter()
            .map(|dashboard| DashboardSummary {
                id: dashboard.id,
                name: dashboard.name.clone(),
                profile_id: dashboard.profile_id,
                panel_count: state.dashboards.panels_for_dashboard(dashboard.id).len(),
            })
            .collect();

        let profiles: Vec<(Uuid, String)> = state
            .profiles()
            .iter()
            .map(|profile| (profile.id, profile.name.clone()))
            .collect();

        group_dashboards(
            &dashboards,
            &profiles,
            &dbflux_i18n::t!("sidebar.dashboards.unassigned"),
            &self.dashboards_search_query,
        )
    }

    pub(crate) fn build_dashboards_tree_items(&self, cx: &App) -> Vec<TreeItem> {
        dashboard_tree_items(&self.dashboard_groups(cx))
    }

    /// Rebuild the Dashboards rows, keeping the selected row when it is still
    /// listed.
    pub(crate) fn refresh_dashboards_tree(&mut self, cx: &mut Context<Self>) {
        let selected_id = self
            .dashboards_tree_state
            .read(cx)
            .selected_entry()
            .map(|entry| entry.item().id.to_string());

        let items = self.build_dashboards_tree_items(cx);
        let selected_index = selected_id
            .as_deref()
            .and_then(|id| items.iter().position(|item| item.id.as_ref() == id));

        self.dashboards_tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);
            state.set_selected_index(selected_index, cx);
        });
        cx.notify();
    }

    /// The Dashboards view body: the filter field, then the grouped rows or
    /// an empty state.
    pub(crate) fn render_dashboards_content(
        &mut self,
        filter_field: AnyElement,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let groups = self.dashboard_groups(cx);
        let has_rows = !groups.is_empty();
        let has_search = !self.dashboards_search_query.is_empty();

        let rows = Rc::new(self.dashboard_rows(&groups, cx));
        let sidebar = cx.entity().clone();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(filter_field)
            .when(has_rows, |el| {
                el.child(div().flex_1().overflow_hidden().child(tree(
                    &self.dashboards_tree_state,
                    move |ix, entry, selected, _window, cx| {
                        render_dashboard_row(ix, entry, selected, &rows, &sidebar, cx)
                    },
                )))
            })
            .when(!has_rows, |el| {
                el.child(
                    div()
                        .flex_1()
                        .flex()
                        .items_center()
                        .justify_center()
                        .px(Spacing::MD)
                        .child(dashboards_empty_state(has_search, cx)),
                )
            })
    }

    /// Per-row drawing data keyed by row id: the driver logo and count of a
    /// heading, the panel count of a dashboard.
    fn dashboard_rows(&self, groups: &[DashboardGroup], cx: &App) -> HashMap<String, DashboardRow> {
        let state = self.app_state.read(cx);
        let mut rows = HashMap::new();

        for group in groups {
            let driver = group.profile_id.and_then(|profile_id| {
                let profile = state
                    .profiles()
                    .iter()
                    .find(|profile| profile.id == profile_id)?;
                let driver = state.drivers().get(&profile.driver_id())?;
                let metadata = driver.metadata();

                Some((
                    AppIcon::for_driver(metadata.icon, metadata.category),
                    DriverIconTone::for_driver(metadata.icon, metadata.category).resolve(cx),
                ))
            });

            let (icon, icon_color) = match driver {
                Some((icon, color)) => (icon, Some(color)),
                None => (AppIcon::Folder, None),
            };

            rows.insert(
                group_item_id(group.profile_id),
                DashboardRow::Group {
                    icon,
                    icon_color,
                    count: group.dashboards.len(),
                },
            );

            for dashboard in &group.dashboards {
                rows.insert(
                    dashboard_item_id(dashboard),
                    DashboardRow::Dashboard {
                        panel_count: dashboard.panel_count,
                    },
                );
            }
        }

        rows
    }
}

/// Empty Dashboards view: the EmptyState composite with a New dashboard
/// button, or, while a search filters every dashboard out, the no-match
/// variant without the button.
fn dashboards_empty_state(has_search: bool, cx: &mut Context<Sidebar>) -> gpui::Div {
    if has_search {
        return div().child(
            EmptyState::new(
                AppIcon::Search,
                dbflux_i18n::t!("sidebar.dashboards.no_matches_hint"),
            )
            .title(dbflux_i18n::t!("sidebar.dashboards.no_matches_title")),
        );
    }

    div()
        .flex()
        .flex_col()
        .items_center()
        .gap(Spacing::MD)
        .child(
            EmptyState::new(
                AppIcon::ChartColumnBig,
                dbflux_i18n::t!("sidebar.dashboards.empty_hint"),
            )
            .title(dbflux_i18n::t!("sidebar.dashboards.empty_title")),
        )
        .child(
            Button::new(
                "sidebar-dashboards-empty-new",
                dbflux_i18n::t!("sidebar.header.new_dashboard"),
            )
            .primary()
            .icon(AppIcon::Plus)
            .tab_stop(false)
            .on_click(cx.listener(|_, _, _, cx| {
                cx.emit(SidebarEvent::RequestNewDashboard);
            })),
        )
}

/// One row of the Dashboards view, styled as a tree row: the selected row
/// takes the tint wash and the 2 px tint bar on its left edge.
fn render_dashboard_row(
    ix: usize,
    entry: &TreeEntry,
    selected: bool,
    rows: &HashMap<String, DashboardRow>,
    sidebar: &Entity<Sidebar>,
    cx: &mut App,
) -> ListItem {
    let theme = cx.theme();
    let tint = ChromeColors::tint(theme);
    let strong = ChromeColors::strong(theme);
    let muted = theme.muted_foreground;

    let item_id = entry.item().id.to_string();
    let label = entry.item().label.clone();
    let row = rows.get(&item_id).cloned();

    let (leading, label_color, meta) = match row {
        Some(DashboardRow::Group {
            icon,
            icon_color,
            count,
        }) => (
            Icon::new(icon)
                .size(TreeMetrics::ICON)
                .color(icon_color.unwrap_or(muted))
                .into_any_element(),
            strong,
            count.to_string(),
        ),
        Some(DashboardRow::Dashboard { panel_count }) => (
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .pl(TreeMetrics::INDENT)
                .child(
                    Icon::new(AppIcon::ChartColumnBig)
                        .size(TreeMetrics::ICON)
                        .color(muted),
                )
                .into_any_element(),
            theme.foreground,
            panel_count.to_string(),
        ),
        None => (div().into_any_element(), theme.foreground, String::new()),
    };

    let sidebar_for_click = sidebar.clone();
    let id_for_click = item_id.clone();
    let sidebar_for_menu = sidebar.clone();
    let id_for_menu = item_id.clone();

    ListItem::new(ix)
        .h(TreeMetrics::ROW_HEIGHT)
        .px(TreeMetrics::PADDING_X)
        .when(selected, |el| {
            el.bg(theme.list_active)
                .border_l_2()
                .border_color(tint)
                .pl(TreeMetrics::PADDING_X - TreeMetrics::SELECTION_BAR)
        })
        .child(
            div()
                .id(SharedString::from(format!("dashboard-row-{item_id}")))
                .w_full()
                .flex()
                .items_center()
                .gap(TreeMetrics::GAP)
                .text_size(TreeMetrics::FONT)
                .cursor_pointer()
                .child(leading)
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .text_color(label_color)
                        .child(label),
                )
                .child(
                    div()
                        .flex_shrink_0()
                        .text_size(TreeMetrics::META_FONT)
                        .text_color(muted)
                        .child(meta),
                )
                .on_click(move |event, _, cx| {
                    let click_count = event.click_count();
                    sidebar_for_click.update(cx, |this, cx| {
                        this.handle_item_click(&id_for_click, click_count, false, false, cx);
                    });
                })
                .on_mouse_down(MouseButton::Right, move |event, _, cx| {
                    cx.stop_propagation();
                    let position = event.position;
                    sidebar_for_menu.update(cx, |this, cx| {
                        cx.emit(SidebarEvent::RequestFocus);
                        this.open_menu_for_item(&id_for_menu, position, cx);
                    });
                }),
        )
}

#[cfg(test)]
mod tests {
    use super::{
        DashboardSummary, UNASSIGNED_GROUP_ID, dashboard_item_id, dashboard_tree_items,
        group_dashboards, group_item_id,
    };
    use dbflux_core::SchemaNodeId;
    use uuid::Uuid;

    fn dashboard(name: &str, profile_id: Option<Uuid>) -> DashboardSummary {
        DashboardSummary {
            id: Uuid::new_v4(),
            name: name.to_string(),
            profile_id,
            panel_count: 0,
        }
    }

    fn names(group: &super::DashboardGroup) -> Vec<&str> {
        group
            .dashboards
            .iter()
            .map(|dashboard| dashboard.name.as_str())
            .collect()
    }

    #[test]
    fn groups_follow_profile_order_and_put_unassigned_last() {
        let prod = Uuid::new_v4();
        let staging = Uuid::new_v4();
        let profiles = vec![(prod, "Prod".to_string()), (staging, "Staging".to_string())];

        let dashboards = vec![
            dashboard("Orphan", None),
            dashboard("Latency", Some(staging)),
            dashboard("Errors", Some(prod)),
        ];

        let groups = group_dashboards(&dashboards, &profiles, "No connection", "");

        let group_names: Vec<&str> = groups.iter().map(|group| group.name.as_str()).collect();
        assert_eq!(group_names, ["Prod", "Staging", "No connection"]);
        assert_eq!(groups[2].profile_id, None);
    }

    #[test]
    fn profiles_without_dashboards_get_no_group() {
        let empty = Uuid::new_v4();
        let used = Uuid::new_v4();
        let profiles = vec![(empty, "Empty".to_string()), (used, "Used".to_string())];

        let groups = group_dashboards(
            &[dashboard("Board", Some(used))],
            &profiles,
            "No connection",
            "",
        );

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].profile_id, Some(used));
    }

    #[test]
    fn dashboards_of_a_deleted_profile_are_unassigned() {
        let gone = Uuid::new_v4();

        let groups = group_dashboards(&[dashboard("Stale", Some(gone))], &[], "No connection", "");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].profile_id, None);
        assert_eq!(names(&groups[0]), ["Stale"]);
    }

    #[test]
    fn dashboards_are_sorted_by_name_ignoring_case() {
        let profile = Uuid::new_v4();
        let profiles = vec![(profile, "Main".to_string())];

        let groups = group_dashboards(
            &[
                dashboard("zeta", Some(profile)),
                dashboard("Alpha", Some(profile)),
                dashboard("beta", Some(profile)),
            ],
            &profiles,
            "No connection",
            "",
        );

        assert_eq!(names(&groups[0]), ["Alpha", "beta", "zeta"]);
    }

    #[test]
    fn query_keeps_matching_dashboards_and_whole_matching_groups() {
        let prod = Uuid::new_v4();
        let staging = Uuid::new_v4();
        let profiles = vec![(prod, "Prod".to_string()), (staging, "Staging".to_string())];

        let dashboards = vec![
            dashboard("Errors", Some(prod)),
            dashboard("Latency", Some(prod)),
            dashboard("Queue depth", Some(staging)),
            dashboard("Errors by host", Some(staging)),
        ];

        let by_dashboard = group_dashboards(&dashboards, &profiles, "No connection", "ERR");
        assert_eq!(by_dashboard.len(), 2);
        assert_eq!(names(&by_dashboard[0]), ["Errors"]);
        assert_eq!(names(&by_dashboard[1]), ["Errors by host"]);

        let by_group = group_dashboards(&dashboards, &profiles, "No connection", "stag");
        assert_eq!(by_group.len(), 1);
        assert_eq!(names(&by_group[0]), ["Errors by host", "Queue depth"]);

        let none = group_dashboards(&dashboards, &profiles, "No connection", "missing");
        assert!(none.is_empty());
    }

    #[test]
    fn rows_are_flat_with_each_heading_before_its_dashboards() {
        let profile = Uuid::new_v4();
        let profiles = vec![(profile, "Main".to_string())];
        let assigned = dashboard("Board", Some(profile));
        let orphan = dashboard("Orphan", None);

        let groups = group_dashboards(
            &[assigned.clone(), orphan.clone()],
            &profiles,
            "No connection",
            "",
        );
        let items = dashboard_tree_items(&groups);

        let ids: Vec<String> = items.iter().map(|item| item.id.to_string()).collect();
        assert_eq!(
            ids,
            [
                group_item_id(Some(profile)),
                dashboard_item_id(&assigned),
                UNASSIGNED_GROUP_ID.to_string(),
                dashboard_item_id(&orphan),
            ]
        );
        assert!(items.iter().all(|item| item.children.is_empty()));
    }

    #[test]
    fn dashboard_rows_reuse_the_connections_tree_node_ids() {
        let profile = Uuid::new_v4();
        let assigned = dashboard("Board", Some(profile));
        let orphan = dashboard("Orphan", None);

        assert_eq!(
            crate::parse_node_id(&dashboard_item_id(&assigned)),
            Some(SchemaNodeId::DashboardItem {
                profile_id: profile,
                dashboard_id: assigned.id,
            })
        );
        assert_eq!(
            crate::parse_node_id(&dashboard_item_id(&orphan)),
            Some(SchemaNodeId::DashboardItem {
                profile_id: Uuid::nil(),
                dashboard_id: orphan.id,
            })
        );
        assert_eq!(
            crate::parse_node_id(&group_item_id(Some(profile))),
            Some(SchemaNodeId::DashboardsFolder {
                profile_id: profile
            })
        );
        assert_eq!(crate::parse_node_id(UNASSIGNED_GROUP_ID), None);
    }

    #[test]
    fn dashboards_strings_resolve_in_every_shipped_locale() {
        let keys = [
            "sidebar.tabs.dashboards",
            "sidebar.dashboards.unassigned",
            "sidebar.dashboards.empty_title",
            "sidebar.dashboards.empty_hint",
            "sidebar.dashboards.no_matches_title",
            "sidebar.dashboards.no_matches_hint",
            "sidebar.filter.dashboards_placeholder",
        ];

        for key in keys {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(value, format!("{locale}.{key}"), "missing {locale}.{key}");
            }
        }
    }
}
