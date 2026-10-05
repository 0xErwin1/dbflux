//! The application shell around the documents: title bar, activity rail and
//! the empty workspace (AppByzTable, P1Empty).

use super::*;
use crate::ui::document::DocumentIcon;
use dbflux_components::composites::{ActivityRail, CommandSearch, ListRow, RailEntry};
use dbflux_components::primitives::{Chamfer, Icon, Kbd, Text};
use dbflux_components::tokens::{ChamferCut, ShellMetrics};
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap};
use dbflux_ui_base::platform;

/// Identifiers of the rail entries, passed back by the rail on click.
pub(super) mod rail_ids {
    pub const CONNECTIONS: &str = "connections";
    pub const SCRIPTS: &str = "scripts";
    pub const DASHBOARDS: &str = "dashboards";
    pub const APPROVALS: &str = "approvals";
    pub const AUDIT: &str = "audit";
    pub const SETTINGS: &str = "settings";
}

/// What the rail reflects: which sidebar view is on screen (`None` while the
/// sidebar is collapsed), and the state of the governance and audit views.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RailState {
    pub sidebar_view: Option<SidebarTab>,
    /// The build carries the MCP approvals view.
    pub approvals_available: bool,
    pub approvals_open: bool,
    pub audit_active: bool,
}

/// The rail state for what is on screen, with one active item at most
/// (P1Sidebar). An open approvals or audit document takes the rail while the
/// document area has focus or the sidebar is collapsed; otherwise the sidebar
/// view on screen does.
pub(super) fn resolve_rail_state(
    sidebar_view: Option<SidebarTab>,
    active_icon: Option<DocumentIcon>,
    document_focused: bool,
    approvals_available: bool,
) -> RailState {
    let document_leads = document_focused || sidebar_view.is_none();

    let audit_active = document_leads && active_icon == Some(DocumentIcon::Audit);
    let approvals_open = document_leads && active_icon == Some(DocumentIcon::McpApprovals);
    let document_active = audit_active || (approvals_open && approvals_available);

    RailState {
        sidebar_view: if document_active { None } else { sidebar_view },
        approvals_available,
        approvals_open,
        audit_active,
    }
}

/// The rail's entries for `state`, top to bottom.
pub(super) fn rail_entries(state: RailState) -> Vec<RailEntry> {
    let mut entries = vec![
        RailEntry::new(
            rail_ids::CONNECTIONS,
            AppIcon::Database,
            dbflux_i18n::t!("workspace.rail.connections"),
        )
        .active(state.sidebar_view == Some(SidebarTab::Connections)),
        RailEntry::new(
            rail_ids::SCRIPTS,
            AppIcon::SquareTerminal,
            dbflux_i18n::t!("workspace.rail.scripts"),
        )
        .active(state.sidebar_view == Some(SidebarTab::Scripts)),
        RailEntry::new(
            rail_ids::DASHBOARDS,
            AppIcon::ChartColumnBig,
            dbflux_i18n::t!("workspace.rail.dashboards"),
        )
        .active(state.sidebar_view == Some(SidebarTab::Dashboards)),
    ];

    if state.approvals_available {
        entries.push(
            RailEntry::new(
                rail_ids::APPROVALS,
                AppIcon::Bot,
                dbflux_i18n::t!("workspace.rail.approvals"),
            )
            .active(state.approvals_open),
        );
    }

    entries.push(
        RailEntry::new(
            rail_ids::AUDIT,
            AppIcon::FingerprintPattern,
            dbflux_i18n::t!("workspace.rail.audit"),
        )
        .active(state.audit_active),
    );

    entries.push(
        RailEntry::new(
            rail_ids::SETTINGS,
            AppIcon::Settings,
            dbflux_i18n::t!("workspace.rail.settings"),
        )
        .bottom(),
    );

    entries
}

/// Display labels of the chord the effective keymap binds to `command` in the
/// global context, so the shell shows the binding that is actually
/// registered, with the platform's modifier (Cmd on macOS).
pub(super) fn global_shortcut_keys(command: Command) -> Option<Vec<SharedString>> {
    effective_keymap()
        .chord_for_command(ContextId::Global, command)
        .map(chord_display_parts)
}

/// One row of the empty workspace's START card.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct StartAction {
    pub command: Command,
    pub icon: AppIcon,
    pub label: SharedString,
    pub keys: Vec<SharedString>,
}

/// The START card's rows. A command without a global binding is left out,
/// so the card never advertises a shortcut that does nothing.
pub(super) fn start_actions() -> Vec<StartAction> {
    [
        (
            Command::NewQueryTab,
            AppIcon::FileCode,
            dbflux_i18n::t!("workspace.hint.new_query"),
        ),
        (
            Command::ToggleCommandPalette,
            AppIcon::Search,
            dbflux_i18n::t!("workspace.hint.command_palette"),
        ),
        (
            Command::OpenScriptFile,
            AppIcon::SquareTerminal,
            dbflux_i18n::t!("workspace.hint.open"),
        ),
        (
            Command::OpenConnectionManager,
            AppIcon::Plus,
            dbflux_i18n::t!("workspace.hint.new_connection"),
        ),
    ]
    .into_iter()
    .filter_map(|(command, icon, label)| {
        global_shortcut_keys(command).map(|keys| StartAction {
            command,
            icon,
            label: label.into(),
            keys,
        })
    })
    .collect()
}

/// How long ago a recent file was opened, in the unit the RECENT card shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RecentAge {
    JustNow,
    Minutes(i64),
    Hours(i64),
    Yesterday,
    Days(i64),
}

impl RecentAge {
    /// Age of a file opened at `opened_at`, both in Unix seconds. A time in
    /// the future (clock change) counts as just now.
    pub fn between(now: i64, opened_at: i64) -> Self {
        let elapsed = now.saturating_sub(opened_at).max(0);

        match elapsed {
            0..60 => Self::JustNow,
            60..3_600 => Self::Minutes(elapsed / 60),
            3_600..86_400 => Self::Hours(elapsed / 3_600),
            86_400..172_800 => Self::Yesterday,
            _ => Self::Days(elapsed / 86_400),
        }
    }

    pub fn label(self) -> String {
        match self {
            Self::JustNow => dbflux_i18n::t!("workspace.recent.just_now"),
            Self::Minutes(count) => dbflux_i18n::t!("workspace.recent.minutes_ago", count = count),
            Self::Hours(count) => dbflux_i18n::t!("workspace.recent.hours_ago", count = count),
            Self::Yesterday => dbflux_i18n::t!("workspace.recent.yesterday"),
            Self::Days(count) => dbflux_i18n::t!("workspace.recent.days_ago", count = count),
        }
    }
}

/// Number of recent files the RECENT card lists.
const RECENT_FILE_LIMIT: usize = 4;

/// One row of the RECENT card.
#[derive(Clone, Debug, PartialEq)]
struct RecentRow {
    path: PathBuf,
    name: SharedString,
    folder: SharedString,
    age: String,
}

fn recent_rows(files: &[dbflux_core::RecentFile], now: i64) -> Vec<RecentRow> {
    files
        .iter()
        .take(RECENT_FILE_LIMIT)
        .map(|file| {
            let name = file
                .path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| file.path.to_string_lossy().to_string());

            let folder = file
                .path
                .parent()
                .and_then(|parent| parent.file_name())
                .map(|folder| format!("{}/", folder.to_string_lossy()))
                .unwrap_or_default();

            RecentRow {
                path: file.path.clone(),
                name: name.into(),
                folder: folder.into(),
                age: RecentAge::between(now, file.last_opened).label(),
            }
        })
        .collect()
}

impl Workspace {
    fn rail_state(&self, cx: &App) -> RailState {
        let sidebar_view = (!self.sidebar_dock.read(cx).is_collapsed())
            .then(|| self.sidebar.read(cx).active_tab());

        let active_icon = self
            .tab_manager
            .read(cx)
            .active_tab()
            .map(|tab| tab.meta_snapshot(cx).icon);

        resolve_rail_state(
            sidebar_view,
            active_icon,
            self.focus_target == FocusTarget::Document,
            cfg!(feature = "mcp"),
        )
    }

    /// Shows a sidebar view from the rail. Choosing the view already on
    /// screen collapses the sidebar; any other choice switches to it,
    /// expands the sidebar and gives it focus.
    pub(super) fn show_sidebar_view(&mut self, tab: SidebarTab, cx: &mut Context<Self>) {
        let expanded = !self.sidebar_dock.read(cx).is_collapsed();

        if expanded && self.sidebar.read(cx).active_tab() == tab {
            self.toggle_sidebar(cx);
            return;
        }

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.set_active_tab(tab, cx);
        });
        self.sidebar_dock.update(cx, |dock, cx| dock.expand(cx));
        self.pending_focus = Some(FocusTarget::Sidebar);
        cx.notify();
    }

    pub(super) fn handle_rail_select(
        &mut self,
        id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match id {
            rail_ids::CONNECTIONS => self.show_sidebar_view(SidebarTab::Connections, cx),
            rail_ids::SCRIPTS => self.show_sidebar_view(SidebarTab::Scripts, cx),
            rail_ids::DASHBOARDS => self.show_sidebar_view(SidebarTab::Dashboards, cx),
            rail_ids::AUDIT => self.open_audit_viewer(window, cx),
            rail_ids::SETTINGS => self.open_settings(cx),
            #[cfg(feature = "mcp")]
            rail_ids::APPROVALS => self.open_mcp_approvals(window, cx),
            _ => log::warn!("Unknown activity rail entry: {id}"),
        }
    }

    pub(super) fn render_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let workspace = cx.entity().clone();

        ActivityRail::new("activity-rail", rail_entries(self.rail_state(cx))).on_select(
            move |id, window, cx| {
                workspace.update(cx, |workspace, cx| {
                    workspace.handle_rail_select(id, window, cx);
                });
            },
        )
    }

    fn open_command_palette_from_title_bar(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.command_palette.read(cx).is_visible() {
            self.toggle_command_palette(window, cx);
        }
    }

    /// The 44 px row at the top of the window, on the desk: the command
    /// search centered, the notifications bell at the right end and, on a
    /// client-decorated Linux window, the window controls after it. Empty
    /// space in the row moves the window there.
    pub(super) fn render_title_bar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let client_decorated = platform::prepare_client_decorations(window);
        let window_controls = client_decorated.then(|| {
            let close = self.title_bar_close_handler(cx);
            platform::render_csd_window_controls(window, cx, Some(close))
        });

        let palette_keys = global_shortcut_keys(Command::ToggleCommandPalette).unwrap_or_default();

        let workspace = cx.entity().clone();
        let command_search = CommandSearch::new(
            "title-bar-command-search",
            dbflux_i18n::t!("workspace.title_bar.command_search"),
        )
        .shortcut(palette_keys)
        .focus_handle(&self.command_search_focus)
        .on_open(move |window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_command_palette_from_title_bar(window, cx);
            });
        });

        let bell = self.render_notification_bell(cx);

        div()
            .id("title-bar")
            .flex()
            .flex_shrink_0()
            .items_center()
            .w_full()
            .h(ShellMetrics::TITLE_BAR_HEIGHT)
            .pl(ShellMetrics::TITLE_BAR_PADDING_X)
            .when(!client_decorated, |bar| {
                bar.pr(ShellMetrics::TITLE_BAR_PADDING_X)
            })
            .child(
                platform::csd_drag_area("title-bar-drag-area")
                    .flex_1()
                    .flex_basis(px(0.0))
                    .h_full(),
            )
            .child(command_search)
            .child(
                div()
                    .flex()
                    .flex_1()
                    .flex_basis(px(0.0))
                    .h_full()
                    .items_center()
                    .justify_end()
                    .child(
                        platform::csd_drag_area("title-bar-drag-area-end")
                            .flex_1()
                            .h_full(),
                    )
                    .child(bell)
                    .children(window_controls),
            )
    }

    /// The empty workspace (P1Empty): the app glyph and title over a START
    /// card of global commands and, when files were opened before, a RECENT
    /// card of them.
    pub(super) fn render_empty_workspace(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();
        let now = chrono::Utc::now().timestamp();
        let recent = recent_rows(self.app_state.read(cx).recent_files(), now);

        let glyph_path = match dbflux_core::ReleaseChannel::current() {
            dbflux_core::ReleaseChannel::Nightly => "branding/nightly/mark-small-256.png",
            _ => "branding/stable/mark-small-256.png",
        };

        let head = div()
            .flex()
            .items_center()
            .gap(ShellMetrics::EMPTY_HEAD_GAP)
            .child(
                img(glyph_path)
                    .size(ShellMetrics::EMPTY_GLYPH)
                    .flex_shrink_0(),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(ShellMetrics::EMPTY_TITLE_GAP)
                    .child(
                        div()
                            .font_family(dbflux_components::fonts::display_family(cx))
                            .font_weight(FontWeight::BLACK)
                            .text_size(ShellMetrics::EMPTY_TITLE_FONT)
                            .text_color(dbflux_components::tokens::ChromeColors::strong(theme))
                            .child(dbflux_i18n::t!("workspace.empty.title").to_uppercase()),
                    )
                    .child(
                        Text::body(dbflux_i18n::t!("workspace.empty.subtitle")).muted_foreground(),
                    ),
            );

        let start_card = self.render_start_card(cx);
        let has_recent = !recent.is_empty();
        let lone_card_width = (dbflux_components::fonts::ui_px(cx, ShellMetrics::EMPTY_WIDTH)
            - ShellMetrics::EMPTY_HEAD_GAP)
            / 2.0;

        let cards = div()
            .flex()
            .gap(ShellMetrics::EMPTY_HEAD_GAP)
            .when(!has_recent, |cards| cards.justify_center())
            .child(
                div()
                    .when(has_recent, |card| card.flex_1())
                    .when(!has_recent, |card| card.w(lone_card_width))
                    .child(start_card),
            )
            .when(has_recent, |cards| {
                cards.child(div().flex_1().child(self.render_recent_card(recent, cx)))
            });

        div()
            .id("empty-state")
            .relative()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(ShellMetrics::EMPTY_GAP)
                    .w(ShellMetrics::EMPTY_WIDTH)
                    .when(!has_recent, |column| column.items_center())
                    .child(head)
                    .child(cards),
            )
    }

    fn empty_card(label: SharedString, rows: Vec<AnyElement>, cx: &App) -> Div {
        let theme = cx.theme();

        div()
            .relative()
            .flex()
            .flex_col()
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(
                div()
                    .pt(ShellMetrics::CARD_LABEL_PADDING_TOP)
                    .px(ShellMetrics::CARD_PADDING_X)
                    .pb(ShellMetrics::CARD_LABEL_PADDING_BOTTOM)
                    .child(Text::label(label).font_size(ShellMetrics::SECTION_LABEL_FONT)),
            )
            .children(rows)
    }

    fn render_start_card(&self, cx: &mut Context<Self>) -> Div {
        let divider = cx.theme().table_row_border;
        let muted = cx.theme().muted_foreground;

        let rows = start_actions()
            .into_iter()
            .map(|action| {
                let workspace = cx.entity().clone();
                let command = action.command;

                ListRow::new(SharedString::from(format!("empty-start-{}", command.id())))
                    .build(cx)
                    .flex()
                    .items_center()
                    .gap(ShellMetrics::START_ROW_GAP)
                    .h(ShellMetrics::START_ROW_HEIGHT)
                    .px(ShellMetrics::CARD_PADDING_X)
                    .border_b_1()
                    .border_color(divider)
                    .child(
                        Icon::new(action.icon)
                            .size(ShellMetrics::START_ROW_ICON)
                            .color(muted),
                    )
                    .child(div().flex_1().min_w_0().child(Text::body(action.label)))
                    .children(action.keys.into_iter().map(Kbd::new))
                    .on_click(move |_, window, cx| {
                        workspace.update(cx, |workspace, cx| {
                            workspace.dispatch(command, window, cx);
                        });
                    })
                    .into_any_element()
            })
            .collect();

        Self::empty_card(dbflux_i18n::t!("workspace.empty.start").into(), rows, cx)
    }

    fn render_recent_card(&self, recent: Vec<RecentRow>, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let divider = theme.table_row_border;
        let muted = theme.muted_foreground;
        let strong = dbflux_components::tokens::ChromeColors::strong(theme);

        let rows = recent
            .into_iter()
            .enumerate()
            .map(|(index, row)| {
                let workspace = cx.entity().clone();
                let path = row.path.clone();

                ListRow::new(SharedString::from(format!("empty-recent-{index}")))
                    .build(cx)
                    .flex()
                    .items_center()
                    .gap(ShellMetrics::RECENT_ROW_GAP)
                    .h(ShellMetrics::RECENT_ROW_HEIGHT)
                    .px(ShellMetrics::CARD_PADDING_X)
                    .border_b_1()
                    .border_color(divider)
                    .child(
                        Icon::new(AppIcon::File)
                            .size(ShellMetrics::RECENT_ROW_ICON)
                            .color(muted),
                    )
                    .child(
                        div()
                            .flex_shrink_0()
                            .max_w(ShellMetrics::EMPTY_WIDTH / 4.0)
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_ellipsis()
                            .child(Text::body(row.name).color(strong)),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_ellipsis()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(ShellMetrics::RECENT_META_FONT)
                            .text_color(muted)
                            .child(row.folder),
                    )
                    .child(
                        div()
                            .flex_shrink_0()
                            .text_size(ShellMetrics::RECENT_META_FONT)
                            .text_color(muted)
                            .child(row.age),
                    )
                    .on_click(move |_, _, cx| {
                        let path = path.clone();
                        workspace.update(cx, |workspace, cx| {
                            workspace.open_script_from_path(path, cx);
                        });
                    })
                    .into_any_element()
            })
            .collect();

        Self::empty_card(dbflux_i18n::t!("workspace.empty.recent").into(), rows, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RailState, RecentAge, global_shortcut_keys, rail_entries, rail_ids, recent_rows,
        resolve_rail_state, start_actions,
    };
    use crate::keymap::{Command, KeyChord, Modifiers};
    use crate::ui::document::DocumentIcon;
    use dbflux_ui_base::keymap::chord_display_parts;
    use dbflux_ui_sidebar::SidebarTab;
    use std::path::PathBuf;

    fn ids(state: RailState) -> Vec<String> {
        rail_entries(state)
            .into_iter()
            .map(|entry| entry.id.to_string())
            .collect()
    }

    #[test]
    fn rail_lists_the_views_with_settings_pinned_last() {
        let state = RailState {
            approvals_available: true,
            ..RailState::default()
        };

        assert_eq!(
            ids(state),
            [
                rail_ids::CONNECTIONS,
                rail_ids::SCRIPTS,
                rail_ids::DASHBOARDS,
                rail_ids::APPROVALS,
                rail_ids::AUDIT,
                rail_ids::SETTINGS,
            ]
        );

        let entries = rail_entries(state);
        let settings = entries.last().expect("settings entry");
        assert_eq!(
            settings.placement,
            dbflux_components::composites::RailPlacement::Bottom
        );
    }

    #[test]
    fn rail_omits_approvals_without_mcp_support() {
        let ids = ids(RailState::default());

        assert!(!ids.iter().any(|id| id == rail_ids::APPROVALS));
    }

    #[test]
    fn rail_marks_the_sidebar_view_on_screen_as_active() {
        let entries = rail_entries(RailState {
            sidebar_view: Some(SidebarTab::Scripts),
            ..RailState::default()
        });

        let active: Vec<&str> = entries
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| entry.id.as_ref())
            .collect();
        assert_eq!(active, [rail_ids::SCRIPTS]);

        let collapsed = rail_entries(RailState::default());
        assert!(collapsed.iter().all(|entry| !entry.active));
    }

    #[test]
    fn rail_flags_the_open_views() {
        let entries = rail_entries(RailState {
            approvals_available: true,
            approvals_open: true,
            audit_active: true,
            ..RailState::default()
        });

        let approvals = entries
            .iter()
            .find(|entry| entry.id.as_ref() == rail_ids::APPROVALS)
            .expect("approvals entry");
        assert!(approvals.active);

        let audit = entries
            .iter()
            .find(|entry| entry.id.as_ref() == rail_ids::AUDIT)
            .expect("audit entry");
        assert!(audit.active);

        let idle = rail_entries(RailState {
            approvals_available: true,
            ..RailState::default()
        });
        assert!(idle.iter().all(|entry| !entry.active));
    }

    fn active_ids(state: RailState) -> Vec<String> {
        rail_entries(state)
            .into_iter()
            .filter(|entry| entry.active)
            .map(|entry| entry.id.to_string())
            .collect()
    }

    #[test]
    fn rail_has_one_active_item_when_a_panel_and_the_audit_document_are_open() {
        let document_focused = resolve_rail_state(
            Some(SidebarTab::Dashboards),
            Some(DocumentIcon::Audit),
            true,
            true,
        );
        assert_eq!(active_ids(document_focused), [rail_ids::AUDIT]);

        let sidebar_focused = resolve_rail_state(
            Some(SidebarTab::Dashboards),
            Some(DocumentIcon::Audit),
            false,
            true,
        );
        assert_eq!(active_ids(sidebar_focused), [rail_ids::DASHBOARDS]);

        let sidebar_collapsed = resolve_rail_state(None, Some(DocumentIcon::Audit), false, true);
        assert_eq!(active_ids(sidebar_collapsed), [rail_ids::AUDIT]);
    }

    #[test]
    fn rail_keeps_the_panel_active_beside_an_ordinary_document() {
        let state = resolve_rail_state(
            Some(SidebarTab::Connections),
            Some(DocumentIcon::Table),
            true,
            true,
        );

        assert_eq!(active_ids(state), [rail_ids::CONNECTIONS]);
    }

    #[test]
    fn start_actions_show_the_registered_global_chords() {
        let expected = [
            (
                Command::NewQueryTab,
                KeyChord::new("n", Modifiers::primary()),
            ),
            (
                Command::ToggleCommandPalette,
                KeyChord::new("p", Modifiers::primary_shift()),
            ),
            (
                Command::OpenScriptFile,
                KeyChord::new("o", Modifiers::primary()),
            ),
            (
                Command::OpenConnectionManager,
                KeyChord::new("n", Modifiers::primary_shift()),
            ),
        ];

        let actions = start_actions();
        assert_eq!(actions.len(), expected.len());

        for (action, (command, chord)) in actions.iter().zip(expected) {
            assert_eq!(action.command, command);
            assert_eq!(action.keys, chord_display_parts(&chord));
            assert_eq!(global_shortcut_keys(command), Some(action.keys.clone()));
        }
    }

    #[test]
    fn new_connection_hint_uses_the_platform_modifier() {
        #[cfg(target_os = "macos")]
        let expected = ["Shift", "Cmd", "N"];
        #[cfg(not(target_os = "macos"))]
        let expected = ["Ctrl", "Shift", "N"];

        let keys = global_shortcut_keys(Command::OpenConnectionManager)
            .expect("the Connection Manager must have a global binding");
        let keys: Vec<&str> = keys.iter().map(|key| key.as_ref()).collect();

        assert_eq!(keys, expected);
    }

    #[test]
    fn recent_age_picks_the_largest_whole_unit() {
        let now = 1_000_000;

        assert_eq!(RecentAge::between(now, now), RecentAge::JustNow);
        assert_eq!(RecentAge::between(now, now + 30), RecentAge::JustNow);
        assert_eq!(RecentAge::between(now, now - 120), RecentAge::Minutes(2));
        assert_eq!(RecentAge::between(now, now - 3_600), RecentAge::Hours(1));
        assert_eq!(RecentAge::between(now, now - 90_000), RecentAge::Yesterday);
        assert_eq!(
            RecentAge::between(now, now - 3 * 86_400),
            RecentAge::Days(3)
        );
    }

    #[test]
    fn recent_rows_show_name_and_parent_folder_and_cap_the_list() {
        let files: Vec<dbflux_core::RecentFile> = (0..6)
            .map(|index| dbflux_core::RecentFile {
                path: PathBuf::from(format!("/home/user/scripts/report-{index}.sql")),
                last_opened: 0,
            })
            .collect();

        let rows = recent_rows(&files, 0);

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].name.as_ref(), "report-0.sql");
        assert_eq!(rows[0].folder.as_ref(), "scripts/");
    }

    #[test]
    fn shell_strings_resolve_in_every_shipped_locale() {
        let keys = [
            "workspace.rail.connections",
            "workspace.rail.scripts",
            "workspace.rail.dashboards",
            "workspace.rail.approvals",
            "workspace.rail.audit",
            "workspace.rail.settings",
            "workspace.title_bar.command_search",
            "workspace.empty.title",
            "workspace.empty.subtitle",
            "workspace.empty.start",
            "workspace.empty.recent",
            "workspace.recent.just_now",
            "workspace.recent.yesterday",
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
