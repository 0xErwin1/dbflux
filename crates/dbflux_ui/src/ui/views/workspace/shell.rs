//! The application shell around the documents: title bar, activity rail and
//! the empty workspace (AppByzTable, P1Empty).

use super::*;
use crate::ui::document::DocumentIcon;
use dbflux_components::composites::{
    ActivityRail, CommandSearch, ListRow, NotificationBell, RailEntry,
};
use dbflux_components::primitives::{Chamfer, Icon, Kbd, Text};
use dbflux_components::tokens::{ChamferCut, ShellMetrics};
use dbflux_components::typography::AppFonts;
use dbflux_ui_base::keymap::chord_display_parts;
use dbflux_ui_base::platform;

/// Identifiers of the rail entries, passed back by the rail on click.
pub(super) mod rail_ids {
    pub const CONNECTIONS: &str = "connections";
    pub const SCRIPTS: &str = "scripts";
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
    pub approvals_pending: usize,
    pub audit_active: bool,
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
    ];

    if state.approvals_available {
        entries.push(
            RailEntry::new(
                rail_ids::APPROVALS,
                AppIcon::Bot,
                dbflux_i18n::t!("workspace.rail.approvals"),
            )
            .active(state.approvals_open)
            .pending(state.approvals_pending > 0),
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

/// Display labels of the chord the default keymap binds to `command` in the
/// global context, so the shell shows the binding that is actually
/// registered, with the platform's modifier (Cmd on macOS).
pub(super) fn global_shortcut_keys(command: Command) -> Option<Vec<SharedString>> {
    default_keymap()
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
    /// MCP executions waiting for a decision, or zero when the build has no
    /// MCP support or the governance service cannot list them.
    pub(super) fn pending_approvals_count(&self, cx: &App) -> usize {
        #[cfg(feature = "mcp")]
        {
            match self.app_state.read(cx).list_mcp_pending_executions() {
                Ok(pending) => pending.len(),
                Err(error) => {
                    log::debug!("Failed to list pending MCP approvals: {error}");
                    0
                }
            }
        }

        #[cfg(not(feature = "mcp"))]
        {
            let _unused = cx;
            0
        }
    }

    fn rail_state(&self, cx: &App) -> RailState {
        let sidebar_view = (!self.sidebar_dock.read(cx).is_collapsed())
            .then(|| self.sidebar.read(cx).active_tab());

        let audit_active = self
            .tab_manager
            .read(cx)
            .active_tab()
            .is_some_and(|tab| tab.meta_snapshot(cx).icon == DocumentIcon::Audit);

        #[cfg(feature = "mcp")]
        let approvals_open = self.active_governance_panel.is_some();
        #[cfg(not(feature = "mcp"))]
        let approvals_open = false;

        RailState {
            sidebar_view,
            approvals_available: cfg!(feature = "mcp"),
            approvals_open,
            approvals_pending: self.pending_approvals_count(cx),
            audit_active,
        }
    }

    /// Shows a sidebar view from the rail. Choosing the view already on
    /// screen collapses the sidebar; any other choice switches to it,
    /// expands the sidebar and gives it focus.
    fn show_sidebar_view(&mut self, tab: SidebarTab, cx: &mut Context<Self>) {
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
            rail_ids::AUDIT => self.open_audit_viewer(window, cx),
            rail_ids::SETTINGS => self.open_settings(cx),
            #[cfg(feature = "mcp")]
            rail_ids::APPROVALS => {
                if self.active_governance_panel.is_some() {
                    self.close_governance_panel(window, cx);
                } else {
                    self.open_mcp_approvals(window, cx);
                }
            }
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

    /// The 42 px row at the top of the window: the sidebar toggle and the
    /// command search over the rail and sidebar, the document tabs, the
    /// approvals bell, and on a client-decorated Linux window the window
    /// controls. Empty space in the row moves the window there.
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

        let theme = cx.theme();
        let collapsed = self.is_sidebar_collapsed(cx);
        let left_width = ShellMetrics::RAIL_WIDTH + self.sidebar_dock.read(cx).expanded_width();
        let palette_keys = global_shortcut_keys(Command::ToggleCommandPalette).unwrap_or_default();

        let collapse_icon = if collapsed {
            AppIcon::ChevronRight
        } else {
            AppIcon::ChevronLeft
        };
        let collapse_label: SharedString = if collapsed {
            dbflux_i18n::t!("workspace.title_bar.expand_sidebar").into()
        } else {
            dbflux_i18n::t!("workspace.title_bar.collapse_sidebar").into()
        };
        let muted = theme.muted_foreground;
        let strong = dbflux_components::tokens::ChromeColors::strong(theme);

        let workspace = cx.entity().clone();
        let collapse_button = div()
            .id("title-bar-sidebar-toggle")
            .aria_label(collapse_label.clone())
            .flex()
            .flex_shrink_0()
            .items_center()
            .px(ShellMetrics::COLLAPSE_PADDING_X)
            .cursor_pointer()
            .text_color(muted)
            .hover(move |button| button.text_color(strong))
            .child(Icon::new(collapse_icon).size(ShellMetrics::COLLAPSE_ICON))
            .tooltip(move |window, cx| {
                gpui_component::tooltip::Tooltip::new(collapse_label.clone()).build(window, cx)
            })
            .on_click(move |_, _, cx| {
                workspace.update(cx, |workspace, cx| workspace.toggle_sidebar(cx));
            });

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

        let left_block = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::TITLE_BLOCK_GAP)
            .w(left_width)
            .h_full()
            .pl(ShellMetrics::TITLE_BLOCK_PADDING_LEFT)
            .pr(ShellMetrics::TITLE_BLOCK_PADDING_RIGHT)
            .border_r_1()
            .border_color(theme.border)
            .child(collapse_button)
            .child(command_search);

        let bell = cfg!(feature = "mcp").then(|| {
            let workspace = cx.entity().clone();

            NotificationBell::new(
                "title-bar-approvals",
                dbflux_i18n::t!("workspace.title_bar.approvals"),
                self.pending_approvals_count(cx),
            )
            .on_click(move |_, window, cx| {
                workspace.update(cx, |workspace, cx| {
                    workspace.handle_rail_select(rail_ids::APPROVALS, window, cx);
                });
            })
        });

        div()
            .id("title-bar")
            .flex()
            .flex_shrink_0()
            .items_stretch()
            .w_full()
            .h(ShellMetrics::TITLE_BAR_HEIGHT)
            .bg(theme.background)
            .border_b_1()
            .border_color(theme.border)
            .child(left_block)
            .child(
                div()
                    .flex()
                    .min_w_0()
                    .overflow_hidden()
                    .child(self.tab_bar.clone()),
            )
            .child(
                platform::csd_drag_area("title-bar-drag-area")
                    .flex_1()
                    .h_full(),
            )
            .children(bell)
            .children(window_controls)
    }

    /// The empty workspace (P1Empty): the app glyph and title over a START
    /// card of global commands and, when files were opened before, a RECENT
    /// card of them.
    pub(super) fn render_empty_workspace(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();
        let now = chrono::Utc::now().timestamp();
        let recent = recent_rows(self.app_state.read(cx).recent_files(), now);

        let mark_path = match dbflux_core::ReleaseChannel::current() {
            dbflux_core::ReleaseChannel::Nightly => "branding/nightly/mark-256.png",
            _ => "branding/stable/mark-256.png",
        };

        let head = div()
            .flex()
            .items_center()
            .gap(ShellMetrics::EMPTY_HEAD_GAP)
            .child(
                img(mark_path)
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
                            .font_family(AppFonts::DISPLAY)
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

        let cards = div()
            .flex()
            .gap(ShellMetrics::EMPTY_HEAD_GAP)
            .when(!has_recent, |cards| cards.justify_center())
            .child(
                div()
                    .when(has_recent, |card| card.flex_1())
                    .when(!has_recent, |card| {
                        card.w((ShellMetrics::EMPTY_WIDTH - ShellMetrics::EMPTY_HEAD_GAP) / 2.0)
                    })
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
                            .font_family(AppFonts::MONO)
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
        start_actions,
    };
    use crate::keymap::{Command, KeyChord, Modifiers};
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
    fn rail_flags_pending_approvals_and_the_open_views() {
        let entries = rail_entries(RailState {
            approvals_available: true,
            approvals_open: true,
            approvals_pending: 2,
            audit_active: true,
            ..RailState::default()
        });

        let approvals = entries
            .iter()
            .find(|entry| entry.id.as_ref() == rail_ids::APPROVALS)
            .expect("approvals entry");
        assert!(approvals.active);
        assert!(approvals.pending);

        let audit = entries
            .iter()
            .find(|entry| entry.id.as_ref() == rail_ids::AUDIT)
            .expect("audit entry");
        assert!(audit.active);

        let idle = rail_entries(RailState {
            approvals_available: true,
            ..RailState::default()
        });
        assert!(idle.iter().all(|entry| !entry.pending && !entry.active));
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
            "workspace.rail.approvals",
            "workspace.rail.audit",
            "workspace.rail.settings",
            "workspace.title_bar.command_search",
            "workspace.title_bar.approvals",
            "workspace.title_bar.collapse_sidebar",
            "workspace.title_bar.expand_sidebar",
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
