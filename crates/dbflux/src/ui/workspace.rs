use crate::app::AppState;
use crate::ui::editor::EditorPane;
use crate::ui::results::{ResultsPane, ResultsReceived};
use crate::ui::sidebar::Sidebar;
use crate::ui::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::tasks_panel::TasksPanel;
use crate::ui::toast::ToastManager;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::notification::NotificationList;
use gpui_component::resizable::{h_resizable, resizable_panel, v_resizable};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PanelState {
    Expanded,
    Collapsed,
}

impl PanelState {
    fn is_expanded(self) -> bool {
        self == PanelState::Expanded
    }

    fn toggle(&mut self) {
        *self = match self {
            PanelState::Expanded => PanelState::Collapsed,
            PanelState::Collapsed => PanelState::Expanded,
        };
    }
}

pub struct Workspace {
    #[allow(dead_code)]
    app_state: Entity<AppState>,
    sidebar: Entity<Sidebar>,
    editor: Entity<EditorPane>,
    results: Entity<ResultsPane>,
    status_bar: Entity<StatusBar>,
    tasks_panel: Entity<TasksPanel>,
    notification_list: Entity<NotificationList>,

    editor_state: PanelState,
    results_state: PanelState,
    tasks_state: PanelState,
}

impl Workspace {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        ToastManager::init(window, cx);

        let results = cx.new(|cx| ResultsPane::new(app_state.clone(), window, cx));
        let editor = cx.new(|cx| EditorPane::new(app_state.clone(), results.clone(), window, cx));
        let sidebar = cx.new(|cx| {
            Sidebar::new(
                app_state.clone(),
                editor.clone(),
                results.clone(),
                window,
                cx,
            )
        });
        let status_bar = cx.new(|cx| StatusBar::new(app_state.clone(), window, cx));
        let tasks_panel = cx.new(|cx| TasksPanel::new(app_state.clone(), window, cx));
        let notification_list = ToastManager::notification_list(cx);

        cx.subscribe(&status_bar, |this, _, _: &ToggleTasksPanel, cx| {
            this.toggle_tasks_panel(cx);
        })
        .detach();

        cx.subscribe(&results, |this, _, _: &ResultsReceived, cx| {
            this.on_results_received(cx);
        })
        .detach();

        Self {
            app_state,
            sidebar,
            editor,
            results,
            status_bar,
            tasks_panel,
            notification_list,
            editor_state: PanelState::Expanded,
            results_state: PanelState::Expanded,
            tasks_state: PanelState::Collapsed,
        }
    }

    pub fn toggle_tasks_panel(&mut self, cx: &mut Context<Self>) {
        self.tasks_state.toggle();
        cx.notify();
    }

    pub fn toggle_editor(&mut self, cx: &mut Context<Self>) {
        self.editor_state.toggle();
        cx.notify();
    }

    pub fn toggle_results(&mut self, cx: &mut Context<Self>) {
        self.results_state.toggle();
        cx.notify();
    }

    fn on_results_received(&mut self, cx: &mut Context<Self>) {
        if !self.results_state.is_expanded() {
            self.results_state = PanelState::Expanded;
            cx.notify();
        }
    }

    fn render_panel_header(
        &self,
        title: &'static str,
        is_expanded: bool,
        on_toggle: impl Fn(&mut Self, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let chevron = if is_expanded { "▼" } else { "▶" };

        div()
            .id(SharedString::from(format!("panel-header-{}", title)))
            .flex()
            .items_center()
            .justify_between()
            .h(px(24.0))
            .px_2()
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .hover(|s| s.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                on_toggle(this, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme.foreground)
                    .child(chevron)
                    .child(title),
            )
    }
}

impl Render for Workspace {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let sidebar = self.sidebar.clone();
        let editor = self.editor.clone();
        let results = self.results.clone();
        let status_bar = self.status_bar.clone();
        let tasks_panel = self.tasks_panel.clone();
        let notification_list = self.notification_list.clone();

        let editor_expanded = self.editor_state.is_expanded();
        let results_expanded = self.results_state.is_expanded();
        let tasks_expanded = self.tasks_state.is_expanded();

        let theme = cx.theme();
        let bg_color = theme.background;

        let editor_header =
            self.render_panel_header("Editor", editor_expanded, Self::toggle_editor, cx);
        let results_header =
            self.render_panel_header("Results", results_expanded, Self::toggle_results, cx);
        let tasks_header = self.render_panel_header(
            "Background Tasks",
            tasks_expanded,
            Self::toggle_tasks_panel,
            cx,
        );

        let header_size = px(25.0);

        let right_pane = v_resizable("main-panels")
            .child(
                resizable_panel()
                    .size(if editor_expanded {
                        px(300.0)
                    } else {
                        header_size
                    })
                    .size_range(if editor_expanded {
                        px(100.0)..px(2000.0)
                    } else {
                        header_size..header_size
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(editor_header)
                            .when(editor_expanded, |el| {
                                el.child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .flex_1()
                                        .overflow_hidden()
                                        .child(editor),
                                )
                            }),
                    ),
            )
            .child(
                resizable_panel()
                    .size(if results_expanded {
                        px(300.0)
                    } else {
                        header_size
                    })
                    .size_range(if results_expanded {
                        px(100.0)..px(2000.0)
                    } else {
                        header_size..header_size
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(results_header)
                            .when(results_expanded, |el| {
                                el.child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .flex_1()
                                        .overflow_hidden()
                                        .child(results),
                                )
                            }),
                    ),
            )
            .child(
                resizable_panel()
                    .size(if tasks_expanded {
                        px(150.0)
                    } else {
                        header_size
                    })
                    .size_range(if tasks_expanded {
                        px(80.0)..px(2000.0)
                    } else {
                        header_size..header_size
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(tasks_header)
                            .when(tasks_expanded, |el| {
                                el.child(div().flex_1().overflow_hidden().child(tasks_panel))
                            }),
                    ),
            );

        div()
            .relative()
            .size_full()
            .bg(bg_color)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .size_full()
                    .child(
                        div().flex().flex_1().overflow_hidden().child(
                            h_resizable("workspace")
                                .child(
                                    resizable_panel()
                                        .size(px(240.0))
                                        .size_range(px(150.0)..px(500.0))
                                        .child(sidebar),
                                )
                                .child(resizable_panel().child(right_pane)),
                        ),
                    )
                    .child(status_bar),
            )
            .child(notification_list)
    }
}
