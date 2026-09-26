use super::*;
use dbflux_components::composites::{ListRow, MenuItem, menu_frame, menu_row};
use dbflux_components::primitives::Icon;
use dbflux_components::primitives::Text;

/// Minimum width of the sidebar's add menu.
const ADD_MENU_MIN_WIDTH: Pixels = px(200.0);
use gpui_component::scroll::ScrollableElement;

fn format_child_timestamp(timestamp_ms: Option<i64>) -> String {
    let Some(timestamp_ms) = timestamp_ms else {
        return "-".to_string();
    };

    dbflux_core::chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn child_picker_filtered_total(picker: &ChildPickerState) -> usize {
    let query = picker.filter_query.to_lowercase();

    picker
        .children
        .iter()
        .filter(|child| query.is_empty() || child.label.to_lowercase().contains(&query))
        .count()
}

/// Returns the filtered + sorted children in current display order, ignoring pagination.
fn sorted_visible_children(picker: &ChildPickerState) -> Vec<CollectionChildInfo> {
    let query = picker.filter_query.to_lowercase();
    let mut rows: Vec<CollectionChildInfo> = picker
        .children
        .iter()
        .filter(|child| query.is_empty() || child.label.to_lowercase().contains(&query))
        .cloned()
        .collect();

    match picker.sort_column {
        ChildPickerSortColumn::Name => rows.sort_by(|left, right| left.label.cmp(&right.label)),
        ChildPickerSortColumn::LastEvent => rows.sort_by(|left, right| {
            left.last_event_ts_ms
                .cmp(&right.last_event_ts_ms)
                .then_with(|| left.label.cmp(&right.label))
        }),
    }

    if picker.sort_descending {
        rows.reverse();
    }

    rows
}

/// Adjusts `picker.page` so the row at `selected_index` falls within the visible page.
fn ensure_selected_in_page(picker: &mut ChildPickerState) {
    if picker.page_size == 0 {
        return;
    }
    picker.page = picker.selected_index / picker.page_size;
}

impl Sidebar {
    pub(super) fn render_add_menu(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let sidebar_for_close = cx.entity().clone();

        let items = if self.active_tab == SidebarTab::Connections {
            self.add_connections_menu_items(cx)
        } else {
            self.add_scripts_menu_items(cx)
        };

        div()
            .absolute()
            .inset_0()
            .child(
                div()
                    .id("add-menu-overlay")
                    .absolute()
                    .inset_0()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        sidebar_for_close.update(cx, |this, cx| {
                            this.close_add_menu(cx);
                        });
                    }),
            )
            .child(
                menu_frame(cx)
                    .absolute()
                    .top(dbflux_components::tokens::ShellMetrics::SIDEBAR_HEADER_HEIGHT)
                    .right(Spacing::XS)
                    .min_w(ADD_MENU_MIN_WIDTH)
                    .children(items),
            )
    }

    /// One row of the sidebar's add menu; clicking closes the menu first.
    fn add_menu_row(
        &self,
        id: &'static str,
        icon: AppIcon,
        label: String,
        on_select: impl Fn(&mut Self, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let sidebar = cx.entity().clone();

        menu_row(id, &MenuItem::new(label).icon(icon), false, cx)
            .on_click(move |_, _, cx| {
                sidebar.update(cx, |this, cx| {
                    this.close_add_menu(cx);
                    on_select(this, cx);
                });
            })
            .into_any_element()
    }

    fn add_connections_menu_items(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        vec![
            self.add_menu_row(
                "add-folder-option",
                AppIcon::Folder,
                dbflux_i18n::t!("sidebar.overlay.add_folder"),
                |this, cx| this.create_root_folder(cx),
                cx,
            ),
            self.add_menu_row(
                "add-connection-option",
                AppIcon::Plug,
                dbflux_i18n::t!("sidebar.overlay.add_connection"),
                |_, cx| cx.emit(SidebarEvent::RequestOpenConnectionManager),
                cx,
            ),
        ]
    }

    fn add_scripts_menu_items(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        vec![
            self.add_menu_row(
                "add-script-file",
                AppIcon::ScrollText,
                dbflux_i18n::t!("sidebar.overlay.add_script_file"),
                |this, cx| this.create_script_file(cx),
                cx,
            ),
            self.add_menu_row(
                "add-script-folder",
                AppIcon::Folder,
                dbflux_i18n::t!("sidebar.overlay.add_script_folder"),
                |this, cx| this.create_script_folder(cx),
                cx,
            ),
            self.add_menu_row(
                "import-script",
                AppIcon::Download,
                dbflux_i18n::t!("sidebar.overlay.import_file"),
                |this, cx| this.import_script(cx),
                cx,
            ),
        ]
    }

    pub(super) fn open_child_picker_modal(
        &mut self,
        item_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        let Some(collection) = self.collection_info_for_item(item_id, cx) else {
            self.pending_toast = Some(PendingToast {
                message: dbflux_i18n::t!("sidebar.overlay.child_picker.unsupported"),
                is_error: true,
            });
            return;
        };

        let filter_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("sidebar.filter.stream_placeholder"))
        });
        let input_for_subscription = filter_input.clone();
        let filter_subscription = cx.subscribe_in(
            &input_for_subscription,
            window,
            |this, input_state, event: &InputEvent, _, cx| match event {
                InputEvent::Change => {
                    if let Some(picker) = this.child_picker.as_mut() {
                        picker.filter_query = input_state.read(cx).value().to_string();
                        picker.page = 0;
                        picker.selected_index = 0;
                        cx.notify();
                    }
                }
                InputEvent::Focus => {
                    if let Some(picker) = this.child_picker.as_mut() {
                        picker.filter_focused = true;
                        cx.notify();
                    }
                }
                InputEvent::Blur => {
                    if let Some(picker) = this.child_picker.as_mut() {
                        picker.filter_focused = false;
                        cx.notify();
                    }
                }
                InputEvent::PressEnter { .. } => {
                    this.picker_execute(cx);
                }
            },
        );

        self._subscriptions.push(filter_subscription);
        let children = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connection| {
                connection
                    .collection_children
                    .get(&(database.clone(), name.clone()))
                    .map(|cache| cache.items.clone())
            })
            .or(collection.child_items)
            .unwrap_or_default();

        let focus_handle = cx.focus_handle();
        self.child_picker = Some(ChildPickerState {
            profile_id,
            database,
            collection: name.clone(),
            title: super::labels::child_picker_title(&name),
            focus_handle: focus_handle.clone(),
            children,
            filter_input,
            filter_query: String::new(),
            page: 0,
            page_size: 50,
            sort_column: ChildPickerSortColumn::LastEvent,
            sort_descending: true,
            selected_index: 0,
            filter_focused: false,
        });

        focus_handle.focus(window, cx);
        cx.notify();
    }

    pub fn close_child_picker(&mut self, cx: &mut Context<Self>) {
        if self.child_picker.take().is_some() {
            cx.notify();
        }
    }

    pub fn has_child_picker_open(&self) -> bool {
        self.child_picker.is_some()
    }

    pub fn child_picker_filter_is_focused(&self) -> bool {
        self.child_picker
            .as_ref()
            .is_some_and(|picker| picker.filter_focused)
    }

    pub fn child_picker_focus_handle(&self) -> Option<FocusHandle> {
        self.child_picker
            .as_ref()
            .map(|picker| picker.focus_handle.clone())
    }

    /// Total filtered/sorted row count, used to clamp the selection index.
    fn child_picker_visible_total(picker: &ChildPickerState) -> usize {
        child_picker_filtered_total(picker)
    }

    pub fn picker_select_next(&mut self, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_mut() {
            let total = Self::child_picker_visible_total(picker);
            if total == 0 {
                return;
            }
            picker.selected_index = (picker.selected_index + 1).min(total - 1);
            ensure_selected_in_page(picker);
            cx.notify();
        }
    }

    pub fn picker_select_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_mut() {
            let total = Self::child_picker_visible_total(picker);
            if total == 0 {
                return;
            }
            picker.selected_index = picker.selected_index.saturating_sub(1);
            ensure_selected_in_page(picker);
            cx.notify();
        }
    }

    pub fn picker_select_first(&mut self, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_mut() {
            picker.selected_index = 0;
            ensure_selected_in_page(picker);
            cx.notify();
        }
    }

    pub fn picker_select_last(&mut self, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_mut() {
            let total = Self::child_picker_visible_total(picker);
            if total == 0 {
                return;
            }
            picker.selected_index = total - 1;
            ensure_selected_in_page(picker);
            cx.notify();
        }
    }

    pub fn picker_focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_ref() {
            let filter_input = picker.filter_input.clone();
            filter_input.update(cx, |input, cx| input.focus(window, cx));
        }
    }

    pub fn picker_focus_list(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(picker) = self.child_picker.as_ref() {
            picker.focus_handle.focus(window, cx);
            cx.notify();
        }
    }

    pub fn picker_execute(&mut self, cx: &mut Context<Self>) {
        let Some(picker) = self.child_picker.as_ref() else {
            return;
        };

        let Some(child) = sorted_visible_children(picker)
            .into_iter()
            .nth(picker.selected_index)
        else {
            return;
        };

        let target = EventStreamTarget {
            collection: CollectionRef::new(&picker.database, &picker.collection),
            child_id: Some(child.id.clone()),
        };

        let profile_id = picker.profile_id;
        let title = child.label.clone();

        cx.emit(SidebarEvent::OpenCollectionChild {
            profile_id,
            target,
            title,
        });

        self.close_child_picker(cx);
    }

    pub fn render_child_picker_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let app: &App = cx;
        let theme = app.theme();
        let Some(picker) = self.child_picker.as_ref() else {
            return div();
        };

        let rows = sorted_visible_children(picker);

        let total = rows.len();
        let page_count = total.max(1).div_ceil(picker.page_size);
        let page = picker.page.min(page_count.saturating_sub(1));
        let start = page.saturating_mul(picker.page_size);
        let end = (start + picker.page_size).min(total);
        let visible_rows = rows[start..end].to_vec();
        let selected_index = picker.selected_index;

        let sidebar = cx.entity().clone();
        let prev_sidebar = sidebar.clone();
        let next_sidebar = sidebar.clone();
        let name_sort_sidebar = sidebar.clone();
        let last_event_sort_sidebar = sidebar.clone();
        let profile_id = picker.profile_id;
        let database = picker.database.clone();
        let collection = picker.collection.clone();
        let subtitle = picker.title.clone();

        let can_prev = page > 0;
        let can_next = page + 1 < page_count;
        let page_label = if total == 0 {
            super::labels::page_label(0, 0, 0, 0)
        } else {
            super::labels::page_label(page + 1, page_count, start + 1, end)
        };

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(
                div()
                    .px(Spacing::MD)
                    .pt(Spacing::SM)
                    .child(Text::caption(subtitle).font_size(FontSizes::XS)),
            )
            .child(
                div()
                    .px(Spacing::MD)
                    .py(Spacing::SM)
                    .child(Input::new(&picker.filter_input).cleanable(true)),
            )
            .child(
                div()
                    .flex()
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .gap(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .text_size(FontSizes::XS)
                    .child(
                        div()
                            .id("child-picker-sort-name")
                            .cursor_pointer()
                            .on_click(move |_, _, cx| {
                                name_sort_sidebar.update(cx, |this, cx| {
                                    if let Some(picker) = this.child_picker.as_mut() {
                                        if picker.sort_column == ChildPickerSortColumn::Name {
                                            picker.sort_descending = !picker.sort_descending;
                                        } else {
                                            picker.sort_column = ChildPickerSortColumn::Name;
                                            picker.sort_descending = false;
                                        }

                                        picker.page = 0;
                                    }

                                    cx.notify();
                                });
                            })
                            .flex_1()
                            .child(Text::caption(dbflux_i18n::t!(
                                "sidebar.overlay.child_picker.column_name"
                            ))),
                    )
                    .child(
                        div()
                            .id("child-picker-sort-last-event")
                            .cursor_pointer()
                            .on_click(move |_, _, cx| {
                                last_event_sort_sidebar.update(cx, |this, cx| {
                                    if let Some(picker) = this.child_picker.as_mut() {
                                        if picker.sort_column == ChildPickerSortColumn::LastEvent {
                                            picker.sort_descending = !picker.sort_descending;
                                        } else {
                                            picker.sort_column = ChildPickerSortColumn::LastEvent;
                                            picker.sort_descending = true;
                                        }

                                        picker.page = 0;
                                    }

                                    cx.notify();
                                });
                            })
                            .flex_1()
                            .child(Text::caption(dbflux_i18n::t!(
                                "sidebar.overlay.child_picker.column_last_event"
                            ))),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .when(visible_rows.is_empty(), |el| {
                        el.child(
                            div().px(Spacing::MD).py(Spacing::SM).child(
                                Text::caption(dbflux_i18n::t!(
                                    "sidebar.overlay.child_picker.empty"
                                ))
                                .font_size(FontSizes::SM),
                            ),
                        )
                    })
                    .children(visible_rows.into_iter().enumerate().map(
                        move |(row_index, child)| {
                            let row_sidebar = sidebar.clone();
                            let database = database.clone();
                            let collection = collection.clone();
                            let child_id = child.id.clone();
                            let child_label = child.label.clone();
                            let timestamp = format_child_timestamp(child.last_event_ts_ms);
                            let absolute_index = start + row_index;
                            let is_selected = absolute_index == selected_index;

                            ListRow::new(("child-picker-row", row_index))
                                .selected(is_selected)
                                .build(app)
                                .flex()
                                .gap(Spacing::SM)
                                .px(Spacing::MD)
                                .py(Spacing::XS)
                                .on_click(move |_, _, cx| {
                                    row_sidebar.update(cx, |this, cx| {
                                        let target = EventStreamTarget {
                                            collection: CollectionRef::new(&database, &collection),
                                            child_id: Some(child_id.clone()),
                                        };

                                        cx.emit(SidebarEvent::OpenCollectionChild {
                                            profile_id,
                                            target,
                                            title: child_label.clone(),
                                        });

                                        this.close_child_picker(cx);
                                    });
                                })
                                .child(div().flex_1().child(Text::caption(child.label)))
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Text::caption(timestamp).muted_foreground()),
                                )
                        },
                    )),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_center()
                    .gap(Spacing::SM)
                    .px(Spacing::MD)
                    .py(Spacing::SM)
                    .border_t_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .id("child-picker-prev")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .when(can_prev, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(move |_, _, cx| {
                                        prev_sidebar.update(cx, |this, cx| {
                                            if let Some(picker) = this.child_picker.as_mut() {
                                                picker.page = picker.page.saturating_sub(1);
                                            }
                                            cx.notify();
                                        });
                                    })
                            })
                            .when(!can_prev, |d| d.opacity(0.5))
                            .child(Icon::new(AppIcon::ChevronLeft).size(Spacing::MD).color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(
                                Text::caption(dbflux_i18n::t!("sidebar.overlay.child_picker.prev"))
                                    .font_size(FontSizes::XS)
                                    .color(if can_prev {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            ),
                    )
                    .child(Text::caption(page_label).font_size(FontSizes::XS))
                    .child(
                        div()
                            .id("child-picker-next")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .when(can_next, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(move |_, _, cx| {
                                        next_sidebar.update(cx, |this, cx| {
                                            if let Some(picker) = this.child_picker.as_mut() {
                                                let page_count =
                                                    child_picker_filtered_total(picker)
                                                        .max(1)
                                                        .div_ceil(picker.page_size);
                                                picker.page = (picker.page + 1)
                                                    .min(page_count.saturating_sub(1));
                                            }
                                            cx.notify();
                                        });
                                    })
                            })
                            .when(!can_next, |d| d.opacity(0.5))
                            .child(
                                Text::caption(dbflux_i18n::t!("sidebar.overlay.child_picker.next"))
                                    .font_size(FontSizes::XS)
                                    .color(if can_next {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            )
                            .child(Icon::new(AppIcon::ChevronRight).size(Spacing::MD).color(
                                if can_next {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    ),
            )
    }
}
