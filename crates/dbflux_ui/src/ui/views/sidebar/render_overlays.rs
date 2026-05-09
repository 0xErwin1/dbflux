use super::*;
use crate::platform;
use dbflux_components::primitives::Icon;
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
        let theme = cx.theme();
        let sidebar_for_close = cx.entity().clone();

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
                div()
                    .absolute()
                    .top(Heights::TOOLBAR)
                    .right(Spacing::XS)
                    .bg(theme.sidebar)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::SM)
                    .py(Spacing::XS)
                    .min_w(px(140.0))
                    .shadow_md()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .when(self.active_tab == SidebarTab::Connections, |el| {
                        self.add_connections_menu_items(el, cx)
                    })
                    .when(self.active_tab == SidebarTab::Scripts, |el| {
                        self.add_scripts_menu_items(el, cx)
                    }),
            )
    }

    fn add_connections_menu_items(&self, el: Div, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar_for_folder = cx.entity().clone();
        let sidebar_for_conn = cx.entity().clone();
        let hover_bg = theme.list_active;

        el.child(
            div()
                .id("add-folder-option")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .hover(move |d| d.bg(hover_bg))
                .on_click(move |_, _, cx| {
                    sidebar_for_folder.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.create_root_folder(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(Icon::new(AppIcon::Folder).size(px(16.0)).muted())
                        .child("New Folder"),
                ),
        )
        .child(
            div()
                .id("add-connection-option")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .hover(move |d| d.bg(theme.list_active))
                .on_click(move |_, _, cx| {
                    sidebar_for_conn.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                    });
                    let app_state = app_state.clone();
                    let mut options = WindowOptions {
                        app_id: Some("dbflux".into()),
                        titlebar: Some(TitlebarOptions {
                            title: Some("Connection Manager".into()),
                            ..Default::default()
                        }),
                        window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                            None,
                            size(px(600.0), px(550.0)),
                            cx,
                        ))),
                        ..Default::default()
                    };
                    platform::apply_window_options(&mut options, 600.0, 500.0);

                    let _ = cx.open_window(options, |window, cx| {
                        let manager =
                            cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
                        cx.new(|cx| Root::new(manager, window, cx))
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(Icon::new(AppIcon::Plug).size(px(16.0)).muted())
                        .child("New Connection"),
                ),
        )
    }

    fn add_scripts_menu_items(&self, el: Div, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let sidebar_for_file = cx.entity().clone();
        let sidebar_for_folder = cx.entity().clone();
        let sidebar_for_import = cx.entity().clone();
        let hover_bg = theme.list_active;
        let hover_bg2 = theme.list_active;
        let hover_bg3 = theme.list_active;

        el.child(
            div()
                .id("add-script-file")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .hover(move |d| d.bg(hover_bg))
                .on_click(move |_, _, cx| {
                    sidebar_for_file.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.create_script_file(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(Icon::new(AppIcon::ScrollText).size(px(16.0)).muted())
                        .child("New File"),
                ),
        )
        .child(
            div()
                .id("add-script-folder")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .hover(move |d| d.bg(hover_bg2))
                .on_click(move |_, _, cx| {
                    sidebar_for_folder.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.create_script_folder(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(Icon::new(AppIcon::Folder).size(px(16.0)).muted())
                        .child("New Folder"),
                ),
        )
        .child(
            div()
                .id("import-script")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .hover(move |d| d.bg(hover_bg3))
                .on_click(move |_, _, cx| {
                    sidebar_for_import.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.import_script(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(Icon::new(AppIcon::Download).size(px(16.0)).muted())
                        .child("Import File"),
                ),
        )
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
                message: "Event streams are not available for this collection".to_string(),
                is_error: true,
            });
            return;
        };

        let filter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter stream names..."));
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
            title: format!("Event streams: {}", name),
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
        let theme = cx.theme();
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
            "Page 0/0".to_string()
        } else {
            format!("Page {}/{} ({}-{})", page + 1, page_count, start + 1, end)
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
                    .child(Text::muted(subtitle).font_size(FontSizes::XS)),
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
                            .child(Text::caption("Stream name")),
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
                            .child(Text::caption("Last event")),
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
                                Text::muted("No event streams found").font_size(FontSizes::SM),
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

                            div()
                                .id(("child-picker-row", row_index))
                                .flex()
                                .gap(Spacing::SM)
                                .px(Spacing::MD)
                                .py(Spacing::XS)
                                .cursor_pointer()
                                .when(is_selected, |d| d.bg(theme.list_active))
                                .hover(|d| d.bg(theme.list_active))
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
                            .child(Icon::new(AppIcon::ChevronLeft).size(px(12.0)).color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(Text::caption("Prev").font_size(FontSizes::XS).color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
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
                            .child(Text::caption("Next").font_size(FontSizes::XS).color(
                                if can_next {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child(Icon::new(AppIcon::ChevronRight).size(px(12.0)).color(
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
