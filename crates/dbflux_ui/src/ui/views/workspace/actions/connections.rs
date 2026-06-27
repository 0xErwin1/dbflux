use super::*;

impl Workspace {
    pub(in crate::ui::views::workspace) fn open_connection_manager(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Connection Manager".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        match cx.open_window(options, |window, cx| {
            let manager = cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            Ok(handle) => {
                // Explicitly activate the window and force initial render (X11 fix)
                if let Err(e) = handle.update(cx, |_root, window, cx| {
                    window.activate_window();
                    cx.notify();
                }) {
                    log::warn!("Failed to activate connection manager window: {:?}", e);
                }
            }
            Err(error) => {
                log::warn!("Failed to open connection manager window: {:?}", error);
            }
        }
    }

    pub(in crate::ui::views::workspace) fn open_connection_manager_for_edit(
        &self,
        profile_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();

        let profile = app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::warn!(
                "open_connection_manager_for_edit: profile {} not found",
                profile_id
            );
            return;
        };

        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);
        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Edit Connection".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            let manager =
                cx.new(|cx| ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!("Failed to open connection editor window: {:?}", error);
        }
    }

    pub(in crate::ui::views::workspace) fn open_connection_manager_in_folder(
        &self,
        folder_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Connection Manager".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            let manager = cx
                .new(|cx| ConnectionManagerWindow::new_in_folder(app_state, folder_id, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!(
                "Failed to open connection manager window for folder: {:?}",
                error
            );
        }
    }

    /// Open the in-app export modal for a single connection profile.
    pub(in crate::ui::views::workspace) fn open_export_connection_modal(
        &self,
        profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.export_modal.update(cx, |modal, cx| {
            modal.open(profile_id, window, cx);
        });
    }

    pub(in crate::ui::views::workspace) fn disconnect_active(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_id = self.app_state.read(cx).active_connection_id();

        if let Some(id) = profile_id {
            let name = self
                .app_state
                .read(cx)
                .connections()
                .get(&id)
                .map(|c| c.profile.name.clone());

            self.sidebar.update(cx, |sidebar, cx| {
                sidebar.disconnect_profile(id, cx);
            });

            if let Some(name) = name {
                Toast::info(format!("Disconnecting from {}...", name))
                    .meta_right(now_hms())
                    .push(cx);
            }
        }
    }

    pub(in crate::ui::views::workspace) fn refresh_schema(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            Toast::warning("No active connection")
                .meta_right(now_hms())
                .push(cx);
            return;
        };

        let conn = active.connection.clone();
        let profile_id = active.profile.id;
        let app_state = self.app_state.clone();

        let task = cx.background_executor().spawn(async move { conn.schema() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| match result {
                Ok(schema) => {
                    app_state.update(cx, |state, cx| {
                        if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                            connected.schema = Some(schema);
                        }
                        cx.emit(AppStateChanged);
                    });
                }
                Err(e) => {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            format!("Failed to refresh schema: {e}"),
                        ),
                        cx,
                    );
                }
            }) {
                log::warn!(
                    "Failed to apply refreshed schema to workspace state: {:?}",
                    error
                );
            }
        })
        .detach();

        Toast::info("Refreshing schema...")
            .meta_right(now_hms())
            .push(cx);
    }
}
