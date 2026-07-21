use super::*;

impl Workspace {
    pub(in crate::ui::views::workspace) fn open_settings(&self, cx: &mut Context<Self>) {
        let workspace = cx.entity().clone();
        dbflux_ui_windows::settings::open_or_focus_settings(
            self.app_state.clone(),
            None,
            cx,
            move |settings, cx| {
                cx.subscribe(
                    settings,
                    move |_settings, event: &dbflux_ui_windows::settings::SettingsEvent, cx| {
                        workspace.update(cx, |this, cx| match event {
                            dbflux_ui_windows::settings::SettingsEvent::OpenScript { path } => {
                                this.open_script_from_path(path.clone(), cx);
                            }
                            dbflux_ui_windows::settings::SettingsEvent::OpenLoginModal {
                                provider_name,
                                profile_name,
                                url,
                            } => {
                                this.pending_login_modal_open = Some((
                                    provider_name.clone(),
                                    profile_name.clone(),
                                    url.clone(),
                                ));
                                cx.notify();
                            }
                        });
                    },
                )
                .detach();
            },
        );
    }

    pub(in crate::ui::views::workspace) fn open_login_modal(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_name = self
            .app_state
            .read(cx)
            .active_connection()
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_else(|| "connection".to_string());

        self.login_modal.update(cx, |modal, cx| {
            modal.open_manual("Auth Provider", profile_name, None, window, cx);
        });
    }

    pub(in crate::ui::views::workspace) fn open_auth_profiles_settings(
        &self,
        cx: &mut Context<Self>,
    ) {
        dbflux_ui_windows::settings::open_or_focus_settings(
            self.app_state.clone(),
            Some(dbflux_ui_windows::settings::SettingsSectionId::AuthProfiles),
            cx,
            |_settings, _cx| {},
        );
    }

    pub(in crate::ui::views::workspace) fn open_sso_wizard(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sso_wizard.update(cx, |wizard, cx| {
            wizard.open(window, cx);
        });
    }
}
