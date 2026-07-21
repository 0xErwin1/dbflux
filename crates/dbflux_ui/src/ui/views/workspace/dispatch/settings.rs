use super::*;

impl Workspace {
    pub(super) fn dispatch_settings(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::OpenLoginModal => {
                self.open_login_modal(window, cx);
                Some(true)
            }
            Command::OpenSsoWizard => {
                self.open_sso_wizard(window, cx);
                Some(true)
            }
            Command::OpenSettings => {
                self.open_settings(cx);
                Some(true)
            }
            _ => None,
        }
    }
}
