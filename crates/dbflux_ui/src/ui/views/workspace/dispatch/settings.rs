use super::*;
use dbflux_core::InterfaceSizeChange;

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
            Command::IncreaseInterfaceSize => {
                self.change_interface_size(InterfaceSizeChange::Increase, cx)
            }
            Command::DecreaseInterfaceSize => {
                self.change_interface_size(InterfaceSizeChange::Decrease, cx)
            }
            Command::ResetInterfaceSize => {
                self.change_interface_size(InterfaceSizeChange::Reset, cx)
            }
            _ => None,
        }
    }

    /// Runs an interface size command. It is handled even when the sizes are
    /// already at their limit, so the keys never fall through to whatever
    /// has focus.
    fn change_interface_size(
        &mut self,
        change: InterfaceSizeChange,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        dbflux_ui_base::interface_size::change_interface_size(&self.app_state, change, cx);
        Some(true)
    }
}
