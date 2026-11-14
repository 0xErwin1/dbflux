use super::*;

impl Workspace {
    pub(super) fn dispatch_scripts(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::OpenScriptFile => {
                self.open_script_file(window, cx);
                Some(true)
            }
            Command::AddExternalScriptsFolder => {
                self.show_sidebar_view(SidebarTab::Scripts, cx);
                self.sidebar
                    .update(cx, |sidebar, cx| sidebar.add_external_scripts_folder(cx));
                Some(true)
            }
            _ => None,
        }
    }
}
