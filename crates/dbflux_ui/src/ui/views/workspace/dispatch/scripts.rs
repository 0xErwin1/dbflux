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
            _ => None,
        }
    }
}
