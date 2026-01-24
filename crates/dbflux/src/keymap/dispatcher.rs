use super::Command;
use gpui::{Context, Window};

/// Trait for components that can dispatch commands.
///
/// The Workspace is the main dispatcher, routing commands to appropriate
/// components based on the current focus target.
pub trait CommandDispatcher: Sized {
    /// Attempts to dispatch and execute a command.
    ///
    /// Returns `true` if the command was handled, `false` otherwise.
    fn dispatch(&mut self, cmd: Command, window: &mut Window, cx: &mut Context<Self>) -> bool;
}
