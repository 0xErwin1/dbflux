use gpui::prelude::*;
use gpui::{AnyElement, App, Context, KeyDownEvent, Window, div};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SettingsSectionId {
    General,
    Audit,
    #[cfg(feature = "mcp")]
    McpClients,
    #[cfg(feature = "mcp")]
    McpRoles,
    #[cfg(feature = "mcp")]
    McpPolicies,
    Keybindings,
    Proxies,
    SshTunnels,
    AuthProfiles,
    Services,
    Hooks,
    Drivers,
    About,
}

#[allow(dead_code)]
pub trait SettingsSection: 'static {
    fn section_id(&self) -> SettingsSectionId;

    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> AnyElement
    where
        Self: Sized,
    {
        div().size_full().into_any_element()
    }

    fn handle_key_event(
        &mut self,
        _event: &KeyDownEvent,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) where
        Self: Sized,
    {
    }

    fn focus_in(&mut self, _window: &mut Window, _cx: &mut Context<Self>)
    where
        Self: Sized,
    {
    }

    fn focus_out(&mut self, _window: &mut Window, _cx: &mut Context<Self>)
    where
        Self: Sized,
    {
    }

    fn is_dirty(&self, _cx: &App) -> bool
    where
        Self: Sized,
    {
        false
    }
}

#[derive(Clone, Debug)]
pub enum SectionFocusEvent {
    RequestFocusReturn,
}
