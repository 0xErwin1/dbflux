mod auth_profiles_section;
mod drivers;
mod drivers_section;
mod form_nav;
mod general;
mod general_section;
mod hooks;
mod hooks_section;
mod keybindings;
mod keybindings_section;
mod lifecycle;
mod proxies;
mod proxies_section;
mod render;
mod rpc_services;
mod section_trait;
mod services_section;
mod sidebar_nav;
mod ssh_tunnels;
mod ssh_tunnels_section;

use crate::app::AppState;
use crate::ui::components::tree_nav::TreeNav;
use auth_profiles_section::AuthProfilesSection;
use drivers_section::DriversSection;
use general_section::GeneralSection;
use gpui::prelude::*;
use gpui::*;
use hooks_section::HooksSection;
use keybindings_section::KeybindingsSection;
use proxies_section::ProxiesSection;
use services_section::ServicesSection;
use ssh_tunnels_section::SshTunnelsSection;

pub use self::section_trait::{SettingsSection, SettingsSectionId};

#[derive(Clone, Copy, PartialEq, Eq)]
enum SettingsFocus {
    Sidebar,
    Content,
}

struct EmptySettingsSection {
    section_id: SettingsSectionId,
}

impl EmptySettingsSection {
    fn new(section_id: SettingsSectionId) -> Self {
        Self { section_id }
    }
}

impl SettingsSection for EmptySettingsSection {
    fn section_id(&self) -> SettingsSectionId {
        self.section_id
    }
}

impl Render for EmptySettingsSection {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full()
    }
}

enum ActiveSettingsSection {
    Empty(Entity<EmptySettingsSection>),
    AuthProfiles(Entity<AuthProfilesSection>),
    Drivers(Entity<DriversSection>),
    General(Entity<GeneralSection>),
    Hooks(Entity<HooksSection>),
    Keybindings(Entity<KeybindingsSection>),
    Proxies(Entity<ProxiesSection>),
    Services(Entity<ServicesSection>),
    SshTunnels(Entity<SshTunnelsSection>),
}

impl ActiveSettingsSection {
    fn as_view(&self) -> AnyView {
        match self {
            Self::Empty(section) => AnyView::from(section.clone()),
            Self::AuthProfiles(section) => AnyView::from(section.clone()),
            Self::Drivers(section) => AnyView::from(section.clone()),
            Self::General(section) => AnyView::from(section.clone()),
            Self::Hooks(section) => AnyView::from(section.clone()),
            Self::Keybindings(section) => AnyView::from(section.clone()),
            Self::Proxies(section) => AnyView::from(section.clone()),
            Self::Services(section) => AnyView::from(section.clone()),
            Self::SshTunnels(section) => AnyView::from(section.clone()),
        }
    }

    fn handle_key_event(
        &self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<SettingsCoordinator>,
    ) {
        match self {
            Self::Empty(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::AuthProfiles(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::Drivers(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::General(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::Hooks(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::Keybindings(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::Proxies(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::Services(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
            Self::SshTunnels(section) => {
                section.update(cx, |section, cx| {
                    section.handle_key_event(event, window, cx)
                });
            }
        }
    }

    fn focus_in(&self, window: &mut Window, cx: &mut Context<SettingsCoordinator>) {
        match self {
            Self::Empty(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::AuthProfiles(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::Drivers(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::General(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::Hooks(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::Keybindings(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::Proxies(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::Services(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
            Self::SshTunnels(section) => {
                section.update(cx, |section, cx| section.focus_in(window, cx));
            }
        }
    }

    fn focus_out(&self, window: &mut Window, cx: &mut Context<SettingsCoordinator>) {
        match self {
            Self::Empty(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::AuthProfiles(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::Drivers(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::General(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::Hooks(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::Keybindings(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::Proxies(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::Services(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
            Self::SshTunnels(section) => {
                section.update(cx, |section, cx| section.focus_out(window, cx));
            }
        }
    }

    fn is_dirty(&self, cx: &App) -> bool {
        match self {
            Self::Empty(section) => section.read(cx).is_dirty(cx),
            Self::AuthProfiles(section) => section.read(cx).is_dirty(cx),
            Self::Drivers(section) => section.read(cx).is_dirty(cx),
            Self::General(section) => section.read(cx).is_dirty(cx),
            Self::Hooks(section) => section.read(cx).is_dirty(cx),
            Self::Keybindings(section) => section.read(cx).is_dirty(cx),
            Self::Proxies(section) => section.read(cx).is_dirty(cx),
            Self::Services(section) => section.read(cx).is_dirty(cx),
            Self::SshTunnels(section) => section.read(cx).is_dirty(cx),
        }
    }
}

pub struct SettingsCoordinator {
    app_state: Entity<AppState>,
    sidebar_tree: TreeNav,
    focus_area: SettingsFocus,
    focus_handle: FocusHandle,
    active_section: SettingsSectionId,
    active_section_entity: ActiveSettingsSection,
    active_section_view: AnyView,
    pending_section_confirm: Option<SettingsSectionId>,
    _section_subscription: Option<Subscription>,
}

pub type SettingsWindow = SettingsCoordinator;

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for SettingsCoordinator {}

#[derive(Clone, Debug)]
pub enum SettingsEvent {
    OpenScript { path: std::path::PathBuf },
}

impl EventEmitter<SettingsEvent> for SettingsCoordinator {}
