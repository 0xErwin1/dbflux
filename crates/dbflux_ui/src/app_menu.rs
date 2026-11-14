//! The application menu bar.
//!
//! macOS draws the menu bar of the focused application along the top of the
//! screen and expects the first menu, named after the application, to carry
//! the items users and the system reach for: About, Services, Hide and Quit.
//! gpui builds that bar only from what the application passes to
//! [`gpui::App::set_menus`], so without this module the menu bar shows the
//! application name over an empty menu: clicking it does nothing, and neither
//! `Cmd+Q` nor `Cmd+H` exists.
//!
//! The module centralizes no application commands. Quit carries the action the
//! keymap binds for [`Command::Quit`], so the chord the menu shows is the
//! effective one, and the command still runs through the workspace, which asks
//! about a running query first. Only the items the system provides or expects
//! (Services, Hide, the window list) are declared here.
//!
//! The bar is built once, when the application starts, because that is when
//! gpui reads the keymap for each item's chord and the i18n locale for its
//! label. A rebinding or a language change reaches the bar on the next start;
//! the commands themselves follow the change immediately, and the Quit item
//! keeps the chord it was built with, which is macOS's own quit accelerator.
//!
//! Every platform compiles this module — a menu built where it is not drawn
//! cannot rot — but only macOS installs it. The other platforms store a menu
//! without showing it, and do not implement every visibility action.

use crate::keymap::{Command, ContextId};
use dbflux_core::ReleaseChannel;
use dbflux_i18n::t;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::{self, RunCommand};
use dbflux_ui_windows::settings::{SettingsSectionId, open_or_focus_settings};
use gpui::{App, Entity, KeyBinding, Menu, MenuItem, SharedString, SystemMenuType, actions};

actions!(app_menu, [About, Hide, HideOthers, ShowAll]);

/// Installs the application menu bar and the actions its items run.
///
/// `app_state` is what the About item opens the Settings window with.
pub fn install(cx: &mut App, app_state: Entity<AppStateEntity>) {
    if !cfg!(target_os = "macos") {
        return;
    }

    // About opens the section that already shows the version, the license and
    // the third-party notices.
    cx.on_action({
        let app_state = app_state.clone();

        move |_: &About, cx| {
            open_or_focus_settings(
                app_state.clone(),
                Some(SettingsSectionId::About),
                cx,
                |_, _| {},
            );
        }
    });

    // Hiding and unhiding the application is the system's own: AppKit hides
    // and restores the windows, and Show All brings back the applications the
    // user hid.
    cx.on_action(|_: &Hide, cx| cx.hide());
    cx.on_action(|_: &HideOthers, cx| cx.hide_other_apps());
    cx.on_action(|_: &ShowAll, cx| cx.unhide_other_apps());

    // The chords macOS draws beside the visibility items are the system's own
    // accelerators rather than in-app shortcuts, so they are bound here and
    // not in the keymap model. Bound before the bar is built: every item reads
    // its chord from the keymap.
    cx.bind_keys([
        KeyBinding::new("cmd-h", Hide, None),
        KeyBinding::new("cmd-alt-h", HideOthers, None),
    ]);

    cx.set_menus(menu_bar());
}

fn menu_bar() -> Vec<Menu> {
    let app_name: SharedString = ReleaseChannel::current().display_name().into();

    vec![
        Menu::new(app_name.clone()).items([
            MenuItem::action(t!("app_menu.about", app = app_name.clone()), About),
            MenuItem::separator(),
            // Filled by the system with the services other applications
            // publish for the focused selection.
            MenuItem::os_submenu(t!("app_menu.services"), SystemMenuType::Services),
            MenuItem::separator(),
            MenuItem::action(t!("app_menu.hide", app = app_name.clone()), Hide),
            MenuItem::action(t!("app_menu.hide_others"), HideOthers),
            MenuItem::action(t!("app_menu.show_all"), ShowAll),
            MenuItem::separator(),
            MenuItem::action(t!("app_menu.quit", app = app_name.clone()), quit_action()),
        ]),
        // Declared empty on purpose: a menu named `Window` is the one AppKit
        // fills with Minimize, Zoom, Enter Full Screen and the list of open
        // windows.
        Menu::new("Window"),
    ]
}

/// The action the Quit item runs: the one the keymap binds for the quit
/// command, so the chord the item shows is the effective one and a user
/// rebinding moves it. Falls back to the command's default action when the
/// user removed the binding, which still quits from the menu.
fn quit_action() -> RunCommand {
    keymap::run_command_for(ContextId::Global, Command::Quit)
        .unwrap_or_else(|| RunCommand::new(Command::Quit.action_id()))
}

#[cfg(test)]
mod tests {
    use super::{Command, MenuItem, SystemMenuType, menu_bar};
    use dbflux_core::ReleaseChannel;
    use dbflux_ui_base::keymap::RunCommand;

    /// The bar declares the items macOS expects: an application menu carrying
    /// the system's Services submenu, and a window menu AppKit fills itself.
    /// The Quit item carries the quit command, which is what gives it its
    /// chord.
    #[test]
    fn the_bar_declares_the_application_and_window_menus() {
        let menus = menu_bar();

        assert_eq!(menus.len(), 2, "the application menu and the window menu");
        assert_eq!(
            menus[0].name.as_ref(),
            ReleaseChannel::current().display_name()
        );
        assert_eq!(menus[1].name.as_ref(), "Window");

        assert!(
            menus[0].items.iter().any(|item| matches!(
                item,
                MenuItem::SystemMenu(services) if services.menu_type == SystemMenuType::Services
            )),
            "the application menu must declare the Services submenu"
        );

        let Some(MenuItem::Action { action, .. }) = menus[0].items.last() else {
            panic!("the last item of the application menu must be Quit");
        };
        let quit = action
            .as_any()
            .downcast_ref::<RunCommand>()
            .expect("the quit item runs the quit command");

        assert_eq!(quit.command.as_ref(), Command::Quit.action_id().as_ref());
    }

    /// Every label resolves, so the bar never shows a raw key, and the labels
    /// that name the application carry its release channel's name.
    #[test]
    fn every_menu_label_resolves_with_the_application_name() {
        let app_name = ReleaseChannel::current().display_name();

        for key in [
            "app_menu.about",
            "app_menu.services",
            "app_menu.hide",
            "app_menu.hide_others",
            "app_menu.show_all",
            "app_menu.quit",
        ] {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale, app = app_name);

                assert!(!value.is_empty(), "key {key} resolved empty for {locale}");
                assert_ne!(value, key, "key {key} did not resolve for {locale}");
                assert!(
                    !value.contains("{app}"),
                    "key {key} left its placeholder unfilled for {locale}: {value}"
                );
            }
        }

        for key in ["app_menu.about", "app_menu.hide", "app_menu.quit"] {
            let value = dbflux_i18n::t!(key, locale = "en", app = app_name);

            assert!(
                value.contains(app_name),
                "key {key} must name the application: {value}"
            );
        }
    }
}
