// Explicit imports rather than the parent glob: combining `use super::*`
// with `#[gpui::test]` sends the gpui_macros expansion into unbounded
// recursion.
use super::SettingsCoordinator;
use crate::settings::{ActiveSettingsSection, SettingsSectionId};
use dbflux_core::ThemeSetting;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;

fn open_settings(cx: &mut TestAppContext) -> (Entity<SettingsCoordinator>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let slot: Rc<RefCell<Option<Entity<SettingsCoordinator>>>> = Rc::default();
    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let settings = cx.new(|cx| {
                SettingsCoordinator::new_with_section(
                    app_state,
                    SettingsSectionId::General,
                    window,
                    cx,
                )
            });
            slot.replace(Some(settings.clone()));
            gpui_component::Root::new(settings, window, cx)
        }
    });
    window.run_until_parked();

    let settings = slot.borrow().clone().expect("the settings window is built");
    (settings, window)
}

/// Stages an unsaved theme change so the section reports dirty.
fn stage_unsaved_theme_change(
    settings: &Entity<SettingsCoordinator>,
    window: &mut VisualTestContext,
) {
    window.update(|_, cx| {
        let section = match &settings.read(cx).active_section_entity {
            ActiveSettingsSection::General(section) => section.clone(),
            _ => unreachable!("the general section is open"),
        };
        section.update(cx, |section, _| {
            section.gen_settings.theme = ThemeSetting::Light;
        });
    });
}

fn active_section(
    window: &mut VisualTestContext,
    settings: &Entity<SettingsCoordinator>,
) -> SettingsSectionId {
    window.update(|_, cx| settings.read(cx).active_section)
}

#[gpui::test]
fn a_dirty_section_switch_confirms_through_the_shared_modal(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx);
    stage_unsaved_theme_change(&settings, window);

    // The dirty section opens the shared confirmation instead of
    // switching, and the confirmation takes focus.
    window.update(|window, cx| {
        settings.update(cx, |settings, cx| {
            settings.request_section_transition(SettingsSectionId::About, window, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        active_section(window, &settings),
        SettingsSectionId::General,
        "a dirty section does not switch before the confirmation"
    );
    assert!(
        window.update(|_, cx| settings.read(cx).section_confirm.read(cx).is_visible()),
        "the discard confirmation opens"
    );
    assert!(
        window.update(|window, cx| settings
            .read(cx)
            .section_confirm
            .read(cx)
            .contains_focus(window, cx)),
        "opening the confirmation moves focus into it"
    );

    // Cancelling keeps the section and closes the confirmation.
    window.update(|_, cx| {
        settings.update(cx, |settings, cx| {
            settings
                .section_confirm
                .update(cx, |confirm, cx| confirm.cancel(cx));
        });
    });
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| settings.read(cx).section_confirm.read(cx).is_visible()),
        "cancelling closes the confirmation"
    );
    assert_eq!(
        active_section(window, &settings),
        SettingsSectionId::General,
        "cancelling keeps the current section"
    );
    assert!(
        window.update(|_, cx| settings.read(cx).pending_section_confirm.is_none()),
        "cancelling clears the pending switch"
    );

    // Confirming switches to the requested section.
    window.update(|window, cx| {
        settings.update(cx, |settings, cx| {
            settings.request_section_transition(SettingsSectionId::About, window, cx);
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        settings.update(cx, |settings, cx| {
            settings
                .section_confirm
                .update(cx, |confirm, cx| confirm.confirm(cx));
        });
    });
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| settings.read(cx).section_confirm.read(cx).is_visible()),
        "confirming closes the confirmation"
    );
    assert_eq!(
        active_section(window, &settings),
        SettingsSectionId::About,
        "confirming switches to the requested section"
    );
}
