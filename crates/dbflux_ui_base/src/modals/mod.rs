pub mod add_panel_picker;
pub mod create_dashboard;
pub mod delete_confirm;
pub mod rename_item;

pub use add_panel_picker::{
    AddPanelOutcome, AddPanelRequest, AddPanelTab, ModalAddPanelPicker, RequestMetricsForNamespace,
};
pub use create_dashboard::{CreateDashboardOutcome, CreateDashboardRequest, ModalCreateDashboard};
pub use delete_confirm::{
    DeleteDashboardOutcome, DeleteDashboardRequest, DeleteSavedChartOutcome,
    DeleteSavedChartRequest, ModalDeleteDashboardConfirm, ModalDeleteSavedChartConfirm,
};
pub use rename_item::{ModalRenameItem, RenameItemOutcome, RenameItemRequest, RenameTarget};

/// Hosts one modal in a test window, the way the workspace renders it.
#[cfg(any(test, feature = "test-support"))]
pub mod test_host {
    use gpui::{
        AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
        Modifiers, ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext,
        Window, div, point, px,
    };

    /// A focusable surface with the modal drawn over it.
    pub struct Host<M: Render> {
        outside: FocusHandle,
        modal: Entity<M>,
    }

    impl<M: Render> Render for Host<M> {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .track_focus(&self.outside)
                .child(self.modal.clone())
        }
    }

    /// Opens a window hosting a closed modal built by `build`, with focus on
    /// the surface behind it. Returns the modal, that surface's handle and the
    /// window.
    pub fn host_modal<M: Render>(
        cx: &mut TestAppContext,
        build: impl FnOnce(&mut Window, &mut Context<M>) -> M,
    ) -> (Entity<M>, FocusHandle, &mut VisualTestContext) {
        cx.update(gpui_component::init);

        let (host, window) = cx.add_window_view(|window, cx| Host {
            outside: cx.focus_handle(),
            modal: cx.new(|cx| build(window, cx)),
        });

        let (modal, outside) = window.update(|window, cx| {
            let host = host.read(cx);
            let pair = (host.modal.clone(), host.outside.clone());
            pair.1.focus(window, cx);
            pair
        });
        window.run_until_parked();

        (modal, outside, window)
    }

    /// Clicks the backdrop next to the window's top-left corner, well away
    /// from the centered card.
    pub fn click_backdrop(window: &mut VisualTestContext) {
        window.simulate_click(point(px(2.0), px(2.0)), Modifiers::default());
        window.run_until_parked();
    }

    /// Whether `handle` has focus again.
    pub fn has_focus(window: &mut VisualTestContext, handle: &FocusHandle) -> bool {
        window.update(|window, _| handle.is_focused(window))
    }
}
