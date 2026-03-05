use crate::ui::components::dropdown::DropdownSelectionChanged;
use dbflux_core::ProxyProfile;
use gpui::*;

use super::ConnectionManagerWindow;

impl ConnectionManagerWindow {
    pub(super) fn handle_proxy_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if let Some(uuid) = self.proxy_uuids.get(event.index).copied() {
            self.pending_proxy_selection = Some(uuid);
            cx.notify();
        }
    }

    pub(super) fn apply_proxy(&mut self, proxy: &ProxyProfile, _cx: &mut Context<Self>) {
        self.selected_proxy_id = Some(proxy.id);
    }

    pub(super) fn clear_proxy_selection(&mut self, cx: &mut Context<Self>) {
        self.selected_proxy_id = None;
        cx.notify();
    }
}
