use gpui::{App, AppContext as _, Context, Entity, Global, Styled, Window, px};
use gpui_component::notification::{Notification, NotificationList, NotificationType};

pub struct ToastManager {
    notification_list: Entity<NotificationList>,
}

impl Global for ToastManager {}

impl ToastManager {
    pub fn init(window: &mut Window, cx: &mut App) {
        let notification_list = cx.new(|cx| NotificationList::new(window, cx));
        cx.set_global(Self { notification_list });
    }

    pub fn notification_list(cx: &App) -> Entity<NotificationList> {
        cx.global::<Self>().notification_list.clone()
    }
}

pub trait ToastExt {
    fn toast_success(&mut self, message: impl Into<String>, window: &mut Window);
    fn toast_warning(&mut self, message: impl Into<String>, window: &mut Window);
    fn toast_error(&mut self, message: impl Into<String>, window: &mut Window);
}

impl<T> ToastExt for Context<'_, T> {
    fn toast_success(&mut self, message: impl Into<String>, window: &mut Window) {
        let list = ToastManager::notification_list(self);
        list.update(self, |list, cx| {
            let notification = Notification::new()
                .title("Success")
                .message(message.into())
                .with_type(NotificationType::Success)
                .border_color(gpui::rgb(0x22C55E))
                .shadow_lg()
                .rounded(px(8.0));
            list.push(notification, window, cx);
        });
    }

    fn toast_warning(&mut self, message: impl Into<String>, window: &mut Window) {
        let list = ToastManager::notification_list(self);
        list.update(self, |list, cx| {
            let notification = Notification::new()
                .title("Warning")
                .message(message.into())
                .with_type(NotificationType::Warning)
                .border_color(gpui::rgb(0xF59E0B))
                .shadow_lg()
                .rounded(px(8.0));
            list.push(notification, window, cx);
        });
    }

    fn toast_error(&mut self, message: impl Into<String>, window: &mut Window) {
        let list = ToastManager::notification_list(self);
        list.update(self, |list, cx| {
            let notification = Notification::new()
                .title("Error")
                .message(message.into())
                .with_type(NotificationType::Error)
                .autohide(false)
                .border_color(gpui::rgb(0xEF4444))
                .shadow_lg()
                .rounded(px(8.0));
            list.push(notification, window, cx);
        });
    }
}
