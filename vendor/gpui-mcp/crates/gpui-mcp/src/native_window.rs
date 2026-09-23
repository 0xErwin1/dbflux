use gpui::Window;
use raw_window_handle::RawWindowHandle;

pub(crate) fn id(window: &Window) -> Option<u32> {
    let handle = raw_window_handle::HasWindowHandle::window_handle(window)
        .ok()?
        .as_raw();
    match handle {
        RawWindowHandle::Win32(handle) => u32::try_from(handle.hwnd.get()).ok(),
        RawWindowHandle::Xlib(handle) => {
            #[cfg(any(windows, target_pointer_width = "32"))]
            {
                Some(handle.window)
            }
            #[cfg(all(not(windows), target_pointer_width = "64"))]
            {
                u32::try_from(handle.window).ok()
            }
        }
        RawWindowHandle::Xcb(handle) => Some(handle.window.get()),
        #[cfg(target_os = "macos")]
        RawWindowHandle::AppKit(handle) => appkit_window_number(handle),
        _ => None,
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn appkit_window_number(handle: raw_window_handle::AppKitWindowHandle) -> Option<u32> {
    use objc2_app_kit::NSView;

    // SAFETY: `HasWindowHandle` ties the raw handle to `window`'s borrow. AppKit's
    // contract guarantees `ns_view` points to a live NSView for that lifetime,
    // and GPUI window operations run on the macOS main thread.
    let view = unsafe { handle.ns_view.cast::<NSView>().as_ref() };
    let number = view.window()?.windowNumber();
    u32::try_from(number).ok()
}
