pub mod components;
pub mod dock;
pub mod document;
pub mod icons;
pub mod overlays;
pub mod theme;
pub mod tokens;
pub mod views;
pub mod windows;

/// Extension trait for `anyhow::Result` from async `cx.update()` calls.
///
/// Replaces bare `.ok()` on fallible update calls inside detached tasks,
/// ensuring dropped updates are logged instead of silently discarded.
pub(crate) trait AsyncUpdateResultExt<T> {
    /// Consume the result, logging at debug level if the async context
    /// update failed (typically because the entity was released).
    fn log_if_dropped(self);

    /// Like `.ok().flatten()` but logs the error instead of silently
    /// discarding it. Returns `None` when the update context is gone.
    fn unwrap_or_log_dropped(self) -> T
    where
        T: Default;
}

impl<T> AsyncUpdateResultExt<T> for anyhow::Result<T> {
    #[track_caller]
    fn log_if_dropped(self) {
        if let Err(error) = self {
            log::debug!("Async update dropped (entity released): {:#}", error);
        }
    }

    #[track_caller]
    fn unwrap_or_log_dropped(self) -> T
    where
        T: Default,
    {
        self.unwrap_or_else(|error| {
            log::debug!("Async update dropped (entity released): {:#}", error);
            T::default()
        })
    }
}
