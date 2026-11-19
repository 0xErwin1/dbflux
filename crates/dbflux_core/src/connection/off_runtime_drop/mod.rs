use std::ops::{Deref, DerefMut};
use std::{mem, sync::mpsc, thread};

/// Owns a blocking resource whose destructor must run outside Tokio.
pub struct OffRuntimeDrop<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> OffRuntimeDrop<T> {
    pub fn new(value: T) -> Self {
        Self(Some(value))
    }
}

impl<T: Send + 'static> Deref for OffRuntimeDrop<T> {
    type Target = T;

    #[expect(clippy::expect_used, reason = "only Drop takes the value")]
    fn deref(&self) -> &T {
        self.0.as_ref().expect("value is present until drop")
    }
}

impl<T: Send + 'static> DerefMut for OffRuntimeDrop<T> {
    #[expect(clippy::expect_used, reason = "only Drop takes the value")]
    fn deref_mut(&mut self) -> &mut T {
        self.0.as_mut().expect("value is present until drop")
    }
}

impl<T: Send + 'static> Drop for OffRuntimeDrop<T> {
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let Some(value) = self.0.take() else { return };
        // Capturing the value directly would drop it on this thread if spawn failed.
        let (sender, receiver) = mpsc::channel::<T>();
        if let Err(error) = thread::Builder::new()
            .name("dbflux-connection-close".into())
            .spawn(move || drop(receiver.recv()))
        {
            mem::forget(value);
            log::error!("Leaked connection after failing to spawn its close thread: {error}");
        } else if let Err(error) = sender.send(value) {
            mem::forget(error.0);
            log::error!("Leaked connection because its close thread disconnected before handoff");
        }
    }
}

#[cfg(test)]
mod tests;
