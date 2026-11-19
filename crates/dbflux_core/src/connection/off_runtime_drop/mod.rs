use std::mem::{self, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::{sync::mpsc, thread};

/// Owns a blocking resource whose destructor must run outside Tokio.
pub struct OffRuntimeDrop<T: Send + 'static>(ManuallyDrop<T>);

impl<T: Send + 'static> OffRuntimeDrop<T> {
    pub fn new(value: T) -> Self {
        Self(ManuallyDrop::new(value))
    }
}

impl<T: Send + 'static> Deref for OffRuntimeDrop<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Send + 'static> DerefMut for OffRuntimeDrop<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Send + 'static> Drop for OffRuntimeDrop<T> {
    #[expect(
        unsafe_code,
        reason = "`ManuallyDrop` moves the value out exactly once, in its owner's destructor"
    )]
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_err() {
            // SAFETY: `drop` runs once and nothing reads `self.0` afterwards.
            unsafe { ManuallyDrop::drop(&mut self.0) };
            return;
        }
        // SAFETY: `drop` runs once and nothing reads `self.0` afterwards.
        let value = unsafe { ManuallyDrop::take(&mut self.0) };
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
