#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::OffRuntimeDrop;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

struct DropProbe(mpsc::Sender<(thread::ThreadId, bool)>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0
            .send((
                thread::current().id(),
                tokio::runtime::Handle::try_current().is_ok(),
            ))
            .expect("receiver is alive");
    }
}

#[test]
fn drops_inline_outside_runtime() {
    let (sender, receiver) = mpsc::channel();
    drop(OffRuntimeDrop::new(DropProbe(sender)));
    assert_eq!(
        receiver.try_recv().unwrap(),
        (thread::current().id(), false)
    );
}

#[tokio::test]
async fn last_shared_owner_drops_off_runtime() {
    let (sender, receiver) = mpsc::channel();
    let owner = Arc::new(OffRuntimeDrop::new(DropProbe(sender)));
    let last_owner = Arc::clone(&owner);
    drop(owner);
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    tokio::spawn(async move { drop(last_owner) }).await.unwrap();
    let (thread_id, in_runtime) = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_ne!(thread_id, thread::current().id());
    assert!(!in_runtime);
}
