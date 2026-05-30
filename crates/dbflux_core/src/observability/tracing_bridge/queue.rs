use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::observability::source::EventSink;
use crate::observability::types::EventRecord;

const DRAIN_RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Holds the sender side of the bounded audit event channel.
#[allow(dead_code)]
pub(crate) struct BridgeQueue {
    pub(crate) sender: Arc<SyncSender<EventRecord>>,
    pub(crate) drop_counter: Arc<AtomicU64>,
    pub(crate) in_flight: Arc<AtomicUsize>,
}

impl BridgeQueue {
    /// Creates a new bounded queue with the given capacity.
    pub(crate) fn new(capacity: usize) -> (Self, Receiver<EventRecord>) {
        let (tx, rx) = sync_channel(capacity);
        let queue = BridgeQueue {
            sender: Arc::new(tx),
            drop_counter: Arc::new(AtomicU64::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
        };
        (queue, rx)
    }
}

/// Spawns the background drain thread.
///
/// The drain thread pulls `EventRecord`s from `rx` and forwards them to the
/// audit sink once installed. Events arriving before the sink is installed
/// are dropped (counted). The thread exits when `stop` is true and the
/// channel is empty.
pub(crate) fn spawn_drain_thread(
    rx: Receiver<EventRecord>,
    sink_slot: Arc<std::sync::OnceLock<Arc<dyn EventSink>>>,
    drop_counter: Arc<AtomicU64>,
    in_flight: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("dbflux-audit-drain".to_string())
        .spawn(move || {
            drain_loop(rx, sink_slot, drop_counter, in_flight, stop);
        })
        .expect("failed to spawn audit drain thread")
}

fn drain_loop(
    rx: Receiver<EventRecord>,
    sink_slot: Arc<std::sync::OnceLock<Arc<dyn EventSink>>>,
    drop_counter: Arc<AtomicU64>,
    in_flight: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) {
    loop {
        match rx.recv_timeout(DRAIN_RECV_TIMEOUT) {
            Ok(record) => {
                in_flight.fetch_sub(1, Ordering::Relaxed);

                match sink_slot.get() {
                    Some(sink) => {
                        let _ = sink.record(record);
                    }
                    None => {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::source::{EventSink, EventSinkError};
    use crate::observability::types::{EventCategory, EventOutcome, EventRecord, EventSeverity};
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn minimal_record() -> EventRecord {
        let now_ms = 1_000_000i64;
        EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::System,
            EventOutcome::Success,
        )
        .with_action("log_event")
        .with_summary("test event")
    }

    #[derive(Clone)]
    struct CountingSink {
        count: Arc<AtomicUsize>,
    }

    impl EventSink for CountingSink {
        fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(event)
        }
    }

    #[test]
    fn overflow_increments_drop_counter() {
        let capacity = 4;
        let (queue, rx) = BridgeQueue::new(capacity);

        let stop = Arc::new(AtomicBool::new(false));
        let sink_slot: Arc<OnceLock<Arc<dyn EventSink>>> = Arc::new(OnceLock::new());

        let handle = spawn_drain_thread(
            rx,
            sink_slot.clone(),
            queue.drop_counter.clone(),
            queue.in_flight.clone(),
            stop.clone(),
        );

        // Immediately stop the drain so the queue fills up
        stop.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(200));

        let mut sent = 0usize;
        for _ in 0..600 {
            if queue.sender.try_send(minimal_record()).is_ok() {
                queue.in_flight.fetch_add(1, Ordering::Relaxed);
                sent += 1;
            } else {
                queue.drop_counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = queue.drop_counter.load(Ordering::Relaxed);
        assert!(
            dropped >= 596,
            "expected at least 596 drops for capacity-4 queue with 600 sends, got {dropped}"
        );

        drop(queue.sender);
        let _ = handle.join();
        let _ = sent;
    }

    #[test]
    fn drain_thread_exits_on_stop() {
        let (queue, rx) = BridgeQueue::new(64);
        let stop = Arc::new(AtomicBool::new(false));
        let sink_slot: Arc<OnceLock<Arc<dyn EventSink>>> = Arc::new(OnceLock::new());

        let handle = spawn_drain_thread(
            rx,
            sink_slot,
            queue.drop_counter.clone(),
            queue.in_flight.clone(),
            stop.clone(),
        );

        stop.store(true, Ordering::Relaxed);
        drop(queue.sender);

        let joined = {
            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            loop {
                if handle.is_finished() {
                    break true;
                }
                if std::time::Instant::now() > deadline {
                    break false;
                }
                thread::sleep(Duration::from_millis(20));
            }
        };

        assert!(joined, "drain thread did not exit within 500ms");
    }

    #[test]
    fn drain_delivers_to_sink() {
        let (queue, rx) = BridgeQueue::new(64);
        let stop = Arc::new(AtomicBool::new(false));
        let sink_slot: Arc<OnceLock<Arc<dyn EventSink>>> = Arc::new(OnceLock::new());

        let received = Arc::new(AtomicUsize::new(0));
        let sink = CountingSink {
            count: received.clone(),
        };
        sink_slot.set(Arc::new(sink)).ok();

        let handle = spawn_drain_thread(
            rx,
            sink_slot,
            queue.drop_counter.clone(),
            queue.in_flight.clone(),
            stop.clone(),
        );

        for _ in 0..5 {
            if queue.sender.try_send(minimal_record()).is_ok() {
                queue.in_flight.fetch_add(1, Ordering::Relaxed);
            }
        }

        thread::sleep(Duration::from_millis(300));

        stop.store(true, Ordering::Relaxed);
        drop(queue.sender);
        let _ = handle.join();

        assert_eq!(
            received.load(Ordering::Relaxed),
            5,
            "sink should have received 5 events"
        );
    }
}
