//! Test support: the `user_error` tracing events a closure emits, which is
//! what the audit bridge turns into `user_error` audit rows.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const USER_ERROR_TARGET: &str = "dbflux_ui::user_error";

#[derive(Default)]
struct Capture {
    events: Mutex<Vec<BTreeMap<String, String>>>,
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != USER_ERROR_TARGET {
            return;
        }

        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));

        if let Ok(mut events) = self.events.lock() {
            events.push(fields);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Runs `run` and returns its result with the fields of every `user_error`
/// event it emitted on this thread.
pub fn capture_user_error_events<R>(run: impl FnOnce() -> R) -> (R, Vec<BTreeMap<String, String>>) {
    let capture = Arc::new(Capture::default());
    let result = tracing::subscriber::with_default(capture.clone(), run);
    let events = capture
        .events
        .lock()
        .map(|events| events.clone())
        .unwrap_or_default();

    (result, events)
}
