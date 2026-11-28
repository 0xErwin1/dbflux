//! Driver cancel dispatch of query tasks.

use super::*;

struct CancelProbeConnection {
    metadata: dbflux_core::DriverMetadata,
    cancels: std::sync::mpsc::Sender<()>,
}

impl CancelProbeConnection {
    fn probe(
        capabilities: dbflux_core::DriverCapabilities,
    ) -> (
        Arc<dyn dbflux_core::Connection>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (cancels, received) = std::sync::mpsc::channel();
        let metadata = DriverMetadataBuilder::new(
            "cancel-probe",
            "Probe DB",
            DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .capabilities(capabilities)
        .build();

        (Arc::new(Self { metadata, cancels }), received)
    }
}

impl dbflux_core::Connection for CancelProbeConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, DbError> {
        Err(DbError::NotSupported("probe".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancels.send(()).map_err(|_| DbError::Cancelled)
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }
}

fn messages() -> CancelMessages {
    CancelMessages {
        unsupported: |driver| format!("{driver} does not support cancelling a running command."),
        not_sent: |driver, error| format!("could not send the cancel to {driver}: {error}"),
    }
}

fn hook_for(connection: Option<Arc<dyn dbflux_core::Connection>>) -> dbflux_core::CancelHook {
    driver_cancel_hook(connection, CancelFailureSink::default(), messages())
}

#[test]
fn query_cancel_hook_dispatches_the_driver_cancel_and_requests_the_stop() {
    let (connection, received) =
        CancelProbeConnection::probe(dbflux_core::DriverCapabilities::QUERY_CANCELLATION);
    let hook = hook_for(Some(connection));

    assert_eq!(hook(), dbflux_core::CancelOutcome::Requested);
    received
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the driver was asked to cancel");
}

#[test]
fn query_cancel_hook_reports_a_driver_without_cancel_support() {
    let (connection, received) =
        CancelProbeConnection::probe(dbflux_core::DriverCapabilities::empty());
    let hook = hook_for(Some(connection));

    assert_eq!(
        hook(),
        dbflux_core::CancelOutcome::NotSupported {
            reason: "Probe DB does not support cancelling a running command.".to_string()
        }
    );
    assert!(
        received
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err(),
        "no cancel may be sent to a driver that cannot cancel"
    );
}

#[test]
fn query_cancel_hook_without_a_connection_only_requests_the_stop() {
    assert_eq!(hook_for(None)(), dbflux_core::CancelOutcome::Requested);
}

#[test]
fn query_cancel_hook_refuses_and_stays_retryable_when_the_cancel_cannot_be_dispatched() {
    let (connection, _received) =
        CancelProbeConnection::probe(dbflux_core::DriverCapabilities::QUERY_CANCELLATION);
    let sink = CancelFailureSink::default();
    let hook = driver_cancel_hook_with(Some(connection), sink.clone(), messages(), |_, _| {
        Err("out of threads".to_string())
    });

    assert_eq!(
        hook(),
        dbflux_core::CancelOutcome::DeliveryFailed {
            reason: "could not send the cancel to Probe DB: out of threads".to_string()
        }
    );
    assert!(sink.lock().expect("sink").is_empty());
}

#[test]
fn a_driver_cancel_that_fails_is_recorded_for_the_foreground_to_report() {
    let (connection, received) =
        CancelProbeConnection::probe(dbflux_core::DriverCapabilities::QUERY_CANCELLATION);
    drop(received);
    let sink = CancelFailureSink::default();
    let hook = driver_cancel_hook(Some(connection), sink.clone(), messages());

    assert_eq!(
        hook(),
        dbflux_core::CancelOutcome::Requested,
        "the request was dispatched; the failure happens on the cancel thread"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let failures = loop {
        let failures = std::mem::take(&mut *sink.lock().expect("sink"));
        if !failures.is_empty() || std::time::Instant::now() > deadline {
            break failures;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    assert_eq!(
        failures,
        vec![CancelDispatchFailure {
            driver: "Probe DB".to_string(),
            error: DbError::Cancelled.to_string(),
        }]
    );
}
