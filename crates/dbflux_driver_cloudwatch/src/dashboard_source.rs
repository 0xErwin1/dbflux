//! CloudWatch `DashboardSource` implementation.
//!
//! Lists dashboards via `ListDashboards` and fetches a dashboard body via
//! `GetDashboard`. Dashboards are browsed read-only; nothing is persisted.
//!
//! Unit tests stub the AWS client behind the [`CloudWatchApi`] trait so the
//! mapping logic is exercised without live AWS calls.

use async_trait::async_trait;
use aws_sdk_cloudwatch::Client as CloudWatchMetricsClient;
use dbflux_core::{DashboardRef, DashboardSource, DbError, RemoteDashboard};

/// Minimal CloudWatch dashboard API surface used by [`CloudWatchDashboardSource`].
///
/// Exists so unit tests can stub `GetDashboard` / `ListDashboards` without a
/// live AWS client.
#[async_trait]
pub trait CloudWatchApi: Send + Sync {
    /// Fetches the JSON body of a dashboard by name.
    async fn get_dashboard_body(&self, name: &str) -> Result<String, DbError>;

    /// Lists dashboards visible to the configured credentials.
    async fn list_dashboards(&self) -> Result<Vec<DashboardListEntry>, DbError>;
}

/// One entry returned by `list_dashboards`.
#[derive(Debug, Clone)]
pub struct DashboardListEntry {
    pub name: String,
    /// ISO8601 timestamp when known.
    pub last_modified: Option<String>,
}

/// Real implementation of [`CloudWatchApi`] backed by `aws-sdk-cloudwatch`.
pub struct RealCloudWatchDashboardApi {
    client: CloudWatchMetricsClient,
}

impl RealCloudWatchDashboardApi {
    pub fn new(client: CloudWatchMetricsClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CloudWatchApi for RealCloudWatchDashboardApi {
    async fn get_dashboard_body(&self, name: &str) -> Result<String, DbError> {
        let output = self
            .client
            .get_dashboard()
            .dashboard_name(name)
            .send()
            .await
            .map_err(|e| DbError::QueryFailed(format!("GetDashboard failed: {e}").into()))?;

        output.dashboard_body.ok_or_else(|| {
            DbError::Parse(format!(
                "GetDashboard for '{name}' returned no dashboard_body"
            ))
        })
    }

    async fn list_dashboards(&self) -> Result<Vec<DashboardListEntry>, DbError> {
        let mut next_token: Option<String> = None;
        let mut out: Vec<DashboardListEntry> = Vec::new();

        loop {
            let mut req = self.client.list_dashboards();
            if let Some(token) = next_token.as_ref() {
                req = req.next_token(token.clone());
            }

            let resp = req
                .send()
                .await
                .map_err(|e| DbError::QueryFailed(format!("ListDashboards failed: {e}").into()))?;

            if let Some(entries) = resp.dashboard_entries {
                for entry in entries {
                    let Some(name) = entry.dashboard_name else {
                        continue;
                    };
                    let last_modified = entry.last_modified.map(|dt| dt.to_string());
                    out.push(DashboardListEntry {
                        name,
                        last_modified,
                    });
                }
            }

            next_token = resp.next_token;
            if next_token.is_none() {
                break;
            }
        }

        Ok(out)
    }
}

/// Driver-level implementation of [`DashboardSource`].
pub struct CloudWatchDashboardSource {
    api: Box<dyn CloudWatchApi>,
}

impl CloudWatchDashboardSource {
    /// Builds a new source from a boxed [`CloudWatchApi`] implementation.
    pub fn new(api: Box<dyn CloudWatchApi>) -> Self {
        Self { api }
    }
}

#[async_trait]
impl DashboardSource for CloudWatchDashboardSource {
    async fn fetch_dashboard(&self, name: &str) -> Result<RemoteDashboard, DbError> {
        let body_json = self.api.get_dashboard_body(name).await?;

        Ok(RemoteDashboard {
            name: name.to_string(),
            body_json,
            last_modified: None,
        })
    }

    async fn list_dashboards(&self) -> Result<Vec<DashboardRef>, DbError> {
        let entries = self.api.list_dashboards().await?;
        Ok(entries
            .into_iter()
            .map(|e| DashboardRef {
                name: e.name,
                last_modified: e.last_modified,
            })
            .collect())
    }

    fn container_label(&self) -> &str {
        // Distinguish the upstream listing from the local, user-created
        // "Dashboards" folder shown for every connection.
        "CloudWatch Dashboards"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct StubApi {
        body: String,
        list: Vec<DashboardListEntry>,
        fail_get: bool,
        fail_list: bool,
        last_name: Mutex<Option<String>>,
    }

    impl StubApi {
        fn fixed(body: &str) -> Self {
            Self {
                body: body.to_string(),
                list: vec![],
                fail_get: false,
                fail_list: false,
                last_name: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl CloudWatchApi for StubApi {
        async fn get_dashboard_body(&self, name: &str) -> Result<String, DbError> {
            *self.last_name.lock().unwrap() = Some(name.to_string());
            if self.fail_get {
                return Err(DbError::QueryFailed(
                    "simulated GetDashboard failure".into(),
                ));
            }
            Ok(self.body.clone())
        }

        async fn list_dashboards(&self) -> Result<Vec<DashboardListEntry>, DbError> {
            if self.fail_list {
                return Err(DbError::QueryFailed(
                    "simulated ListDashboards failure".into(),
                ));
            }
            Ok(self.list.clone())
        }
    }

    fn body() -> &'static str {
        r#"{
          "widgets": [
            {
              "type": "metric",
              "properties": {
                "metrics": [["AWS/EC2","CPUUtilization","InstanceId","i-1"]],
                "period": 300,
                "stat": "Average",
                "region": "us-east-1"
              }
            }
          ]
        }"#
    }

    #[tokio::test]
    async fn fetch_dashboard_returns_remote_body() {
        let api = Box::new(StubApi::fixed(body()));
        let src = CloudWatchDashboardSource::new(api);

        let remote = src.fetch_dashboard("prod-overview").await.expect("ok");
        assert_eq!(remote.name, "prod-overview");
        assert!(!remote.body_json.is_empty());
    }

    #[tokio::test]
    async fn fetch_dashboard_propagates_api_error_as_db_error() {
        let mut api = StubApi::fixed(body());
        api.fail_get = true;
        let src = CloudWatchDashboardSource::new(Box::new(api));

        let err = src.fetch_dashboard("d").await.unwrap_err();
        assert!(
            matches!(err, DbError::QueryFailed(_)),
            "expected QueryFailed, got {err:?}"
        );
    }

    #[tokio::test]
    async fn list_dashboards_maps_entries_to_dashboard_refs() {
        let mut api = StubApi::fixed("{}");
        api.list = vec![
            DashboardListEntry {
                name: "a".into(),
                last_modified: Some("2026-05-01T00:00:00Z".into()),
            },
            DashboardListEntry {
                name: "b".into(),
                last_modified: None,
            },
        ];
        let src = CloudWatchDashboardSource::new(Box::new(api));

        let refs = src.list_dashboards().await.unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].name, "a");
        assert_eq!(
            refs[0].last_modified.as_deref(),
            Some("2026-05-01T00:00:00Z")
        );
        assert_eq!(refs[1].name, "b");
        assert!(refs[1].last_modified.is_none());
    }

    #[tokio::test]
    async fn list_dashboards_propagates_api_error() {
        let mut api = StubApi::fixed("{}");
        api.fail_list = true;
        let src = CloudWatchDashboardSource::new(Box::new(api));

        let err = src.list_dashboards().await.unwrap_err();
        assert!(matches!(err, DbError::QueryFailed(_)));
    }
}
