//! CloudWatch `DashboardSource` implementation.
//!
//! Fetches dashboards via `GetDashboard` and `ListDashboards` and computes
//! the canonical content hash via `dbflux_core::dashboard_content_hash`.
//!
//! Unit tests stub the AWS client behind the [`CloudWatchApi`] trait so
//! parsing + hash computation are exercised without live AWS calls.

use async_trait::async_trait;
use aws_sdk_cloudwatch::Client as CloudWatchMetricsClient;
use dbflux_core::{
    DashboardRef, DashboardSource, DbError, RemoteDashboard, dashboard_content_hash,
};

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
///
/// Holds a cached `account_id` (best-effort; `None` when STS lookup failed
/// at driver construction) and the `home_region` recorded at connect time.
pub struct CloudWatchDashboardSource {
    api: Box<dyn CloudWatchApi>,
    account_id: Option<String>,
    home_region: String,
}

impl CloudWatchDashboardSource {
    /// Builds a new source from a boxed [`CloudWatchApi`] implementation.
    ///
    /// `account_id` is best-effort: pass `None` when STS resolution failed
    /// so dashboards are marked detached per spec R7.1.
    pub fn new(
        api: Box<dyn CloudWatchApi>,
        account_id: Option<String>,
        home_region: String,
    ) -> Self {
        Self {
            api,
            account_id,
            home_region,
        }
    }
}

#[async_trait]
impl DashboardSource for CloudWatchDashboardSource {
    async fn fetch_dashboard(&self, name: &str) -> Result<RemoteDashboard, DbError> {
        let body_json = self.api.get_dashboard_body(name).await?;
        let content_hash = dashboard_content_hash(&body_json)?;

        Ok(RemoteDashboard {
            name: name.to_string(),
            // Caller treats empty account_id as detached on its own; we
            // still return the empty string here so the value type stays
            // simple. Persistence converts back to NULL via Option.
            account_id: self.account_id.clone().unwrap_or_default(),
            home_region: self.home_region.clone(),
            body_json,
            content_hash,
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

    fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    fn home_region(&self) -> &str {
        &self.home_region
    }
}

/// Resolves the calling account id via STS `GetCallerIdentity`.
///
/// Best-effort per spec R7.1: returns `None` on failure (logged) so import
/// can still succeed and downstream code marks the dashboard detached.
pub async fn resolve_account_id(shared_config: &aws_config::SdkConfig) -> Option<String> {
    let client = aws_sdk_sts::Client::new(shared_config);
    match client.get_caller_identity().send().await {
        Ok(resp) => resp.account,
        Err(e) => {
            log::warn!("STS GetCallerIdentity failed; dashboard marks detached: {e}");
            None
        }
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
    async fn fetch_dashboard_returns_remote_with_v1_hash() {
        let api = Box::new(StubApi::fixed(body()));
        let src =
            CloudWatchDashboardSource::new(api, Some("123456789012".into()), "us-east-1".into());

        let remote = src.fetch_dashboard("prod-overview").await.expect("ok");
        assert_eq!(remote.name, "prod-overview");
        assert_eq!(remote.account_id, "123456789012");
        assert_eq!(remote.home_region, "us-east-1");
        assert!(remote.content_hash.starts_with("v1:"));
        assert!(!remote.body_json.is_empty());
    }

    #[tokio::test]
    async fn fetch_dashboard_hash_is_deterministic_across_calls() {
        let src1 = CloudWatchDashboardSource::new(
            Box::new(StubApi::fixed(body())),
            Some("a".into()),
            "us-east-1".into(),
        );
        let src2 = CloudWatchDashboardSource::new(
            Box::new(StubApi::fixed(body())),
            Some("a".into()),
            "us-east-1".into(),
        );
        let h1 = src1.fetch_dashboard("d").await.unwrap().content_hash;
        let h2 = src2.fetch_dashboard("d").await.unwrap().content_hash;
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn fetch_dashboard_propagates_api_error_as_db_error() {
        let mut api = StubApi::fixed(body());
        api.fail_get = true;
        let src =
            CloudWatchDashboardSource::new(Box::new(api), Some("a".into()), "us-east-1".into());

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
        let src =
            CloudWatchDashboardSource::new(Box::new(api), Some("123".into()), "us-east-1".into());

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
    async fn detached_dashboard_when_account_id_is_none() {
        let src = CloudWatchDashboardSource::new(
            Box::new(StubApi::fixed(body())),
            None,
            "us-east-1".into(),
        );
        assert!(src.account_id().is_none());
        // The RemoteDashboard.account_id falls back to empty string;
        // persistence is responsible for converting to NULL.
        let remote = src.fetch_dashboard("d").await.unwrap();
        assert!(remote.account_id.is_empty());
    }
}
