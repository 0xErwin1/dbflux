use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};

pub(crate) trait FixtureClient {
    async fn call_json(&mut self, tool: &str, arguments: Value) -> Result<Value, String>;
}

/// Endpoint discovery precedes rendering. Require the controls, not their
/// expected states, so readiness does not hide a semantic regression.
pub(crate) async fn wait_for_fixture(
    client: &mut impl FixtureClient,
    ids: &[&str],
    deadline: Duration,
) -> Result<(), String> {
    let mut last = "the fixture has not answered".to_owned();
    let ready = async {
        loop {
            let attempt = async {
                let apps = client.call_json("list_apps", json!({})).await?;
                let count = apps.get("count").and_then(Value::as_u64).unwrap_or(0);
                if count != 1 {
                    return Err(format!(
                        "the endpoint directory published {count} applications"
                    ));
                }
                let tree = client.call_json("get_ui_tree", json!({})).await?;
                let missing = missing_nodes(&tree, ids);
                if missing.is_empty() {
                    Ok(())
                } else {
                    Err(format!("missing controls {missing:?}; last tree: {tree}"))
                }
            }
            .await;
            match attempt {
                Ok(()) => return Ok(()),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(deadline, ready)
        .await
        .unwrap_or_else(|_| {
            Err(format!(
                "fixture was not rendered within {deadline:?}: {last}"
            ))
        })
}

fn missing_nodes<'a>(tree: &Value, ids: &[&'a str]) -> Vec<&'a str> {
    ids.iter()
        .copied()
        .filter(|id| tree.get("nodes").and_then(|nodes| nodes.get(*id)).is_none())
        .collect()
}

/// Include only the bounded tail of fixture stderr in a failed test result.
pub(crate) fn fixture_failure(log: &Path, error: &str) -> String {
    let tail = (|| {
        let mut file = File::open(log)?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(length.saturating_sub(16_384)))?;
        let mut bytes = Vec::new();
        file.take(16_384).read_to_end(&mut bytes)?;
        Ok::<_, std::io::Error>(String::from_utf8_lossy(&bytes).into_owned())
    })()
    .unwrap_or_else(|error| format!("could not read fixture stderr: {error}"));
    format!("{error}\nfixture stderr ({}):\n{tail}", log.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DelayedTree {
        trees: usize,
    }
    impl FixtureClient for DelayedTree {
        async fn call_json(&mut self, tool: &str, _: Value) -> Result<Value, String> {
            if tool == "list_apps" {
                return Ok(json!({ "count": 1 }));
            }
            self.trees += 1;
            Ok(if self.trees < 2 {
                json!({ "nodes": {} })
            } else {
                // Readiness accepts an incorrect enabled state: the actual
                // semantic assertion must remain responsible for rejecting it.
                json!({ "nodes": { "locked": { "state": { "enabled": true } } } })
            })
        }
    }

    #[tokio::test]
    async fn discovery_waits_for_the_first_tree_without_asserting_state() -> Result<(), String> {
        let mut client = DelayedTree { trees: 0 };
        wait_for_fixture(&mut client, &["locked"], Duration::from_secs(2)).await?;
        assert_eq!(client.trees, 2);
        Ok(())
    }

    #[tokio::test]
    async fn missing_controls_fail_with_the_last_tree() {
        let mut client = DelayedTree { trees: 0 };
        let error = wait_for_fixture(&mut client, &["missing"], Duration::from_millis(20)).await;
        assert!(error.is_err_and(|error| error.contains("missing") && error.contains("last tree")));
    }

    struct Unresponsive;
    impl FixtureClient for Unresponsive {
        async fn call_json(&mut self, _: &str, _: Value) -> Result<Value, String> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn the_deadline_also_bounds_an_unresponsive_rpc() {
        assert!(
            wait_for_fixture(&mut Unresponsive, &["locked"], Duration::from_millis(20))
                .await
                .is_err()
        );
    }
}
