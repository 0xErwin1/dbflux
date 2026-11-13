use std::time::Duration;

use gpui_mcp_protocol::{Capability, MAX_MESSAGE_PAGE, MessagePage, MessageSender, NewMessage};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;

use super::{
    BridgeResult, GpuiMcp, Json, MAX_WAIT_MS, Operation, Parameters, ToolRouter, Value, json,
    object_output, tool, tool_router,
};
use crate::client::BridgeRegistry;

/// Resource URI of the message log.
pub(crate) const MESSAGES_URI: &str = "gpui://messages";
/// Messages the resource shows.
const RESOURCE_MESSAGES: u64 = 50;
/// How long one subscription poll waits in the bridge.
const SUBSCRIPTION_WAIT_MS: u64 = 25_000;

#[derive(Debug, Deserialize, JsonSchema)]
struct ReadMessagesArgs {
    /// Return messages with an id greater than this; pass the `latest_id` of the
    /// previous read, or 0 for everything retained.
    #[serde(default)]
    since: u64,
    /// `app` (the default: what the person or app sent you), `agent`, or `all`.
    #[serde(default)]
    from: FromFilter,
    /// Maximum messages, from 1 through 128.
    #[serde(default = "default_message_limit")]
    #[schemars(range(min = 1, max = 128))]
    limit: u16,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct WaitMessagesArgs {
    /// Wait for a message with an id greater than this; pass the `latest_id` of
    /// the previous read.
    since: u64,
    /// `app` (the default), `agent`, or `all`.
    #[serde(default)]
    from: FromFilter,
    /// Longest wait in milliseconds, from 1 through 30000; defaults to 25000.
    #[serde(default = "default_message_wait_ms")]
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendMessageArgs {
    /// Message text shown to the person in the application.
    text: String,
    /// Id of the app message this answers.
    reply_to: Option<u64>,
    /// Optional lowercase kind, such as `chat` (the default) or `status`.
    kind: Option<String>,
    /// Optional structured payload for the application.
    data: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FromFilter {
    #[default]
    App,
    Agent,
    All,
}

impl FromFilter {
    const fn sender(self) -> Option<MessageSender> {
        match self {
            Self::App => Some(MessageSender::App),
            Self::Agent => Some(MessageSender::Agent),
            Self::All => None,
        }
    }
}

#[tool_router(router = message_router)]
impl GpuiMcp {
    #[tool(
        description = "Read messages the application posted for you (its chat box, events), oldest first, after the id `since`. Reading marks them read. Remember `latest_id` and pass it as `since` next time"
    )]
    async fn read_messages(
        &self,
        Parameters(args): Parameters<ReadMessagesArgs>,
    ) -> Result<Json<Value>, String> {
        if args.limit == 0 || usize::from(args.limit) > MAX_MESSAGE_PAGE {
            return Err("limit must be between 1 and 128".to_owned());
        }
        let page = self
            .message_page(args.since, args.from.sender(), args.limit, 0, true)
            .await?;
        Ok(page_output(&page, false))
    }

    #[tool(
        description = "Block until the application posts a message after the id `since`, or the timeout passes, then return it like read_messages. Use this to wait for the person's next chat message"
    )]
    async fn wait_for_messages(
        &self,
        Parameters(args): Parameters<WaitMessagesArgs>,
    ) -> Result<Json<Value>, String> {
        if args.timeout_ms == 0 || args.timeout_ms > MAX_WAIT_MS {
            return Err("timeout_ms must be between 1 and 30000".to_owned());
        }
        let page = self
            .message_page(
                args.since,
                args.from.sender(),
                default_message_limit(),
                args.timeout_ms,
                true,
            )
            .await?;
        let timed_out = page.messages.is_empty();
        Ok(page_output(&page, timed_out))
    }

    #[tool(
        description = "Send a message to the application, for example a chat reply to the person. Fails when the application does not read messages, or when it has 64 of yours unread"
    )]
    async fn send_message(
        &self,
        Parameters(args): Parameters<SendMessageArgs>,
    ) -> Result<Json<Value>, String> {
        let client = self.client().await?;
        if !client
            .descriptor()
            .capabilities
            .supports(Capability::Messages)
        {
            return Err(
                "the selected application does not read messages (it has not registered BridgeHandle::on_message)"
                    .to_owned(),
            );
        }
        let message = NewMessage {
            text: args.text,
            kind: args.kind,
            reply_to: args.reply_to,
            data: args.data,
        };
        message.validate().map_err(str::to_owned)?;
        let BridgeResult::Message(message) =
            client.call(Operation::SendMessage { message }).await?
        else {
            return Err("bridge returned the wrong result for a message".to_owned());
        };
        Ok(object_output(json!({ "message": message })))
    }
}

impl GpuiMcp {
    async fn message_page(
        &self,
        after: u64,
        from: Option<MessageSender>,
        limit: u16,
        wait_ms: u64,
        mark_read: bool,
    ) -> Result<MessagePage, String> {
        read_page(
            &self
                .call(Operation::ReadMessages {
                    after,
                    from,
                    limit,
                    wait_ms,
                    mark_read,
                })
                .await?,
        )
    }

    /// The newest messages, as the `gpui://messages` resource shows them.
    pub(crate) async fn messages_resource_text(&self) -> Result<String, String> {
        let latest = self.message_page(u64::MAX, None, 1, 0, false).await?;
        let page = self
            .message_page(
                latest.latest_id.saturating_sub(RESOURCE_MESSAGES),
                None,
                u16::try_from(RESOURCE_MESSAGES).unwrap_or(u16::MAX),
                0,
                true,
            )
            .await?;
        serde_json::to_string_pretty(&json!({
            "latest_id": page.latest_id,
            "unread_from_app": page.unread_from_app,
            "messages": page.messages,
        }))
        .map_err(|_| "could not encode the message log".to_owned())
    }
}

fn read_page(result: &BridgeResult) -> Result<MessagePage, String> {
    match result {
        BridgeResult::Messages(page) => Ok(page.clone()),
        _ => Err("bridge returned the wrong result for messages".to_owned()),
    }
}

fn page_output(page: &MessagePage, timed_out: bool) -> Json<Value> {
    object_output(json!({
        "messages": page.messages,
        "latest_id": page.latest_id,
        "has_more": page.has_more,
        "truncated": page.truncated,
        "unread_from_app": page.unread_from_app,
        "unread_from_agent": page.unread_from_agent,
        "timed_out": timed_out,
    }))
}

fn default_message_limit() -> u16 {
    50
}

fn default_message_wait_ms() -> u64 {
    25_000
}

/// Call `notify` whenever the selected application posts a message, until
/// `cancellation` fires or `notify` fails.
///
/// Each wait is a bounded long poll in the bridge, so this costs nothing while
/// the application is quiet. A restarted or reselected application starts its
/// ids again, which resets the position.
pub(crate) async fn watch_app_messages<F, Fut>(
    registry: BridgeRegistry,
    cancellation: CancellationToken,
    mut notify: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let mut after = None;
    loop {
        let poll = async {
            let client = registry.client().await?;
            let wait_ms = if after.is_some() {
                SUBSCRIPTION_WAIT_MS
            } else {
                0
            };
            let page = read_page(
                &client
                    .call(Operation::ReadMessages {
                        after: after.unwrap_or(u64::MAX),
                        from: Some(MessageSender::App),
                        limit: 1,
                        wait_ms,
                        mark_read: false,
                    })
                    .await?,
            )?;
            Ok::<_, String>(page)
        };
        let page = tokio::select! {
            () = cancellation.cancelled() => return,
            page = poll => page,
        };
        if let Ok(page) = page {
            // Following the newest id also restarts the position when a
            // different application instance, whose ids start over, is selected.
            let previous = after.replace(page.latest_id);
            if !page.messages.is_empty() && previous.is_some() && !notify().await {
                return;
            }
        } else {
            // No application yet, or it went away; look again shortly.
            after = None;
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::message_router()
}
