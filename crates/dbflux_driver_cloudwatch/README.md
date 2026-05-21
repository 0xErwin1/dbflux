# dbflux_driver_cloudwatch

AWS CloudWatch Logs driver for DBFlux, built on the [`aws-sdk-cloudwatchlogs`](https://crates.io/crates/aws-sdk-cloudwatchlogs) SDK.

## Features

- Log-streaming driver classified as `DatabaseCategory::LogStream`; `deployment_class` is `CloudManaged`. The only declared capability is `AUTHENTICATION`.
- AWS connection configuration via region, named profile, and optional endpoint override, aligned with the DynamoDB AWS connection flow.
- Query execution through `StartQuery` + polling `GetQueryResults` (poll interval 500 ms, up to 120 attempts), with an editor-managed source context that supplies the target log groups and time range.
- Three query syntaxes selectable from the source-context "Syntax" dropdown:
  - CloudWatch Logs Insights QL (`cwli`, the default) — `QueryLanguage::CloudWatchLogsInsightsQl`.
  - OpenSearch PPL (`ppl`) — `QueryLanguage::OpenSearchPpl`.
  - OpenSearch SQL (`sql`) — `QueryLanguage::OpenSearchSql`.
  These map to the SDK's `Cwli`, `Ppl`, and `Sql` query-language values.
- Source-context spec (`SourceContextSpec`) exposes a "Log groups" target selector and Start/End time-range controls; CWLI and PPL queries pass the selected log groups to `StartQuery` via `set_log_group_names`.
- Schema discovery enumerates log groups (`fetch_log_groups`) as the single logical database (`SchemaLoadingStrategy::SingleDatabase`, default database `logs`).
- Log streams are surfaced as paginated collection children (`collection_children` over `fetch_log_stream_page`) and open as event streams (`CollectionPresentation::EventStream`).
- Event-stream browsing (`browse_event_stream` / `EventStreamTarget`) backed by `FilterLogEvents`, with a default 24-hour browse window and support for filter pattern, stream-name prefix, explicit stream names, and a most-recent toggle.
- Insights column names are classified into semantic `ColumnKind`s (e.g. `@timestamp`, `@ingestionTime` recognized as timestamps) for chart auto-detection.

## Limitations

- Query cancellation is not implemented; `cancel()` returns `NotSupported`.
- OpenSearch SQL mode does not receive external log groups: SQL queries must declare their queried log groups in the SQL text, because the CloudWatch API does not accept external log-group parameters for SQL mode (only CWLI and PPL get `set_log_group_names`).
- Editor syntax highlighting remains generic (`query_language` is reported as `Sql` at the metadata level); mode selection drives execution semantics and completion keywords rather than per-mode highlighting.
- Read-only: no mutation, DDL, transaction, or pagination capabilities are declared (`query`, `mutation`, `ddl`, `transactions`, `limits` are all `None`); `schema_features` is empty.
- No SSL form (TLS handled by the AWS SDK transport).
