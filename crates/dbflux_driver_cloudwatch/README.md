# dbflux_driver_cloudwatch

## Features

- Built-in CloudWatch Logs driver registration for DBFlux connection profiles.
- AWS region/profile/endpoint form handling aligned with the existing DynamoDB AWS connection flow.
- CloudWatch query execution through `StartQuery` with editor-managed time range and log-group source context.
- CloudWatch query documents can run Logs Insights QL, OpenSearch PPL, and OpenSearch SQL.
- Schema discovery enumerates log groups and exposes log streams as event-stream children.
- CloudWatch Metrics via `GetMetricData`: executes a single `MetricDataQuery` per request, maps the response to a two-column (timestamp, value) `QueryResult` ordered ascending by timestamp. Timestamps from AWS (second-precision) are converted to milliseconds. Multi-metric pivot to wide format is supported when multiple `MetricDataResult` entries are returned.

## Limitations

- Query cancellation is not implemented yet.
- OpenSearch SQL queries must declare their queried log groups in the SQL text because the CloudWatch API does not accept external log-group parameters for SQL mode.
- Editor syntax highlighting remains generic; mode selection currently focuses on execution semantics and completion keywords.
- Metrics execution supports a single `MetricDataQuery` per request in this release (W2); a `ListMetrics`-backed picker for namespace/metric/dimension selection is deferred to a follow-up.
- Live integration tests for metrics (`live_execute_cloudwatch_metric`) require real AWS credentials and are `#[ignore]`d by default. LocalStack Community does not support the CloudWatch Metrics API.
