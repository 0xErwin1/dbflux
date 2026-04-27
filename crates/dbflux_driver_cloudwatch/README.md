# dbflux_driver_cloudwatch

## Features

- Built-in CloudWatch Logs driver registration for DBFlux connection profiles.
- AWS region/profile/endpoint form handling aligned with the existing DynamoDB AWS connection flow.
- CloudWatch query execution through `StartQuery` with editor-managed time range and log-group source context.
- CloudWatch query documents can run Logs Insights QL, OpenSearch PPL, and OpenSearch SQL.
- Schema discovery enumerates log groups and exposes log streams as event-stream children.

## Limitations

- Query cancellation is not implemented yet.
- OpenSearch SQL queries must declare their queried log groups in the SQL text because the CloudWatch API does not accept external log-group parameters for SQL mode.
- Editor syntax highlighting remains generic; mode selection currently focuses on execution semantics and completion keywords.
