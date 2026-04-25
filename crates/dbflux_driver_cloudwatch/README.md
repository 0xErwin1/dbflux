# dbflux_driver_cloudwatch

## Features

- Built-in CloudWatch Logs driver registration for DBFlux connection profiles.
- AWS region/profile/endpoint form handling aligned with the existing DynamoDB AWS connection flow.
- SQL-mode editor metadata so CloudWatch query documents can reuse the existing code document pipeline.

## Limitations

- Connection establishment and query execution are not implemented in this batch.
- Schema discovery does not yet enumerate CloudWatch log groups.
- Session-aware source selection and execution-context consumption are implemented in later tasks, not this foundation batch.
