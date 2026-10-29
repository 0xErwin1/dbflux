# AWS Secrets

AWS Secrets Manager and Systems Manager Parameter Store, browsed from one connection.

## At a glance

- **Category** — Secret store
- **Query language** — Secret browser (no query language)
- **URI scheme** — `aws-secrets`

AWS secret-store driver for DBFlux, built on the [`aws-sdk-secretsmanager`](https://crates.io/crates/aws-sdk-secretsmanager) and [`aws-sdk-ssm`](https://crates.io/crates/aws-sdk-ssm) SDKs. Gated behind the `aws_secrets` feature flag.

## Features

- Secret-store driver classified as `DatabaseCategory::SecretStore` with the `SECRET_STORE` capability. One connection exposes two stores through the `SecretStoreSource` seam, in this order: **Secrets Manager** (`secrets-manager`: versions, version labels, rotation, soft delete) and **Parameter Store** (`parameter-store`: versions, hierarchical paths, typed values).
- Connects through an AWS profile/SSO `AuthProfileRef` or static access-key credentials (mutually exclusive: a connection with both a profile and an Access Key ID is rejected on save and on connect), or the default AWS credential chain when neither is set, with an optional endpoint override (for example LocalStack).
- Connecting succeeds when either service answers, so credentials scoped to only Secrets Manager or only Parameter Store still connect.
- The region of a live connection is switched from the Secrets Manager and Parameter Store documents, without editing the profile: the connection is replaced in place, the profile saved on disk keeps the region it was saved with, and the open listings reload in the new region. The offered regions are the ones EC2 `DescribeRegions` reports for the connection's own credentials, so a region the account has not enabled is never offered.
  - The switch needs `ec2:DescribeRegions` on the credentials. Without it the documents show the denial with a retry and no picker, and switching stays available only by reconnecting with the region set in the profile.
- Listing never fetches a value. Secrets Manager lists with `ListSecrets`, including secrets scheduled for deletion. Parameter Store lists with `DescribeParameters` only, never `GetParametersByPath`.
- Server-side pagination on both stores through the services' page tokens, with page sizes clamped to the service limits (100 for Secrets Manager, 50 for Parameter Store).
- Name search on the server side: a prefix match on the secret name in Secrets Manager (`Name` filter) and a substring match on the full parameter name in Parameter Store (`Name` filter with `Contains`).
- Parameter paths (`/app/prod/db`) are browsed one level at a time: a level returns the parameters directly under the path and the sub-paths below it. A recursive listing returns every parameter under the path. Names without a leading `/` belong to the root.
- Secret details from `DescribeSecret`: name, ARN, description, KMS key (`aws/secretsmanager` when the secret uses the AWS managed key), created, last changed and last accessed dates, rotation state, schedule, window, function, last and next rotation, version count, staging labels, tags, owning service, and deletion date.
- Parameter details from `DescribeParameters` and `ListTagsForResource`: name, ARN, description, type, tier, data type, KMS key, version, last modified date and user, and tags.
- Value retrieval only on an explicit call: `GetSecretValue` (current version, or a version id) and `GetParameter` with decryption (current version, or a version number). `SecretString` and parameter values are returned as text and `SecretBinary` as binary. Values travel as `secrecy` types and are never logged or written into error messages.
- Secrets owned by another service report their `OwningService`, so the UI can show them as read only.
- Errors are mapped by AWS error code: denied access, including a denied `kms:Decrypt` on a `SecureString` parameter or an encrypted secret (`DecryptionFailure`), is a permission error carrying AWS's message, never an empty value; missing secrets, parameters and parameter versions are not-found errors; throttling is marked retriable. Each error names the store and the secret.
- Client identity: every request carries `dbflux-<version>` as the AWS SDK app name, visible in CloudTrail's `userAgent` field.

## Limitations

- Read only in this release: creating, updating and deleting secrets and parameters, restoring scheduled deletions, editing tags, listing versions, and starting a rotation are not available.
- A connection reaches one region at a time, so replicated secrets are listed in the region the connection is on, and secrets shared from other accounts are not listed. Offering the other regions needs `ec2:DescribeRegions` on the credentials.
- Listing counts and sub-paths cover the pages loaded so far. A Parameter Store level is grouped from the page of parameters below the path, so a page can hold few or no direct entries, and a later page can repeat a sub-path an earlier page returned.
- Secrets Manager reports one deletion timestamp (`DeletedDate`) and not the recovery window, so the deletion date shown for a scheduled deletion is that timestamp as AWS reports it.
- Parameter Store reports no creation date, last access date or version count without reading the parameter history, which returns values, so those details stay empty.
- A version is selected by version id in Secrets Manager and by version number in Parameter Store. Staging labels and parameter labels are not accepted as selectors.
- The LocalStack-backed live integration tests require Docker and run with `cargo test -p dbflux_driver_aws_secrets --test live_integration -- --ignored`. LocalStack ignores `MaxResults` on `DescribeParameters`, so Parameter Store pagination is not covered by them.
