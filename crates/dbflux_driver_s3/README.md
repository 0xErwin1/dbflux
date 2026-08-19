# Amazon S3

AWS S3 and S3-compatible object storage, including Cloudflare R2 and MinIO.

## At a glance

- **Category** — Object storage
- **Query language** — Object browser (no query language)
- **URI scheme** — `s3`

AWS S3 and S3-compatible object-storage driver for DBFlux, built on the [`aws-sdk-s3`](https://crates.io/crates/aws-sdk-s3) SDK. Gated behind the `s3` feature flag.

## Features

- Object-storage driver classified as `DatabaseCategory::ObjectStorage`, connecting to AWS S3 and to S3-compatible endpoints (Cloudflare R2, MinIO) via either an AWS profile/SSO `AuthProfileRef` or static access-key credentials, with endpoint override and path-style addressing.
- Bucket browsing: a dedicated buckets table (name, region, object count, size, versioning, created) with search, refresh, and bucket creation; the sidebar lists buckets flat, one level deep.
- Paginated per-level object navigation by default (AWS-console style, using the driver-returned continuation token), with an optional tree-mode toggle for non-paginated full expansion.
- Split tree/preview layout in the object browser: object rows show key, size, storage class, and last-modified; directory rows show child object counts.
- Preview matrix: images render natively via `img()`; text-like objects (txt, md, json, csv, log, ...) open in an inline editable buffer with a dirty badge, Ctrl+S save-back (`put_object`), Discard, and an unsaved-edits confirmation on navigate-away; PDF and other binary objects fall back to metadata plus download/open-externally, with no in-app PDF rendering.
- Full object metadata (key, size, content-type, last modified, ETag, storage class, encryption, versions where available), with storage-class styling distinguishing STANDARD/STANDARD_IA/GLACIER and archived objects degrading to metadata-only (no body fetch attempted).
- Configurable preview size limit, checked via a HEAD request before any object body is fetched.
- CRUD: single-object delete, recursive prefix/bucket delete (type-to-confirm danger modal, batched `DeleteObjects` up to 1000 keys/request), folder creation (zero-byte prefix marker), bucket creation (name validation, region, versioning/block-public-access/object-lock, default encryption with graceful degradation on unsupported endpoints), simple streaming upload (`put_object`/`ByteStream`, no multipart), and object rename (copy-then-delete, bound to `r`, never `F2`).
- Presigned URLs: GET/PUT method choice, 15-minute/1-hour/12-hour/7-day expiry, copy-URL action, and warning text naming the expiry and signing identity.
- Every CRUD/mutation operation (upload, delete, recursive delete, folder/bucket create, bucket delete, save-back edit, rename, presign) is audited under the object-storage `EventCategory`, with credentials and presigned URLs never logged or persisted.
- Permission and not-found errors (`AccessDenied`, `NoSuchBucket`, `NoSuchKey`) are formatted with the affected bucket/key named in the message, not just AWS's generic error text.

## Limitations

- No multipart upload and no dedicated Transfers panel — uploads always go through a single streaming `put_object` call, regardless of file size.
- No embedded PDF viewer; PDF and other non-image/text objects only offer metadata, download, and open-externally.
- No lifecycle rule or ACL management, and no S3 Select.
- Rename is copy-then-delete with no rollback: if the delete after a successful copy fails, both the source and destination keys are left in place and the user must retry the delete manually.
- `delete_bucket` only succeeds on an empty bucket; deleting a non-empty bucket is rejected client-side with a message pointing at the recursive prefix/bucket delete flow instead.
- Preview and metadata display are HEAD-gated: an object's size and storage class are always fetched via `head_object` before any body preview is attempted.
- MinIO-backed live integration tests require Docker and run with `cargo nextest run -p dbflux_driver_s3 --run-ignored all`.
