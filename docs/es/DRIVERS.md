# Drivers de DBFlux

Este documento es una visión comparativa de los drivers de base de datos que se
distribuyen con DBFlux. Para el detalle de cada driver, sigue el enlace al
`README.md` del crate correspondiente. Para la arquitectura interna de los
drivers (traits, registro, la interfaz `DbDriver`/`Connection`), consulta la
sección **Driver System** de [`ARCHITECTURE.md`](../ARCHITECTURE.md). Los
contribuidores que vayan a implementar un driver deben empezar por la [Guía de
autoría de drivers](DRIVER_AUTHORING.md).

## Cómo se abstraen los drivers

Cada driver expone un valor `DriverMetadata` (definido en
`crates/dbflux_core/src/driver/capabilities.rs`). La UI es agnóstica al driver y
se adapta únicamente a partir de estos metadatos. Los campos relevantes son:

- **`DatabaseCategory`** — selecciona el modelo de vista y la terminología.
  Valores: `Relational`, `Document`, `KeyValue`, `Graph`, `TimeSeries`,
  `WideColumn`, `LogStream`. (No todos los valores tienen un driver que los
  implemente.)
- **`QueryLanguage`** — determina el modo del editor, el texto de placeholder y
  el parseo de queries. Incluye `Sql`, `MongoQuery`, `RedisCommands`, `Cypher`,
  `InfluxQuery`, `Flux`, `Cql`, `CloudWatchLogsInsightsQl`, `OpenSearchPpl`,
  `OpenSearchSql`, los lenguajes de script `Lua` / `Python` / `Bash`, y
  `Custom(String)`.
- **`DriverCapabilities`** — un conjunto de bitflags `u64` que declara las
  funcionalidades soportadas (transactions, pagination, schemas, operaciones
  key-value, etc.). Las bases de conveniencia `RELATIONAL_BASE`, `DOCUMENT_BASE`
  y `KEYVALUE_BASE` agrupan los flags comunes de cada categoría.

Los flags de capacidad listados abajo son exactamente los que cada driver fija
en su `DriverMetadata` en código; nada se infiere.

## Comparación

| Driver          | Categoría      | Lenguaje de query          | Capacidades clave                                                                                                                                                                                                  | Notas / limitaciones                                                                                                                                                               |
| --------------- | -------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL      | Relacional     | SQL                        | Base relacional + schemas, túnel SSH, SSL, auth, foreign keys, constraints check/unique, tipos personalizados, `RETURNING`, DDL transaccional, routines, multi-statement                                           | Driver SQL completo; el visor de routines es de solo lectura; DDL transaccional salvo `CREATE INDEX CONCURRENTLY`.                                                                 |
| Amazon Redshift | Relacional     | SQL                        | Múltiples bases de datos, schemas, views, túnel SSH, SSL/certificados de cliente, auth, cancelación de queries, prepared statements, paginación, ordenamiento, filtrado, export CSV/JSON                           | Solo lectura sobre el protocolo wire de PostgreSQL; single-statement; expone hints de almacenamiento de Redshift; sin writes/DDL, IAM/SSO ni índices.                              |
| MySQL           | Relacional     | SQL                        | Base relacional + túnel SSH, SSL, auth, foreign keys, constraints check/unique, routines, multi-statement                                                                                                          | El DDL no es transaccional; los scripts multi-statement se dividen por texto y se ejecutan en secuencia; el listado de routines cubre solo FUNCTION/PROCEDURE.                     |
| MariaDB         | Relacional     | SQL                        | Mismo crate y capacidades que MySQL                                                                                                                                                                                | Registrado como metadatos `mariadb` separados que comparten la implementación de MySQL.                                                                                            |
| SQLite          | Relacional     | SQL                        | Views, índices, foreign keys, constraints check/unique, prepared statements, insert/update/delete, paginación, ordenamiento, filtrado, export CSV/JSON, cancelación de queries, DDL transaccional, multi-statement | Driver de archivo embebido: sin red, túnel SSH ni TLS; sin namespace multi-schema.                                                                                                 |
| SQL Server      | Relacional     | SQL                        | Base relacional + schemas, túnel SSH, SSL, auth, foreign keys, constraints check/unique, DDL transaccional, routines, multi-statement                                                                              | Construido sobre `tiberius`; la búsqueda por instancia nombrada no está disponible a través de túnel SSH; los batches multi-result-set devuelven el último set como primario.      |
| MongoDB         | Documental     | MongoQuery                 | Base document + aggregation, túnel SSH, índices                                                                                                                                                                    | Solo sintaxis estilo shell de MongoDB (sin SQL); sin cancelación de queries; el parser está limitado a los patrones de comando soportados.                                         |
| Redis           | Key-Value      | RedisCommands              | Base key-value + múltiples bases de datos, TTL, tipos de key, tamaño de valor, rename, bulk get, stream range/add/delete, auth, túnel SSH, SSL                                                                     | Solo sintaxis de comandos de Redis (sin SQL); sin cancelación de queries; el túnel SSH no está disponible en modo URI.                                                             |
| DynamoDB        | Documental     | Custom("DynamoDB")         | Auth, paginación, filtrado, insert/update/delete, documentos anidados, arrays                                                                                                                                      | Gestionado por AWS; envelope de comandos nativo (`scan`/`query`/`put`/`update`/`delete`); sin PartiQL/transactions; sin cancelación de queries; `update many+upsert` no soportado. |
| CloudWatch Logs | Log Stream     | Sql (default de metadatos) | Auth                                                                                                                                                                                                               | Gestionado por AWS; ejecuta Logs Insights QL, OpenSearch PPL y OpenSearch SQL vía contexto de fuente gestionado por el editor; aún sin cancelación de queries.                     |
| InfluxDB        | Time Series    | InfluxQuery                | Auth, múltiples bases de datos, paginación, export CSV/JSON                                                                                                                                                        | v1 y v2 en un solo crate; InfluxQL en ambas, Flux solo en v2; solo lectura (sin INSERT/UPDATE/DELETE); sin transactions.                                                           |
| ClickHouse      | Relacional     | SQL                        | Múltiples bases de datos, views, auth, paginación, ordenamiento, filtrado, agrupación, joins, CTEs, windows, export CSV/JSON                                                                                       | HTTP(S), incluyendo ClickHouse Cloud; integración orientada a lectura sin mutaciones estructuradas, DDL, transactions, túnel SSH ni parámetros de query.                           |
| Amazon S3       | Object Storage | Custom("S3")               | Auth (profile/SSO o credenciales estáticas, endpoint personalizado), navegación de buckets, navegación paginada de objetos, preview, CRUD completo, URLs presignadas                                               | Compatible con S3 (Cloudflare R2, MinIO); sin panel de multipart upload/transfers, sin visor de PDF embebido, sin gestión de lifecycle/ACL ni S3 Select.                           |

## Resumen por driver

### PostgreSQL

Driver SQL completo con descubrimiento de schema, routines almacenadas (visor de
solo lectura), SSL, túnel SSH, cancelación de queries vía cancel tokens, DDL
transaccional y generación de código específica de PostgreSQL. Los scripts
multi-statement se ejecutan como batch vía el simple query protocol. Ver
[`crates/dbflux_driver_postgres/README.md`](../crates/dbflux_driver_postgres/README.md).

### Amazon Redshift

Driver SQL relacional de solo lectura que usa el protocolo wire de PostgreSQL.
Soporta introspección de schema, table, view y column; túnel SSH; TLS y
certificados de cliente; cancelación de queries; y hints de
distribución/sort-key de almacenamiento de Redshift. No soporta writes ni DDL,
autenticación IAM/SSO, queries multi-statement ni índices. Ver
[`crates/dbflux_driver_redshift/README.md`](../crates/dbflux_driver_redshift/README.md).

### MySQL / MariaDB

Un solo crate implementa MySQL y MariaDB. Soporta ejecución SQL, descubrimiento
de schema, cancelación de queries vía `KILL QUERY`, generación de código y
descubrimiento de routines para functions y procedures. El DDL no es
transaccional y la división multi-statement se hace por texto. Ver
[`crates/dbflux_driver_mysql/README.md`](../crates/dbflux_driver_mysql/README.md).

### SQLite

Driver embebido basado en archivo con descubrimiento de schema, cancelación de
queries vía interrupt handles, DDL transaccional y generación de código. Sin
transporte de red, túnel SSH ni TLS, y sin namespace multi-schema. Ver
[`crates/dbflux_driver_sqlite/README.md`](../crates/dbflux_driver_sqlite/README.md).

### SQL Server

Construido sobre el cliente TDS `tiberius`. Soporta SQL Server / Azure SQL,
modos TLS, instancias nombradas (resueltas vía SQL Browser), túnel SSH, cambio
de base de datos por pestaña y batches multi-result-set. Ver
[`crates/dbflux_driver_mssql/README.md`](../crates/dbflux_driver_mssql/README.md).

### MongoDB

Driver document con navegación de collections, CRUD de documentos, parseo de
queries estilo shell de MongoDB, aggregation y metadatos de schema orientados a
documentos. SQL no está soportado y la cancelación de queries no está
disponible. Ver
[`crates/dbflux_driver_mongodb/README.md`](../crates/dbflux_driver_mongodb/README.md).

### Redis

Driver key-value que cubre strings, hashes, lists, sets, sorted sets y streams,
además de key scanning, operaciones TTL, rename, bulk get y múltiples bases de
datos lógicas. SQL no está soportado y el túnel SSH no está disponible en modo
URI. Ver
[`crates/dbflux_driver_redis/README.md`](../crates/dbflux_driver_redis/README.md).

### DynamoDB

Driver NoSQL de AWS construido sobre `aws-sdk-dynamodb` con configuración de
region/profile/endpoint. El descubrimiento de tables mapea metadatos de PK/SK y
GSI/LSI; la ejecución usa un envelope de comandos nativo (`scan`, `query`,
`put`, `update`, `delete`). PartiQL y las transactions de DynamoDB no se
exponen. Ver
[`crates/dbflux_driver_dynamodb/README.md`](../crates/dbflux_driver_dynamodb/README.md).

### CloudWatch Logs

Driver de AWS CloudWatch Logs que ejecuta queries a través de `StartQuery` con
time range y contexto de fuente de log-group gestionados por el editor. Los
documentos de query pueden ejecutar Logs Insights QL, OpenSearch PPL y
OpenSearch SQL; el descubrimiento de schema enumera log groups y expone los log
streams como hijos de event-stream. Su `DriverMetadata.query_language` se fija
en `Sql` como modo de editor por defecto, mientras que el modo real se elige por
documento de query. Ver
[`crates/dbflux_driver_cloudwatch/README.md`](../crates/dbflux_driver_cloudwatch/README.md).

### InfluxDB

Driver time-series que soporta InfluxDB v1 y v2 en un solo crate. InfluxQL se
ejecuta en ambas versiones; Flux solo en v2. La API de queries es de solo
lectura (sin INSERT/UPDATE/DELETE, sin transactions), con bucket/database por
defecto opcional y routing de bucket por query. Ver
[`crates/dbflux_driver_influxdb/README.md`](../crates/dbflux_driver_influxdb/README.md).

### ClickHouse

Driver SQL relacional para ClickHouse autoalojado y ClickHouse Cloud vía
HTTP(S). Descubre databases, tables, views, columns y metadatos de engine, y
soporta flujos SQL orientados a lectura con paginación y generación visual de
SELECT. Las mutaciones estructuradas, el DDL, las transactions, el túnel SSH y
los parámetros de query genéricos no están soportados en este alcance inicial.
Ver
[`crates/dbflux_driver_clickhouse/README.md`](../crates/dbflux_driver_clickhouse/README.md).

### Amazon S3

Driver de almacenamiento de objetos para AWS S3 y endpoints compatibles con S3
(Cloudflare R2, MinIO), autenticando vía profile/SSO de AWS o credenciales
estáticas con override de endpoint y direccionamiento path-style. La raíz de la
conexión abre una tabla de buckets; la navegación de buckets pagina por nivel
(estilo consola de AWS) con un modo de árbol no paginado opcional. El preview de
objetos cubre imágenes de forma nativa, objetos de tipo texto en un buffer
editable inline con save-back, y metadatos más download/abrir-externamente para
PDF y otros objetos binarios; las clases de almacenamiento archivadas (GLACIER,
DEEP_ARCHIVE) omiten por completo el preview del cuerpo. Soporta upload, delete,
borrado recursivo de prefix/bucket con confirmación por escritura, creación de
folder/bucket, rename (copy-then-delete) y URLs presignadas. No soporta
multipart upload, un panel de transfers, un visor de PDF embebido, gestión de
lifecycle/ACL ni S3 Select. Ver
[`crates/dbflux_driver_s3/README.md`](../crates/dbflux_driver_s3/README.md).

## Drivers RPC externos

DBFlux puede cargar drivers que se ejecutan fuera de proceso y se comunican por
IPC local, implementados a través de `dbflux_driver_ipc` y alojados vía
`dbflux_driver_host`. Estos drivers se registran con el formato de ID sintético
`rpc:<socket_id>` y proveen su propio `DriverMetadata` (category, query
language, capabilities) por el wire, de modo que la UI los trata exactamente
igual que a los drivers integrados. Para el handshake de descubrimiento, el
ciclo de vida del servicio y los detalles del protocolo, ver
[`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md).
