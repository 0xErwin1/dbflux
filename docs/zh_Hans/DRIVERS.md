# DBFlux 驱动程序

本文档对 DBFlux 随附的各数据库驱动程序做对照概览。各驱动程序的细节，请点击链接查看对应 crate 的 `README.md`。驱动程序内部的架构（trait、注册、`DbDriver`/`Connection` 接缝），参见 [`ARCHITECTURE.md`](../ARCHITECTURE.md) 的 **Driver System** 一节。要动手实现驱动程序的贡献者，建议从[驱动程序开发指南](DRIVER_AUTHORING.md)开始。

## 驱动程序如何抽象

每个驱动程序都会暴露一个 `DriverMetadata` 值（定义在 `crates/dbflux_core/src/driver/capabilities.rs`）。界面不依赖特定驱动程序，完全依据这份元数据来适配。相关字段如下：

- **`DatabaseCategory`** —— 决定视图模型与术语。取值：`Relational`、`Document`、`KeyValue`、`Graph`、`TimeSeries`、`WideColumn`、`LogStream`。（并非每个取值都有已发布的驱动程序。）
- **`QueryLanguage`** —— 决定编辑器模式、占位提示文本与查询解析。取值包括 `Sql`、`MongoQuery`、`RedisCommands`、`Cypher`、`InfluxQuery`、`Flux`、`Cql`、`CloudWatchLogsInsightsQl`、`OpenSearchPpl`、`OpenSearchSql`，脚本语言 `Lua` / `Python` / `Bash`，以及 `Custom(String)`。
- **`DriverCapabilities`** —— 一个 `u64` 位标志集合，声明所支持的功能（事务、分页、Schema、键值操作等）。便捷基值 `RELATIONAL_BASE`、`DOCUMENT_BASE` 和 `KEYVALUE_BASE` 把各类别的通用标志归为一组。

下文列出的能力标志，正是每个驱动程序的 `DriverMetadata` 在代码中实际设置的项，没有任何推断成分。

## 对照表

| 驱动程序 | 类别 | 查询语言 | 主要能力 | 说明 / 限制 |
| --- | --- | --- | --- | --- |
| PostgreSQL | 关系型 | SQL | 关系型基础能力 + Schema、SSH 隧道、SSL、认证、外键、CHECK/唯一约束、自定义类型、`RETURNING`、事务性 DDL、例程、多语句 | 完整的 SQL 驱动程序；例程查看器为只读；除 `CREATE INDEX CONCURRENTLY` 外均为事务性 DDL。 |
| Amazon Redshift | 关系型 | SQL | 多数据库、Schema、视图、SSH 隧道、SSL/客户端证书、认证、查询取消、预编译语句、分页、排序、筛选、CSV/JSON 导出 | 基于 PostgreSQL 通信协议的只读驱动；仅支持单语句；会展示 Redshift 存储提示；不支持写入/DDL、IAM/SSO 或索引。 |
| MySQL | 关系型 | SQL | 关系型基础能力 + SSH 隧道、SSL、认证、外键、CHECK/唯一约束、例程、多语句 | DDL 非事务性；多语句脚本按文本切分并顺序执行；例程列表仅涵盖 FUNCTION/PROCEDURE。 |
| MariaDB | 关系型 | SQL | 与 MySQL 使用同一个 crate，能力也相同 | 以独立的 `mariadb` 元数据注册，复用 MySQL 的实现。 |
| SQLite | 关系型 | SQL | 视图、索引、外键、CHECK/唯一约束、预编译语句、插入/更新/删除、分页、排序、筛选、CSV/JSON 导出、查询取消、事务性 DDL、多语句 | 嵌入式文件驱动程序：没有网络、SSH 隧道或 TLS；没有多 Schema 命名空间。 |
| SQL Server | 关系型 | SQL | 关系型基础能力 + Schema、SSH 隧道、SSL、认证、外键、CHECK/唯一约束、事务性 DDL、例程、多语句 | 基于 `tiberius` 构建；经 SSH 隧道时无法使用命名实例查找；多结果集批处理会把最后一个结果集作为主结果集返回。 |
| MongoDB | 文档型 | MongoQuery | 文档型基础能力 + 聚合、SSH 隧道、索引 | 仅支持 MongoDB shell 风格语法（不支持 SQL）；不支持查询取消；解析器仅覆盖受支持的命令形式。 |
| Redis | 键值 | RedisCommands | 键值基础能力 + 多数据库、TTL、键类型、值大小、重命名、批量获取、流的范围查询/添加/删除、认证、SSH 隧道、SSL | 仅支持 Redis 命令语法（不支持 SQL）；不支持查询取消；URI 模式下无法使用 SSH 隧道。 |
| DynamoDB | 文档型 | Custom("DynamoDB") | 认证、分页、筛选、插入/更新/删除、嵌套文档、数组 | 由 AWS 托管；原生命令信封（`scan`/`query`/`put`/`update`/`delete`）；不支持 PartiQL/事务；不支持查询取消；不支持 `update many+upsert`。 |
| CloudWatch Logs | 日志流 | Sql（元数据默认值） | 认证 | 由 AWS 托管；通过编辑器管理的源上下文执行 Logs Insights QL、OpenSearch PPL 与 OpenSearch SQL；暂不支持查询取消。 |
| InfluxDB | 时序 | InfluxQuery | 认证、多数据库、分页、CSV/JSON 导出 | 同一个 crate 同时支持 v1 与 v2；两者都支持 InfluxQL，仅 v2 支持 Flux；只读（不支持 INSERT/UPDATE/DELETE）；不支持事务。 |
| ClickHouse | 关系型 | SQL | 多数据库、视图、认证、分页、排序、筛选、分组、join、CTE、窗口函数、CSV/JSON 导出 | 走 HTTP(S)，包括 ClickHouse Cloud；DBFlux 的集成以读取为主，不支持结构化变更、DDL、事务、SSH 隧道或查询参数。 |
| Amazon S3 | 对象存储 | Custom("S3") | 认证（profile/SSO 或静态凭据、自定义端点）、存储桶浏览、分页的对象导航、预览、完整 CRUD、预签名 URL | 兼容 S3（Cloudflare R2、MinIO）；不支持分段上传/传输面板，不支持内嵌 PDF 查看器，不支持生命周期/ACL 管理或 S3 Select。 |

## 各驱动程序概要

### PostgreSQL

完整的 SQL 驱动程序，具备 Schema 发现、存储例程（只读查看器）、SSL、SSH 隧道、通过取消令牌实现的查询取消、事务性 DDL，以及 PostgreSQL 专属的代码生成。多语句脚本通过简单查询协议作为一个批次执行。参见 [`crates/dbflux_driver_postgres/README.md`](../crates/dbflux_driver_postgres/README.md)。

### Amazon Redshift

基于 PostgreSQL 通信协议的只读关系型 SQL 驱动程序。它支持 Schema、表、视图与列的元数据探查；SSH 隧道；TLS 与客户端证书；查询取消；以及 Redshift 的分布键/排序键存储提示。它不支持写入或 DDL、IAM/SSO 身份验证、多语句查询或索引。参见 [`crates/dbflux_driver_redshift/README.md`](../crates/dbflux_driver_redshift/README.md)。

### MySQL / MariaDB

同一个 crate 同时实现 MySQL 与 MariaDB。支持 SQL 执行、Schema 发现、通过 `KILL QUERY` 实现的查询取消、代码生成，以及针对函数与存储过程的例程发现。DDL 非事务性，多语句切分基于文本。参见 [`crates/dbflux_driver_mysql/README.md`](../crates/dbflux_driver_mysql/README.md)。

### SQLite

嵌入式、基于文件的驱动程序，具备 Schema 发现、通过中断句柄实现的查询取消、事务性 DDL 与代码生成。没有网络传输、SSH 隧道或 TLS，也没有多 Schema 命名空间。参见 [`crates/dbflux_driver_sqlite/README.md`](../crates/dbflux_driver_sqlite/README.md)。

### SQL Server

基于 `tiberius` TDS 客户端构建。支持 SQL Server / Azure SQL、TLS 模式、命名实例（通过 SQL Browser 解析）、SSH 隧道、按标签页切换数据库，以及多结果集批处理。参见 [`crates/dbflux_driver_mssql/README.md`](../crates/dbflux_driver_mssql/README.md)。

### MongoDB

文档型驱动程序，具备集合浏览、文档 CRUD、MongoDB shell 风格的查询解析、聚合，以及面向文档的 Schema 元数据。不支持 SQL，也无法取消查询。参见 [`crates/dbflux_driver_mongodb/README.md`](../crates/dbflux_driver_mongodb/README.md)。

### Redis

键值驱动程序，覆盖字符串、哈希、列表、集合、有序集合与流，另支持键扫描、TTL 操作、重命名、批量获取，以及多个逻辑数据库。不支持 SQL，且 URI 模式下无法使用 SSH 隧道。参见 [`crates/dbflux_driver_redis/README.md`](../crates/dbflux_driver_redis/README.md)。

### DynamoDB

基于 `aws-sdk-dynamodb` 的 AWS NoSQL 驱动程序，支持区域/配置文件/端点配置。表发现会映射 PK/SK 与 GSI/LSI 元数据；执行时使用原生命令信封（`scan`、`query`、`put`、`update`、`delete`）。不暴露 PartiQL 与 DynamoDB 事务。参见 [`crates/dbflux_driver_dynamodb/README.md`](../crates/dbflux_driver_dynamodb/README.md)。

### CloudWatch Logs

AWS CloudWatch Logs 驱动程序，通过 `StartQuery` 执行查询，时间范围与日志组源上下文由编辑器管理。查询文档可以运行 Logs Insights QL、OpenSearch PPL 与 OpenSearch SQL；Schema 发现会枚举日志组，并把日志流作为事件流子节点暴露出来。它的 `DriverMetadata.query_language` 设为 `Sql`，作为默认编辑器模式，而实际模式按查询文档逐个选择。参见 [`crates/dbflux_driver_cloudwatch/README.md`](../crates/dbflux_driver_cloudwatch/README.md)。

### InfluxDB

时序驱动程序，在同一个 crate 中同时支持 InfluxDB v1 与 v2。InfluxQL 在两个版本上都能运行，Flux 仅在 v2 上运行。查询 API 为只读（没有 INSERT/UPDATE/DELETE，也没有事务），支持可选的默认存储桶/数据库，以及按查询路由存储桶。参见 [`crates/dbflux_driver_influxdb/README.md`](../crates/dbflux_driver_influxdb/README.md)。

### ClickHouse

面向自托管 ClickHouse 与 ClickHouse Cloud 的关系型 SQL 驱动程序，走 HTTP(S)。它可以发现数据库、表、视图、列与引擎元数据，并支持以读取为主的 SQL 工作流，包括分页与可视化 SELECT 生成。在当前的初始范围内，不支持结构化变更、DDL、事务、SSH 隧道与通用查询参数。参见 [`crates/dbflux_driver_clickhouse/README.md`](../crates/dbflux_driver_clickhouse/README.md)。

### Amazon S3

面向 AWS S3 及兼容 S3 的端点（Cloudflare R2、MinIO）的对象存储驱动程序，通过 AWS 配置文件/SSO 或静态凭据进行身份验证，支持端点覆盖与路径风格寻址。连接根节点会打开一个存储桶表；存储桶浏览按层级分页（AWS 控制台风格），并可切换到不分页的树形模式。对象预览原生支持图片，对类文本对象提供可内联编辑并回写的编辑器缓冲区，对 PDF 及其他二进制对象则提供元数据与下载/用外部程序打开；归档存储类别（GLACIER、DEEP_ARCHIVE）会完全跳过内容预览。支持上传、删除、需输入确认的递归前缀/存储桶删除、创建文件夹/存储桶、重命名（先复制后删除），以及预签名 URL。不支持分段上传、传输面板、内嵌 PDF 查看器、生命周期/ACL 管理或 S3 Select。参见 [`crates/dbflux_driver_s3/README.md`](../crates/dbflux_driver_s3/README.md)。

## 外部 RPC 驱动程序

DBFlux 可以加载在进程外运行、通过本地 IPC 通信的驱动程序，它们由 `dbflux_driver_ipc` 实现、通过 `dbflux_driver_host` 承载。这些驱动程序以合成 ID 格式 `rpc:<socket_id>` 注册，并通过网络传输提供自己的 `DriverMetadata`（类别、查询语言、能力），因此界面会像对待内置驱动程序一样对待它们。发现握手、服务生命周期与协议细节，参见 [`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md)。
