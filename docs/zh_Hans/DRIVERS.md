# DBFlux 驱动程序

本文档对 DBFlux 随附的各数据库驱动程序做对照概览。各驱动程序的细节，请点击链接查看对应 crate 的 `README.md`。驱动程序内部的架构（trait、注册、`DbDriver`/`Connection` 接缝），参见 [`ARCHITECTURE.md`](../ARCHITECTURE.md) 的 **Driver System** 一节。要动手实现驱动程序的贡献者，建议从[驱动程序开发指南](DRIVER_AUTHORING.md)开始。

## 驱动程序如何抽象

每个驱动程序都会暴露一个 `DriverMetadata` 值（定义在 `crates/dbflux_core/src/driver/capabilities.rs`）。界面不依赖特定驱动程序，完全依据这份元数据来适配。相关字段如下：

- **`DatabaseCategory`** —— 决定视图模型与术语。取值：`Relational`、`Document`、`KeyValue`、`TimeSeries`、`LogStream`、`ObjectStorage`、`SecretStore`。新类别必须与声明它的驱动程序一同加入。
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
| TursoDB | 关系型 | SQL | 认证令牌、视图、索引、外键、CHECK/唯一约束、预编译语句、插入/更新/删除、分页、排序、筛选、CSV/JSON 导出、事务、事务性 DDL、多语句 | 走 HTTP 的远程 Turso / libSQL（`libsql://`，本地 `sqld` 用 `http://`）；交互式事务在每个文档独立的服务端流上运行；不支持查询取消、SSH 隧道、副本或切换数据库。 |
| DuckDB | 关系型 | SQL | 多数据库、schema、视图、索引、外键、CHECK/唯一约束、预编译语句、带 RETURNING 的插入/更新/删除、分页、排序、筛选、CSV/JSON 导出、事务、事务性 DDL、查询取消、行数限制、只读强制、多语句 | 嵌入式文件或内存数据库；连接时挂载 DuckLake 目录；同一文件只能由一个进程以写模式打开；不支持语句超时。 |
| Amazon S3 | 对象存储 | Custom("S3") | 认证（profile/SSO 或静态凭据、自定义端点）、存储桶浏览、分页的对象导航、预览、完整 CRUD、预签名 URL | 兼容 S3（Cloudflare R2、MinIO）；不支持分段上传/传输面板，不支持内嵌 PDF 查看器，不支持生命周期/ACL 管理或 S3 Select。 |
| AWS Secrets | 密钥存储 | Custom("AWS Secrets") | 认证（profile/SSO 或静态凭据、自定义端点）、一个连接内的 Secrets Manager 与 Parameter Store、支持按名称搜索的分页列表、参数路径层级、详情、显式读取值 | 可编辑值并保存为新版本，可浏览版本历史，并可在 UI 中创建、回滚、删除、恢复、轮换、打标签和修改描述，均经由受管控的写入路径；每个连接一个区域；LocalStack 不遵循 Parameter Store 的分页大小。 |

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

### TursoDB

面向 Turso Cloud 与自托管 `sqld` 的关系型 SQL 驱动程序，走 HTTP，基于 `turso_serverless` SDK。它使用 SQLite 方言，通过 `sqlite_master` 与 PRAGMA 发现表、视图、列、索引、外键与约束，并支持类型化 CRUD、绑定参数、批量脚本，以及在每个文档独立的服务端流上运行的交互式事务（编辑器每次运行只能执行一条事务控制语句）。不支持查询取消、保存点、SSH 隧道、嵌入式副本与切换数据库。参见 [`crates/dbflux_driver_turso/README.md`](../crates/dbflux_driver_turso/README.md)。

### DuckDB

基于内置 DuckDB 引擎的嵌入式分析型 SQL 驱动程序。配置可以打开一个数据库文件；路径留空时打开内存数据库。它还可以挂载一个 DuckLake 目录（本地元数据文件，或 PostgreSQL、SQLite、MySQL 目录，数据可放在本地磁盘或对象存储上），该目录会成为默认数据库。每个已挂载的目录都在侧边栏中显示为一个数据库，其 schema、表与视图按需加载。支持绑定参数、多语句脚本、行数限制、查询取消、带 `RETURNING` 的类型化 CRUD，以及通过 `READ ONLY` 事务实现、并拒绝语句中直接引用的表函数读取文件与网络的只读强制（不检查此前创建的视图和宏，也不检查扩展添加的标量函数）。不支持语句超时。参见 [`crates/dbflux_driver_duckdb/README.md`](../crates/dbflux_driver_duckdb/README.md)。

### Amazon S3

面向 AWS S3 及兼容 S3 的端点（Cloudflare R2、MinIO）的对象存储驱动程序，通过 AWS 配置文件/SSO 或静态凭据进行身份验证，支持端点覆盖与路径风格寻址。连接根节点会打开一个存储桶表；存储桶浏览按层级分页（AWS 控制台风格），并可切换到不分页的树形模式。对象预览原生支持图片，对类文本对象提供可内联编辑并回写的编辑器缓冲区，对 PDF 及其他二进制对象则提供元数据与下载/用外部程序打开；归档存储类别（GLACIER、DEEP_ARCHIVE）会完全跳过内容预览。支持上传、删除、需输入确认的递归前缀/存储桶删除、创建文件夹/存储桶、重命名（先复制后删除），以及预签名 URL。不支持分段上传、传输面板、内嵌 PDF 查看器、生命周期/ACL 管理或 S3 Select。参见 [`crates/dbflux_driver_s3/README.md`](../crates/dbflux_driver_s3/README.md)。

### AWS Secrets

面向 AWS Secrets Manager 与 Systems Manager Parameter Store 的密钥存储驱动程序，通过 AWS 配置文件/SSO 或静态凭据进行身份验证，并支持可选的端点覆盖。一个连接会在侧边栏中同时显示两个存储库。列表在服务端分页并支持按名称搜索，Parameter Store 路径按层级逐级浏览。列表和详情面板从不读取值：只有当用户要求时才会读取值：执行 获取值、打开历史中的某个版本，或对尚未加载的值执行编辑值（底部栏按钮或 `e`，它会先获取该值；值尚未加载时，行的右键菜单提供获取值，加载后才提供编辑值）。每一种都是一次读取，仅当审计已启用且审计写入成功时才会记录一条不含该值的审计事件；即使没有审计记录，读取本身也可能成功。在 Parameter Store 参数上确认编辑描述时，驱动程序还会在内部再次读取其值并写回：这次读取不会显示，也不是获取值。键值 JSON 密钥以表格显示，其他文本以纯文本显示，二进制密钥以 base64 显示，由其他服务拥有的密钥标记为只读。缺少 `kms:Decrypt` 权限时会报告错误，绝不会返回空值。区域选择器会列出 `ec2:DescribeRegions` 返回的区域；当凭据无法调用它时（没有该权限的 IAM 用户、没有 EC2 的 LocalStack），文档会静默显示连接配置的区域，不显示选择器，每次失败的发现都会记录一条警告，Refresh 会再次查询。包括 JSON 键值密钥在内的值，可通过编辑值（详情面板按钮、行的右键菜单，或在选中行上按 `e`）编辑并保存为新版本。保存走受管控的写入路径：拒绝只读或需要审批的连接，并在 DBFlux 审计日志开启时记录一条不含该值的审计事件；审计关闭时，保存可能在没有 DBFlux 审计记录的情况下成功。若写入生效后记录该事件失败，写入依然有效，编辑器会显示警告而不是错误。以 JSON 存储的值以缩进的格式化视图打开，可切换到原始视图。保存会写入当前编辑模式的文本：原始视图按原样保存文本，包括给单行值添加的换行；格式化视图会把原本为单行的值还原为单行，把原本为多行的值按编辑后的文本写入。两种模式下，不再是有效 JSON 的文本都会被拒绝。编辑器显示行号、标出已更改的行，并在页脚显示大小、版本、已更改行数和光标位置。二进制值保持只读视图。详情面板、其底部栏和行的右键菜单提供驱动程序支持的其余所有写入，除恢复（它只取消已安排的删除，一次点击即执行）和值编辑器的保存（是按钮，不是对话框）外，每一项都会先在对话框中询问，确认之前不会写入任何内容，并且都经由同一条受管控的写入路径（只读或需要审批的连接会拒绝；DBFlux 审计日志开启时，每次尝试记录一条审计事件，审计关闭或事件记录失败时，该次尝试没有 DBFlux 审计记录，已生效的写入依然有效并会如此报告；失败只报告一次并附关联 ID）：对带有阶段标签的存储中每个较早版本执行回滚（`b` 为最近的一个打开它）、删除（`x`）——Secrets Manager 在 7 至 30 天的恢复期安排（从安排时起密钥即不可用，直到恢复；恢复期只限制可恢复的时长），绝不提供无法恢复的删除；Parameter Store 的删除不可撤销，且只有重新输入完全一致的名称后才会执行——以及对已安排删除的密钥执行恢复、立即轮换（其对话框会说明它运行轮换函数并创建一个在轮换完成后成为当前版本的新版本）与编辑轮换、编辑标签、编辑描述（在 Parameter Store 上会把值重新写成新版本，对话框会如实说明），以及工具栏中的新建密钥或新建参数。由其他服务管理的密钥会拒绝这些写入，已安排删除的密钥同样如此，但恢复除外，它是回头路。参见 [`crates/dbflux_driver_aws_secrets/README.md`](../crates/dbflux_driver_aws_secrets/README.md)。 密钥可从详情面板的展开按钮或行菜单的“在标签页中打开”在自己的标签页中打开，每个连接、存储、名称、版本和区域对应一个标签页：它显示相同的详情、值、历史、轮换和标签，并通过相同的受管路径提供相同的写入，其值在经过审计的检索之前保持隐藏。区域与连接不同或密钥已被删除的标签页不会发起新的读取或写入，并说明原因；已在进行的保存不会被取消，仍可能写入先前的区域，标签页会报告这一点。 详情面板或行菜单中的编辑值会打开该标签页并激活编辑器，值尚未加载时在那里先检索（会被审计）；侧边面板不再承载编辑器，可拖动其左边缘调整大小。

## 外部 RPC 驱动程序

DBFlux 可以加载在进程外运行、通过本地 IPC 通信的驱动程序，它们由 `dbflux_driver_ipc` 实现、通过 `dbflux_driver_host` 承载。这些驱动程序以合成 ID 格式 `rpc:<socket_id>` 注册，并通过网络传输提供自己的 `DriverMetadata`（类别、查询语言、能力），因此界面会像对待内置驱动程序一样对待它们。发现握手、服务生命周期与协议细节，参见 [`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md)。
