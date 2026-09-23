# DynamoDB

AWS 托管的 NoSQL 键值与文档数据库。

## 速览

- **类别** —— 文档型
- **查询语言** —— DynamoDB 表达式
- **URI 方案** —— `dynamodb`

面向 DBFlux 的 AWS DynamoDB 驱动程序，基于 [`aws-sdk-dynamodb`](https://crates.io/crates/aws-sdk-dynamodb) SDK 构建。

## 功能

- 托管型 NoSQL 驱动程序，归类为 `DatabaseCategory::Document`，使用 `QueryLanguage::Custom("DynamoDB")` 命令信封；编辑器使用的是 DynamoDB 专用语法，而非 SQL。
- 通过区域、具名配置文件以及可选的端点覆盖（用于 DynamoDB Local 或 VPC 端点）完成 AWS 连接配置。`deployment_class` 为 `CloudManaged`。
- 通过 `ListTables` 与 `DescribeTable` 发现表，把分区键（PK）、排序键（SK）以及全局/本地二级索引（GSI/LSI）的键元数据映射为 DBFlux 的 Schema 抽象。
- `scan`、`query`、`put`、`update` 与 `delete` 的原生命令信封执行。查询生成器会发出 scan 形状的预览信封，并注明当筛选条件匹配表的键结构时，执行可能会优化为 `Query`。读取限制保留零行结果；当信封和请求均指定限制时采用较小值。
- 读取选项支持索引定向、一致性读控制，以及筛选转换的回退策略（服务端筛选 vs. 客户端回退；当回退策略设为拒绝时，客户端筛选会被拒绝）。
- 语义筛选中的 WHERE 运算符：`Eq`、`Ne`、`Gt`、`Gte`、`Lt`、`Lte`、`In`、`NotIn`，以及逻辑 `And`/`Or`（`Not` 见限制部分）。
- 变更：插入（`put`）、更新与删除（`INSERT`/`UPDATE`/`DELETE`）。批量写入最多支持 25 项（`max_insert_values: 25`，`supports_batch: true`），并对未处理的批量写入项做有界重试。
- 通过条件更新加 put 回退支持单条 upsert；键映射从筛选条件或更新载荷中解析（分区键必填，表定义了排序键时排序键也必填）。
- 多条更新路径（`many=true` 的 `update`）使用共享的更新表达式。
- 嵌套文档与数组映射到文档树视图（`NESTED_DOCUMENTS`、`ARRAYS`）。
- DDL：删除表（`supports_drop_table: true`）。
- 通过分页令牌分页（`PaginationStyle::PageToken`）。
- 客户端身份：每个请求都以 `dbflux-<version>` 作为 AWS SDK 应用名，可在 CloudTrail 的 `userAgent` 字段中看到。

## 限制

- `profile` 字段（AWS 具名配置文件）是一个 `AuthProfileRef` 表单字段。通用的可移植性接缝（`DbDriver::export_field_hint`）把所有 `AuthProfileRef` 字段映射为 `RequiredOnImport`，因此该字段值不会出现在任何导出的包中，接收方必须在导入时提供或创建匹配的认证配置文件。无需为此做针对特定驱动程序的覆盖。
- 显式语句超时（包括零时长）在执行前被拒绝。不支持查询取消；驱动程序对取消请求返回 `NotSupported`。
- 信封写入和 PartiQL 写入中的显式请求行数限制在变更前被拒绝。PartiQL `SELECT` 通过 SDK 支持正数限制；零限制在发送请求前被拒绝。PartiQL 只返回第一页并提供下一页令牌，但 `QueryRequest` 无法传入令牌。
- 行数限制仅约束返回的行数，不限制字节数或 DynamoDB 服务端工作量。没有观察到遗漏行时不宣称结果被截断。
- 命令信封 API 不暴露 PartiQL 或 DynamoDB 事务操作；事务被禁用（`supports_transactions: false`）。
- 支持单条 upsert（`supports_upsert: true`）；`many=true` 与 `upsert=true` 同时出现的 `update` 会被拒绝（`update_many_with_upsert`）。
- 不支持批量更新与批量删除（`supports_bulk_update: false`、`supports_bulk_delete: false`），也不支持 `RETURNING`。
- 语义筛选不支持 `NOT` 表达式，也不支持受支持集合之外的运算符；不受支持的运算符返回 `NotSupported`。
- 没有 SSL 表单（TLS 由 AWS SDK 的传输层处理）、没有 schema，也没有除删除表之外的 DDL（不能创建/修改表，也不能创建索引）。
- 语义规划器不支持聚合请求。
- 核心请求层的集合浏览仍基于偏移量，而底层 API 基于分页令牌。
- 没有写入权限探测：`Connection::probe_write_privilege` 有意保持 trait 默认值（`WritePrivilege::Unknown`），因为可靠的检查需要 `iam:SimulatePrincipalPolicy`，而连接所用的角色通常不具备该权限。
- 没有实例指标与实例检查器（未声明 `INSTANCE_METRICS`/`INSTANCE_INSPECTOR`）：DynamoDB 自身的服务端指标已经存在于 CloudWatch 中，因此再提供一个按驱动程序的 `InstanceCatalog` 只会重复这一层，而不是新增能力。
