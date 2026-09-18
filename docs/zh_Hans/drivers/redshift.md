# Amazon Redshift

AWS 托管的数据仓库，与 PostgreSQL 线协议兼容。只读。

## 速览

- **类别** —— 关系型
- **查询语言** —— SQL
- **默认端口** —— 5439
- **URI 方案** —— `redshift`

面向 DBFlux 的 Amazon Redshift 驱动程序（只读 v1），直接基于 [`postgres`](https://crates.io/crates/postgres) 线协议客户端构建，而不是基于 `dbflux_driver_postgres`。

## 功能

- 关系型驱动程序（`DatabaseCategory::Relational`、`QueryLanguage::Sql`），针对 Redshift 集群或 Redshift Serverless 端点使用 PostgreSQL 线协议通信。
- 连接表单包含主机、端口（默认 `5439`）、数据库、用户、密码、SSL/`sslmode`（`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`）、连接 URI 模式（`redshift://...`，内部被规范化为 `postgresql://...`），以及 SSH 隧道。
- 自定义 TLS 信任与双向 TLS：在启用 TLS 的模式下，会把固定的私有根 CA（PEM）叠加到系统根证书之上加入信任库；客户端证书 + 私钥（PEM/PKCS#8）可启用双向 TLS。证书材料按连接从表单中配置的路径加载；校验强度绝不会被削弱（`verify-ca`/`verify-full` 仍会拒绝不受信的证书）。证书/私钥文件缺失、不可读或格式错误会呈现为清晰的连接错误，而不是静默回退到系统信任库；私钥内容从不被记录到日志。
- 基于 `information_schema` 的 Schema 探查，覆盖数据库、schema、表、视图与列，并用 `ColumnKind` 分类（时间戳/整数/浮点/文本），与标准 PostgreSQL 的 OID 对应。
- Redshift 专属的扩展类型（`SUPER`、`VARBYTE`、`GEOMETRY`、`GEOGRAPHY`、`HLLSKETCH`）归类为 `ColumnKind::Text` 并按文本渲染；其他任何无法识别的 OID 都会回退为防御性的 UTF-8 文本解码，而不是直接崩溃。
- 表详情通过通用的 `TableInfo.storage_hints` 接缝呈现 Redshift 特有的存储元数据（读取自 `SVV_TABLE_INFO` 与 `PG_TABLE_DEF`）：分布键（`KEY`/`EVEN`/`ALL`/`AUTO`，并在适用时给出键列）与排序键（复合或交错，并给出其有序的列）。已声明的主键、外键与唯一约束仍通过标准的核心元数据结构呈现，但每一项都标注为建议性/非强制 —— Redshift 接受但从不强制执行这些约束，也不会据此虚构出索引列表。
- 查询执行（`SELECT`/浏览）通过标准的 `Connection::execute` 路径返回带类型的列；查询取消通过线协议客户端的取消令牌支持。
- `RedshiftErrorFormatter` 把常见的连接与查询失败（超时、连接被拒、认证失败、集群不可达、带 `SQLSTATE` 的查询错误）映射为清晰的、由驱动程序格式化的消息，而不是原始的调试输出。
- 每次连接都上报 `application_name` 为 `dbflux/<version>`，除非连接 URI 自己设置了 `application_name` 查询参数。

## 限制

- 只读：`DriverMetadata.capabilities` 不含 `INSERT`、`UPDATE`、`DELETE`、`RETURNING`、`BULK_INSERT`、`TRUNCATE_TABLE`，以及所有 DDL/事务性 DDL 标志。该驱动程序没有内联的网格编辑，也没有变更/可视化查询构建器。`Connection::execute` 还会在���协议层拒绝任何非读取语句并给出明确错误，因此写入尝试永远不会变成静默的空操作。
- 仅支持单语句：`Connection::execute` 一次只运行一条只读语句。多语句输入（例如 `SELECT 1; SELECT 2`）在到达线协议之前就会被明确拒绝；允许单个可选的结尾 `;`，而字符串字面量、带引号标识符或注释中的 `;` 不被当作分隔符。
- 没有 `INDEXES` 能力：Redshift 没有真正的索引结构，因此 `TableDetails.indexes` 始终为 `None`，而不是由（非强制的）主键合成。
- 没有触发器：Redshift 不支持触发器，因此不会发现或呈现任何触发器。
- 不支持基于 IAM/SSO 的身份认证。仅支持用户名/密码（可选经 SSH 隧道）；Redshift 基于 IAM 的 `GetClusterCredentials` 与浏览器 SSO 流程均未实现。
- 用于双向 TLS 的客户端证书必须以 PEM 证书加 PKCS#8 PEM 私钥的形式提供（两者配置为不同的文件路径）；不接受合并的 PKCS#12 包。根 CA / 客户端证书加载路径的 PEM 解析与错误处理由单元测试覆盖，但针对由私有 CA 前置或要求客户端证书的集群，端到端的 TLS 握手只由一个 `#[ignore]` 的真实集成测试（`redshift_live_verify_full_with_private_ca_and_client_cert`）验证，因为不存在本地或基于 Docker 的 Redshift 引擎。
- 不支持 `COPY`/`UNLOAD`，也没有 Redshift 专属的数据传输/批量导出集成。
- 没有查询计划可视化（不解析也不渲染 `EXPLAIN` 的输出）。
- 没有实例指标与实例检查器（未声明 `INSTANCE_METRICS`/`INSTANCE_INSPECTOR`）。
- 没有写入权限探测：`Connection::probe_write_privilege` 有意保持 trait 默认值（`WritePrivilege::Unknown`），因为该驱动程序已在线协议层拒绝所有变更语句，与所连角色的实际授权无关。
- `SUPER`/`VARBYTE`/`GEOMETRY`/`GEOGRAPHY`/`HLLSKETCH` 所用的扩展类型 OID 取值，以及用于获取存储提示的 `SVV_TABLE_INFO`/`PG_TABLE_DEF` 查询形状，只由 `#[ignore]` 的真实集成测试（`crates/dbflux_driver_redshift/tests/live_integration.rs`）验证，因为不存在本地或基于 Docker 的 Redshift 引擎。可针对真实集群显式运行：`cargo nextest run -p dbflux_driver_redshift --run-ignored all`。
- `NUMERIC`/`DECIMAL` 值**会被**解码：驱动程序直接把 PostgreSQL 二进制 `NUMERIC` 线格式解析为精确的 `Value::Decimal` 字符串（按列声明的小数位重建整数/小数部分，并处理 `NaN`/±`Infinity`）。格式错误的载荷会安全回退，而不会损坏数据。该二进制解码器由基于合成线载荷的单元测试覆盖；端到端的保真度仍只通过那些 `#[ignore]` 的集成测试针对真实集群验证。
