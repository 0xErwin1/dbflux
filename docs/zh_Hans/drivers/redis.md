# Redis

内存中的键值数据库。

## 速览

- **类别** —— 键值
- **查询语言** —— Redis 命令
- **默认端口** —— 6379
- **URI 方案** —— `redis`

面向 DBFlux 的 Redis 键值驱动程序，基于 [`redis`](https://crates.io/crates/redis) crate 构建。

## 功能

- 归类为 `DatabaseCategory::KeyValue`、使用 `RedisCommands` 查询语言的键值驱动程序；编辑器使用 Redis 命令语法，而非 SQL。
- 连接模式：手动（主机/端口/用户/密码/数据库）与 URI 模式。URI 模式接受 `redis://` 与 `rediss://` 连接串。
- 通过 `SELECT <db>` 支持多个逻辑数据库（`MULTIPLE_DATABASES`）。当前数据库索引由连接记录。
- 支持可选用户名 + 密码的身份认证（`AUTHENTICATION`）。
- 连接时通过 `CLIENT SETNAME` 把客户端身份上报给服务器（`dbflux/<version>`，可在 `CLIENT LIST` 中看到）；这是尽力而为的行为，因为部分托管服务商会限制 `CLIENT` 命令。
- 连接时进行尽力而为的写入权限探测（`probe_write_privilege`）：先尝试 `ACL WHOAMI` + `ACL DRYRUN`，当 `ACL` 不可用时（旧版服务器或受限的托管服务）回退到一次带命名空间的短 TTL `SET ... NX` / `DEL`；副本返回的 `READONLY` 或 `NOPERM` 拒绝会判定为只读连接。
- TLS/SSL 支持三种模式（`off`、`on`、`verify`）：
  - `off` —— 明文 `redis://` 连接。
  - `on` —— `rediss://`，信任证书但不校验证书链（标记为不安全）。
  - `verify` —— `rediss://`，使用提供的根证书以及可选的客户端证书/私钥，通过 `Client::build_with_tls` 构建。
- 支持 SSH 隧道，以便通过堡垒机访问 Redis（仅限手动模式；见限制部分）。
- 部署拓扑可自动检测，也可显式设置（`standalone`、`cluster`、`sentinel`）：
  - 自动检测会在连接时探测 `ROLE` 与 `INFO cluster`，并路由到单机（standalone）或 Cluster 的处理路径。
  - `cluster` 跳过检测，通过 `ClusterClient` 直接连接，使用主主机/端口以及任何已配置的额外种子节点。Cluster 连接只暴露数据库 0；Cluster 配置上出现非零数据库会在连接时被拒绝，而不是被静默套用到 db 0。
  - `sentinel` 通过 `SentinelClient` 连接，从一个或多个 Sentinel 节点解析具名的主节点（主主机/端口加上任何已配置的额外节点）。解析完成后，驱动程序会执行 `CLIENT SETNAME`、`PING`，以及一次 `ROLE` 健全性检查，确认解析到的节点确实是主节点。
  - Sentinel 故障转移恢复：在由 Sentinel 支撑的连接上发生连接类失败（连接断开、IO 错误）时，会恰好触发一次通过 Sentinel 的重新解析，以及对失败命令的一次重试，之后才把错误呈现出来。
- 键浏览与发现：
  - 基于游标的键扫描（`KV_SCAN`、`PaginationStyle::Cursor`）。一次分页请求会持续执行 `SCAN`，直到该页包含所请求数量的键或扫描结束，因此匹配稀疏的 `MATCH` 过滤器不会产生空页。每页最多 1000 次 `SCAN` 往返、耗时最多 500 ms；达到上限时，该页返回目前找到的键以及待处理的游标。`SCAN` 重复返回的键在每页中只列出一次，一页最多可能比请求的大小多出一个 `SCAN` 批次。该页的键类型通过一个管道一次获取。在 Cluster 连接上，单纯的 `SCAN` 没有单节点含义，因此驱动程序在同一分页上限内依次扫描待处理的主节点：每个节点的游标独立跟踪，聚合后的游标以不透明的 JSON 对象往返，把 `"<host>:<port>"` 映射到其待处理的 `SCAN` 游标。当所有主节点都报告游标 0 时，整体扫描才算结束。
  - 按键的类型发现（`KV_KEY_TYPES`），覆盖字符串、哈希、列表、集合、有序集合与流。
  - TTL 检查（`KV_TTL`）与值大小上报（`KV_VALUE_SIZE`）。
  - 存在性检查（`KV_GET`/`KV_EXISTS`）、键重命名（`KV_RENAME`），以及多键的批量获取（`KV_BULK_GET`）。
- 值类型覆盖：字符串、哈希、列表、集合、有序集合与流，包括流的范围读取、流条目添加与流条目删除（`KV_STREAM_RANGE`、`KV_STREAM_ADD`、`KV_STREAM_DELETE`）。
- 可配置的流预览上限，作为一项连接设置暴露。
- 变更：插入、更新、删除、批量操作与批量删除。`RedisCommandGenerator` 会为 set/delete、hash set/delete、list push/set/remove、set add/remove、sorted-set add/remove 以及 stream add/delete 生成 Redis 命令，用于预览与「复制为命令」。
- 结果的 JSON 导出（`EXPORT_JSON`）。
- 整体负载读取的大小闸门：当请求带有字节预算时，字符串/JSON 值会在 `GET` 之前先用 `STRLEN` 探测，超大的值会返回一个带真实大小的占位符，而不是传输整个负载；集合类型不受影响，触及获取上限的流读取会把自己报告为被截断。
- 离线 RDB 转储分析（`DumpAnalyzer`）：无需连接服务器即可逐键扫描 `.rdb` 文件，以 I/O 速度流式读取该文件，内存占用平稳（键值从不解码，只取键名与值类型）。报告内容包括键总数、按类型的分布、最大的 500 个键，以及按前缀的大小汇总。所报告的大小是**每个键在磁盘上的序列化大小**，而不是它在 Redis 活动内存中的占用 —— 分配器开销与内存编码方式会让这两个数字不一致。

在 Cluster 连接上，Schema 探查会报告一个聚合后的 `db0` 键空间：键数与平均 TTL 是对每个主节点的 `DBSIZE`/键空间统计做求和/平均，而不是按节点分别上报。

### 实例指标

基于 `INFO` 命令输出精选的一组实时服务器指标。在 Cluster 连接上不可用 —— 没有可供采样 `INFO` 的单一节点，因此 `instance_catalog()` 返回 `None`，Cluster 配置也就没有实例概览/指标/检查器可用。

- `redis.connected_clients` —— 当前连接的客户端数
- `redis.blocked_clients` —— 正在等待阻塞命令的客户端数
- `redis.used_memory` —— Redis 分配器已分配的字节数
- `redis.used_memory_rss` —— 操作系统分配的字节数（常驻集大小）
- `redis.total_commands_processed` —— 累计已处理的命令数
- `redis.total_connections_received` —— 累计已接受的连接数
- `redis.instantaneous_ops_per_sec` —— 每秒处理的命令数（服务端速率）
- `redis.keyspace_hits` —— 键查找的缓存命中数
- `redis.keyspace_misses` —— 键查找的缓存未命中数
- `redis.evicted_keys` —— 因 `maxmemory` 策略而被逐出的键数
- `redis.expired_keys` —— 因 TTL 而过期的键数
- `redis.rdb_changes_since_last_save` —— 上次 RDB 快照以来的变更数
- `redis.connected_slaves` —— 已挂接的副本数

每个指标以单行 `(timestamp_ms, value)` 返回，用于实时绘图。

### 实例检查器

以表格形式呈现运行中服务器状态的快照：

- `redis.client_list` —— 来自 `CLIENT LIST` 的活动客户端（id、cmd、age、idle、flags、db、sub、multi）

敏感字段（`addr`、`laddr`、`name`）会被脱敏为 `[redacted]`，以免把客户端 IP 地址与主机名暴露出来。

## 限制

- `execute()` 对任何请求的行数限制（包括零）或语句超时，在发送命令前返回 `NotSupported`。未请求这些保护的命令仍可执行；默认编辑器无法保证这些保护。

- 不支持 SQL；查询必须以 Redis 命令的形式书写。

- 实例指标每次调用只返回一个数据点（`INFO` 的当前快照），而非历史时序。累计计数器（例如 `redis.total_commands_processed`）单调递增 —— 应将其理解为两次采样之间的增量，而不是绝对速率。

- `CLIENT LIST` 检查器会把每一行中的 `addr`、`laddr` 与 `name` 字段脱敏，以免把客户端 IP 地址与用户自定义名称暴露给界面。

- 不支持查询取消（未设置 `QUERY_CANCELLATION`）；长时间运行的命令无法从界面中止。
- 不支持 upsert（`supports_upsert: false`）、不支持 `RETURNING`，也不支持批量更新（`supports_bulk_update: false`）。
- DDL 能力全部禁用（没有表、视图、索引、schema）—— 这是键值存储，不是关系型数据库。
- 事务在能力层面被声明为支持（`supports_transactions: true`），但不支持隔离级别、保存点、嵌套事务、只读事务与可延迟（deferrable）事务。
- 未暴露 Pub/Sub（未设置 `PUBSUB` 能力）。
- 启用 URI 模式时无法使用 SSH 隧道；隧道路径只为手动连接模式接入。不支持把 SSH 隧道与 Cluster/Sentinel 的额外种子节点组合使用：隧道只转发主主机/端口，因此额外节点无法通过它访问。
- 未对流消费组建模；只支持范围读取、条目添加与条目删除。
- Sentinel 与 Cluster 的额外种子节点始终通过明文 `redis://` 访问；不支持为这些额外节点配置按节点的 TLS。本轮迭代中，解析出的 Sentinel 主节点连接本身也是明文（无 TLS）。
- Sentinel 的身份认证只作用于解析出的主节点连接（使用配置的用户名/密码）；访问 Sentinel 节点本身时不带认证。
