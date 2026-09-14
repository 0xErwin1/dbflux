# 驱动程序开发指南

本文档用于选择并实现 DBFlux 的数据库驱动程序集成，只讲贡献者需要走的路径，不再重复更宏观的架构说明与 RPC 协议参考。

## 选择集成方式

| 选择 | 内置 Rust 驱动程序 | 外部 RPC 驱动程序 |
| --- | --- | --- |
| 适用场景 | 驱动程序应随 DBFlux 工作区与主进程一起发布 | 驱动程序需要运行在进程外，或需要独立开发、独立部署 |
| 实现方式 | 新建 `crates/dbflux_driver_<name>/` crate，实现核心 Rust 契约 | 实现一个遵循驱动程序 RPC 协议的服务 |
| 注册方式 | 编译期的 feature 接线，以及 `AppState::build_builtin_drivers()` | 设置 > RPC 服务，指定 `kind=driver` 与 `socket_id` |
| 稳定键 | `builtin:<name>` | `rpc:<socket_id>` |
| 配置 | 由驱动程序自带的 `DriverFormDef` 转换为某个内置的 `DbConfig` 变体 | 表单数据由握手提供，并以 `DbConfig::External` 的形式存储 |

## 内置驱动程序：常规流程

1. 参照最接近的现有 `crates/dbflux_driver_*/` crate 搭建结构。
2. 实现 `DbDriver` 与 `Connection`，包括元数据、表单/配置转换、连接行为、错误处理，以及带类型的结果列。
3. 只声明确实有实现支撑的能力；只有在驱动程序支持时才添加可选接缝。
4. 在工作区、app 和二进制三处完成 crate 与 feature 的接线，然后在 `build_builtin_drivers()` 中注册。
5. 补上针对性的测试、crate README，并更新驱动程序支持矩阵。

[内置驱动程序清单](#内置驱动程序清单)会把上面的每一步展开说明。

## 外部 RPC 驱动程序：常规流程

1. 实现规范的[驱动程序 RPC 协议](DRIVER_RPC_PROTOCOL.md)，可以[自定义驱动程序示例](../examples/custom_driver/README.md)为起点。
2. 构建并运行该服务，既可以独立运行，也可以通过受管命令启动。
3. 在**设置 > RPC 服务**中添加它，指定 `kind=driver`、一个稳定的 `socket_id`，以及可选的受管命令。
4. 重启 DBFlux，确认握手提供的元数据与表单出现在连接管理器中。

当前的配置行为参见 [RPC 服务配置参考](RPC_SERVICES_CONFIG.md)。不要照抄本指南中的启动参数 —— 协议文档、配置参考与示例才是权威依据。

## 核心契约与解耦规则

主契约是 [`DbDriver` 与 `Connection`](../crates/dbflux_core/src/core/traits.rs)：

- `DbDriver` 提供驱动程序元数据、连接表单定义、配置的构建与提取、连接的构造，以及一个稳定的 `DriverKey`。
- `Connection` 提供运行时的查询、Schema、变更，以及与能力相关的可选行为。许多不支持的操作都有默认实现；但必需的方法与对外声明的能力仍然必须保持一致。
- 内置驱动程序的 `driver_key()` 取值为 `builtin:<name>`，外部驱动程序为 `rpc:<socket_id>`。

元数据与适配逻辑由 [`DriverMetadata`、`DatabaseCategory`、`QueryLanguage` 和 `DriverCapabilities`](../crates/dbflux_core/src/driver/capabilities.rs) 定义，其中也包含通用的编辑器呈现元数据。运行时的来源与呈现行为，则通过 `Connection` 上的通用接缝暴露，定义在 [`traits.rs`](../crates/dbflux_core/src/core/traits.rs) 中。

**严格规则：** 界面与应用工作流代码不得依据具体的驱动程序 ID 做分支。请依据元数据、类别、查询语言、能力标志、表单定义，以及通用的来源/呈现接缝来适配。如果确实需要新的界面区分方式，就新增一个其他驱动程序也能实现的通用核心契约。

结果数据方面，请用 [`ColumnMeta.kind` 填好 `ColumnKind`](../crates/dbflux_core/src/query/types.rs)。图表及其他使用方使用的都是这一语义类型；它们不会从驱动程序 ID 或 `type_name` 去推断。

## 内置驱动程序清单

### 1. crate 与契约

- [ ] 添加 `crates/dbflux_driver_<name>/Cargo.toml`、`src/lib.rs`、实现模块与测试。请参照最接近的驱动程序，不要假定每个驱动程序的模块结构都相同。
- [ ] 依据 [`crates/dbflux_core/src/core/traits.rs`](../crates/dbflux_core/src/core/traits.rs) 实现 `DbDriver` 与一个线程安全的 `Connection`。
- [ ] 返回形式为 `builtin:<name>` 的稳定 `DriverKey`。
- [ ] 把数据库客户端类型与驱动程序特有的行为留在该 crate 内部；对外只通过核心契约暴露行为。

### 2. 元数据与能力

- [ ] 定义如实反映情况的 `DriverMetadata`：身份、显示字段、`DatabaseCategory`、`QueryLanguage`、`DriverCapabilities`、连接默认值，以及适用的通用能力结构。
- [ ] 用元数据与通用的呈现/来源接缝来适配界面。不要在界面或应用工作流中添加基于驱动程序 ID 的条件判断。
- [ ] 只有在对应操作或可选接缝确实可用时才声明该能力。既要验证已支持的行为，也要核实那些声明为不支持的部分。

### 3. 表单与配置

- [ ] 定义并维护本 crate 的 `DriverFormDef`；连接界面会通用地渲染它。
- [ ] 实现 `build_config()` 的校验，以及 `extract_values()` 的编辑往返。
- [ ] 密钥走既定的密钥通路，不要把它们嵌进持久化的表单值里。
- [ ] 仅在适用时实现 URI 的解析/构建，或导出字段的覆盖。

### 4. 连接、错误与结果

- [ ] 通过 `DbDriver` 的方法构造并测试连接，包括必需的密钥处理与连接测试。
- [ ] 通过 [`QueryErrorFormatter` 与 `ConnectionErrorFormatter`](../crates/dbflux_core/src/core/error_formatter.rs) 实现结构化的查询与连接错误格式化。保留有用的数据库上下文，但不泄露密钥。
- [ ] 通过核心类型返回 Schema 与查询数据，并为每一个结果列把正确的 `ColumnKind` 填到 `ColumnMeta.kind`。
- [ ] 直接测试类型映射。不要让使用方从原始的 `type_name` 字符串去推导语义。

### 5. 可选接缝

仅在数据库确实支持时才实现以下几项，并保持能力标志与实现同步：

- [ ] 非默认的 `LanguageService`，用于语言特定的校验与变更分类。
- [ ] SQL 方言、代码生成器、查询生成器或语义规划器行为（如适用）。
- [ ] 源上下文、指标目录、仪表盘导入器或仪表盘来源行为（如适用）。
- [ ] 用于指标或检查器的实例目录（如适用）。
- [ ] 核心 trait 与能力标志所涵盖的其他 Schema、CRUD、取消、传输或键值接缝。

### 6. feature 接线与注册

- [ ] 在根 [`Cargo.toml`](../Cargo.toml) 中加入工作区成员与工作区依赖。
- [ ] 在 [`crates/dbflux_app/Cargo.toml`](../crates/dbflux_app/Cargo.toml) 中添加可选依赖与 feature 转发。
- [ ] 在 [`crates/dbflux/Cargo.toml`](../crates/dbflux/Cargo.toml) 中转发二进制 feature。
- [ ] 在 [`AppState::build_builtin_drivers()`](../crates/dbflux_app/src/app_state/bootstrap.rs) 中添加按 feature 门控的导入与注册。
- [ ] 分别验证「启用该 feature」的构建与一个代表性的「关闭该 feature」的构建，确保注册逻辑的门控始终正确。

### 7. 测试与文档

- [ ] 测试元数据、能力声明、表单/配置往返、错误、连接行为、Schema 映射、查询结果，以及每一项已声明的可选接缝。
- [ ] 在行为跨越驱动程序与核心边界处补充集成测试；涉及真实服务的测试按各 crate 的既有约定保持 `#[ignore]` 或加门控。
- [ ] 添加 `crates/dbflux_driver_<name>/README.md`，写清 **功能** 与 **限制** 两节。
- [ ] 更新 [`docs/DRIVERS.md`](DRIVERS.md)，并保持其中的能力声明与 crate README 及实现一致。

## 外部 RPC 驱动程序清单

- [ ] 依据[驱动程序 RPC 协议](DRIVER_RPC_PROTOCOL.md)实现握手、表单、会话、查询，以及所支持的可选操作。
- [ ] 通过协议握手提供元数据、能力与表单定义；确保每一项能力声明都与已实现的 RPC 操作一致。
- [ ] 在**设置 > RPC 服务**下把该服务配置为 `kind=driver`，并指定一个稳定的 `socket_id`；只有在需要 DBFlux 接管进程生命周期时才添加受管命令。
- [ ] 预期运行时键为 `rpc:<socket_id>`，配置则通过 `DbConfig::External` 以通用方式存储。
- [ ] 持久化配置与生命周期语义遵循 [RPC 服务配置](RPC_SERVICES_CONFIG.md)。
- [ ] 基于[自定义驱动程序示例](../examples/custom_driver/README.md)构建并做冒烟测试，然后测试重启、握手失败、不支持的操作，以及连接表单的往返。

外部 RPC 驱动程序不使用内置的 Cargo feature 接线，也不走 `build_builtin_drivers()` 注册路径。

## 评审清单

提交拉取请求前请确认：

- [ ] 所选的内置或 RPC 路径被一致地使用，两条注册路径没有混用。
- [ ] 界面与应用工作流中没有基于具体驱动程序 ID 的分支。
- [ ] 元数据、能力标志、可选接缝、测试、crate README 与 `docs/DRIVERS.md` 的说法一致。
- [ ] 表单/配置的编辑往返可用，且密钥没有被意外持久化或记录到日志。
- [ ] 查询结果正确填充了 `ColumnMeta.kind`。
- [ ] 连接与查询失败会产生结构化的、有参考价值且不会泄露密钥的错误。
- [ ] 「关闭 feature」与「启用 feature」两种构建都能通过，或该 RPC 服务能通过握手与重启冒烟测试。
- [ ] 在 [CONTRIBUTING.md](../CONTRIBUTING.md) 中列出的仓库检查全部通过。
