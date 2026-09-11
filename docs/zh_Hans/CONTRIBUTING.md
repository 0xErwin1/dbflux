# 为 DBFlux 做贡献

感谢你考虑为 DBFlux 做贡献。本文档说明如何提交问题反馈、发起拉取请求，以及遵循 DBFlux 在发布与标签方面的约定。

## 快速链接

- [架构概览](ARCHITECTURE.md)
- [驱动程序开发指南](docs/DRIVER_AUTHORING.md)
- [发布流程与分支模型](docs/RELEASE.md)
- [审计事件结构](docs/AUDIT.md)
- [驱动程序 RPC 协议](docs/DRIVER_RPC_PROTOCOL.md)
- [Lua 脚本](docs/LUA.md)
- [MCP / AI 集成](docs/MCP_AI_INTEGRATION.md)

## 项目搭建

DBFlux 是一个 Rust 工作区，界面使用 [GPUI](https://github.com/zed-industries/zed)。完整功能需要启用数据库驱动程序的 feature 标志：

```bash
cargo check --workspace
cargo build
cargo run
```

在 Linux 上，本地构建**必须**使用 [`mold`](https://github.com/rui314/mold) 链接器：`.cargo/config.toml` 为 `x86_64-unknown-linux-gnu` 目标链接时加上了 `-fuse-ld=mold`，以减少整个工作区的链接时间与内存占用。请通过包管理器安装（例如 `apt install mold`）；Nix 开发 shell 会自动提供它。Windows 与 macOS 不受影响。

发起拉取请求前请运行：

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

也可以使用 [`cargo-nextest`](https://nexte.st) 运行测试（在这个工作区上更快，由 Nix 开发 shell 提供）。注意 nextest 不会运行文档测试：

```bash
cargo nextest run --workspace
cargo test --doc --workspace
```

另外提供了 Nix 开发 shell：`nix develop`。

## 分支模型

DBFlux 采用**基于主干的开发方式，并配合短期存在的发布分支**：

- `main` 是唯一长期存在的分支。所有工作都以 `main` 为目标。
- `release/vX.Y` 分支仅在某个次版本需要为稳定版发布而固化时，才从 `main` 切出。它们只接受从 `main` 拣选的修复 —— 不接收新功能。

贡献者的拉取请求**一律**应以 `main` 为目标。向发布分支回移是维护者的职责。

完整规则（标签、版本更新、切出流程、CHANGELOG 规范）见 [`docs/RELEASE.md`](docs/RELEASE.md)。

## 提交规范

在合适的情况下使用[约定式提交（Conventional Commits）](https://www.conventionalcommits.org/)：

- `feat(scope): …` —— 面向用户的新能力
- `fix(scope): …` —— 缺陷修复
- `refactor(scope): …` —— 内部改动，不改变行为
- `perf(scope): …` —— 性能改进
- `docs(scope): …` —— 仅文档
- `test(scope): …` —— 仅测试
- `ci(scope): …` —— CI / 发布工作流改动
- `chore(scope): …` —— 仓库杂务（依赖、工具链、版本更新）

scope 指受影响的范围：可以是某个驱动程序名（`postgres`、`mongodb`），也可以是 `ui`、`mcp`、`audit`、`rpc`、`release` 等。标题行请控制在 70 字符以内；当原因不够明显时，在正文中说明*为什么*这样改。

## 拉取请求

1. 从 `main` 切出分支。拉取请求应聚焦在单一议题上。
2. 填写[拉取请求模板](.github/pull_request_template.md)：概述、解决的问题、解决方式、验证依据，以及测试环境。
3. 在描述中用 `Resolves #N` 关联它所对应的问题。
4. 打上能描述该改动的标签。参见下文[标签指南](#标签指南)。
5. 保持差异可评审。超过约 400 行改动的拉取请求，应拆分为堆叠/链式拉取请求，除非维护者批准使用 `size:exception`。
6. CI 必须通过（`tests.yml`、`style.yml`）。如有失败，推送前请在本地重新运行。
7. 文档改动应与译文一并提交。当你修改 `docs/` 下的页面、某个驱动程序的 README，或是网站会渲染的根文档（`ARCHITECTURE.md`、`CONTRIBUTING.md`、`SECURITY.md`、`TRADEMARK.md`、`PRIVACY.md`）时，请在同一个拉取请求中，把同样的改动同步到 `docs/es/` 与 `docs/zh_Hans/` 下的所有对应文件。若某页面还没有对应译文，则无需处理。参见[翻译](docs/TRANSLATIONS.md)。

### 提交信息至关重要

DBFlux 使用 [git-cliff](https://git-cliff.org) 直接从 git 历史生成变更日志与发布说明。**不要手动编辑 `CHANGELOG.md` 或 `[Unreleased]`。** 呈现给用户看的就是你的提交信息。

变更日志的收录规则：

| 类型 | 是否进入变更日志？ |
|------|------------------------|
| `feat` | 会 —— 归入 **Added** |
| `fix` | 会 —— 归入 **Fixed** |
| `perf` | 会 —— 归入 **Changed** |
| `refactor`、`test`、`ci`、`chore`、`docs`、`style`、`build` | 否 —— 仅内部可见 |
| 带 `(security)` scope 或 `Security:` 尾部说明的任何类型 | 会 —— 归入 **Security** |

破坏性变更（`feat!:`、`fix!:`，或带 `BREAKING CHANGE:` 尾部说明）无论类型如何，总会呈现。

**实际影响：**

- 用户可见的改动**必须**使用 `feat`、`fix` 或 `perf` 作为类型。`chore` 或 `refactor` 提交在变更日志中对用户不可见。
- 标题行要清晰、使用祈使句式 —— 它会原样成为变更日志中的一条。
- 如果一个拉取请求同时包含内部改动与用户可见改动，请把它们拆成类型恰当的多个提交。
- 安全修复：使用 `fix(security): ...`，或添加 `Security: ...` 尾部说明，使该改动归入 Security 一节。

## 问题反馈

提交问题反馈前：

- 搜索现有问题，避免重复。
- 如果可以，请在较新的构建上复现。

请包含：

- DBFlux 版本（`dbflux --version`）、操作系统 / 显示服务器（Linux 上是 X11 还是 Wayland），以及数据库引擎及版本。
- 复现步骤。
- 期望行为与实际行为。
- 相关日志（如有）。请对密钥做脱敏。

打上能描述该问题的标签。参见[标签指南](#标签指南)。

## 标签指南

本仓库使用一套结构化的标签分类体系。提交问题或拉取请求时，请在**每个适用的维度上各选一个标签**。维护者可能在分诊（triage）时进行调整。

### 类别（每个受影响的领域取 `*:bug` 或 `*:feature` 之一）

存在缺陷/功能区分的领域：

| 领域 | 缺陷 | 功能 |
|-----------|--------------------|----------------------|
| AWS | `aws:bug` | `aws:feature` |
| 审计 | `audit:bug` | `audit:feature` |
| 驱动程序 | `driver:bug` | `driver:feature` |
| MCP | `mcp:bug` | `mcp:feature` |
| 流水线 | `pipeline:bug` | `pipeline:feature` |
| 代理 | `proxy:bug` | `proxy:feature` |
| 查询 | `query:bug` | `query:feature` |
| RPC | `rpc:bug` | `rpc:feature` |
| SSH | `ssh:bug` | `ssh:feature` |
| 存储 | `storage:bug` | `storage:feature` |
| 界面 | `ui:bug` | `ui:feature` |

此外还有 GitHub 默认的通用标签：`bug`、`documentation`、`question`、`help wanted`、`good first issue`、`invalid`。

### 子系统标志（相关时应用）

- `aws`、`proxy`、`ssh`、`query`、`driver`、`mcp`

### 驱动程序（当改动针对特定驱动程序时）

`driver:mongodb`、`driver:postgres`、`driver:sqlite`、`driver:mysql/mariadb`、`driver:dynamodb`、`driver:redis`

### 数据模型类别（针对存储/驱动程序层面的改动）

`kind:sql`、`kind:document`、`kind:kv`、`kind:log`

### 平台 / 架构（当行为与平台相关时）

- 平台：`platform:linux`、`platform:macos`、`platform:windows`
- 架构：`arch:amd64`、`arch:arm64`

### RPC 子类型（涉及由 RPC 支持的服务时）

`rpc:auth`、`rpc:driver`（在 `rpc:bug` / `rpc:feature` 之外额外应用）

### 优先级

`priority:high`、`priority:medium`、`priority:low` —— 通常由维护者在分诊时应用。

### 状态（由维护者应用）

`status:needs-review`、`status:approved`、`status:rejected`

### 组合示例

- Linux 上的 PostgreSQL JSON 查询缺陷：
  `driver:bug`、`driver:postgres`、`query:bug`、`platform:linux`、`kind:sql`
- 新的 Redis 发布/订阅功能：
  `driver:feature`、`driver:redis`、`kind:kv`
- Windows 上 MCP 审批流程的回归缺陷：
  `mcp:bug`、`platform:windows`
- SSH 隧道的界面改进：
  `ui:feature`、`ssh:feature`、`ssh`

如果不确定，就按自己的判断尽量打标签 —— 维护者会在分诊时进一步完善。

## 安全

不要公开提交安全问题。请发送邮件给维护者，或使用私密渠道。日志与复现步骤必须对密钥（令牌、密码、连接字符串）做脱敏处理。

## 许可

提交贡献即表示你同意自己的贡献采用本项目的 MIT / Apache-2.0 双许可。DBFlux 的名称与标识不在该许可范围内，参见 [TRADEMARK.md](TRADEMARK.md)。
