# 发布流程

DBFlux 采用**基于主干的开发模式，配合短生命周期的发布分支**。一条长期存在的分支（`main`）作为集成目标；每个次版本在稳定化阶段会从 `main` 切出一条 `release/vX.Y` 分支，并在 EOL 之后废弃。

本文档是供人工查阅的参考。自动化的 `dbflux-release` 技能（`skills/dbflux-release/SKILL.md`）遵循相同规则。

## 渠道

| 渠道（源分支）          | 标签模式              | GitHub Release            |
|-------------------------|-----------------------|---------------------------|
| **nightly**（`main` HEAD） | `nightly`（滚动）     | 预发布，由每日 cron 构建  |
| **rc**（`release/vX.Y`）   | `vX.Y.Z-rc.N`         | 预发布，在推送标签时构建  |
| **stable**（`release/vX.Y`） | `vX.Y.Z`          | 已发布，在推送标签时构建  |

`-dev.N` 渠道已**废弃**。nightly 已取代它。旧的 `-dev.N` 标签仍保留在 GitHub 上，但不会再创建新的。

各渠道的应用程序图标在 [issue #183](https://github.com/0xErwin1/dbflux/issues/183) 中跟踪。请勿在此处实现。

## 更新日志模型（git-cliff，模型 B）

更新日志由 [git-cliff](https://git-cliff.org) **根据 git 历史自动生成**。不要手动编辑 `[Unreleased]`。

- 仓库根目录下的 `cliff.toml` 配置生成器。
- `[Unreleased]` 表示“自上一个**稳定版**标签以来的每个用户可见约定式提交”。rc 与 nightly 标签是透明的：它们不会关闭 `[Unreleased]` 窗口（`cliff.toml` 中的 `skip_tags`）。
- **提交信息具有实际效力。** 类型为 `feat`、`fix` 或 `perf` 的提交会出现在更新日志中；而 `chore`、`ci`、`docs`、`test`、`refactor` 或 `style` 类型的提交则会被丢弃。与安全相关的改动使用 `fix(security):` 或 `Security:` 脚注。
- `[Unreleased]` 区块**仅在发布稳定版时**关闭。当推送一个稳定版标签时，git-cliff 会将该标签对应的、自上一个稳定版以来的全部用户可见提交渲染为该标签的发布说明。
- 在切出 rc 或 nightly 时，不要手动重命名 `[Unreleased]`。在此模型下，rc 的切出流程更简单——见下文。

`CHANGELOG.md` 保存在仓库中，并在发布时通过 `git-cliff --prepend` **前置插入**新版本章节来更新。它从不被手动编辑，也从不整体重新生成——整体重新生成（`git-cliff -o CHANGELOG.md`）会把所有历史章节合并为自上一个稳定版起的一个区间，从而破坏 `## [0.6.0]` 与 `## [0.6.0-dev.N]` 条目。

> **v0.7.0 过渡说明：** git-cliff 更新日志生成从 v0.7.0 开始适用。`## [0.6.0]` 与 `## [0.6.0-dev.N]` 章节是手工编写、已提交到 `CHANGELOG.md` 的基线内容。绝不能重新生成它们——否则会导致重复或合并。前置插入工作流从第一个 v0.7.0 rc 开始。

## 分支

| 分支          | 生命周期 | 接受的内容                       | 产生的标签                                  |
|---------------|----------|----------------------------------|---------------------------------------------|
| `main`        | 永久     | 每个新提交（功能、修复、重构）   | （无——nightly 滚动）                        |
| `release/vX.Y` | 直至 EOL | 仅拣选的修复（不含新功能）       | `vX.Y.Z-rc.N`、`vX.Y.Z`、`vX.Y.(Z+1)`       |

### 不可违反的规则

- 提交**绝不**在发布分支上直接创建。它总是先合入 `main`，再通过 `git cherry-pick -x <sha>` 拣选到发布分支。
- 发布分支**绝不**合并回 `main`。
- 发布分支一旦切出，便不再加入新功能。只允许修复缺陷以及该发布自身的版本制品更新。
- `main` 始终开放开发。`main` 上无需手动添加更新日志条目——提交信息已包含相应内容。

## 标签

标签必须使用带注释（annotated）的形式：

```bash
git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
git push origin vX.Y.Z[-suffix.N]
```

发布工作流（`.github/workflows/release.yml`）会自动对标签进行分类：

| 标签模式（允许的源分支）        | GitHub Release 类型   |
|--------------------------------|-----------------------|
| `vX.Y.Z-rc.N`（来自 `release/vX.Y`） | 预发布          |
| `vX.Y.Z`（来自 `release/vX.Y`）     | 稳定版（已发布）|
| 其他情况（兜底）               | 草稿                  |

## 版本规则

工作区版本（`Cargo.toml` 中的 `[workspace.package].version`）是唯一事实来源。所有其他清单文件必须与之保持同步。

**`main` 上：**

清单版本为 `X.(Y+1).0-dev.0`，其中 `X.Y` 是正在 `release/vX.Y` 上稳定化的次版本。该标记在切出 `release/vX.Y` 时设置，并在整个稳定化窗口及之后一直保留在 `main` 上，直到下一次切出。它仅作为开发标记——永远不会发布 `-dev.N` 版本。nightly 工作流会剥离预发布后缀并追加 `-nightly+<short-sha>`，由此派生出 `X.(Y+1).0-nightly+<sha>`。

**`release/vX.Y` 上：**

- 下一个 rc：若最后一个标签是 `vX.Y.Z-rc.N` → `-rc.(N+1)`；若没有 → `-rc.0`。
- 升级为稳定版：去掉 rc 后缀 → `vX.Y.0`。
- 补丁：递增 `Z` → `vX.Y.(Z+1)`。绝不在发布分支上更新次版本号。

## 周期示例：`0.7.0`

1. 新功能合入 `main`，无需手动添加更新日志条目。
2. 准备稳定化时，从 `main` HEAD 切出 `release/v0.7`。
   - 在 `release/v0.7` 上：将每个带版本号的制品更新到 `0.7.0-rc.0`。提交并推送。
   - 在 `main` 上：将每个带版本号的制品更新到 `0.8.0-dev.0`。提交并推送。`main` 现在指向下一个次版本。
   - 在发布分支上打标签 `v0.7.0-rc.0`。git-cliff 会自动将未发布区间渲染为 rc 的发布说明正文。
3. rc 期间发现一个 bug：
   - 在 `main` 上提交修复。
   - 通过 `git cherry-pick -x <sha>` 拣选到 `release/v0.7`。
   - 更新到 `v0.7.0-rc.1` 并打标签。
4. 当状态干净时，将发布分支从 `v0.7.0-rc.N` 更新到 `v0.7.0`。打标签 `v0.7.0`。git-cliff 会将完整的未发布区间（自 `v0.6.0` 起）渲染为稳定版发布说明。
5. `main` 已经处于 `0.8.0-dev.0`——稳定版发布后无需再更新。
6. 补丁（`v0.7.1`、`v0.7.2`……）通过从 `main` 拣选提交，来自同一条发布分支。

## 切出流程：`main` → `release/vX.Y`

1. 确认你在 `main` 上、工作树干净、且与 `origin/main` 同步。
2. 确认 `main` 上的 `.github/workflows/release.yml` 包含 `Classify release` 作业。若缺失，先在 `main` 上修复——否则稳定版标签会被发布为草稿。
3. 创建分支（若你使用裸仓库布局，请使用独立工作树，以便 `main` 保持检出状态）：

   ```bash
   git worktree add ../release-vX.Y -b release/vX.Y main
   # 或在单一检出仓库中：
   git checkout -b release/vX.Y
   ```

4. 在 `release/vX.Y` 上：
   - 将每个带版本号的制品更新到 `X.Y.0-rc.0`（见[需要更新的文件](#需要更新的文件)）。
   - 将新 rc 章节前置插入 `CHANGELOG.md`：

     ```bash
     git-cliff --tag vX.Y.0-rc.0 --unreleased --prepend CHANGELOG.md
     git add CHANGELOG.md
     # 合并到与版本更新相同的 chore(release) 提交中
     ```

     > **警告：** 不要使用 `git-cliff -o CHANGELOG.md`。该命令会整体重新生成文件，并将自上一个稳定版起的所有历史章节合并为一个区块。

   - 提交信息：`chore(release): cut release/vX.Y at vX.Y.0-rc.0`。
   - 推送：`git push -u origin release/vX.Y`。
5. 回到 `main` 后：
   - 将每个带版本号的制品更新到 `X.(Y+1).0-dev.0`（`main` 现在指向下一个次版本）。
   - 提交信息：`chore(version): move main to X.(Y+1).0-dev.0 marker`。
   - 推送。
6. 在发布分支上打标签 `vX.Y.0-rc.0`。

在 git-cliff 模型下**没有更新日志重命名步骤**。rc 的发布说明正文由约定式提交自动生成。

## 升级为稳定版：`release/vX.Y` → `vX.Y.0`

当 rc 状态干净时，在 `release/vX.Y` 上执行：

1. 将每个带版本号的制品从 `X.Y.0-rc.N` 更新到 `X.Y.0`。
2. 将稳定版章节前置插入 `CHANGELOG.md`：

   ```bash
   git-cliff --tag vX.Y.0 --unreleased --prepend CHANGELOG.md
   git add CHANGELOG.md
   # 合并到与版本更新相同的 chore(release) 提交中
   ```

   > **警告：** 不要使用 `git-cliff -o CHANGELOG.md`。该命令会整体重新生成文件，并将自上一个稳定版起的所有历史章节合并为一个区块。

3. 提交信息：`chore(release): promote release/vX.Y to vX.Y.0`。
4. 在发布分支上打标签 `vX.Y.0`，并推送分支与标签。

git-cliff 会基于自上一个稳定版标签以来的全部用户可见提交，生成经过整理的发布说明。没有手动整理更新日志的步骤。

> **可选的整理：** 若你想为稳定版发布说明正文添加人工编写的简介或编辑性说明，可在工作流发布后，直接在 GitHub Release 的编辑界面中操作。这不会改动 `CHANGELOG.md`。

## 下一开发周期

`main` 会在**切出 `release/vX.Y` 时**更新到 `X.(Y+1).0-dev.0`（见切出流程第 5 步）。稳定版标签发布后无需再更新 `main`。nightly 构建会从 `main` HEAD 自动持续进行，在整个稳定化窗口期间生成 `X.(Y+1).0-nightly+<sha>`。

## 需要更新的文件

每次发布，将以下所有文件更新为完全相同的版本：

- `Cargo.toml` — `[workspace.package].version`。工作区各 crate 通过 `version.workspace = true` 继承该版本。
- `flake.nix`
- `resources/windows/installer.iss`
- 人工审查（不继承）：`examples/custom_driver/Cargo.toml`。

在该标签的 GitHub Release 制品发布之后，还需更新：

- `nix/release-info.nix` — `version` 以及两个预构建 tarball 的 `url` 与 `hash`（见[本仓库的 Nix flake](#本仓库的-nix-flake) 下文）。这是一个按分支的渠道指针。它需要已发布的制品，因此在发布工作流完成后才作为后续提交合入。

AUR 的 `PKGBUILD` 位于**外部 AUR 仓库**，而非本仓库。它仅在稳定版标签时更新。

## nightly 工作机制

`.github/workflows/nightly.yml` 每天 UTC 时间 03:17 运行：

1. 从 `Cargo.toml` 读取工作区版本，剥离已有的预发布后缀，并追加 `-nightly+<short-sha>`（例如当 `main` 携带 `0.8.0-dev.0` 时生成 `0.8.0-nightly+abc1234`）。无需提交 `Cargo.toml`。由于从切出 `release/vX.Y` 的那一刻起，`main` 就跟踪**下一个**次版本，因此 nightly 版本始终明确地领先于正在稳定化的产品线。
2. 以 `channel: nightly` 调用 `build.yml`。
3. 计算每个 Linux tarball 的 SHA256 SRI 哈希，并使用真实哈希与滚动发布 URL 重新生成 `nix/nightly-info.nix`。
4. 将更新后的 `nix/nightly-info.nix` 提交到当前 `main` HEAD 之上。该提交**不会被推送到 `main`**——它将成为 `nightly` 标签唯一指向的目标。
5. 将 `nightly` 标签强制移动到被固定的提交，并推送该标签。仅推送标签就足以让该提交在远程可达；无需推送分支。
6. 发布或更新滚动 `nightly` 的 GitHub 预发布，附带新制品以及由 git-cliff 生成的、涵盖自上一个稳定版起所有提交的正文。该发布的标签指向被固定的提交，因此 `nightly` 引用上的 `nix/nightly-info.nix` 始终与已发布的制品一致。

`nightly` 标签会被强制推送，发布内容在每次运行时被替换。只有官方仓库（`0xErwin1/dbflux`）会执行该定时任务。

**当 `main` 没有推进时跳过。** 定时运行会先将当前 `main` HEAD 与上一个 nightly 所基于的提交（`git rev-parse nightly^`，即被固定提交的第一个父提交）进行比较。若两者相同，则整次运行直接跳过：不重新构建、不移动标签、不会对发布造成无意义变动。这避免了用一个新的、不可复现的哈希重新发布一份完全相同的构建，从而不必要地破坏 Nix 的固定引用。手动触发的 `workflow_dispatch` 运行始终会构建，即使没有新提交。

### Nix nightly 包

该工作流在每次运行时将 `nix/nightly-info.nix` 固定在 `nightly` 引用上。下游用户无需从源码编译即可获取预构建的 nightly 二进制：

```bash
# 直接运行 nightly
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly

# 安装到 profile
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

从源码构建的 nightly（无需哈希固定）同样可用：

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-source
```

**请勿从 `main` 分支获取 `#dbflux-nightly`。** 在 `main` 上，`nix/nightly-info.nix` 包含的是占位哈希，无法拉取。请始终使用上文所示的 `nightly` 引用。

## 拣选规范

发布分支绝不应包含 `main` 中没有的提交，发布专属提交（`chore(release): ...`、`chore(version): ...`）除外。

```bash
# 在 main 上：合入修复。
git checkout main
# …提交、推送…

# 在发布分支上：使用 -x 拣选以记录源 SHA。
git checkout release/vX.Y
git cherry-pick -x <sha>
```

审计：自分支切出以来，`release/vX.Y` 上每个非发布的提交，其信息中都应包含 `(cherry picked from commit ...)`。

```bash
git log --grep='cherry picked from' release/vX.Y
```

## 下游渠道

| 标签类型（GitHub Release）   | AUR       | Nix flake（本仓库）                              | nixpkgs（未来） |
|------------------------------|-----------|--------------------------------------------------|-----------------|
| nightly（预发布）            | 跳过      | 自动固定——nightly 引用上的 `#dbflux-nightly`    | 跳过            |
| `-rc.N`（预发布）            | 跳过      | 更新发布分支与 `main` 的 `release-info`          | 跳过            |
| 稳定版 `vX.Y.Z`（已发布）    | 更新并推送 | 更新发布分支与 `main` 的 `release-info`          | 更新并发起 PR   |

### AUR

AUR 的 `pkgver` 不允许包含 `-`（该字符保留给 `pkgrel`）。对于稳定版发布，转换无需改动（`pkgver=X.Y.Z`）。对于假设性的 AUR 预发布：

- `vX.Y.Z-rc.N` → `pkgver=X.Y.Z.rc.N`

### 本仓库的 Nix flake

该 flake 在 Linux（x86_64 与 aarch64）上提供以下包：

| 包                | 提供的内容                                              |
|-------------------|---------------------------------------------------------|
| `dbflux`（默认）  | 有预构建稳定版/rc 时提供预构建二进制，否则提供源码构建  |
| `dbflux-bin`      | 基于 `nix/release-info.nix` 的显式预构建                |
| `dbflux-source`   | 通过 crane 进行的源码构建（全平台）                     |
| `dbflux-nightly`  | 基于 `nix/nightly-info.nix` 的滚动 nightly 预构建（使用 nightly 引用） |

**稳定版 / RC（`nix/release-info.nix`）：** 按分支的渠道指针。`main` 跟踪任意类型中最新发布的标签；每条 `release/vX.Y` 跟踪各自产品线中最新的标签。在某标签的制品发布后，于该标签所推进渠道对应的每条分支上刷新 `release-info.nix`。

```bash
ver=X.Y.Z
for arch in amd64 arm64; do
  hex=$(curl -fsSL "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256" | awk '{print $1}')
  nix-hash --to-sri --type sha256 "$hex"
done
```

更新 `nix/release-info.nix` 中的 `version`、两个 `url` 与两个 `hash`。本地验证：

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

**nightly（`nix/nightly-info.nix`）：** 由 nightly 工作流在 `nightly` 引用上自动更新。请勿手动更新此文件。通过以下方式使用：

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
```

### nixpkgs（未来）

尚未合入上游。一旦合入，只有稳定版标签会向 `NixOS/nixpkgs` 提交 PR。PR 标题规范：`dbflux: A -> B`。

## 反模式（务必避免）

- 在 HEAD 位于 `main` 时打 `vX.Y.Z` 或 `vX.Y.Z-rc.N` 标签。
- 在 HEAD 位于 `main` 时打 rc 标签。
- 将 `release/vX.Y` 合并回 `main`。
- 在 `release/*` 分支上创建新功能（非修复提交）。
- 在 `release/*` 分支内更新次版本或主版本号。
- 在工作树不干净时推送标签。
- 推送 AUR 更新时 `pkgver` 含有连字符。
- 从 `main` HEAD 切出 `release/vX.Y`，但该 HEAD 的 `release.yml` 中不包含 `Classify release` 作业。
- 创建新的 `-dev.N` 标签（该渠道已废弃，请改用 nightly）。

## 打标签前的本地校验

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## 相关文件

- `.github/workflows/release.yml` — 分类逻辑与制品发布
- `.github/workflows/nightly.yml` — 每日 nightly 构建
- `.github/workflows/build.yml` — 可复用的构建作业（由 release 与 nightly 调用）
- `.github/release-template.md` — 追加到每个发布说明正文中的安装章节
- `cliff.toml` — 用于更新日志生成的 git-cliff 配置
- `skills/dbflux-release/SKILL.md` — 自动化此流程、面向智能体的技能
