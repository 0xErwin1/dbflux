# 安装 DBFlux

## Linux

### Tarball（推荐）

```bash
# 安装到 /usr/local（需要 sudo）
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# 安装到 ~/.local（无需 sudo）
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

### AppImage（便携版）

```bash
# 从 Releases 页面下载（ARM 架构请将 amd64 替换为 arm64）
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

### Arch Linux

可从 AUR 获取：

```bash
# 使用 AUR 助手
paru -S dbflux
# 或
yay -S dbflux
```

### Debian / Ubuntu

从 [Releases 页面](https://github.com/0xErwin1/dbflux/releases) 下载 `.deb` 安装包：

```bash
# ARM 架构请将 amd64 替换为 arm64
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.deb
sudo dpkg -i dbflux-linux-amd64.deb
```

### Fedora / RHEL / CentOS

从 [Releases 页面](https://github.com/0xErwin1/dbflux/releases) 下载 `.rpm` 安装包：

```bash
# ARM 架构请将 amd64 替换为 arm64
sudo dnf install https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.rpm
```

### Nix

使用 flakes（默认包为 Linux x86_64 / aarch64 的**预构建二进制文件**，无需编译）：

```bash
# 直接运行（预构建）
nix run github:0xErwin1/dbflux

# 安装到 profile（预构建）
nix profile install github:0xErwin1/dbflux

# 开发 shell
nix develop github:0xErwin1/dbflux
```

从源码构建，而不使用预构建二进制文件：

```bash
nix run    github:0xErwin1/dbflux#dbflux-source
nix build  github:0xErwin1/dbflux#dbflux-source
```

Nightly 构建跟踪 `main` 分支，与稳定版并列安装（应用 id、图标以及 `dbflux-nightly.db` 数据库均相互独立）。可通过 `nightly` ref 使用：

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

有关渠道模型的说明，参见[docs/RELEASE.md](RELEASE.md)。

通过 overlay 在 NixOS / nix-darwin 中使用：

```nix
{
  inputs.dbflux.url = "github:0xErwin1/dbflux";

  outputs = { nixpkgs, dbflux, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        ({ pkgs, ... }: {
          nixpkgs.overlays = [ dbflux.overlays.default ];
          environment.systemPackages = [
            pkgs.dbflux         # 预构建二进制文件，无需本地编译
            # pkgs.dbflux-source  # 另一种选择：从源码构建
          ];
        })
      ];
    };
  };
}
```

## macOS

DBFlux 的 macOS 版本未使用 Apple 开发者证书签名。首次打开时，会看到关于“身份不明的开发者”的警告。

### 安装

1. 从 [Releases 页面](https://github.com/0xErwin1/dbflux/releases) 下载适用于你的架构的 DMG：
   - **Intel Mac**：`dbflux-macos-amd64.dmg`
   - **Apple Silicon（M1/M2/M3/M4）**：`dbflux-macos-arm64.dmg`
2. 打开 DMG，将 DBFlux 拖入“应用程序”文件夹
3. 当看到“身份不明的开发者”警告时：
   - 前往 **系统设置 → 隐私与安全性**
   - 在安全警告旁点击**仍要打开**
   - 确认要打开该应用程序

### 从终端绕过 Gatekeeper

```bash
# 移除隔离属性（无需 GUI 确认即可打开）
xattr -cr /Applications/DBFlux.app

# 现在可以正常打开
open /Applications/DBFlux.app
```

### 系统要求

- macOS 11.0（Big Sur）或更高版本

## Windows

### 安装程序

1. 从 [Releases 页面](https://github.com/0xErwin1/dbflux/releases) 下载 `dbflux-windows-amd64-setup.exe`
2. 运行安装程序并按向导完成安装

### 便携版

1. 从 [Releases 页面](https://github.com/0xErwin1/dbflux/releases) 下载 `dbflux-windows-amd64.zip`
2. 解压到任意文件夹
3. 运行 `dbflux.exe`

> **注意**：该可执行文件未使用 Windows 代码签名证书签名，Windows SmartScreen 可能会显示警告。点击“详细信息” → “仍要运行”即可继续。

### 系统要求

- Windows 10 或更高版本
- x86_64（暂不支持 ARM64）

## 从源码构建

```bash
# 通过安装脚本（Linux）
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# 或手动构建
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux

# 推荐：使用完整的默认特性集构建
cargo build --release --features sqlite,postgres,mysql,mssql,mongodb,redis,dynamodb,cloudwatch,influxdb,clickhouse,lua,aws,mcp

# 最小构建（仅关系型驱动，无 AI/MCP，无 Lua）
cargo build --release --no-default-features --features sqlite,postgres,mysql

./target/release/dbflux
```

## 卸载（Linux）

```bash
# 如果通过 install.sh 安装
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# 从 ~/.local 卸载
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# 同时移除用户配置和数据
./scripts/uninstall.sh --remove-config
```

## 后续步骤

- [使用指南](USAGE.md) — 首次启动、创建连接并执行第一个查询
- [连接 — 高级设置](CONNECTIONS.md) — SSH 隧道、代理、AWS SSO 和值来源
