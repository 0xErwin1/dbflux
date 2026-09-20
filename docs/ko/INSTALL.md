# DBFlux 설치

## Linux

### 타르볼 (권장)

```bash
# /usr/local에 설치 (sudo 필요)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# ~/.local에 설치 (sudo 불필요)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

### AppImage (포터블)

```bash
# 릴리스에서 내려받기 (ARM의 경우 amd64를 arm64로 바꾸세요)
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

### Arch Linux

AUR에서 이용할 수 있습니다:

```bash
# AUR 헬퍼 사용
paru -S dbflux
# 또는
yay -S dbflux
```

### Debian / Ubuntu

[Releases](https://github.com/0xErwin1/dbflux/releases)에서 `.deb` 패키지를 내려받습니다:

```bash
# ARM의 경우 amd64를 arm64로 바꾸세요
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.deb
sudo dpkg -i dbflux-linux-amd64.deb
```

### Fedora / RHEL / CentOS

[Releases](https://github.com/0xErwin1/dbflux/releases)에서 `.rpm` 패키지를 내려받습니다:

```bash
# ARM의 경우 amd64를 arm64로 바꾸세요
sudo dnf install https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.rpm
```

### Nix

flakes 사용 (기본 패키지는 Linux x86_64 / aarch64용 **사전 빌드된 바이너리**이며, 컴파일이 필요하지 않습니다):

```bash
# 직접 실행 (사전 빌드)
nix run github:0xErwin1/dbflux

# 프로필에 설치 (사전 빌드)
nix profile install github:0xErwin1/dbflux

# 개발 셸
nix develop github:0xErwin1/dbflux
```

사전 빌드된 바이너리 대신 소스에서 빌드하려면:

```bash
nix run    github:0xErwin1/dbflux#dbflux-source
nix build  github:0xErwin1/dbflux#dbflux-source
```

나이틀리 빌드는 `main`을 추적하며 안정 버전과 나란히 설치됩니다 (별개의 app id, 아이콘, `dbflux-nightly.db` 데이터베이스를 사용합니다). `nightly` ref에서 이용하세요:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

채널 모델에 대해서는 [docs/RELEASE.md](RELEASE.md)를 참고하세요.

overlay를 통한 NixOS / nix-darwin:

```nix
{
  inputs.dbflux.url = "github:0xErwin1/dbflux";

  outputs = { nixpkgs, dbflux, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        ({ pkgs, ... }: {
          nixpkgs.overlays = [ dbflux.overlays.default ];
          environment.systemPackages = [
            pkgs.dbflux         # 사전 빌드된 바이너리, 로컬 컴파일 없음
            # pkgs.dbflux-source  # 대안: 소스에서 빌드
          ];
        })
      ];
    };
  };
}
```

## macOS

macOS용 DBFlux는 Apple 개발자 인증서로 서명되어 있지 않습니다. 처음 열 때 "인증되지 않은 개발자"에 대한 경고가 표시됩니다.

### 설치

1. [Releases](https://github.com/0xErwin1/dbflux/releases)에서 아키텍처에 맞는 DMG를 내려받습니다:
   - **Intel Mac**: `dbflux-macos-amd64.dmg`
   - **Apple Silicon (M1/M2/M3/M4)**: `dbflux-macos-arm64.dmg`
2. DMG를 열고 DBFlux를 Applications 폴더로 드래그합니다
3. "인증되지 않은 개발자" 경고가 표시되면:
   - **시스템 설정 → 개인정보 보호 및 보안**(System Settings → Privacy & Security)으로 이동합니다
   - 보안 경고 옆의 **Open Anyway**를 클릭합니다
   - 애플리케이션을 열 것인지 확인합니다

### 터미널에서 Gatekeeper 우회

```bash
# 격리 속성 제거 (GUI 확인 없이 열 수 있도록 허용)
xattr -cr /Applications/DBFlux.app

# 이제 정상적으로 열 수 있습니다
open /Applications/DBFlux.app
```

### 요구 사항

- macOS 11.0 (Big Sur) 이상

## Windows

### 설치 프로그램

1. [Releases](https://github.com/0xErwin1/dbflux/releases)에서 `dbflux-windows-amd64-setup.exe`를 내려받습니다
2. 설치 프로그램을 실행하고 마법사를 따릅니다

### 포터블

1. [Releases](https://github.com/0xErwin1/dbflux/releases)에서 `dbflux-windows-amd64.zip`을 내려받습니다
2. 원하는 폴더에 압축을 풉니다
3. `dbflux.exe`를 실행합니다

> **참고**: 실행 파일은 Windows 코드 서명 인증서로 서명되어 있지 않습니다. Windows SmartScreen에서 경고가 표시될 수 있습니다. "More info" → "Run anyway"를 클릭하여 계속 진행합니다.

### 요구 사항

- Windows 10 이상
- x86_64 (ARM64는 아직 지원되지 않음)

## 소스에서 빌드

```bash
# 설치 스크립트를 통한 방법 (Linux)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# 또는 수동으로
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux

# 권장: 전체 기본 기능 세트로 빌드
cargo build --release --features sqlite,postgres,mysql,mssql,mongodb,redis,dynamodb,cloudwatch,influxdb,clickhouse,lua,aws,mcp

# 최소 빌드 (관계형 드라이버만 포함, AI/MCP 및 Lua 없음)
cargo build --release --no-default-features --features sqlite,postgres,mysql

./target/release/dbflux
```

## 제거 (Linux)

```bash
# install.sh로 설치한 경우
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# ~/.local에 설치한 경우
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# 사용자 설정과 데이터도 함께 제거
./scripts/uninstall.sh --remove-config
```

## 다음 단계

- [사용 안내](USAGE.md) — 첫 실행, 연결 만들기, 첫 쿼리 실행
- [연결 — 고급 설정](CONNECTIONS.md) — SSH 터널, 프록시, AWS SSO 및 값 소스
