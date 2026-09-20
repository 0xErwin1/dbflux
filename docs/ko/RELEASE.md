# 릴리스 프로세스

DBFlux는 **수명이 짧은 릴리스 브랜치를 곁들인 트렁크 기반 개발**을 사용합니다. 하나의 장기 브랜치(`main`)가 통합 대상이며, `release/vX.Y` 브랜치는 마이너 시리즈마다 안정화 기간에 생성되고 EOL 이후 폐기됩니다.

이 문서는 사람이 읽는 참조 문서입니다. 자동화된 `dbflux-release` 스킬(`skills/dbflux-release/SKILL.md`)도 동일한 규칙을 따릅니다.
## 채널

| 채널 (소스 브랜치)          | 태그 패턴           | GitHub 릴리스                      |
|-----------------------------|---------------------|-----------------------------------|
| **nightly** (`main` HEAD)   | `nightly` (롤링)    | 프리릴리스, 매일 cron으로 빌드      |
| **rc** (`release/vX.Y`)     | `vX.Y.Z-rc.N`       | 프리릴리스, 태그 푸시 시 빌드       |
| **stable** (`release/vX.Y`) | `vX.Y.Z`            | 게시됨, 태그 푸시 시 빌드           |

`-dev.N` 채널은 **폐기되었습니다**. nightly가 그 자리를 대체합니다. 기존 `-dev.N` 태그는 GitHub에 남아 있지만 새로 만들지는 않습니다.

채널별 애플리케이션 아이콘은 [issue #183](https://github.com/0xErwin1/dbflux/issues/183)에서 추적 중입니다. 이 페이지에서 구현하지 않습니다.

## 변경 내역 모델

아티팩트는 둘이고, 각각 하나의 작성 원본을 가집니다:

- **이 리포지토리의 `CHANGELOG.md`는 손으로 작성하는 공식 변경 내역입니다.** 각 릴리스의 큐레이션된 사용자 대상 산문을 담습니다.
- **GitHub 릴리스 본문은 생성됩니다.** 모든 채널에 대해 CI가 [git-cliff](https://git-cliff.org)로 conventional commits에서 생성합니다. 리포지토리 루트의 `cliff.toml`이 이를 설정하며, 절대 손으로 편집하지 않습니다.

하나의 커밋이 양쪽에 반영됩니다: 그 커밋의 conventional 타입이 생성되는 노트에서 어느 섹션에 들어갈지 결정하고, `## [Unreleased]` 아래에 작성한 불릿이 리포지토리 변경 내역에 무엇이 적히는지 결정합니다. 둘은 서로의 복사본이 아닙니다. 릴리스 본문은 간결하고(제목, PR 번호, 섹션), 리포지토리 변경 내역은 산문입니다.

### 리포지토리 변경 내역 (`CHANGELOG.md`)

- 사용자에게 보이는 모든 변경(`feat`, `fix`, `perf`)은 그 변경과 같은 커밋에서 `### Added`, `### Fixed`, 또는 `### Changed` 아래 `## [Unreleased]`에 불릿을 추가합니다. PR 템플릿의 체크리스트가 이를 요구합니다.
- `[Unreleased]`는 다음 **마이너**로 함께 출시될 작업을 모읍니다. rc와 nightly 태그는 투명하게 취급됩니다: 이 태그들이 `[Unreleased]`를 닫지는 않습니다.
- 안정 승격 시점에 제목 이름을 **한 번** 바꿉니다. `## [X.Y.0] - <date>`로, 릴리스 브랜치에서, 버전 올림과 같은 커밋에서 수행합니다(아래 안정으로 승격 참조). 릴리스된 섹션이 `main`으로 반입되면 `main`은 새로운 `[Unreleased]`를 엽니다.
- **패치가 예외입니다:** 패치 릴리스용 `## [X.Y.Z]` 섹션은 릴리스 브랜치에서 `git-cliff --prepend`로 생성됩니다. 패치 섹션은 큐레이션하지 않습니다.
- `git-cliff -o CHANGELOG.md`를 절대 실행하지 않습니다. 전체 재생성은 모든 과거 섹션을 마지막 안정 태그 이후의 하나의 범위로 접어 버립니다. 앞에 붙이거나(prepend) 손으로 편집하는 것이 유일하게 안전한 쓰기 방법입니다.

> **v0.7.0 전환:** 패치 섹션은 v0.7.x부터 git-cliff로 생성해 왔습니다. `## [0.6.0]`과 `## [0.6.0-dev.N]` 섹션은 `CHANGELOG.md`에 커밋된 손으로 작성된 기준선입니다. 절대 다시 생성해서는 안 됩니다 — 그렇게 하면 복제되거나 하나로 접혀 버립니다.

### 릴리스 본문 (GitHub)

- `.github/workflows/release.yml`이 git-cliff의 출력으로 구성합니다. `feat`, `fix`, `perf` 커밋은 노출되고, `chore`, `ci`, `docs`, `test`, `refactor`, `style` 커밋은 버려집니다. 보안과 관련된 변경은 `fix(security):` 또는 `Security:` 푸터를 사용합니다. 주요 변경(`feat!:`, `fix!:`, 또는 `BREAKING CHANGE:` 푸터)은 항상 노출됩니다.
- 안정 본문은 이전 **안정** 태그 이후의 모든 사용자 대상 커밋을 담습니다. rc와 nightly 태그는 투명하게 취급됩니다(`cliff.toml`의 `skip_tags`).
- 게시된 본문을 GitHub UI에서 편집하는 것은 허용됩니다. 편집용 서문이나 정정을 위해서입니다. 이는 `CHANGELOG.md`에 영향을 주지 않습니다.
- 버전에 대응하는 `## [X.Y.Z]` 섹션이 `CHANGELOG.md`에 없는 안정 태그는 게시하는 대신 릴리스 작업을 실패시킵니다.

## 브랜치

| 브랜치         | 수명      | 허용하는 변경                                     | 생성되는 태그            |
|----------------|-----------|--------------------------------------------------|--------------------------|
| `main`         | 영구      | 모든 새 커밋 (기능, 수정, 리팩터링)               | (없음 — nightly 롤링)    |
| `release/vX.Y` | EOL까지   | cherry-pick된 수정만 (새 기능 없음)               | `vX.Y.Z-rc.N`, `vX.Y.Z`, `vX.Y.(Z+1)` |

### 지켜야 할 필수 규칙

- 릴리스 브랜치에서 커밋을 **절대 직접 작성하지 않습니다**. 항상 `main`에 먼저 반영한 뒤 `git cherry-pick -x <sha>`로 릴리스 브랜치에 옮깁니다. 거기에서 작성되는 유일한 커밋은 릴리스 자체의 버전 아티팩트 올림으로서, `CHANGELOG.md` 제목 이름 바꾸기와 생성된 패치 섹션을 함께 담습니다.
- 릴리스 브랜치는 **절대** `main`으로 다시 병합하지 않습니다.
- 릴리스 브랜치는 만들어진 뒤 **새 기능을 받지 않습니다**. 버그 수정과 릴리스 자체의 버전 아티팩트 올림만 가능합니다.
- `main`은 항상 개발에 열려 있습니다. 모든 사용자 대상 변경은 그 변경을 만드는 같은 커밋에서 `[Unreleased]` 불릿을 추가합니다.

## 태그

태그는 주석이 달린(annotated) 태그여야 합니다:

```bash
git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
git push origin vX.Y.Z[-suffix.N]
```

릴리스 워크플로(`.github/workflows/release.yml`)는 태그를 자동으로 분류합니다:

| 태그 패턴 (허용된 소스 브랜치)      | GitHub 릴리스 종류  |
|-------------------------------------|---------------------|
| `release/vX.Y`의 `vX.Y.Z-rc.N`      | 프리릴리스          |
| `release/vX.Y`의 `vX.Y.Z`           | 안정 (게시됨)       |
| 그 외 모든 것 (안전망)              | 드래프트            |

## 버전 관리 규칙

워크스페이스 버전(`Cargo.toml`의 `[workspace.package].version`)이 참 원본입니다. 다른 모든 매니페스트는 이와 맞춰 유지되어야 합니다.

**`main`에서:**

매니페스트 버전은 `X.(Y+1).0-dev.0`이며, 여기서 `X.Y`는 현재 `release/vX.Y`에서 안정화 중인 마이너입니다. 이 마커는 `release/vX.Y`를 생성할 때 설정되며, 안정화 기간 내내와 그 이후, 다음 생성까지 `main`에 남아 있습니다. 이는 개발 마커일 뿐이며 `-dev.N` 릴리스는 절대 게시되지 않습니다. nightly 워크플로는 이 버전에서 프리릴리스 접미사를 제거하고 `-nightly+<short-sha>`를 붙여 `X.(Y+1).0-nightly+<sha>`를 만듭니다.

**`release/vX.Y`에서:**

- 다음 RC: 마지막 태그가 `vX.Y.Z-rc.N`이면 → `-rc.(N+1)`. 없으면 → `-rc.0`.
- 안정으로 승격: RC 접미사를 제거 → `vX.Y.0`.
- 패치: `Z`를 올림 → `vX.Y.(Z+1)`. 릴리스 브랜치에서 마이너를 절대 올리지 않습니다.

## 주기 예시: `0.7.0`

1. 기능이 `main`에 반영되며, 각 커밋은 같은 커밋에서 `CHANGELOG.md`의 `## [Unreleased]`에 불릿을 추가합니다.
2. 안정화할 준비가 되면, `main` HEAD에서 `release/v0.7`을 생성합니다.
   - `release/v0.7`에서: 모든 버전 관리 아티팩트를 `0.7.0-rc.0`으로 올립니다. 커밋하고 푸시합니다.
   - `main`에서: 모든 버전 관리 아티팩트를 `0.8.0-dev.0`으로 올립니다. 커밋하고 푸시합니다. `main`은 이제 다음 마이너를 목표로 합니다.
   - 릴리스 브랜치에 `v0.7.0-rc.0` 태그를 붙입니다. git-cliff는 미릴리스 범위를 RC 본문으로 자동 렌더링합니다.
3. RC 중 버그가 발견되면:
   - `main`에서 수정을 커밋합니다.
   - `git cherry-pick -x <sha>`로 `release/v0.7`에 옮깁니다.
   - `v0.7.0-rc.1`로 올리고 태그를 붙입니다.
4. 깨끗해지면, 릴리스 브랜치를 `v0.7.0-rc.N`에서 `v0.7.0`으로 올리고 같은 커밋에서 `CHANGELOG.md`의 최상위 제목을 `## [0.7.0] - <date>`로 이름 바꿉니다. `v0.7.0` 태그를 붙입니다. git-cliff는 `v0.6.0` 이후 전체 범위를 안정 릴리스 본문으로 렌더링합니다.
5. `main`은 이미 `0.8.0-dev.0`이므로, 안정 이후 추가 버전 올림은 필요하지 않습니다. 하나의 커밋이 릴리스된 `[Unreleased]` 섹션을 닫고 그 위에 새 섹션을 엽니다.
6. 패치(`v0.7.1`, `v0.7.2`, …)는 같은 릴리스 브랜치에서 `main`의 cherry-pick으로 만들어지며, 각 패치는 생성된 `## [0.7.N]` 섹션을 `CHANGELOG.md` 앞에 붙입니다.

## 아티팩트 식별과 서명

"이것을 누가 빌드했는가" 신호를 담는 별개의 세 가지가 있으며, 서로 겹치지 않습니다:

| 계층 | 다루는 것 | 시크릿 | 없을 때 |
|-------|----------------|--------|--------------|
| GPG 분리 서명(`.asc`)과 `.sha256` | 다운로드한 파일 | `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` | 빌드가 실패합니다. 모든 릴리스는 서명됩니다. |
| 빌드 출처 증명(attestation) | 아티팩트를 만든 워크플로 실행과 커밋 | 없음 (키 없는 OIDC) | 항상 켜져 있습니다. |
| `.app`의 macOS 코드 서명 | 키체인과 Gatekeeper가 보는 애플리케이션 식별 | `MACOS_CERTIFICATE_P12`, `MACOS_CERTIFICATE_PASSWORD` | 번들은 경고와 함께 애드혹 서명됩니다. |

Windows 실행 파일도 설치 관리자도 Authenticode로 서명되지 않으므로, 첫 실행에서 SmartScreen이 경고합니다. 이는 CA의 인증서가 필요하며 별도로 추적됩니다.

### macOS 코드 서명

macOS는 키체인 접근 권한을 그것을 요청한 애플리케이션의 서명에 묶습니다. 애드혹 서명은 빌드할 때마다 달라지므로, 안정적인 식별이 없으면 매 업데이트마다 DBFlux가 저장된 데이터베이스 비밀번호를 다시 읽을 수 있게 되기 전에 사용자가 로그인 비밀번호를 다시 입력해야 합니다. 모든 릴리스를 하나의 인증서로 서명하면 그 프롬프트는 한 번만 나타납니다.

자체 서명 인증서면 키체인에는 충분합니다. Gatekeeper는 **충족하지** 못합니다: 프로젝트가 유료 Developer ID 인증서와 공증(notarisation)을 갖추기 전까지 앱은 첫 실행에서 "확인되지 않음"으로 남습니다. 같은 두 시크릿이 이를 함께 담당합니다.

인증서는 한 번만 만들면 되며, OpenSSL이 있는 어떤 머신에서든 가능합니다:

```bash
scripts/macos-signing-cert.sh ~/secure/dbflux-signing
```

스크립트는 설정할 두 리포지토리 시크릿을 출력합니다. `.p12`와 그 비밀번호는 오래 보존될 곳에 보관합니다. 나중에 새 인증서를 발급하면 모든 사용자가 키체인을 한 번 더 다시 승인해야 합니다. 인증서는 10년간 유효합니다.

번들은 DMG가 만들어지기 전에 `build.yml`에서 서명됩니다. 작업은 인증서를 임시 키체인으로 가져오고, 러너에서 코드 서명용으로 신뢰하고, 서명하고, `codesign --verify --deep --strict`으로 검증한 뒤 키체인을 삭제합니다.

### Windows 실행 파일 식별

`crates/dbflux/build.rs`는 빌드 시점에 채널 아이콘과 `VERSIONINFO` 블록을 `dbflux.exe`에 넣으므로, Explorer, 작업 표시줄, 연결 프로그램 대화 상자가 DBFlux 아이콘을 표시하고 속성 창이 제품 이름과 버전을 표시합니다. 아이콘은 `packaging/icons/`(`dbflux.ico`, `dbflux-nightly.ico`) 아래에 커밋되어 있고, 같은 파일들이 포터블 zip과 설치 관리자 바로 가기에도 사용됩니다. 아트워크가 바뀌면 `resources/branding/<channel>/`에서 다시 생성합니다:

```bash
magick -background none resources/branding/stable/mark.svg -resize 256x256 256.png
# ... mark.svg에서 128, 64 생성; mark-small.svg에서 48, 32, 16 생성
magick 16.png 32.png 48.png 64.png 128.png 256.png packaging/icons/dbflux.ico
```

그 옆의 macOS `.icns` 파일들(`dbflux.icns`, `dbflux-nightly.icns`)은 `libicns`의 `png2icns`로 같은 방식으로 빌드되며, 512와 1024 px 크기를 추가하고, 번들에는 `AppIcon.icns`로 들어갑니다.

## 생성 절차: `main` → `release/vX.Y`

1. `main`에 있고, 트리가 깨끗하며, `origin/main`과 최신 상태인지 확인합니다.
2. `main`의 `.github/workflows/release.yml`에 `Classify release` 작업이 있는지 확인합니다. 없으면 먼저 `main`에서 고칩니다 — 그렇지 않으면 안정 태그가 드래프트로 게시됩니다.
3. 브랜치를 생성합니다. 베어 리포지토리 레이아웃을 사용한다면 `main`이 체크아웃된 상태로 유지되도록 전용 워크트리를 사용합니다:

   ```bash
   git worktree add ../release-vX.Y -b release/vX.Y main
   # 또는 단일 체크아웃 리포지토리에서는:
   git checkout -b release/vX.Y
   ```

4. `release/vX.Y`에서:
   - 모든 버전 관리 아티팩트를 `X.Y.0-rc.0`으로 올립니다([수정해야 할 파일](#수정해야-할-파일) 참조).
   - `CHANGELOG.md`는 그대로 둡니다. 브랜치는 `main`에서 물려받은 `[Unreleased]` 블록을 담고 있고, 여기에 cherry-pick되는 수정들은 각자의 불릿을 가져옵니다. 제목 이름은 안정 승격에서 바뀌며, 여기서가 아닙니다: RC는 기록으로서의 릴리스가 아닙니다.
   - 커밋: `chore(release): cut release/vX.Y at vX.Y.0-rc.0`.
   - 푸시: `git push -u origin release/vX.Y`.

5. 다시 `main`에서:
   - 모든 버전 관리 아티팩트를 `X.(Y+1).0-dev.0`으로 올립니다(`main`은 이제 다음 마이너를 목표로 합니다).
   - 커밋: `chore(version): move main to X.(Y+1).0-dev.0 marker`.
   - 푸시합니다.

6. 릴리스 브랜치에 `vX.Y.0-rc.0` 태그를 붙입니다.

RC 릴리스 본문은 conventional commits에서 자동 생성되므로, RC에는 CHANGELOG 단계가 전혀 필요하지 않습니다.

## 안정으로 승격: `release/vX.Y` → `vX.Y.0`

RC가 깨끗할 때 `release/vX.Y`에서 수행합니다:

1. 모든 버전 관리 아티팩트를 `X.Y.0-rc.N`에서 `X.Y.0`으로 올립니다.
2. 변경 내역 섹션을 닫습니다: `CHANGELOG.md`의 최상위 `## [Unreleased]` 제목을 `## [X.Y.0] - <date>`로 이름 바꾸고, 오늘 날짜를 넣습니다. `main`에서 모아져 cherry-pick으로 반입된 불릿이 이미 이 릴리스의 내용입니다. 여기서 파일에 생성되는 것은 없습니다.

   ```text
   ## [Unreleased]     ->     ## [X.Y.0] - 2026-07-31
   ```

   > **경고:** 생성된 섹션을 앞에 붙이지 않고, `git-cliff -o CHANGELOG.md`를 사용하지 않습니다. 리포지토리 변경 내역은 큐레이션되고, 릴리스 본문은 별도로 생성됩니다.

3. 커밋: `chore(release): promote release/vX.Y to vX.Y.0`.
4. 릴리스 브랜치에 `vX.Y.0` 태그를 붙이고 브랜치 + 태그를 푸시합니다.
5. CI가 이전 안정 태그 이후의 모든 사용자 대상 커밋에서 안정 릴리스 본문을 생성합니다.

릴리스 워크플로는 버전에 대응하는 `## [X.Y.Z]` 섹션이 `CHANGELOG.md`에 없는 안정 태그의 게시를 거부하므로, 2단계는 조용히 건너뛸 수 없습니다.

## 다음 개발 주기

`main`은 `release/vX.Y`를 **생성할 때** `X.(Y+1).0-dev.0`으로 올라갑니다(생성 절차, 5단계 참조). 안정 태그 이후 `main`에 추가 올림은 필요하지 않습니다. nightly 빌드는 안정화 기간 내내 `main` HEAD에서 자동으로 계속되어 `X.(Y+1).0-nightly+<sha>`를 만들어냅니다.

안정 태그가 푸시되면, `main`은 릴리스된 섹션을 같은 방식으로 닫는 하나의 커밋(`## [Unreleased]`를 `## [X.Y.0] - <date>`로 이름 바꾸기)을 받고, 그 위에 새 `## [Unreleased]`를 엽니다. 그래서 리포지토리 변경 내역은 릴리스된 이력을 유지합니다. `7a13aceb`이 그런 커밋의 예입니다. 생성 이후 `main`에 반영되었으나 함께 출시되지 않은 작업은 릴리스된 섹션이 아니라 새 `[Unreleased]` 아래에 들어갑니다. 그 구분이 이 모델의 유일한 수작업 구분입니다.

## 수정해야 할 파일

릴리스마다 다음 모두를 정확히 같은 버전으로 업데이트합니다:

- `Cargo.toml` — `[workspace.package].version`. 워크스페이스 크레이트는 `version.workspace = true`로 상속합니다.
- `flake.nix`
- `resources/windows/installer.iss`
- 수동 검토 대상(상속하지 않음): `examples/custom_driver/Cargo.toml`.

태그의 GitHub Release 아티팩트가 게시된 뒤에는 다음도 업데이트합니다:

- `nix/release-info.nix` — `version`과 두 prebuilt-tarball `url`과 `hash`(아래 [Nix](#nix-이-리포지토리의-flake) 참조). 이는 브랜치별 채널 포인터입니다. 게시된 아티팩트가 필요하므로, 릴리스 워크플로가 끝나면 후속 커밋으로 반영됩니다.

AUR `PKGBUILD`는 이 리포지토리가 아닌 **외부 AUR 리포지토리**에 있습니다. 안정 태그에만 올립니다.
## 나이틀리 동작 방식

`.github/workflows/nightly.yml`은 매일 03:17 UTC에 실행됩니다:

1. `Cargo.toml`에서 워크스페이스 버전을 읽고, 기존 프리릴리스 접미사가 있으면 제거한 뒤 `-nightly+<short-sha>`를 붙입니다(예: `main`이 `0.8.0-dev.0`을 담고 있을 때 `0.8.0-nightly+abc1234`). `Cargo.toml` 커밋은 필요하지 않습니다. `release/vX.Y`가 생성되는 순간부터 `main`은 **다음** 마이너를 추적하므로, 나이틀리 버전은 항상 안정화 라인보다 명확하게 앞서 있습니다.
2. `channel: nightly`로 `build.yml`을 호출합니다.
3. 각 Linux tarball의 SHA256 SRI 해시를 계산하고, 실제 해시와 롤링 릴리스 URL로 `nix/nightly-info.nix`를 다시 생성합니다.
4. 갱신된 `nix/nightly-info.nix`를 현재 `main` HEAD 위에 커밋합니다. 이 커밋은 **`main`으로 푸시되지 않으며** `nightly` 태그의 유일한 대상이 됩니다.
5. `nightly` 태그를 고정 커밋으로 강제 이동하고 태그를 푸시합니다. 태그를 푸시하는 것만으로 커밋이 원격에서 도달 가능해지며, 별도의 브랜치 푸시는 필요하지 않습니다.
6. 새 아티팩트와, 마지막 안정 태그 이후의 커밋을 다루는 git-cliff 생성 본문으로 롤링 `nightly` GitHub 프리릴리스를 게시하거나 갱신합니다. 릴리스의 태그는 고정 커밋을 가리키므로, `nightly` ref의 `nix/nightly-info.nix`는 항상 게시된 아티팩트와 일치합니다.

나이틀리 태그는 강제 푸시되고 릴리스는 매 실행마다 교체됩니다. 이 일정은 공식 리포지토리(`0xErwin1/dbflux`)에서만 실행됩니다.

**`main`이 진전되지 않았으면 건너뜁니다.** 정기 실행은 먼저 현재 `main` HEAD를 마지막 나이틀리가 빌드된 커밋(`git rev-parse nightly^`, 고정 커밋의 첫 번째 부모)과 비교합니다. 두 커밋이 같으면 실행 전체를 건너뜁니다: 재빌드도, 태그 이동도, 릴리스 갱신도 없습니다. 이는 재현할 수 없는 새 해시로 동일한 빌드를 다시 게시해 Nix 고정을 불필요하게 깨는 일을 피합니다. 수동 `workflow_dispatch` 실행은 새 커밋이 없더라도 항상 빌드합니다.

### Nix 나이틀리 패키지

워크플로는 매 실행마다 `nightly` ref의 `nix/nightly-info.nix`를 고정합니다. 다운스트림 사용자는 소스를 컴파일하지 않고도 미리 빌드된 나이틀리 바이너리를 받을 수 있습니다:

```bash
# 나이틀리를 직접 실행합니다
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly

# 프로필에 설치합니다
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

소스에서 빌드하는 나이틀리(해시 고정이 필요 없음)도 동작합니다:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-source
```

**`main`에서는 `#dbflux-nightly`를 사용해서는 안 됩니다.** `main`의 `nix/nightly-info.nix`는 가져올 수 없는 자리 표시자 해시를 담고 있습니다. 항상 위에 보인 것처럼 `nightly` ref를 사용해야 합니다.

## 체리픽 규율

릴리스 브랜치는 릴리스 전용 커밋(`chore(release): ...`, `chore(version): ...`)을 제외하면 `main`에 없는 커밋을 담아서는 안 됩니다.

```bash
# main에서: 수정 사항을 반영합니다.
git checkout main
# ...커밋, 푸시...

# 릴리스 브랜치에서: 출발 SHA를 기록하도록 -x로 체리픽합니다.
git checkout release/vX.Y
git cherry-pick -x <sha>
```

감사: 분기 이후 `release/vX.Y`의 모든 비(非)릴리스 커밋은 메시지에 `(cherry picked from commit ...)`를 포함해야 합니다.

```bash
git log --grep='cherry picked from' release/vX.Y
```

## 다운스트림 채널

| 태그 종류 (GitHub Release)  | AUR         | Nix flake (이 리포지토리)                    | nixpkgs (향후)   |
|-----------------------------|-------------|-----------------------------------------------|------------------|
| nightly (프리릴리스)        | 건너뜀      | 자동 고정 — nightly ref의 `#dbflux-nightly`   | 건너뜀           |
| `-rc.N` (프리릴리스)        | 건너뜀      | 릴리스 브랜치와 main의 `release-info` 갱신    | 건너뜀           |
| 안정 `vX.Y.Z` (게시됨)      | 갱신 + 푸시 | 릴리스 브랜치와 main의 `release-info` 갱신    | 갱신 + PR        |

### AUR

AUR `pkgver`에는 `-`를 사용할 수 없습니다(`pkgrel`용으로 예약되어 있습니다). 안정 릴리스에서는 변환이 없습니다(`pkgver=X.Y.Z`). 가상의 AUR 프리릴리스라면:

- `vX.Y.Z-rc.N` → `pkgver=X.Y.Z.rc.N`

### Nix (이 리포지토리의 flake)

이 flake는 Linux(x86_64 및 aarch64)에서 여러 패키지를 제공합니다:

| 패키지             | 제공 내용                                                    |
|-------------------|--------------------------------------------------------------|
| `dbflux` (기본값) | 사용 가능할 때는 미리 빌드된 안정/RC 바이너리, 그 외에는 소스 |
| `dbflux-bin`      | `nix/release-info.nix`의 명시적 프리빌드                     |
| `dbflux-source`   | crane을 통한 소스 빌드(모든 플랫폼)                          |
| `dbflux-nightly`  | `nix/nightly-info.nix`의 롤링 나이틀리 프리빌드(`nightly` ref 사용) |

**안정 / RC (`nix/release-info.nix`):** 브랜치별 채널 포인터입니다. `main`은 종류 불문 가장 최근에 게시된 태그를 추적하고, 각 `release/vX.Y`는 자기 라인의 최신 태그를 추적합니다. 태그의 아티팩트가 게시된 후에는 그 태그가 진전시키는 채널에 속한 모든 브랜치에서 `release-info.nix`를 갱신합니다.

```bash
ver=X.Y.Z
for arch in amd64 arm64; do
  hex=$(curl -fsSL "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256" | awk '{print $1}')
  nix-hash --to-sri --type sha256 "$hex"
done
```

`nix/release-info.nix`에서 `version`, 두 `url`, 두 `hash`를 갱신합니다. 로컬에서 검증합니다:

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

**나이틀리 (`nix/nightly-info.nix`):** `nightly` ref에서 나이틀리 워크플로가 자동으로 갱신합니다. 이 파일을 수동으로 갱신해서는 안 됩니다. 사용 방법:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
```

### nixpkgs (향후)

아직 업스트림에 없습니다. 업스트림화되면 안정 태그만 `NixOS/nixpkgs`로 풀 리퀘스트를 받습니다. PR 제목 관례: `dbflux: A -> B`.

## 안티 패턴 (하지 말아야 할 것들)

- HEAD가 `main`에 있는 상태에서 `vX.Y.Z` 또는 `vX.Y.Z-rc.N` 태그를 생성하는 것.
- HEAD가 `main`에 있는 상태에서 RC 태그를 생성하는 것.
- `release/vX.Y`를 `main`으로 되돌려 병합하는 것.
- `release/*` 브랜치에서 새 기능(수정이 아닌 커밋)을 만드는 것.
- `release/*` 브랜치 안에서 마이너 또는 메이저 버전을 올리는 것.
- 작업 트리가 깨끗하지 않은 상태에서 태그를 푸시하는 것.
- `pkgver`에 하이픈이 들어간 채 AUR 갱신을 푸시하는 것.
- `release.yml`의 `Classify release` 잡을 담고 있지 않은 `main` HEAD에서 `release/vX.Y`를 생성하는 것.
- 새 `-dev.N` 태그를 만드는 것(이 채널은 폐기되었으니 대신 나이틀리를 사용합니다).

## 태깅 전 로컬 검증

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## 관련 자료

- `.github/workflows/release.yml` — 분류 로직과 아티팩트 게시
- `.github/workflows/nightly.yml` — 매일의 나이틀리 빌드
- `.github/workflows/build.yml` — 재사용 가능한 빌드 잡(릴리스와 나이틀리가 호출)
- `.github/release-template.md` — 모든 릴리스 본문에 덧붙는 설치 섹션
- `cliff.toml` — 변경 내역 생성을 위한 git-cliff 설정
- `skills/dbflux-release/SKILL.md` — 이 과정을 자동화하는 에이전트용 스킬
