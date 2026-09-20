# DBFlux에 기여하기

기여를 고려해 주셔서 감사합니다. 이 가이드는 이슈를 등록하고, 풀 리퀘스트를 열고, DBFlux가 릴리스와 레이블에 사용하는 규칙을 따르는 방법을 설명합니다.
## 빠른 링크

- [아키텍처 개요](ARCHITECTURE.md)
- [드라이버 작성 가이드](docs/DRIVER_AUTHORING.md)
- [릴리스 프로세스와 브랜칭 모델](docs/RELEASE.md)
- [감사 이벤트 스키마](docs/AUDIT.md)
- [드라이버 RPC 프로토콜](docs/DRIVER_RPC_PROTOCOL.md)
- [Lua 스크립팅](docs/LUA.md)
- [MCP / AI 통합](docs/MCP_AI_INTEGRATION.md)

## 프로젝트 설정

DBFlux는 UI에 [GPUI](https://github.com/zed-industries/zed)를 사용하는 Rust
워크스페이스입니다. 전체 기능 세트에는 데이터베이스 드라이버 기능 플래그가
필요합니다:

```bash
cargo check --workspace
cargo build
cargo run
```

Linux에서 로컬 빌드에는 [`mold`](https://github.com/rui314/mold) 링커가
**필수**입니다: `.cargo/config.toml`이 `-fuse-ld=mold`로
`x86_64-unknown-linux-gnu` 대상을 링크해 워크스페이스 전체의 링크 시간과
메모리를 줄입니다. 패키지 관리자로 설치하세요(예: `apt install mold`); Nix
개발 셸은 자동으로 제공합니다. Windows와 macOS는 영향을 받지 않습니다.

PR을 열기 전에 다음을 실행합니다:

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

테스트는 [`cargo-nextest`](https://nexte.st)로도 실행할 수 있습니다(이
워크스페이스에서 더 빠르며, Nix 개발 셸이 제공합니다). nextest는 doctest를
실행하지 않습니다:

```bash
cargo nextest run --workspace
cargo test --doc --workspace
```

Nix 개발 셸을 사용할 수 있습니다: `nix develop`.

## 브랜칭 모델

DBFlux는 **수명이 짧은 릴리스 브랜치가 있는 트렁크 기반 개발**을 사용합니다:

- `main`이 유일한 장기 브랜치입니다. 모든 작업은 `main`을 대상으로 합니다.
- `release/vX.Y` 브랜치는 안정 릴리스를 위해 마이너를 안정화해야 할 때만
  `main`에서 만듭니다. `main`에서 체리픽한 수정만 받습니다 — 새 기능은 받지
  않습니다.

기여자는 PR을 **항상** `main`을 대상으로 해야 합니다. 릴리스 브랜치로의
백포트는 메인테이너의 책임입니다.

전체 규칙(태그, 버전 올리기, 브랜치 생성 절차, CHANGELOG 규율)은
[`docs/RELEASE.md`](docs/RELEASE.md)에 있습니다.

## 커밋 규칙

자연스럽게 맞는 곳에는
[Conventional Commits](https://www.conventionalcommits.org/)를 사용합니다:

- `feat(scope): …` — 새로운 사용자 대면 기능
- `fix(scope): …` — 버그 수정
- `refactor(scope): …` — 동작 변화가 없는 내부 변경
- `perf(scope): …` — 성능 개선
- `docs(scope): …` — 문서만 변경
- `test(scope): …` — 테스트만 변경
- `ci(scope): …` — CI / 릴리스 워크플로 변경
- `chore(scope): …` — 리포지토리 잡무(종속성, 도구, 버전 올리기)

스코프는 영향받은 영역입니다: 드라이버 이름(`postgres`, `mongodb`), `ui`,
`mcp`, `audit`, `rpc`, `release` 등. 제목은 70자 이내로 유지하고, 명확하지
않으면 본문에서 *이유*를 설명합니다.

## 풀 리퀘스트

1. `main`에서 브랜치를 만듭니다. PR은 하나의 관심사에 집중합니다.
2. [PR 템플릿](.github/pull_request_template.md)을 채웁니다: 요약, 무엇을
   해결하는지, 어떻게 해결했는지, 검증 증거, 어디서 테스트했는지.
3. 설명에 `Resolves #N`으로 닫는 이슈를 연결합니다.
4. 변경을 설명하는 레이블을 적용합니다. 아래
   [레이블 가이드](#레이블-가이드)를 참고하세요.
5. diff를 검토 가능하게 유지합니다. 메인테이너가 `size:exception`을 승인하지
   않는 한 약 400줄이 넘는 PR은 스택/체인 PR로 나눠야 합니다.
6. CI가 통과해야 합니다(`tests.yml`, `style.yml`). 실패하는 것이 있으면
   푸시하기 전에 로컬에서 다시 실행합니다.
7. 문서 변경은 번역과 함께 배포됩니다. `docs/` 아래의 페이지, 드라이버
   README, 또는 사이트가 렌더링하는 루트 문서(`ARCHITECTURE.md`,
   `CONTRIBUTING.md`, `SECURITY.md`, `TRADEMARK.md`, `PRIVACY.md`)를 편집할
   때는 같은 PR에서 `docs/es/`와 `docs/zh_Hans/` 아래의 기존 모든 대응
   문서에 같은 변경을 적용합니다. 아직 대응 문서가 없는 페이지는 필요하지
   않습니다. [번역](docs/TRANSLATIONS.md)을 참고하세요.


### 커밋 메시지와 변경 내역

DBFlux는 하나의 변경에서 두 산출물을 함께 관리합니다: 이 리포지토리의 정리된
`CHANGELOG.md`, 그리고 CI에서 [git-cliff](https://git-cliff.org)가 git
히스토리에서 생성하는 GitHub 릴리스 노트입니다. 변경과 같은 커밋에
`## [Unreleased]` 항목을 추가하세요; 커밋 타입이 생성되는 릴리스 노트에
무엇이 실리는지 결정합니다.

생성되는 릴리스 노트에 나타나는 규칙:

| 타입 | 변경 내역에 표시? |
|------|------------------------|
| `feat` | 예 — **Added** 아래 |
| `fix` | 예 — **Fixed** 아래 |
| `perf` | 예 — **Changed** 아래 |
| `refactor`, `test`, `ci`, `chore`, `docs`, `style`, `build` | 아니요 — 내부 전용 |
| `(security)` 스코프가 있거나 `Security:` 푸터가 있는 모든 타입 | 예 — **Security** 아래 |

호환성을 깨는 변경(`feat!:`, `fix!:`, 또는 `BREAKING CHANGE:` 푸터)은 타입과
무관하게 항상 표시됩니다.

**실제로 이것이 의미하는 것:**

- 사용자에게 보이는 변경은 **반드시** 타입으로 `feat`, `fix`, 또는 `perf`를
  사용하고 `## [Unreleased]` 아래(`### Added`, `### Fixed`, 또는
  `### Changed`)에 항목을 추가합니다. `chore`나 `refactor` 커밋은 사용자에게
  보이지 않습니다.
- 명확하고 명령형의 제목 줄을 쓰세요 — 생성되는 릴리스 노트의 항목이 그대로
  됩니다.
- `[Unreleased]` 항목은 검토자가 아니라 사용자를 위해 쓰세요: 리포지토리의
  변경 내역에 적히는 내용입니다.
- 하나의 PR에 내부 변경과 사용자 대면 변경이 모두 있으면 적절한 타입으로
  별도의 커밋으로 나눕니다.
- 보안 수정: `fix(security): ...`를 사용하거나 `Security: ...` 트레일러를
  추가해 Security 섹션에 실리게 합니다.

## 이슈

이슈를 열기 전에:

- 중복을 피하려면 기존 이슈를 검색하세요.
- 가능하면 최근 빌드에서 재현하세요.

포함할 내용:

- DBFlux 버전(`dbflux --version`), OS / 디스플레이 서버(Linux에서 X11 vs
  Wayland), 데이터베이스 엔진 + 버전.
- 재현 단계.
- 기대 동작과 실제 동작.
- 관련이 있다면 로그. 비밀은 가려서 올리세요.

이슈를 설명하는 레이블을 적용합니다.
[레이블 가이드](#레이블-가이드)를 참고하세요.

## 레이블 가이드

리포지토리는 구조화된 레이블 분류를 사용합니다. 이슈나 PR을 열 때 적용
가능한 각 축에서 **하나의 레이블**을 적용합니다. 메인테이너가 분류 중에
조정할 수 있습니다.

### Kind (영향받은 영역마다 `*:bug` 또는 `*:feature` 중 하나)

버그/기능 분리가 있는 영역:

| 영역      | 버그                | 기능              |
|-----------|--------------------|----------------------|
| AWS       | `aws:bug`          | `aws:feature`        |
| 감사      | `audit:bug`        | `audit:feature`      |
| 드라이버  | `driver:bug`       | `driver:feature`     |
| MCP       | `mcp:bug`          | `mcp:feature`        |
| 파이프라인| `pipeline:bug`     | `pipeline:feature`   |
| 프록시    | `proxy:bug`        | `proxy:feature`      |
| 쿼리      | `query:bug`        | `query:feature`      |
| RPC       | `rpc:bug`          | `rpc:feature`        |
| SSH       | `ssh:bug`          | `ssh:feature`        |
| 저장소    | `storage:bug`      | `storage:feature`    |
| UI        | `ui:bug`           | `ui:feature`         |

그리고 일반적인 GitHub 기본 레이블인 `bug`, `documentation`, `question`,
`help wanted`, `good first issue`, `invalid`이 있습니다.

### 서브시스템 플래그 (관련될 때 적용)

- `aws`, `proxy`, `ssh`, `query`, `driver`, `mcp`

### 드라이버 (변경이 드라이버별일 때)

`driver:mongodb`, `driver:postgres`, `driver:sqlite`, `driver:mysql/mariadb`,
`driver:dynamodb`, `driver:redis`

### 데이터 모델 종류 (스토어/드라이버 수준 작업)

`kind:sql`, `kind:document`, `kind:kv`, `kind:log`

### 플랫폼 / 아키텍처 (동작이 플랫폼별일 때)

- 플랫폼: `platform:linux`, `platform:macos`, `platform:windows`
- 아키텍처: `arch:amd64`, `arch:arm64`

### RPC 하위 유형 (RPC 기반 서비스를 다룰 때)

`rpc:auth`, `rpc:driver` (`rpc:bug`/`rpc:feature`에 추가로)

### 우선순위

`priority:high`, `priority:medium`, `priority:low` — 보통 메인테이너가 분류
중에 적용합니다.

### 상태 (메인테이너가 적용)

`status:needs-review`, `status:approved`, `status:rejected`

### 조합 예시

- Linux에서 PostgreSQL JSON 쿼리 버그:
  `driver:bug`, `driver:postgres`, `query:bug`, `platform:linux`, `kind:sql`
- 새로운 Redis pub/sub 기능:
  `driver:feature`, `driver:redis`, `kind:kv`
- Windows에서 MCP 승인 흐름 회귀:
  `mcp:bug`, `platform:windows`
- SSH 터널 UI 개선:
  `ui:feature`, `ssh:feature`, `ssh`

확실하지 않으면 최선을 다해 레이블을 붙이세요 — 메인테이너가 분류 중에
다듬습니다.

## 보안

보안 이슈를 공개적으로 등록하지 마세요. 메인테이너에게 이메일을 보내거나
비공개 채널을 사용하세요. 로그와 재현 자료는 토큰, 비밀번호, 연결 문자열 같은
비밀을 가려야 합니다.

## 라이선스

기여함으로써 기여 내용이 프로젝트의 듀얼 MIT / Apache-2.0 라이선스에 따라
라이선스가 부여되는 데 동의합니다. DBFlux 이름과 로고는 그 라이선스에 포함되지
않습니다; [TRADEMARK.md](TRADEMARK.md)를 참고하세요.
