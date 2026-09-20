# DBFlux

[English](README.md) · [Español](README.es.md) · **한국어** · [简体中文](README.zh_Hans.md)

Rust + GPUI 데스크톱 클라이언트로 제공되는, 확장 가능한 키보드 우선 데이터 플랫폼입니다.

**[dbflux.dev](https://dbflux.dev)** &middot; [문서](https://docs.dbflux.dev/) &middot; [설치](https://docs.dbflux.dev/install/)

## 개요

DBFlux는 관계형 및 비관계형 데이터베이스용 내장 드라이버를 갖춘 오픈 소스 데스크톱 클라이언트입니다. 핵심 계약은 드라이버 중립적이며, 외부 드라이버는 RPC를 통해 통합할 수 있습니다.

이 클라이언트는 성능, 깔끔한 UX, 키보드 우선 워크플로에 중점을 둡니다. 장기적인 목표는 사용하는 모든 데이터베이스를 다루는 하나의 완전한 오픈 소스 클라이언트입니다.

![DBFlux](resources/dbflux.png)

## 문서

아래의 모든 문서는 **[docs.dbflux.dev](https://docs.dbflux.dev/)**에 게시되며, 검색과 버전 선택기를 갖추고
이 동일한 파일들에서 렌더링됩니다. 여기의 링크는 소스를 가리킵니다. 원하시면 사이트에서 읽으셔도 됩니다.

하고자 하는 작업에 맞는 경로를 선택하세요.

### 여기서 시작하기

| 목표                                     | 가이드                                                                                                                                                                                              |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 연결 만들기                              | [사용 가이드](docs/USAGE.md#1-첫-실행-및-연결-만들기)부터 시작하세요. SSH 터널, 프록시, AWS SSO, 값 소스는 [연결 — 고급 설정](docs/CONNECTIONS.md)을 사용하세요.                                   |
| 쿼리 실행과 일반적인 워크플로 따라 하기  | 쿼리 작성, 결과 탐색, 차트 작성, 내보내기, 키보드 탐색은 [사용 가이드](docs/USAGE.md)를 따르세요.                                                                                                   |
| 감사 이벤트 보기                         | [대시보드 및 감사 사용자 가이드](docs/DASHBOARDS_AND_AUDIT.md#감사-뷰어)에서 감사 뷰어를 여세요.                                                                                                    |
| MCP 사용                                 | [AI + MCP 통합 가이드](docs/MCP_AI_INTEGRATION.md)를 따르세요.                                                                                                                                      |
| 드라이버 지원 현황과 제한 사항 확인      | 기능과 제한 사항의 공식 개요인 [드라이버 개요](docs/DRIVERS.md)를 사용하세요.                                                                                                                       |

### 추가 사용자 가이드

- [Settings & Hooks](docs/SETTINGS.md) — 설정, 연결 훅, 접근 프로필
- [Data & Privacy](docs/DATA_AND_PRIVACY.md) — 데이터와 비밀 저장, 백업, 재설정
- [Lua Scripting](docs/LUA.md) — 훅을 위한 내장 Lua 런타임

### 기여자

- [Contributing](CONTRIBUTING.md) — 설정, 검사, 기여 워크플로
- [Key Concepts](docs/CONCEPTS.md) — 계약과 하위 시스템 경계에 대한 짧은 개념 모델
- [Driver Authoring](docs/DRIVER_AUTHORING.md) — 내장 Rust 드라이버 또는 외부 RPC 드라이버 선택 및 구현
- [Architecture](ARCHITECTURE.md) — 크레이트 경계와 크레이트 간 흐름을 포함하는 공식 아키텍처 및 크레이트 맵

### 번역

DBFlux는 [Hosted Weblate](https://hosted.weblate.org/engage/dbflux/)에서 번역됩니다.
카탈로그는 `crates/dbflux_i18n/locales/`에 언어별 YAML 파일 하나씩으로 있으며,
번역 업데이트는 Weblate에서 풀 리퀘스트로 들어옵니다.
[번역 기여](docs/TRANSLATIONS.md)는 애플리케이션 UI, 문서, 웹 사이트라는
번역 가능한 모든 표면을 다룹니다.

<a href="https://hosted.weblate.org/engage/dbflux/"><img src="https://hosted.weblate.org/widget/dbflux/multi-auto.svg" alt="Translation status"></a>

### 참조

- [Charts](docs/CHARTS.md) — 차트 종류, 열 종류, 축 자동 감지
- [Dashboards](docs/DASHBOARDS.md) — 대시보드, 저장된 차트, 인스턴스 지표, 검사기
- [Audit](docs/AUDIT.md) — 감사 이벤트 스키마와 마스킹
- [Driver RPC Protocol](docs/DRIVER_RPC_PROTOCOL.md)
- [RPC Services Config](docs/RPC_SERVICES_CONFIG.md)
- [Release Process](docs/RELEASE.md)
- [Code Style](CODE_STYLE.md)
- [Agent Instructions](AGENTS.md)
- [Claude Instructions](CLAUDE.md)

## 설치

```bash
# Linux — /usr/local에 설치
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash
```

모든 플랫폼의 패키지 — tarball, AUR, `.deb`, `.rpm`, AppImage, Nix, macOS DMG,
Windows 인스톨러 — 는 [Releases](https://github.com/0xErwin1/dbflux/releases)
페이지에 있습니다. 서명되지 않은 macOS 및 Windows 빌드에 대한 Gatekeeper와
SmartScreen 단계를 포함한 전체 가이드는 [DBFlux 설치](docs/INSTALL.md)에 있습니다.

## 기능

### 데이터베이스 지원

- SSL/TLS 모드(Disable, Prefer, Require)를 지원하는 **PostgreSQL**
- PostgreSQL 와이어 프로토콜 위의 읽기 전용 SQL, SSH 터널링, TLS/클라이언트 인증서를 지원하는 **Amazon Redshift**
- **MySQL** / MariaDB
- 로컬 데이터베이스 파일용 **SQLite**
- TLS, SQL Browser 명명된 인스턴스 라우팅, 다중 스키마 인트로스펙션을 지원하는 **Microsoft SQL Server** (TDS)
- 컬렉션 탐색, 문서 CRUD, 셸 쿼리 생성을 지원하는 **MongoDB**
- 모든 타입(String, Hash, List, Set, Sorted Set, Stream)의 키 탐색을 지원하는 **Redis**
- 테이블 탐색, 항목 CRUD, AWS 인증을 지원하는 **DynamoDB**
- **InfluxDB** v1 및 v2 (v1은 InfluxQL, v2는 InfluxQL + Flux)
- HTTP(S)를 통한 **ClickHouse** 및 ClickHouse Cloud: 데이터베이스/테이블 탐색, 시각적 SELECT, 명시적 원시 SQL 실행을 지원합니다
- HTTP를 통한 **TursoDB** 및 libSQL (`sqld`): 스키마 탐색, 타입이 지정된 CRUD, 편집기 탭별 대화형 트랜잭션을 지원합니다
- 로그 그룹/스트림 탐색과 이벤트 스트리밍을 지원하는 **CloudWatch Logs**
- 버킷 탐색, 개체 미리 보기/편집, 전체 CRUD, 사전 서명된 URL을 지원하는 **Amazon S3** — S3 호환 엔드포인트(Cloudflare R2, MinIO) 포함
- **RPC를 통한 외부 드라이버** ([Driver RPC Protocol](docs/DRIVER_RPC_PROTOCOL.md)을 통해 프로세스 외부 드라이버 등록)

전체 기능 매트릭스와 드라이버별 제한 사항은 [docs/DRIVERS.md](docs/DRIVERS.md)를 참조하세요.

### 사용자 인터페이스

- 여러 결과 탭을 갖춘 문서 기반 워크스페이스 (DBeaver/VS Code와 유사)
- ToggleSidebar 명령(Ctrl+B)이 있는 접을 수 있고 크기 조절이 가능한 사이드바
- 대형 데이터베이스를 위한 지연 로딩이 있는 스키마 트리 브라우저
- 스키마 수준 메타데이터: 인덱스, 외래 키, 제약 조건, 사용자 정의 타입 (PostgreSQL)
- 스키마별 저장 프로시저/루틴 폴더 (해당 정보를 노출하는 드라이버)
- 구문 강조와 다중 문 실행이 있는 다중 탭 SQL 편집기 (드라이버가 지원하는 경우 문당 하나의 결과 집합)
- 열 너비 조절, 가로 스크롤, 정렬이 있는 가상화 데이터 테이블
- WHERE 필터, 사용자 지정 LIMIT, 페이지 나누기가 있는 테이블 브라우저
- 행/문서 세부 정보를 위한 워크스페이스 검사기 레일
- INSERT/UPDATE/DELETE를 SQL, MongoDB 셸 또는 Redis 명령으로 복사하는 "쿼리로 복사" 상황에 맞는 메뉴
- 언어별 구문 강조가 있는 쿼리 미리 보기 모달
- 퍼지 검색이 있는 명령 팔레트
- 자동 닫힘이 있는 사용자 지정 토스트 알림 시스템
- 백그라운드 작업 패널
- 세션 복원: 열려 있던 탭이 시작 시 복원되며, 파일과 다른 복구된 초안은 절대 그 파일을 덮어쓰지 않습니다

### 시각적 쿼리 빌더

- 오른쪽 레일 SELECT 빌더: 프로젝션, 조인, 중첩된 WHERE 술어 트리, ORDER BY, LIMIT/OFFSET, 실시간 매개변수화된 SQL 미리 보기
- 집계(COUNT, SUM, AVG, MIN, MAX)와 HAVING이 있는 GROUP BY
- 변경 정책(읽기 전용 / 승인 필요)과 청크 단위의 취소 가능한 실행이 있는 시각적 UPDATE / DELETE 빌더
- 빌더 입력과 결과 WHERE 필터에서의 스키마 인식 자동 완성
- 점으로 구분된 외래 키 경로를 통한 결과 필터 표시줄의 관계형 필터 (예: `created_by.email LIKE '%@acme.com'`)
- 빌더가 생성한 결과가 단일 테이블에 1:1로 대응하는 경우 인라인 셀 편집과 행 삭제
- 연결별 저장된 시각적 쿼리
- SQL 드라이버 전용 (SQLite, PostgreSQL, MySQL/MariaDB, SQL Server); 구조적으로 드라이버 중립적

### 차트 및 시각화

- 모든 쿼리 또는 컬렉션 결과를 차트로 작성: 선(Line), 막대(Bar), 산점도(Scatter), 영역(Area), 누적 막대(Stacked Bar), 파이(Pie)
- 열 종류를 기반으로 한 자동 축 감지 (타임스탬프 X축, 숫자 Y 계열) — 드라이버별 휴리스틱 없음
- 자체 문서 탭으로 다시 열리는 저장된 차트
- 대시보드: 저장된 차트, 구분선, 검사기 패널을 공유 시간 범위가 있는 12열 그리드에 배치
- 연결별 읽기 전용 인스턴스 개요 — 실시간 서버 지표와 표 형식 검사기, "편집 가능으로 저장" 지원; PostgreSQL, MySQL/MariaDB, MongoDB, Redis, SQL Server가 인스턴스 카탈로그를 제공
- 업스트림 공급자 대시보드 탐색 및 가져오기 (CloudWatch)
- 자세한 내용은 [docs/CHARTS.md](docs/CHARTS.md)와 [docs/DASHBOARDS.md](docs/DASHBOARDS.md)를 참조하세요.
### 연결 및 접근

- 키, 비밀번호, 에이전트 인증을 지원하는 SSH 터널; 재사용 가능한 SSH 터널 프로필
- 재사용 가능한 프록시 프로필을 지원하는 SOCKS5 / HTTP CONNECT 프록시 터널
- 포트를 노출하지 않고 연결하기 위한 관리형 접근 제공자(AWS SSM)
- 제공자 기반 인증 프로필(예: AWS SSO/shared/static), `~/.aws/config`에서 가져오기 지원
- PreConnect/PostConnect/PreDisconnect/PostDisconnect 단계에서 실행되는 연결 훅 — 명령, 스크립트 또는 인프로세스 Lua로 실행 가능

### AI 및 MCP 통합

- AI 클라이언트를 위한 내장 Model Context Protocol(MCP) 서버 (`dbflux mcp`)
- 거버넌스 계층: 작업 분류, 역할/정책 엔진, 신뢰할 수 있는 클라이언트, 쓰기/파괴적 작업에 대한 사람의 승인 흐름
- [docs/MCP_AI_INTEGRATION.md](docs/MCP_AI_INTEGRATION.md) 문서를 참고하세요

### 감사 및 스크립팅

- 쿼리, 연결, 훅, 스크립트, MCP, 거버넌스, 설정 이벤트를 대상으로 하며 마스킹과 쿼리 지문 생성을 지원하는 SQLite 기반 감사 로그 — [docs/AUDIT.md](docs/AUDIT.md) 문서를 참고하세요
- 중앙화된 사용자 대면 오류 보고: 실패는 상관 관계 ID와 '감사에서 보기' 작업이 있는 토스트로 표시되고, 상태 표시줄 오류 배지를 구동하며, 해당 감사 행과 연관됩니다
- Lua, Python, Bash 스크립트는 문서로 실행되며 실시간 스트리밍 출력을 제공합니다 — [docs/LUA.md](docs/LUA.md) 문서를 참고하세요

### 키보드 탐색

- 앱 전반에서 Vim 방식 탐색 (`j`/`k`/`h`/`l`)
- 상황 인식 키 바인딩 (Document, Sidebar, BackgroundTasks)
- 문서 포커스와 내부 편집기/결과 탐색
- 결과 도구 모음: `f` 포커스, `h`/`l` 탐색, `Enter` 편집/실행, `Esc` 종료
- `Ctrl+B`로 사이드바 전환
- `Ctrl+Tab` / `Ctrl+Shift+Tab`으로 탭 전환 (MRU 순서)

### 쿼리 관리

- 타임스탬프가 있는 쿼리 기록
- 즐겨찾기가 있는 저장된 쿼리
- 기록과 저장된 쿼리 전반의 검색

### 내보내기

- 형태 기반 내보내기: CSV, JSON (pretty/compact), 텍스트, 바이너리 (raw/hex/base64)
- 내보내기 형식은 결과 유형에 따라 결정됩니다 (테이블, JSON, 텍스트, 바이너리)

## 개발

### 사전 준비 사항

Linux에서는 로컬 빌드에 `mold` 링커가 **필수**입니다: 리포지토리의
`.cargo/config.toml`이 `x86_64-unknown-linux-gnu` 대상을 `-fuse-ld=mold`로
링크하여 60개 이상의 워크스페이스 크레이트 전반에서 링크 시간과 메모리
사용량을 줄입니다. Nix 개발 셸은 이를 자동으로 제공하며, Nix를 사용하지
않는 환경에서는 패키지 관리자를 통해 설치하세요(아래 포함). Windows와
macOS는 기본 링커를 사용하므로 영향을 받지 않습니다.

**Ubuntu/Debian:**

```bash
sudo apt install pkg-config libssl-dev libdbus-1-dev libxkbcommon-dev mold
```

**Fedora:**

```bash
sudo dnf install pkg-config openssl-devel dbus-devel libxkbcommon-devel mold
```

**Arch:**

```bash
sudo pacman -S pkg-config openssl dbus libxkbcommon mold
```

**macOS:**

```bash
# Xcode Command Line Tools (필수)
xcode-select --install
```

**Windows:**

```powershell
# C++ 워크로드가 포함된 Visual Studio Build Tools (필수)
# 다운로드: https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

### 빌드

```bash
cargo build -p dbflux --release
```

### 실행

```bash
cargo run -p dbflux
```

### 명령

```bash
cargo check --workspace                    # 타입 검사
cargo clippy --workspace -- -D warnings    # 린트
cargo fmt --all                            # 포맷
cargo test --workspace                     # 테스트
```

### nextest로 더 빠른 테스트 실행

[`cargo-nextest`](https://nexte.st)는 이 워크스페이스에서 권장되는 테스트
러너입니다: 전역 풀에 걸쳐 각 테스트를 자체 프로세스에서 실행하므로 이
규모의 워크스페이스에서 `cargo test`보다 눈에 띄게 빠릅니다. Nix 개발 셸이
이를 제공하며, 그렇지 않다면 <https://nexte.st/docs/installation>에서
설치할 수 있습니다.

```bash
cargo nextest run --workspace              # 단위 + 통합 테스트
cargo test --doc --workspace               # 문서 테스트 (nextest는 이를 실행하지 않음)
```

라이브 통합 테스트(보통 `#[ignore]`됨)는 nextest에서 다른 플래그를
사용합니다:

```bash
cargo nextest run -p dbflux_driver_sqlite --run-ignored all
```

### 웹사이트

`web/` 아래의 사이트는 Astro 정적 빌드입니다. `docs/`, 드라이버 README,
`ARCHITECTURE.md`, `CONTRIBUTING.md`를 게시된 버전별로 한 세트씩 git에서
직접 읽어오므로, 사이트에 표시되는 내용을 바꾸려면 문서를 편집하기만 하면
됩니다.

```bash
cd web
pnpm install
pnpm dev          # 로컬 서버
pnpm build        # web/dist에 정적 출력
pnpm check        # 타입 검사
pnpm format       # prettier
```

어떤 버전이 게시되는지는 `web/versions.json`에 선언되어 있습니다. 각 항목은
git ref를 가리키며, 해당 항목에 표시되는 제품 버전은 그 ref의 `Cargo.toml`에서
읽어옵니다.

`DOCS_MODE`는 문서가 어디에서 제공되는지 결정합니다: `embedded`(기본값, 모든
것을 하나의 오리진의 `/docs/` 아래에서 제공) 또는 두 호스트에 걸친 분리 배포를
위한 `site`와 `docs`. 로컬 개발에서는 기본값을 사용하므로 여전히 한 명령으로
사이트 전체를 띄울 수 있습니다.

### Nix 개발 셸

Nix를 사용한다면 모든 종속성이 포함된 개발 셸에 들어갈 수 있습니다:

```bash
# flakes 사용 시
nix develop

# 전통 방식
nix-shell
```

## 라이선스

MIT & Apache-2.0. DBFlux라는 이름과 로고는 코드 라이선스가 아닌 [상표 정책](TRADEMARK.md)의 적용을 받습니다.

DBFlux는 데이터를 수집하지 않습니다. [개인정보 처리방침](PRIVACY.md)을 참고하세요.
