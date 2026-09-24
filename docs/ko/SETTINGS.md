# 설정 및 연결 훅

모든 설정 섹션과 연결 훅 — 연결의 수명 주기를 중심으로 DBFlux가 실행하는 명령,
스크립트 또는 Lua 스니펫 — 에 대한 참조 문서입니다.

명령 팔레트(**Open Settings**)나 사이드바에서 설정을 엽니다. 창의 왼쪽에는
섹션별로 정리되어 있습니다.

| 섹션 | 설명 |
|---------|--------|
| [일반](#일반) | 앱 전체 동작: 테마, 시작, 새로고침, 쿼리 안전. |
| [감사](#감사) | 감사 로그가 기록하는 내용과 보존 기간. |
| [키 바인딩](#키-바인딩) | 키맵 탐색(읽기 전용). |
| [인증 프로필](#인증-프로필-프록시-ssh-터널) | AWS SSO / 공유 자격 증명 프로필. |
| [프록시](#인증-프로필-프록시-ssh-터널) | SOCKS5 / HTTP 프록시 프로필. |
| [SSH 터널](#인증-프로필-프록시-ssh-터널) | 재사용 가능한 SSH 터널 프로필. |
| [서비스](#서비스rpc) | 외부 RPC 드라이버 및 인증 제공자. |
| [훅](#연결-훅) | 재사용 가능한 연결 훅 정의. |
| [드라이버](#드라이버) | 드라이버별 재정의 및 설정. |

MCP 관련 섹션(Clients, Roles, Policies)은 바이너리가 `mcp` 기능과 함께 빌드된
경우에만 나타납니다. [AI + MCP Integration](MCP_AI_INTEGRATION.md)을 참고하세요.

---
## 일반

### 모양

| 설정 | 옵션 | 기본값 |
|---------|---------|---------|
| **Theme** | Dark, Mirage, Light | Dark |
| **Style** | Default, Compact | Default |
| **Language** | System, 그 뒤에 번역 카탈로그가 함께 제공되는 모든 언어 | System |

언어 목록은 DBFlux가 함께 제공하는 번역 카탈로그에서 도출됩니다. 영어가 먼저
표시되고, 나머지 언어는 결정적인 순서로 이어지며 고유 언어명으로 표시됩니다.
System은 운영체제의 로캘을 따르며, 함께 제공된 로캘이 명확하게 일치하지 않으면
영어로 대체됩니다. 언어 변경은 DBFlux를 다시 시작해야 적용되므로, 해당 컨트롤에는
이를 알리는 고정된 안내 문구가 표시됩니다. 일부만 번역된 카탈로그는 번역되지 않은
일반 UI 텍스트에 대해 영어로 대체됩니다. 이 릴리스에서는 General 섹션만 번역되며,
나머지 UI는 크레이트 단위로 전환 중이며 당분간 영어로 유지됩니다.

### 편집기

| 설정 | 기본값 | 설명 |
|---------|---------|--------------|
| **Vim mode in code editors** | Off | 코드 편집기의 모드 편집: `h`, `j`, `k`, `l`, `i`, `Escape`, `x`, `u`를 쓰는 노멀 모드와 삽입 모드. 저장하면 열려 있는 편집기에 적용됩니다. [사용 가이드](USAGE.md) 키보드 참조의 Vim 모드 섹션을 참고하세요. |

### 시작 및 세션

| 설정 | 기본값 | 설명 |
|---------|---------|--------------|
| **Restore session on startup** | On | 마지막에 열어 두었던 탭을 다시 엽니다. |
| **Reopen last connections** | Off | 활성 상태였던 연결에 다시 연결합니다. |
| **Default focus** | Sidebar | 실행 시 포커스가 놓이는 위치(사이드바 또는 마지막 탭). |
| **Max history entries** | 1000 | 쿼리 기록 상한(최소 10). |
| **Auto-save interval (ms)** | 2000 | 열려 있는 편집기가 자동 저장되는 주기(최소 500). 파일로 관리되는 스크립트는 해당 파일에 기록되고, 저장하지 않은 제목 없는 콘텐츠는 sessions 폴더에 보관됩니다. |

### 새로고침 및 백그라운드

| 설정 | 기본값 | 설명 |
|---------|---------|--------------|
| **Default refresh policy** | Manual | 데이터 뷰에 대한 수동 또는 간격 자동 새로고침. |
| **Default refresh interval (seconds)** | 5 | 정책이 Interval일 때 사용되는 간격(최소 1). |
| **Max concurrent background tasks** | 8 | 동시에 실행할 수 있는 백그라운드 작업의 상한(최소 1). |
| **Pause auto-refresh on error** | On | 뷰에서 오류가 발생하면 자동 새로고침을 멈춥니다. |
| **Auto-refresh only if tab is visible** | Off | 보고 있지 않은 탭은 새로고침을 건너뜁니다. |

### 실행 안전(위험 쿼리 확인)

이 세 가지 설정은 **모든** 드라이버와 쿼리 언어에 걸쳐 DBFlux가 위험한 쿼리를
다루는 방식을 결정합니다. 데이터베이스별 토글은 없습니다 — 동일한 규칙이 SQL
`DELETE`/`DROP`/`TRUNCATE`, MongoDB `deleteMany`/`drop`, Redis `FLUSHALL`/`FLUSHDB`
등에 똑같이 적용됩니다.

| 설정 | 설명 |
|---------|--------------|
| **Confirm dangerous queries** | 기본적으로 켜짐; 위험한 쿼리를 실행하기 전에 확인을 표시합니다. 끄면 확인 절차 없이 실행할 수 있습니다. |
| **Require WHERE for DELETE/UPDATE** | 기본적으로 켜짐; `WHERE`가 없는 `DELETE`/`UPDATE`를 위험한 쿼리로 취급합니다. |
| **Always require preview (ignore suppressions)** | 기본적으로 꺼짐; 이전에 확인을 생략하기로 선택했던 쿼리에 대해서도 확인/미리 보기 모달을 강제합니다. |

### 스토리지(나이틀리 빌드 전용)

| 설정 | 기본값 | 설명 |
|---------|---------|--------------|
| **Use the stable database** | Off | 나이틀리 빌드가 `dbflux-nightly.db` 대신 안정(stable) 버전의 `dbflux.db`를 공유하도록 합니다. 다음 실행 시 적용됩니다. |

나이틀리 데이터베이스와 안정 데이터베이스가 어떻게 분리되어 있는지는
[Data & Privacy](DATA_AND_PRIVACY.md#데이터-위치)를 참고하세요.

---

## 감사

감사 섹션은 통합 감사 로그를 제어합니다. 사용자가 접하는 주요 컨트롤은
**Log Capture → Minimum Level**(trace / debug / info / warn / error)로, DBFlux의
내부 로깅 중 감사 추적에 얼마나 반영할지를 결정합니다. 저장하면 다시 시작하지
않고도 적용됩니다.

보존(Retention, 이벤트를 얼마나 유지하는지)이 설정되어 있으면 주기적인 백그라운드
제거가 수행됩니다. 일상적인 감사 사용 경험 — 뷰어 열기, 필터링, 내보내기 — 에
대해서는 [Dashboards & Audit](DASHBOARDS_AND_AUDIT.md#감사-뷰어)를 참고하세요.
전체 이벤트 스키마와 마스킹 동작은 [Audit](AUDIT.md) 및
[Data & Privacy](DATA_AND_PRIVACY.md#감사-및-개인정보)를 참고하세요.

---

## 키 바인딩

이 섹션은 **읽기 전용 뷰어**입니다. 컨텍스트별로 그룹화된 활성 키맵을 나열하며,
텍스트 필터와 함께 하나의 키 조합이 둘 이상의 명령에 바인딩된 경우 인라인 경고를
표시합니다. 현재는 UI에서 키를 다시 바인딩하거나 사용자 지정 단축키를 저장할 수
**없습니다**. 바인딩을 확인하고 검증하는 용도로 사용하세요. 전체 기본 키맵은
[Usage → Keyboard Reference](USAGE.md#7-키보드-참조)에 문서화되어 있습니다.

---

## 인증 프로필, 프록시, SSH 터널

이 세 섹션은 이후 각 연결의 Access 탭에서 선택하게 되는 재사용 가능한 프로필을
관리합니다. 필드, AWS SSO 흐름, no-proxy 규칙, SSH 인증 방법 등 전체 내용은
[Connecting to a Database → Advanced Setup](CONNECTIONS.md)에 문서화되어 있습니다:

- [인증 프로필](CONNECTIONS.md#인증-프로필-aws-sso-및-공유-자격-증명)
- [프록시](CONNECTIONS.md#프록시)
- [SSH 터널](CONNECTIONS.md#ssh-터널)

여기에 입력한 자격 증명은 데이터베이스가 아니라 운영체제 키링에 저장됩니다.
[Data & Privacy → Secrets](DATA_AND_PRIVACY.md#비밀과-os-키링)를
참고하세요.

---

## 서비스(RPC)

외부 드라이버와 인증 제공자는 별도의 프로세스로 실행되며, DBFlux는 로컬 소켓을
통해 이들과 통신합니다. 여기에 추가하는 각 서비스는 다음과 같은 항목을 갖습니다:

| 필드 | 설명 |
|-------|-------|
| **Socket ID** | 고유 식별자이며 소켓 파일 이름으로 사용됩니다. ASCII 알파벳, 숫자, `.`, `_`, `-`만 허용됩니다. |
| **Command** | 실행할 실행 파일(일부 구성에서는 선택 사항). |
| **Startup Timeout (ms)** | 프로세스가 뜰 때까지 기다리는 시간. 기본값 5000. |
| **Service Type** | **Driver** 또는 **Auth Provider**. |
| **Enable this service** | 서비스를 시작할지 여부. 기본적으로 켜짐. |
| **Arguments** | 순서가 있는 프로세스 인수. |
| **Environment Variables** | 프로세스에 전달되는 `KEY=value` 쌍. |

여기서 변경한 내용은 **다음 실행 시 적용됩니다**. 전체 참조:
[RPC Services Config](RPC_SERVICES_CONFIG.md) 및
[Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md).

---

## 드라이버

드라이버를 선택하면 해당 동작을 확인하고 재정의할 수 있습니다. 편집할 수 있는
그룹은 두 가지입니다:

**전역 재정의(Global overrides)** — General 설정의 드라이버별 버전입니다. 각 항목은
삼중 상태(Inherit / On / Off, 또는 명시적 값)이며, *Inherit*로 두면 컨트롤 옆에
표시된 General 기본값이 사용됩니다:

- 새로고침 정책 및 간격
- 위험 쿼리 확인
- WHERE 요구
- 미리 보기 요구

**드라이버 설정(Driver settings)** — 드라이버 자체가 정의한 옵션입니다(드라이버의
고유 스키마에서 일반적인 방식으로 렌더링되므로, 사용 가능한 필드는 드라이버에 따라
다릅니다).

이 섹션에는 드라이버의 **기능 매트릭스(capability matrix)**, 카테고리, 쿼리 언어도
읽기 전용으로 표시됩니다.

---

## 연결 훅

훅은 연결의 수명 주기를 중심으로 실행되는 재사용 가능한 명령, 스크립트 또는 Lua
스니펫입니다. **Settings → Hooks**에서 전역으로 훅을 **정의**하고, Connection
Manager의 **Hooks** 탭에서 개별 연결의 단계에 훅을 **바인딩**합니다.

### 빠른 경로

1. **Settings → Hooks에서 훅을 추가합니다.** **Hook ID**를 지정하고, **Type**을
   선택하고, 명령/스크립트를 채웁니다.
2. **Connection Manager → Hooks 탭**에서 연결을 엽니다.
3. 네 단계 드롭다운(Pre-connect, Post-connect,
   Pre-disconnect, Post-disconnect) 중 하나에서 훅을 선택합니다.
4. 연결합니다. 훅 출력은 **Tasks** 패널로 스트리밍됩니다.

### 훅 유형

| 유형 | 실행 내용 | 제공할 항목 |
|------|--------------|------------------|
| **Command** | 실행 파일 | 명령과 공백으로 구분된 인수. |
| **Script** | Bash 또는 Python 파일 | 언어, 파일 경로, 선택적인 인터프리터 재정의(비워 두면 플랫폼에 맞게 `bash` / `python3`). |
| **Lua** | 인프로세스 Lua 스크립트 | 파일 경로와 기능 집합(아래 참고). Lua는 DBFlux 내부에서 실행됩니다 — 외부 인터프리터가 필요 없습니다. |

스크립트는 DBFlux의 편집기에서 편집되며, 기본적으로 `hooks/` 폴더 아래에
저장됩니다.

#### Lua 기능

Lua 훅은 사용자가 활성화한 기능만 얻습니다:

| 기능 | 부여되는 권한 |
|------------|--------|
| **Logging** | 기본적으로 켜짐; 훅 출력에 기록합니다. |
| **Environment read** | 기본적으로 켜짐; 환경 변수를 읽습니다. |
| **Connection metadata** | 기본적으로 켜짐; 연결 중인 프로필의 메타데이터를 읽습니다. |
| **Controlled process run** | 기본적으로 꺼짐; `dbflux.process.run(...)`을 호출해 외부 프로세스를 실행할 수 있습니다. |

> **Controlled process run**을 활성화하면 훅이 임의의 외부 명령을 실행할 수
> 있습니다. 활성화하면 DBFlux가 훅 정의와 연결별 바인딩 양쪽에서 보안 경고를
> 표시합니다. 신뢰하는 훅에만 활성화하세요.

내장 Lua 런타임(사용 가능한 API, 샌드박싱)은 [Lua Scripting](LUA.md)에
문서화되어 있습니다.

### 훅 옵션

| 옵션 | 설명 |
|--------|-------|
| **Enabled** | 비활성화된 훅은 건너뜁니다. |
| **Working Directory** | 프로세스/스크립트의 작업 디렉터리(Lua에서는 사용하지 않음). |
| **Environment** | 추가 `KEY=value` 쌍. |
| **Inherit parent environment** | 기본적으로 켜짐; DBFlux의 환경을 훅에 전달합니다. |
| **Env Denylist** | 상속받은 환경에서 제거할 변수 이름. |
| **Timeout (ms)** | 비워 두면 시간 초과 없음. 시간 초과 시 프로세스 그룹을 종료합니다. |
| **Execution mode** | **Blocking**(기본값)은 훅이 끝날 때까지 기다립니다; **Detached**는 백그라운드에서 실행되며 연결/연결 끊기를 막지 않습니다. |
| **Ready signal** (Detached) | 계속 진행하기 전에 DBFlux가 훅 출력에서 기다리는 텍스트. |
| **On Failure** | 실패 정책 — 아래를 참고하세요. |

DBFlux는 항상 프로세스 훅에 컨텍스트 환경 변수를 주입합니다: `DBFLUX_PROFILE_ID`,
`DBFLUX_PROFILE_NAME`, `DBFLUX_DB_KIND`, 그리고 알려진 경우 `DBFLUX_HOST`,
`DBFLUX_PORT`, `DBFLUX_DATABASE`.

> **비밀은 실수로 훅에 새어 나가지 않습니다.** Env Denylist에 더해, DBFlux는
> 이름에 `SECRET`, `TOKEN`, `PASSWORD`, `_KEY`가 포함된 상속된 변수와 모든
> `AWS_*` 변수를 항상 제거합니다.

### 실패 정책

훅이 실패했을 때(0이 아닌 종료 코드, 시간 초과, 오류) 어떤 일이 일어나는지:

| 정책 | 효과 |
|--------|--------|
| **Disconnect** (기본값) | 단계를 중단합니다 — 연결 또는 연결 끊기 흐름이 멈춥니다. |
| **Warn** | 계속 진행하되 경고를 표시합니다. |
| **Ignore** | 계속 진행하며, 실패는 기록만 됩니다. |

### 단계

| 단계 | 실행 시점 |
|-------|------|
| **Pre-connect** | 연결이 열리기 전. |
| **Post-connect** | 연결에 성공한 후. |
| **Pre-disconnect** | 연결을 끊기 전. |
| **Post-disconnect** | 연결을 끊은 후. |

연결의 Hooks 탭에는 단계별 드롭다운이 하나씩 있습니다(추가 훅 ID를 바인딩하기 위한
"Extra" 입력도 있습니다). 드롭다운에는 Settings → Hooks에서 정의한 재사용 가능한
훅이 나열됩니다. 각 훅은 자체 백그라운드 작업으로 실행되며 Tasks 패널에 실시간
stdout/stderr가 표시됩니다. 출력은 훅당 4 MiB로 제한됩니다.

---

## 관련 문서

- [Usage Guide](USAGE.md) — 핵심 워크플로와 키보드 참조.
- [Connecting → Advanced Setup](CONNECTIONS.md) — SSH, 프록시, 인증, 값 소스.
- [Data & Privacy](DATA_AND_PRIVACY.md) — 설정과 비밀이 저장되는 위치.
- [Lua Scripting](LUA.md) — 훅을 위한 내장 Lua 런타임.
