# 핵심 개념

이 가이드는 기여자와 고급 사용자를 위한 간단한 멘탈 모델입니다.
하위 시스템 간의 계약을 설명하며, [아키텍처](../ARCHITECTURE.md)는 크레이트 경계와 주요 파일의
표준적이고 포괄적인 지도로 남습니다.
## 멘탈 모델

```text
UI 문서
  -> 앱 오케스트레이션 (프로필, 연결, 정책, 수명 주기)
    -> dbflux_core 계약 (메타데이터, 기능, 요청, 값)
      -> 내장 드라이버 또는 RPC 어댑터 드라이버
        -> QueryResult -> 범용 결과 뷰
        -> EventRecord -> 감사 싱크
```

중요한 방향은 안쪽입니다: 표현 계층과 워크플로는 계약에 의존하고, 드라이버 고유 동작은 그
계약 뒤에 머뭅니다. 감사는 별도의 실행 경로를 이루지 않고 흐름 전반의 작업을 관찰합니다.

## 개념 맵

| 개념 | 무엇인가 | 왜 중요한가 | 더 깊이 |
|---|---|---|---|
| 드라이버 | `DbDriver`와 `Connection` 계약의 구현체입니다. | 내장 데이터베이스와 외부 데이터베이스가 같은 경계를 통해 앱에 진입합니다. | [핵심 트레이트](../crates/dbflux_core/src/core/traits.rs), [드라이버 작성](DRIVER_AUTHORING.md) |
| `DriverMetadata` | 드라이버의 선언적 정체성, 카테고리, 언어, 폼, 상세 기능 디스크립터입니다. | 범용 워크플로는 구체적인 드라이버를 식별하지 않고도 표현과 동작을 선택할 수 있습니다. | [메타데이터 정의](../crates/dbflux_core/src/driver/capabilities.rs) |
| `DriverCapabilities` | 드라이버가 선언하고 연결이 노출하는 기능 플래그입니다. | UI는 데이터베이스 이름으로 추측하는 대신 지원되는 작업만 사용합니다. | [기능 플래그](../crates/dbflux_core/src/driver/capabilities.rs) |
| 문서 | 탭으로 관리되는 타입 소거된 창이며, 정체성과 이벤트 계약을 갖습니다. | 새 문서 타입이 닫힌 문서 열거형을 확장하지 않고도 참여할 수 있습니다. | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs), [`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) |
| 쿼리 결과 | 연결이 반환하는 구조화된 형태, 열, 행, 값입니다. | 범용 테이블, 트리, 텍스트 뷰, 내보내기, 차트가 하나의 결과 모델을 소비합니다. | [결과 타입](../crates/dbflux_core/src/query/types.rs), [`Value`](../crates/dbflux_core/src/core/value.rs) |
| MCP 거버넌스 | AI 도구를 둘러싼 신뢰할 수 있는 클라이언트, 연결, 분류, 정책, 승인, 감사 강제입니다. | 에이전트 접근은 명시적이고, 범위가 지정되며, 검토 가능하고, 관측 가능합니다. | [AI + MCP 통합](MCP_AI_INTEGRATION.md) |
| 감사 | 횡단 관심사인 `EventRecord`/`EventSink` 관측 가능성 확장 지점입니다. | 쿼리, 수명 주기 작업, 훅, 거버넌스, 외부 서비스가 상관 관계가 있는 추적을 공유합니다. | [감사 참조](AUDIT.md), [`EventSink`](../crates/dbflux_core/src/observability/source.rs) |
| 훅 | 연결 수명 주기 단계에 붙는 명령, 스크립트, 또는 Lua입니다. | 환경 설정과 정리가 드라이버 구현 밖에 남으며, 명시적인 실패 동작을 갖습니다. | [훅 계약](../crates/dbflux_core/src/connection/hook.rs), [설정과 훅](SETTINGS.md#연결-훅) |
| RPC 서비스 | 시작 시 드라이버 또는 인증 공급자로 어댑트되는 영속화된 디스크립터입니다. | 프로세스 외부 통합이 UI 특수 사례가 되지 않고 런타임에 참여합니다. | [RPC 구성](RPC_SERVICES_CONFIG.md), [프로토콜](DRIVER_RPC_PROTOCOL.md) |

## 드라이버는 UI 사례가 아니라 계약입니다

`DbDriver`는 데이터베이스 통합을 만들고 설명하며, `Connection`은 활성 연결에 대한 작업을
노출합니다. 두 계약의 현재 모습은
[`core/traits.rs`](../crates/dbflux_core/src/core/traits.rs)에 있습니다. 내장 크레이트는 이를
직접 구현하고, 외부 드라이버는 RPC를 통해 어댑트됩니다.

분리 규칙은 엄격합니다: UI와 앱 워크플로 코드는 구체적인 드라이버 ID가 아니라 범용
메타데이터, 기능, 계약을 통해 어댑트됩니다. 어떤 기능이 표현이나 워크플로 코드에서
`if driver == "postgres"`를 요구한다면, 빠져 있는 추상화는 메타데이터, 기능, 또는 코어
계약에 속합니다.

### 메타데이터와 기능

[`DriverMetadata`](../crates/dbflux_core/src/driver/capabilities.rs)는 드라이버가 무엇인지
설명합니다: 표시 정체성, `DatabaseCategory`, 쿼리 언어, 구문 및 작업 디스크립터, 제한, 그 외
범용 표현 입력입니다. `DriverCapabilities`는 지원하는 폭넓은 기능을 선언합니다. 연결은
동일한 메타데이터와 기능을 노출하므로 호출자는 원래 드라이버 개체를 알 필요가 없습니다.

메타데이터로 범용 모드를 선택하고 기능으로 작업을 게이트하세요. 드라이버 키, 아이콘,
네이티브 타입 이름 문자열, UI에 유지되는 목록으로 지원 여부를 추론하지 마세요.

## 문서는 열린 다형성입니다

[`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs)은 문서 다형성 확장 지점입니다. 이는
렌더링, 포커스, 명령, 메타데이터, 수명 주기 동작, 중복 제거, 구독을 위한 클로저 뒤에서 각
구체적 GPUI 엔터티를 타입 소거합니다. 따라서 워크스페이스는 문서를 구체적 문서 타입의 닫힌
열거형으로 모델링하지 않습니다.

[`DocumentKey`](../crates/dbflux_ui_document/src/dedup.rs)는 열린 문서의 정체성을 표현합니다.
각 창이 키와 일치하는지 스스로 판단하고,
[`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs)는 그 계약을 사용해 중복을
열지 않고 기존 탭에 포커스를 맞춥니다.

문서는 [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs)를 발생시킵니다. 탭
관리자와 워크스페이스는 구체적인 문서 구현으로 들어가지 않고 그 이벤트를 문서 간 액션으로
변환합니다. 워크스페이스 코드에 구체 타입 매칭을 추가하는 대신 이 확장 지점에서 창 동작을
추가하세요.

## 쿼리 결과는 구조화된 데이터입니다

현재 결과 경계는 [`QueryResult`](../crates/dbflux_core/src/query/types.rs)입니다: 선언된
`QueryResultShape`, `ColumnMeta` 항목, 코어 `Value` 행, 선택적 텍스트 또는 바이트, 실행
시간, 그리고 추가 결과 집합 가능성입니다.
[`Value`](../crates/dbflux_core/src/core/value.rs)는 모든 것을 JSON이나 표시 문자열로
환원하지 않고 관계형 값과 문서 값을 보존합니다.

구조화된 결과 값과 열 메타데이터는 범용 뷰에 공급됩니다. 특히 `ColumnMeta.kind`는
타임스탬프, 부동 소수점, 정수, 텍스트, 알 수 없음 같은 의미 유형 정보를 담습니다. 차트와
다른 소비자는 그 의미 유형을 사용하며, `ColumnMeta.type_name`을 훔쳐보거나 드라이버
정체성으로 분기해서는 안 됩니다.

## MCP 거버넌스가 실행을 감쌉니다

MCP 프로세스는 신뢰할 수 있는 클라이언트 정체성, 연결별 MCP 게이트, 실행 분류, 할당된 역할과
정책, 필요한 곳의 승인을 통해 요청을 권한 부여합니다. 결정과 실행은 감사됩니다. [거버넌스
모델](MCP_AI_INTEGRATION.md#3-거버넌스-모델-핵심-개념), [`dbflux_mcp` 권한
부여](../crates/dbflux_mcp/src/server/authorization.rs), [정책
엔진](../crates/dbflux_policy/src/engine.rs), [승인
서비스](../crates/dbflux_approval/src/service.rs)를 참고하세요.

안전 속성은 경계의 일부입니다:

- `preview_mutation`은 읽기 거버넌스를 따르며 읽기 전용 계획을 생성합니다; 변경을 실행하지
  않습니다. 구현은 메타데이터/읽기로 분류되지 않은 드라이버 생성 미리보기 쿼리를
  거부합니다([쿼리 도구](../crates/dbflux_mcp_server/src/tools/query.rs)).
- `select_data`는 요청된 조인을 조용히 무시하는 대신 현재는 거부합니다([읽기
  도구](../crates/dbflux_mcp_server/src/tools/read.rs)).
- 변경 미리보기는 DDL 미리보기 표면이 아닙니다. DDL 작업은 별도의 거버넌스 도구이며, 현재
  [도구 카탈로그](../crates/dbflux_mcp/src/tool_catalog.rs)에는 DDL 미리보기 도구가 노출되지
  않습니다.

분류, 정책, 승인, 감사 결정은 거버넌스 경계에 유지하세요. 핸들러는 하위 드라이버가 작업을
수행할 수 있다는 이유로 그 결정을 약화시켜서는 안 됩니다.

## 감사는 관측 가능성 확장 지점입니다

서비스는 [`EventSink`](../crates/dbflux_core/src/observability/source.rs)를 통해 표준
[`EventRecord`](../crates/dbflux_core/src/observability/types.rs) 값을 발생시킵니다. 레코드는
행위자, 소스, 카테고리, 결과, 대상 컨텍스트, 세부 정보, 상관 관계 필드를 담으며, 싱크는
검증과 저장 동작을 소유합니다.

이로써 감사는 횡단 관심사가 됩니다: 쿼리 실행, 연결 수명 주기, 훅, MCP 결정, 구성, 외부 RPC
서비스가 도메인 로직을 SQLite 구현에 결합하지 않고 관측될 수 있습니다. 스키마, 검증,
마스킹, 보존, 트레이싱 브리지 세부 사항은 [감사 참조](AUDIT.md)를 사용하세요.

## 훅은 연결 수명 주기를 감쌉니다

[`ConnectionHook`](../crates/dbflux_core/src/connection/hook.rs)는 `PreConnect`,
`PostConnect`, `PreDisconnect`, `PostDisconnect`에서 명령, 스크립트, 또는 Lua 작업을
정의합니다. 훅은 데이터베이스 드라이버의 쿼리 계약이 아니라 연결을 둘러싼 오케스트레이션에
속합니다.

실패 정책은 명시적입니다: `Disconnect`는 단계를 중단하고, `Warn`는 노출된 경고와 함께
계속하며, `Ignore`는 실패를 로깅하면서 계속합니다. 실행은 시간 초과, 환경, 준비 신호 제어와
함께 블로킹 또는 분리될 수 있습니다. 구성과 안전 세부 사항은 [설정과 연결
훅](SETTINGS.md#연결-훅)을 참고하세요.

## RPC 서비스는 런타임 디스크립터입니다

RPC 서비스는 `Driver` 또는 `AuthProvider`로 분류되는 영속화된 실행 및 호환성
디스크립터입니다. 시작 시
[`dbflux_app::rpc_services`](../crates/dbflux_app/src/rpc_services/)가 디스크립터를 발견하고,
적절한 프로토콜을 검증·프로브한 뒤, 성공한 서비스를 드라이버 또는 인증 공급자 레지스트리로
어댑트합니다. 한 서비스 계열의 실패가 다른 계열을 재정의하지 않습니다.

외부 드라이버 레지스트리 키는 `rpc:<socket_id>`로 남습니다. 인증 공급자는 자신의 공급자
정체성을 사용하며 데이터베이스 드라이버로 표시되지 않습니다. 외부 드라이버의 런타임
메타데이터는 UI 조건문이 아니라 그 핸드셰이크에서 옵니다. 영속화는 [RPC 서비스
구성](RPC_SERVICES_CONFIG.md)을, 전송, 협상, 수명 주기, 감사 이벤트 발생은 [드라이버 RPC
프로토콜](DRIVER_RPC_PROTOCOL.md)을 참고하세요.

## 어디를 수정할지

| 수정해야 할 대상 | 시작 위치 | 판별 규칙 |
|---|---|---|
| 모든 통합에서 사용 가능한 데이터베이스 작업 | [`DbDriver`/`Connection`](../crates/dbflux_core/src/core/traits.rs) | 드라이버를 구현하기 전에 범용 계약을 정의합니다. |
| 범용 기능이 표시되거나 허용되는지 여부 | [메타데이터와 기능](../crates/dbflux_core/src/driver/capabilities.rs) | 지원을 선언합니다; UI/워크플로 코드에서 드라이버를 식별하지 않습니다. |
| 결과 렌더링 또는 의미 유형 동작 | [쿼리 결과 타입](../crates/dbflux_core/src/query/types.rs) | 형태, 값, `ColumnMeta.kind`를 소비합니다. |
| 새 워크스페이스 문서 | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs)과 [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs) | 열린 창 확장 지점과 중복 제거 키를 구현합니다; 구체적 문서 합 타입을 확장하지 않습니다. |
| AI 도구 또는 실행 규칙 | [MCP 통합](MCP_AI_INTEGRATION.md)과 [권한 부여](../crates/dbflux_mcp/src/server/authorization.rs) | 분류, 정책, 승인, 감사 순서를 보존합니다. |
| 관측 가능한 도메인 액션 | [`EventRecord`와 `EventSink`](../crates/dbflux_core/src/observability/types.rs) | 확장 지점을 통해 발생시킵니다; 저장 세부 사항을 도메인 코드 밖에 둡니다. |
| 연결 설정 또는 정리 자동화 | [훅 계약](../crates/dbflux_core/src/connection/hook.rs) | 수명 주기 단계와 명시적 실패 모드를 선택합니다. |
| 프로세스 외부 드라이버 또는 인증 공급자 | [RPC 서비스](RPC_SERVICES_CONFIG.md) | 디스크립터를 영속화하고 `dbflux_app::rpc_services`를 통해 어댑트합니다. |

표준적인 크레이트 경계와 주요 파일은 [아키텍처](../ARCHITECTURE.md)에서, 내장 또는 외부
드라이버 구현은 [드라이버 작성](DRIVER_AUTHORING.md)에서 계속하세요.
