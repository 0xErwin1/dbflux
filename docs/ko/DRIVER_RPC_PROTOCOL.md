# Driver RPC 프로토콜 명세

이 문서는 DBFlux가 로컬 IPC를 통해 RPC 서비스를 발견하고, 실행하고, 통신하는 방법을 정의합니다.

DBFlux는 현재 두 개의 런타임 서비스 패밀리를 활성화합니다:

- `RpcServiceKind::Driver` -> 런타임 데이터베이스 드라이버
- `RpcServiceKind::AuthProvider` -> 앱과 MCP 서버의 런타임 인증 프로바이더 레지스트리

## 진실의 원천

활성 드라이버 서비스의 경우 **서비스가 다음 항목의 단일 진실 공급원(source of truth)**입니다:

- 드라이버 종류 (`DbKind`)
- 드라이버 메타데이터 (`DriverMetadataDto`: 이름, 아이콘, 카테고리, 기능, 쿼리 언어 등)
- 연결 폼 정의 (`DriverFormDefDto`)

DBFlux는 실행 구성을 SQLite 기반의 서비스 구성에 저장합니다. RPC 서비스는 **설정 → RPC 서비스**에서 만들고 편집합니다.

## 통합 모델

앱 시작 시 DBFlux는 `~/.local/share/dbflux/dbflux.db`에서 구성된 RPC 서비스를 불러온 뒤, 각 서비스에 대해:

1. 저장된 서비스 디스크립터(`RpcServiceKind` 포함)를 조회합니다
2. `kind`에 따라 분기합니다
3. 서비스가 실행 중인지 확인하고, 필요하면 시작합니다
4. 패밀리별 `Hello` 핸드셰이크를 수행합니다
5. 서비스에서 런타임 메타데이터를 읽어 옵니다
6. 변환된 런타임 서비스를 적절한 인메모리 레지스트리에 등록합니다

한 단계라도 실패하면 시작을 중단하지 않고 해당 서비스만 건너뜁니다. 드라이버 실패가 인증 프로바이더를 망가뜨리지 않고, 인증 프로바이더 실패가 드라이버를 망가뜨리지도 않습니다.

중요한 동작:

- 서비스 구성은 시작 시점에 읽힙니다. RPC 서비스 설정을 변경한 후에는 DBFlux를 다시 시작하세요.
- `socket_id`는 있는 그대로 사용됩니다(DBFlux가 재작성하지 않습니다).
- 내부 레지스트리 키는 `rpc:<socket_id>`입니다.

## 전송

DBFlux는 `interprocess`를 통해 로컬 소켓을 사용합니다:

- **Linux**: 추상 네임스페이스 Unix 소켓 (`\0name`)
- **macOS**: `/tmp/` 아래의 Unix 소켓
- **Windows**: 네임드 파이프 (`\\.\pipe\...`)

메시지는 다음처럼 프레이밍됩니다:

- 4바이트 리틀 엔디안 길이 (`u32`)
- bincode 페이로드

최대 메시지 크기: `16 MiB`.

소켓 정리는 프로세스 종료/drop 시 자동으로 이루어집니다(`interprocess`가 제공합니다).

## 런타임 구성

기본 저장소: `~/.local/share/dbflux/dbflux.db` (`cfg_services`, `cfg_service_args`, `cfg_service_env`)

설정 UI: **설정 → RPC 서비스**

참고:

- `socket_id`는 필수입니다.
- `kind`는 `driver`와 `auth_provider`를 지원합니다.
- `command`는 선택 사항입니다.
  - `command`가 없고 `args`가 비어 있으면 DBFlux는 서비스가 이미 실행 중이라고 가정합니다.
  - `driver`의 경우 `command`가 없고 `args`가 비어 있지 않으면 DBFlux가 `dbflux-driver-host`를 실행합니다.
  - `auth_provider`의 경우 관리형 실행에는 명시적인 `command`가 필요합니다. DBFlux는 기본 호스트 바이너리를 가정하지 않습니다.
- `args`, `env`, `startup_timeout_ms`는 선택 사항입니다.
- DBFlux는 `rpc:<socket_id>` 형태의 내부 드라이버 레지스트리 키를 만듭니다.
- `driver` 서비스만 데이터베이스 드라이버로 등록됩니다.
- `auth_provider` 서비스는 인증 프로바이더 레지스트리에만 등록되며 `rpc:<socket_id>` 드라이버 식별자를 받지 않습니다.

## 핸드셰이크 계약

DBFlux가 먼저 연결한 뒤 `Hello`를 보냅니다.

현재 활성 드라이버 RPC API 패밀리는 `driver_rpc`입니다. 현재의 전용 드라이버 RPC 전송에서는 이 패밀리가 `Hello` 동안 와이어로 전송되는 대신 프로토콜 자체에 암묵적으로 포함됩니다. 호환성은 드라이버 RPC 엔드포인트와 선택된 프로토콜 메이저 버전으로 강제되며, 마이너 버전은 추가 확장(additive)이며 해당 메이저 라인 안에서 결정적으로 협상됩니다.

클라이언트 요청:

```rust
DriverRequestBody::Hello(DriverHelloRequest {
    client_name: "dbflux_driver_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 0),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 3),
        ProtocolVersion::new(1, 4),
        ProtocolVersion::new(1, 5),
    ],
    requested_capabilities: vec![
        DriverCapability::Cancellation,
        DriverCapability::ChunkedResults,
        DriverCapability::SchemaIntrospection,
        DriverCapability::MultiDatabase,
    ],
})
```

서버 응답에는 다음이 포함되어야 합니다:

- `selected_version`
- `capabilities`
- `driver_kind`
- `driver_metadata`
- `form_definition`

예시:

```rust
DriverResponseBody::Hello(DriverHelloResponse {
    server_name: "my-driver".to_string(),
    server_version: "1.0.0".to_string(),
    selected_version: DRIVER_RPC_VERSION,
    capabilities: vec![DriverCapability::SchemaIntrospection],
    driver_kind: DbKind::SQLite,
    driver_metadata: DriverMetadataDto {
        id: "my-driver".to_string(),
        display_name: "My Driver".to_string(),
        description: "External RPC driver".to_string(),
        category: DatabaseCategory::Relational,
        query_language: QueryLanguageDto::Sql,
        capabilities: DriverCapabilities::RELATIONAL_BASE.bits(),
        default_port: None,
        uri_scheme: "mydriver".to_string(),
        icon: Icon::Database,
    },
    form_definition: DriverFormDefDto {
        tabs: vec![
            // ...
        ],
    },
})
```

호환되는 마이너 버전이 여러 개 겹치면 호스트는 서로가 공유하는 가장 높은 마이너 버전을 선택해야 합니다.

호환되는 버전이 없으면 `DriverRpcErrorCode::VersionMismatch`를 반환합니다.

`Hello` 이후의 모든 요청/응답 엔벨로프는 협상된 `selected_version`을 사용해야 합니다. 핸드셰이크 이후 다른 엔벨로프 버전을 받은 피어는 버전 불일치로 거부해야 합니다.

현재 검증 경계:

- DBFlux는 서비스별 API 패밀리/버전 메타데이터를 저장해 발견(discovery)과 향후 런타임 확장 지점(seam)에 대비합니다.
- 실제 드라이버 핸드셰이크는 현재 협상된 프로토콜 버전을 검증하지만, 드라이버 RPC 전송이 이미 패밀리 전용이므로 API 패밀리 문자열을 와이어로 전송하거나 별도로 재검증하지는 않습니다.

### 드라이버의 감사 이벤트 발행 (v1.2+)

`DriverCapability::AuditEmit`(driver RPC ≥ 1.2)을 광고하는 드라이버는 요청/응답 주기 동안 `EmitAuditEvent` 중간 프레임(`done=false`)을 보내 호스트 감사 로그에 기록할 수 있습니다. 호스트는 모든 이벤트를 `aud_audit_events`에 저장하기 전에 정제(sanitize)합니다.

허용되는 카테고리: `Connection`, `Query`, `System`. 그 외의 카테고리는 조용히 버려집니다.

호스트는 `AppState`에서 오는 신원 필드(`actor_type` → `ExternalDriver`, `actor_id`, `source_id`, `driver_id`, `correlation_id`)와 연결 컨텍스트를 덮어쓰고, `details_json`을 구성된 한도까지 자릅니다. 속도 제한은 인증 프로바이더와 공유됩니다: `socket_id`별로 60초마다 100개 이벤트이며, 초과분은 세션에 오류를 일으키지 않고 버려집니다. v1.2 미만으로 협상하거나 이 기능을 광고하지 않는 피어는 아무 이벤트도 보내지 않습니다. 전체 정제 계약은 [감사 § 외부 감사 이벤트 발행](AUDIT.md)을 참고하세요.

### 키-값 읽기 크기 게이트 (v1.3+)

`KeyGetRequest`는 `max_value_bytes: Option<u64>`를 담고 있으며, 이는 `KvGetKey` 호출이 전송할 수 있는 값 바이트의 선택적 상한입니다. `None`은 무제한을 뜻하며, v1.3 미만으로 협상하는 피어도 동일하게 동작합니다: 이 필드는 추가 확장(additive)이고 와이어 페이로드에 없을 때 `None`이 기본값이라, 오래된 드라이버와 호스트도 여전히 전체 값을 가져옵니다.

`KeyGetResult`는 `load_state: KeyLoadState`를 담고 있으며, `value`가 완전한 페이로드인지를 알려줍니다:

- `Loaded` — 전체 값을 가져왔습니다. 필드가 와이어 페이로드에 없을 때의 기본값입니다.
- `Truncated { returned_bytes, total_bytes }` — 값의 일부만 가져왔습니다(예: 컬렉션 타입에 대한 드라이버 쪽 항목 상한). 드라이버가 알고 있다면 `total_bytes`는 전체 크기입니다.
- `TooLarge { size_bytes, limit_bytes }` — `max_value_bytes`를 초과하여 값을 가져오지 않았습니다. `value`는 비어 있습니다.

두 필드 모두 기존 요청/응답 타입의 평범한 `#[serde(default)]` 구조체 필드이지 새 기능 플래그가 아닙니다: `Hello` 협상이 이를 게이트하지 않으며, `max_value_bytes`를 무시하는 드라이버는 단순히 항상 `Loaded`를 반환합니다.

### 대량 스키마 열 조회 (v1.4+)

`SchemaColumns { database, schema }`는 하나의 호출로 스키마 안의 모든 릴레이션의 열을 가져와 `SchemaColumns { columns: Vec<SchemaColumnInfo> }`로 응답하며, 각 항목은 평소의 `ColumnInfo` 옆에 자신의 `table_name`을 담고 있습니다. 호스트는 `SchemaIndexes` 및 `SchemaForeignKeys`와 정확히 같은 방식으로 이 요청을 연결의 `schema_columns` 확장 지점(seam)으로 전달합니다.

위의 v1.3 필드들과 달리 이 작업은 와이어상 추가 확장(additive)이 아닙니다: 프레임은 postcard로 인코딩되며, postcard는 열거형 변형(variant)에 이름이 아닌 varint 판별자 인덱스를 붙입니다. 따라서 `SchemaColumns`는 요청 열거형과 응답 열거형 양쪽 모두에서 마지막 v1.3 변형 뒤에 덧붙여집니다; 열거형 중간에 삽입하면 이후 모든 변형의 인덱스가 밀려나서, v1.3으로 협상한 피어가 밀려난 변형들을 잘못된 것으로 디코딩하고 프레임의 나머지 부분을 디코딩하지 못해 스트림이 어긋납니다. 클라이언트도 로컬에서 게이트합니다: `IpcConnection::schema_columns`는 `Hello`에서 선택된 버전을 검사하여 협상된 마이너가 1.4 미만이면 아무것도 보내지 않고 `DbError::NotSupported`를 반환하므로, 오래된 호스트는 새 변형을 전혀 받지 않습니다. 소비자는 이 오류를 트레이트 자체의 `NotSupported` 기본값과 동일하게 취급하며 테이블별 `table_details` 로드로 폴백합니다.

### 키 개수 (v1.5+)

`KvKeyCount { keyspace }`는 키스페이스의 키 개수를 요청하며(`None`은 세션의 현재 키스페이스를 뜻합니다) `KvKeyCountResult { count }`로 응답합니다. 호스트는 요청을 연결의 `KeyValueApi::key_count` 시임으로 전달합니다; 이를 구현하지 않은 드라이버는 트레이트의 `NotSupported` 기본값으로 응답하며, 이는 클라이언트에 `UnsupportedMethod`로 전달됩니다.

두 변형 모두 위에서 설명한 와이어 인덱스 이유로 각 열거형의 마지막 v1.4 변형 뒤에 덧붙여집니다. 클라이언트도 같은 방식으로 로컬에서 게이트합니다: 협상된 마이너가 1.5 미만이면 `IpcConnection::key_count`는 아무것도 보내지 않고 `DbError::NotSupported`를 반환합니다. 키 브라우저는 이 오류를 "전체 개수 없음"으로 취급하고 페이지 키 수만 표시합니다.

## 인증 프로바이더 RPC 계약

현재 활성 인증 프로바이더 RPC API 패밀리는 `1.3` 버전의 `auth_provider_rpc`입니다.

DBFlux는 저장된 `api_family` / `api_major` 메타데이터를 시작 프리플라이트로 사용합니다. 호환되는 항목은 이후 `Hello` 동안 서로가 공유하는 가장 높은 마이너 버전을 협상합니다.

클라이언트 요청:

```rust
AuthProviderRequestBody::Hello(AuthProviderHelloRequest {
    client_name: "dbflux_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 3),
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 0),
    ],
    auth_token: Some("<token>".to_string()),
})
```

서버 응답에는 다음이 포함되어야 합니다:

- `selected_version`
- `provider_id`
- `display_name`
- `form_definition`

v1.2 `Hello` 응답에는 `secret_dependency_opt_in` (`bool`)이 추가로 담기며, 이는 동적 옵션 조회를 위해 종속성 맵 안의 비밀 필드 값을 받는 것에 프로바이더가 선택(opt-in)했는지를 선언합니다. `false`(기본값)이면 DBFlux는 `FetchDynamicOptions` 요청을 전달하기 전에 종속성 맵에서 비밀 값을 제거합니다.

v1.3 `Hello` 응답에는 `audit_emit_opt_in` (`bool`)이 추가로 담깁니다. 감사 이벤트 발행을 사용하려면 `true`로 설정하세요(아래 참고). 기본값은 `false`입니다.

지원되는 요청/응답 흐름:

| 요청 → 응답 | 목적 |
|---|---|
| `Hello` → `Hello` | 프로토콜 협상 + 프로바이더 식별 |
| `ValidateSession` → `SessionState` | 캐시된 인증 상태 검증 |
| `Login` → `LoginUrlProgress?` + `LoginResult` | 선택적 검증 URL + 최종 로그인 결과 |
| `ResolveCredentials` → `Credentials` | 런타임 자격 증명 필드 해석 |
| `FetchDynamicOptions` → `DynamicOptions` | `DynamicSelect` 폼 필드의 동적 드롭다운 옵션 해석 (v1.2+) |
| (모든 요청) → `EmitAuditEvent` (중간) | 감사 이벤트 발행 (v1.3+) |

참고:

- `Login`은 `LoginResult` 전에 `LoginUrlProgress` 이벤트를 0개 또는 1개 발행할 수 있습니다.
- 진행 이벤트가 전송되지 않으면 DBFlux는 검증 URL 콜백을 `None`으로 취급합니다.
- `FetchDynamicOptions`는 협상된 버전이 `1.2` 이상일 때만 사용할 수 있습니다. v1.2 미만으로 협상하는 프로바이더는 IPC 왕복 없이 호스트로부터 영구적인 "not supported" 결과를 받습니다.
- `detect_importable_profiles`, 프로필 write-back 훅, 프로바이더별 값-프로바이더 등록은 이번 변경에서 RPC 계약의 범위 밖으로 의도적으로 제외되었습니다.
- 인증 프로바이더의 런타임 실패는 기존 `DbError` 처리를 통해 드러나며 시작을 중단하지 않습니다.

### 인증 프로바이더의 감사 이벤트 발행 (v1.3+)

v1.3 이상으로 협상하고 `audit_emit_opt_in: true`를 설정한 인증 프로바이더는 요청/응답 주기 동안 `EmitAuditEvent` 중간 프레임(`done=false`)을 보낼 수 있습니다. 호스트는 이를 정제한 뒤 `aud_audit_events`에 기록합니다.

허용되는 카테고리는 `Connection`뿐입니다. 그 외의 카테고리는 조용히 버려집니다.

`AuditEventEmitDto` 페이로드는 드라이버 발행 프레임과 동일한 구조를 따릅니다. 호스트는 신원 필드(`actor_type`, `actor_id`, `source_id`, `driver_id`, `correlation_id`)를 덮어씁니다. 속도 제한은 드라이버와 공유됩니다: `socket_id`별로 60초마다 100개 이벤트입니다.

## 폼 계약

DBFlux에 표시되는 연결 폼은 `Hello`에서 반환된 `form_definition`으로 만들어집니다.

- 서비스가 필드/탭/섹션을 정의합니다.
- DBFlux가 UI에서 필수 필드를 검증합니다.
- 연결/저장 시 DBFlux는 수집한 값을 `OpenSession` 프로필 JSON의 `DbConfig::External.values`로 보냅니다.

`form_definition.tabs`가 비어 있으면 연결 폼에 드라이버별 입력이 표시되지 않습니다.

## 세션 수명 주기

1. `Hello`
2. `OpenSession`
3. 요청/응답 작업
4. `CloseSession`

`OpenSession`은 여전히 메타데이터와 함께 `SessionOpened`를 반환합니다. 이 메타데이터를 `Hello`의 메타데이터와 일관되게 유지하세요.

DBFlux는 저장된 프로필 JSON을 `OpenSession`으로 전송합니다. 외부 드라이버의 경우 프로필 설정은 다음과 같습니다:

```rust
DbConfig::External {
    kind: DbKind,
    values: HashMap<String, String>,
}
```

`values`에는 `form_definition`에서 수집한 필드 값이 담깁니다.

서비스는 `profile_json`을 파싱하고 `DbConfig::External`을 예상하며, 필수 필드를 서버 측에서 다시 검증해야 합니다.

## 요청/응답 개요

| 요청 → 응답 | 용도 |
|---|---|
| `Hello` → `Hello` | 프로토콜 협상 + 드라이버 식별 정보 |
| `OpenSession` → `SessionOpened` | 연결/세션 열기 |
| `CloseSession` → `SessionClosed` | 세션 닫기 |
| `Ping` → `Pong` | 활성 여부 |
| `Execute` → `ExecuteResult` | 쿼리 실행 |
| `Schema` → `Schema` | 스키마 스냅샷 |
| `ListDatabases` → `Databases` | 데이터베이스 목록 |

이 프로토콜은 탐색, CRUD, 키-값, 코드 생성 작업도 지원합니다. 전체 열거형 집합은 `crates/dbflux_ipc/src/driver_protocol.rs`를 참고하세요.

**보호된 실행:** 외부 드라이버가 행 제한이나 문장 타임아웃을 적용한다고 보증할 수 있는 협상된 기능이 없으므로 보호된 쿼리는 거부됩니다. `IpcConnection`은 `QueryRequest.limit`가 `Some(n)`(`Some(0)` 포함)이거나 `statement_timeout`이 `Some(...)`이면 RPC를 보내기 전에 `Execute`와 `ExecuteWithHandle`을 `NotSupported`로 거부합니다. 호스트 세션 디스패치도 플러그인 연결을 호출하기 전에 같은 요청을 `UnsupportedMethod`로 독립적으로 거부합니다. 두 옵션이 모두 `None`인 요청은 계속 실행되며, 탐색 및 CRUD 작업은 영향을 받지 않습니다.

## 드라이버의 감사 이벤트 내보내기 (v1.2+)

프로토콜 버전 v1.2 이상으로 협상한 드라이버는 중간 응답 프레임(`done=false`)으로 감사 이벤트를 호스트에 되돌려 보낼 수 있습니다. 호스트는 이를 마스킹하고 속도를 제한한 뒤 `aud_audit_events`에 기록합니다.

### 선택 방법

`Hello` 응답의 `capabilities` 목록에 `DriverCapability::AuditEmit`를 포함하세요. 이 기능을 알리지 않는 드라이버가 보내는 `EmitAuditEvent` 프레임은 호스트가 조용히 버립니다.

### 감사 프레임 보내기

최종 응답 전에 요청 처리 중 어느 시점이든 `done = false`와 `body = DriverResponseBody::EmitAuditEvent(AuditEventEmitDto { .. })`를 담은 `DriverResponseEnvelope`를 내보내세요:

```rust
DriverResponseEnvelope {
    protocol_version: negotiated_version,
    request_id: request.request_id,
    session_id: request.session_id,
    done: false,
    body: DriverResponseBody::EmitAuditEvent(AuditEventEmitDto {
        ts_ms: chrono::Utc::now().timestamp_millis(),
        level: EventSeverityDto::Info,
        category: EventCategoryDto::Connection,
        action: "session.open".to_string(),
        outcome: EventOutcomeDto::Success,
        summary: "Database session opened".to_string(),
        object_type: None,
        object_id: None,
        duration_ms: Some(42),
        error_code: None,
        error_message: None,
        details_json: None,
    }),
}
```

그런 다음 평소처럼 최종 응답을 보냅니다.

### 호스트가 제공하는 값

호스트는 항상 다음 필드를 덮어씁니다. DTO에 포함하지 마세요(의도적으로 `AuditEventEmitDto`에 없습니다):

- `actor_type`, `actor_id`, `source_id`, `driver_id` — 항상 `ExternalDriver` / `rpc:<socket_id>`로 설정됩니다
- `connection_id`, `database_name` — 활성 세션 컨텍스트에서 해석됩니다
- `correlation_id` — 세션당 하나씩, 호스트가 생성합니다

### 허용되는 카테고리

드라이버는 `Connection`, `Query`, `System` 이벤트를 내보낼 수 있습니다. 다른 모든 카테고리는 조용히 버려집니다.

### 속도 제한

`socket_id`당 60초마다 100개의 이벤트입니다. 초과 프레임은 버려지고 `AuditService::external_audit_dropped_count()`에 집계됩니다.

## 오류 처리

구조화된 오류는 `DriverResponseBody::Error(DriverRpcError { ... })`를 통해 반환합니다.

주요 코드:

- `InvalidRequest`
- `UnsupportedMethod`
- `VersionMismatch`
- `SessionNotFound`
- `Timeout`
- `Cancelled`
- `Transport`
- `Driver`
- `Internal`

잘못된 형식의 프로필/폼 값에는 `InvalidRequest`를, 의도적으로 구현하지 않은 메서드에는 `UnsupportedMethod`를 사용하세요. 인증 공급자 RPC는 운영상 의미가 같은 별도의 `AuthProviderRpcErrorCode` 집합을 사용합니다(`VersionMismatch`, `UnsupportedMethod`, `Timeout`, `Transport` 등).

## 프로세스 수명 주기와 정리

DBFlux가 서비스 프로세스를 직접 시작할 때(`command` 또는 지원되는 기본 호스트 명령을 통해) 해당 프로세스는 관리 대상 호스트로 추적됩니다.

DBFlux 종료 시:

- 추적 중인 모든 관리 대상 호스트를 종료합니다 (`kill + wait`)
- DBFlux 밖에서 수동으로 시작한 호스트는 추적하지 않고 종료하지도 않습니다

이를 통해 DBFlux는 자신이 소유한 프로세스만 정리합니다.

관리 대상 호스트가 조기 종료되거나 소켓이 준비되기 전에 시간 초과되면, DBFlux는 문제 해결에 도움이 되도록 서비스 id와 최근 stdout/stderr의 길이 제한된 꼬리 부분을 함께 보고합니다.

## 최소 구현 체크리스트

서비스는 다음을 수행해야 합니다:

1. `interprocess`로 소켓을 바인딩합니다
2. `Hello`를 처리하고 메타데이터/종류를 반환합니다
3. `Hello`에서 폼 정의를 반환합니다
4. `OpenSession`/`CloseSession`을 처리합니다
5. 최소 하나의 유용한 작업(`Execute`)을 구현합니다
6. 구현하지 않은 작업에는 `UnsupportedMethod`를 반환합니다

권장 사항:

7. `OpenSession`에서 `DbConfig::External.values`를 검증합니다
8. 누락되었거나 잘못된 폼 값에는 명확한 `InvalidRequest` 오류를 반환합니다
9. `Hello` 메타데이터와 `SessionOpened` 메타데이터를 일관되게 유지합니다
10. 최신 상수를 가정하는 대신, `Hello` 이후의 모든 엔벨로프에 협상된 버전을 기입합니다

## 이 리포지토리의 작동하는 예제

다음을 사용하세요:

- `examples/custom_driver/src/main.rs`
- `examples/custom_driver/README.md`
- `examples/custom_auth_provider/src/main.rs`
- `examples/custom_auth_provider/README.md`

이 예제들은 현재 활성화된 드라이버 서비스 통합 모델과 호환됩니다.

빠른 테스트 경로:

1. **설정 → RPC Services**에서 새 **Driver** 서비스를 추가합니다
2. `command`가 빌드한 예제 바이너리를 가리키게 합니다
3. `args`를 `--socket <your-socket-id>`로 설정합니다
4. DBFlux를 다시 시작합니다
5. 서비스가 노출하는 UI 폼을 통해 연결(드라이버 예제) 또는 인증 프로필(인증 공급자 예제)을 만듭니다

## 참고 자료

- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux/src/app.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `docs/RPC_SERVICES_CONFIG.md`
