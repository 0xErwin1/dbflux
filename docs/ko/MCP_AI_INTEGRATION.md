# DBFlux AI + MCP 통합 가이드

이 가이드는 독립형 MCP 서버 바이너리를 통해 AI 에이전트를 DBFlux와 통합하는 방법을 설명합니다.

이 문서는 현재 사용할 수 있는 것과 아직 준비되지 않은 것을 의도적으로 명확하게 밝힙니다. 이를 통해 통합 작업이 구현되지 않은 동작에 의존하는 일이 없도록 하기 위함입니다.

## 1. 아키텍처 개요

DBFlux는 `dbflux mcp` 하위 명령을 통해 MCP 서버 기능을 노출하며, 이 명령은 stdio 위에서 Model Context Protocol을 사용합니다. AI 클라이언트(Claude Desktop, Cursor 등)는 이 바이너리를 하위 프로세스로 실행하고 JSON-RPC 2.0으로 줄바꿈 단위 통신을 합니다.

```
AI 클라이언트 (Claude Desktop / Cursor / 모든 MCP 클라이언트)
        |  stdio  (JSON-RPC 2.0, 줄바꿈으로 구분)
        v
  dbflux mcp                    ← 메인 dbflux 바이너리에 통합됨
        |
        +--  dbflux_mcp          거버넌스, 권한 부여, 도구 카탈로그
        +--  dbflux_core         프로필, 구성, 드라이버 트레이트
        +--  dbflux_driver_*     실제 데이터베이스 드라이버
        +--  dbflux_policy       정책 엔진
        +--  dbflux_audit        감사 기록 (SQLite)
```

MCP 서버와 DBFlux GUI 앱은 서로 독립적인 프로세스입니다. 두 프로세스는 `~/.local/share/dbflux/dbflux.db`에 있는 동일한 통합 SQLite 데이터베이스(프로필, 거버넌스, 감사, 기록, 세션)를 공유합니다. GUI에서 구성한 거버넌스(신뢰할 수 있는 클라이언트, 역할, 정책, 연결별 설정)는 서버가 시작 시점에 이 데이터베이스에서 읽습니다. `--config-dir` 플래그는 CLI 호환성을 위해 허용되지만 통합 데이터베이스의 위치를 바꾸지는 않으며, 거버넌스와 감사는 항상 `~/.local/share/dbflux/dbflux.db`에서 읽습니다.

## 2. MCP 서버 실행

### 빌드

```bash
# MCP 지원을 포함한 모든 드라이버 (기본값)
cargo build -p dbflux --release

# SQLite만 MCP와 함께 사용
cargo build -p dbflux --features sqlite,mcp --release

# MCP 지원 없음 (AI 통합 비활성화)
cargo build -p dbflux --no-default-features --features sqlite,postgres,mysql,mongodb,redis,dynamodb,lua,aws --release
```

MCP 서버는 메인 `dbflux` 바이너리에 통합되어 있습니다.

### 사용법

```
dbflux mcp --client-id <id> [--config-dir <path>]
```

| 플래그 | 설명 |
|------|-------------|
| `--client-id <id>` | 이 AI 클라이언트의 신원입니다. 거버넌스 설정에 등록된 신뢰할 수 있는 클라이언트와 일치해야 합니다. **필수.** |
| `--config-dir <path>` | CLI 호환성을 위해 허용됩니다. 거버넌스/감사 데이터베이스는 항상 통합 데이터베이스인 `~/.local/share/dbflux/dbflux.db`로 해석되며, 이 플래그는 그 위치를 바꾸지 않습니다. 격리된 테스트 환경에서는 대신 `HOME`/`XDG_DATA_HOME`을 재정의하세요. |

### Claude Desktop 설정

macOS의 `~/Library/Application Support/Claude/claude_desktop_config.json`에 추가하거나, 사용 중인 플랫폼의 동일한 역할을 하는 파일에 추가합니다:

```json
{
  "mcpServers": {
    "dbflux": {
      "command": "/path/to/dbflux",
      "args": ["mcp", "--client-id", "claude-desktop"]
    }
  }
}
```

`client-id` 값은 DBFlux GUI의 **설정 → MCP → 클라이언트**에서 만든 신뢰할 수 있는 클라이언트 항목과 일치해야 합니다.

**참고**: `mcp` 기능 없이(`--no-default-features`) DBFlux를 빌드했다면 MCP 서버를 사용할 수 없습니다.

## 3. 거버넌스 모델 (핵심 개념)

모든 AI 요청은 다음 계층 전부를 순서대로 거치며 강제됩니다:

1. **신뢰할 수 있는 클라이언트**: 요청자의 신원이 활성 상태이고 등록되어 있어야 합니다.
2. **연결 MCP 게이트**: 대상 연결에서 MCP가 활성화되어 있어야 합니다.
3. **정책 할당**: 액터가 해당 연결에 대해 범위가 지정된 할당을 가지고 있어야 합니다.
4. **도구 + 분류 허용 목록**: 도구 ID와 실행 클래스 둘 다 할당된 정책의 허용 목록에 있어야 합니다.
5. **승인 경로**: 쓰기/파괴적 흐름은 실행 전에 사람의 승인이 필요할 수 있습니다.
6. **감사 기록**: 모든 결정은 통합 SQLite 데이터베이스의 `aud_audit_events`에 추가되며 조회와 내보내기가 가능합니다. 전체 이벤트 스키마는 `docs/AUDIT.md`를 참조하세요.

여섯 계층은 모두 `tools/call` 요청마다 서버 프로세스 내부에서 실행됩니다. 어떤 계층도 클라이언트 쪽에서 우회할 수 없습니다.

## 4. 표준 도구 집합 (v1)

| 그룹 | 도구 ID | 클래스 | 설명 |
|-------|---------|-------|--------------|
| 연결 | `list_connections` | metadata | 구성된 모든 데이터베이스 연결을 나열합니다 |
| 연결 | `connect` | metadata | 구성된 연결에 대해 세션을 엽니다 |
| 연결 | `disconnect` | metadata | 열려 있는 세션을 닫습니다 |
| 연결 | `get_connection_info` | metadata | 드라이버 기능과 연결 메타데이터를 가져옵니다 |
| 스키마 | `list_databases` | metadata | 연결에서 접근할 수 있는 모든 데이터베이스를 나열합니다 |
| 스키마 | `list_schemas` | metadata | 데이터베이스 내의 스키마를 나열합니다 |
| 스키마 | `list_tables` | metadata | 스키마 내의 테이블과 뷰를 나열합니다 |
| 스키마 | `list_collections` | metadata | MongoDB 컬렉션을 나열합니다 |
| 스키마 | `describe_object` | metadata | 테이블의 열/필드 정의와 인덱스를 가져옵니다 |
| 읽기 | `select_data` | read | 테이블이나 컬렉션에 대해 구조화된 `SELECT`를 실행합니다. 지원되지 않는 `joins`는 명시적으로 거부됩니다 |
| 읽기 | `count_records` | read | 대상의 행/문서 개수를 반환합니다 |
| 읽기 | `aggregate_data` | read | 읽기 전용 집계 파이프라인을 실행합니다 |
| 읽기 | `explain_query` | read | 대상 변경을 실행하지 않고 쿼리 실행 계획을 보여줍니다 |
| 읽기 | `preview_mutation` | read | 쓰기 쿼리에 대한 읽기 전용 미리보기/계획을 반환합니다. 항상 읽기 전용이며 변경은 절대 실행되지 않습니다 |
| 쓰기 | `insert_record` | write | 단일 레코드를 삽입합니다 |
| 쓰기 | `update_records` | write | 필터와 일치하는 레코드를 업데이트합니다 |
| 쓰기 | `upsert_record` | write | 키를 기준으로 단일 레코드를 삽입하거나 업데이트합니다 |
| 쓰기 | `delete_records` | destructive | 필터와 일치하는 레코드를 삭제합니다 |
| 파괴적 | `truncate_table` | destructive | 테이블에서 모든 행을 제거합니다 |
| DDL | `create_table` | admin | 테이블을 만듭니다 |
| DDL | `alter_table` | admin_safe / admin / admin_destructive | 테이블을 변경합니다. 분류는 변경 종류별로 계산됩니다 |
| DDL | `create_index` | admin | 인덱스를 만듭니다 |
| DDL 파괴적 | `drop_index` | admin_destructive | 인덱스를 삭제합니다 |
| DDL | `create_type` | admin | 사용자 정의 타입을 만듭니다 |
| DDL 파괴적 | `drop_table` | admin_destructive | 테이블을 삭제합니다 |
| DDL 파괴적 | `drop_database` | admin_destructive | 데이터베이스를 삭제합니다 |
| 스크립트 | `list_scripts` | metadata | 스크립트 디렉터리에 저장된 스크립트를 나열합니다 |
| 스크립트 | `get_script` | read | 특정 저장 스크립트의 소스를 가져옵니다 |
| 스크립트 | `create_script` | write | 새 스크립트를 스크립트 디렉터리에 저장합니다 |
| 스크립트 | `update_script` | write | 기존 저장 스크립트를 덮어씁니다 |
| 스크립트 | `delete_script` | admin | 스크립트를 영구적으로 제거합니다 |
| 스크립트 | `execute_script` | computed | 연결에 대해 저장된 스크립트를 실행합니다. 분류는 스크립트 본문에서 도출됩니다 |
| 승인 | `request_execution` | admin | 실행 전에 사람의 승인을 받도록 변경을 제출합니다 |
| 승인 | `list_pending_executions` | read | 승인 대기 중인 모든 실행을 봅니다 |
| 승인 | `get_pending_execution` | read | 특정 대기 중 실행의 세부 정보를 가져옵니다 |
| 승인 | `approve_execution` | admin | 대기 중인 변경을 승인합니다 (관리자 전용) |
| 승인 | `reject_execution` | admin | 대기 중인 변경을 거부하고 폐기합니다 (관리자 전용) |
| 감사 | `query_audit_logs` | read | 감사 기록을 검색하고 필터링합니다 |
| 감사 | `get_audit_entry` | read | ID로 단일 감사 로그 항목을 가져옵니다 |
| 감사 | `export_audit_logs` | read | 감사 로그 항목을 CSV 또는 JSON으로 내려받습니다 |

보류된 도구 (v1에서는 요청 시점에 명시적으로 거부됨):

- `estimate_query_cost`
- `get_execution_status`

## 5. 실행 클래스

정책은 두 수준에서 도구를 통제합니다: 도구 ID 자체와 실행 분류입니다. 두 항목이 모두 정책의 허용 목록과 일치할 때만 요청이 허용됩니다.

| 클래스 | 포함 범위 |
|-------|---------------|
| `metadata` | 스키마 검사 — 데이터베이스와 테이블 나열, 개체 설명 |
| `read` | 읽기 전용 쿼리 실행, 데이터 가져오기, 읽기 전용 미리보기 |
| `write` | 데이터를 수정하는 삽입, 업데이트 또는 스크립트 실행 |
| `destructive` | DELETE, DROP, TRUNCATE 및 기타 되돌릴 수 없는 작업 |
| `admin_safe` | 추가 스키마 변경, 인덱스 생성 같은 안전한 DDL 작업 |
| `admin` | 위험한 DDL 작업, 승인, 감사 내보내기, 특권 작업 |
| `admin_destructive` | 스키마 개체 삭제나 자르기 같은 되돌릴 수 없는 관리 작업 |

## 6. 내장 정책과 역할

세 개의 정책과 세 개의 역할은 변경할 수 없는 내장 항목으로 제공됩니다. 디스크에 무엇이 저장되어 있든 항상 존재하며, 삭제하거나 수정할 수 없습니다.

### 내장 정책

| ID | 허용되는 클래스 | 범위 |
|----|----------------|-------|
| `builtin/read-only` | metadata, read | 모든 탐색 + 스키마 도구; 읽기 전용 쿼리와 미리보기 도구; 스크립트 나열/가져오기; 감사 읽기 도구 |
| `builtin/write` | metadata, read, write | 모든 읽기 전용 도구에 더해 쓰기 가능 스크립트와 요청/승인 제출 흐름 |
| `builtin/admin` | metadata, read, write, destructive, admin_safe, admin, admin_destructive | 이 브랜치에서 노출되는 모든 표준 도구 |

### 내장 역할

내장 역할은 `builtin/read-only`, `builtin/write`, `builtin/admin`의 세 가지입니다. 각 역할에는 동일한 ID의 정책이 할당됩니다.

내장 항목은 GUI 앱(`AppState`)과 MCP 서버(`dbflux_mcp_server::governance`의 `builtin_policies()` / `builtin_roles()` 루프) 양쪽에서 시작 시점에 주입됩니다. 디스크에 기록되는 일은 없습니다. 내장 항목을 삭제하려는 시도는 오류를 반환합니다.

대부분의 통합에서는 `builtin/read-only`로 시작하고, 쓰기 접근이 명시적으로 필요할 때만 `builtin/write`나 사용자 지정 정책으로 올립니다.

## 7. DBFlux GUI에서의 운영자 설정

MCP 서버를 시작하기 전에 DBFlux GUI에서 거버넌스를 구성합니다.

1. **설정 → MCP → 클라이언트 탭**
   - 각 AI 에이전트를 신뢰할 수 있는 클라이언트로 등록합니다 (안정적인 `client_id`, 사람이 읽을 수 있는 이름, 선택적인 발급자).
   - 클라이언트를 활성 상태로 표시합니다. 비활성 클라이언트는 첫 번째 권한 부여 게이트에서 거부됩니다.

2. **설정 → MCP → 역할 탭**
   - 내장 역할(`Read Only`, `Write`, `Admin`)은 목록 맨 위에 나타나며 삭제할 수 없습니다.
   - 다중 선택 드롭다운으로 여러 정책을 조합해 사용자 지정 역할을 만듭니다.

3. **설정 → MCP → 정책 탭**
   - 내장 정책은 목록 맨 위에 나타나며 수정할 수 없습니다.
   - 도구와 클래스 확인란을 토글해 사용자 지정 정책을 만듭니다.

4. **연결 관리자 → MCP 탭**
   - 대상 연결에 대해 MCP를 활성화합니다.
   - 채워진 드롭다운에서 이 연결의 액터(신뢰할 수 있는 클라이언트), 역할, 정책을 선택합니다.

5. **워크스페이스 → 대기 중인 승인**
   - 승인 경로를 트리거한 쓰기/파괴적 요청을 검토하고 승인하거나 거부합니다.

6. **워크스페이스 → 감사**
   - 액터/도구/결정/시간 범위로 필터링하고 CSV/JSON을 내보냅니다.

MCP 서버는 시작 시점에 이 설정들을 디스크에서 읽습니다. 서버가 실행 중인 동안 GUI에서 거버넌스 설정을 변경했다면 새 구성을 적용하도록 서버를 다시 시작하세요.

## 8. 저장되는 파일과 경로

DBFlux는 모든 상태를 단일 통합 SQLite 데이터베이스와 몇 개의 보조 디렉터리에 저장합니다. 경로는 `dirs`가 해석합니다 (Linux에서는 `XDG_*`, macOS에서는 `~/Library`).

일반적인 Linux 기본값:

| 경로 | 내용 |
|------|----------|
| `~/.local/share/dbflux/dbflux.db` | 통합 데이터베이스: 프로필, 인증, SSH 터널, 거버넌스, 감사 이벤트, 기록, 세션, UI 상태 |
| `~/.local/share/dbflux/sessions/` | 세션 복원과 복구를 위해 유지되는 임시 및 섀도 파일 |
| `~/.local/share/dbflux/scripts/` | 사용자가 작성한 스크립트 디렉터리 |

`dbflux.db` 데이터베이스에는 접두사가 붙은 스키마 아래에 모든 도메인 테이블이 들어 있습니다:

- `cfg_*` — 구성 (프로필, 인증, 거버넌스, 서비스, 훅, 드라이버)
- `st_*` — 상태 (세션, 쿼리 기록, UI 상태, 저장된 쿼리)
- `aud_audit_events` — 통합 감사 로그 (MCP 이벤트, 쿼리 이벤트, 연결, 훅, 스크립트)
- `sys_*` — 시스템 (마이그레이션, 레거시 가져오기 추적)

내장 정책과 역할은 시작 시점에 합성되며 디스크에 기록되지 않습니다.

테스트 시 주의: 실제 사용자 디렉터리를 사용하지 마세요. 격리된 실행을 위해 바이너리에 `--config-dir`을 전달하거나 `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME`을 임시 경로로 설정하세요. `dbflux_audit::temp_sqlite_path(name)` 헬퍼는 감사 테스트용 격리 경로를 생성합니다.

## 9. Rust 통합 패턴

### 인프로세스 (GUI 앱, `AppState`)

```rust
// 신뢰할 수 있는 클라이언트를 등록합니다
state.upsert_mcp_trusted_client(TrustedClientDto {
    id: "agent-a".into(),
    name: "Agent A".into(),
    issuer: None,
    active: true,
})?;

// 연결에 대해 에이전트에 내장 역할을 할당합니다
state.save_mcp_connection_policy_assignment(ConnectionPolicyAssignmentDto {
    connection_id: connection_id.to_string(),
    assignments: vec![ConnectionPolicyAssignment {
        actor_id: "agent-a".into(),
        role_ids: vec!["builtin/read-only".into()],
        policy_ids: vec![],
    }],
})?;
```

### 삭제 전에 내장 ID 확인하기

```rust
if dbflux_mcp::is_builtin(id) {
    // 내장 정책과 역할은 수정하거나 삭제할 수 없습니다
}
```

### 권한 부여 호출 (MCP 서버가 내부적으로 사용)

```rust
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};

let outcome = authorize_request(
    &trusted_clients,
    &policy_engine,
    &audit_service,
    &AuthorizationRequest {
        identity: RequestIdentity { client_id: "agent-a".into(), issuer: None },
        connection_id: connection_id.to_string(),
        tool_id: "select_data".to_string(),
        classification: ExecutionClassification::Read,
        mcp_enabled_for_connection: true,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code와 deny_reason이 이유를 설명합니다
}
```

## 10. 통합 체크리스트

AI 클라이언트를 MCP 서버에 연결하기 전에:

- [ ] MCP 지원이 포함된 `dbflux` 빌드 (기본값으로 활성화되어 있거나 `--features mcp`로 활성화)
- [ ] DBFlux GUI에 신뢰할 수 있는 클라이언트가 등록되어 활성화되어 있음
- [ ] 바이너리에 전달한 `--client-id`가 등록된 클라이언트와 일치함
- [ ] 대상 연결에서 MCP가 활성화되어 있음
- [ ] 액터가 해당 연결에 대한 정책 할당을 보유함
- [ ] 정책이 에이전트가 사용할 도구를 포괄함
- [ ] 쓰기/파괴적 도구에 대한 승인 워크플로를 이해함

## 11. 테스트 위생

테스트 중 개발자 머신을 오염시키지 않으려면:

- `--config-dir`를 임시 디렉터리로 지정하거나 `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME`을 설정합니다.
- 감사 테스트에는 임시 SQLite 경로를 사용합니다.
- 테스트 코드에서 `~/.config/dbflux`나 `~/.local/share/dbflux`를 읽거나 쓰지 않습니다.
- 내장 정책과 역할은 별도 설정 없이 사용할 수 있습니다 — 테스트 픽스처에 직접 삽입하지 않습니다.
- `dbflux_audit::temp_sqlite_path(name)` 헬퍼는 테스트마다 격리된 경로를 생성합니다.

## 12. 문제 해결

### 서버가 즉시 종료됨

- `--client-id` 인수가 없습니다.
- 설정 디렉터리에 접근할 수 없거나 생성할 수 없습니다.

### 신뢰할 수 없는 클라이언트로 요청이 거부됨

- 클라이언트가 존재하고 신뢰할 수 있는 클라이언트 목록에서 활성화되어 있는지 확인합니다.
- `--client-id`가 등록된 `id`와 정확히 일치하는지 확인합니다 (대소문자 구분).

### 연결에서 MCP가 활성화되지 않아 요청이 거부됨

- 대상 연결의 거버넌스 설정에서 MCP를 활성화합니다 (연결 관리자 → MCP 탭).
- 또는 모든 연결을 활성화하려면 설정에서 `mcp_enabled_by_default: true`를 지정합니다.

### 정책에 의해 거부됨

- 액터가 해당 연결 범위에 할당을 보유하는지 확인합니다.
- 도구 ID가 할당된 정책의 허용 도구에 포함되어 있는지 확인합니다.
- 실행 클래스가 정책의 허용 클래스에 포함되어 있는지 확인합니다.
- `builtin/read-only`를 사용하는 경우 쓰기 도구(`create_script` 등)는 설계상 제외됩니다.

### 승인이 대기 상태에서 멈춤

- DBFlux 워크스페이스에서 대기 큐를 확인하고 승인 또는 거부를 명시적으로 수행합니다.
- `approve_execution`에는 `admin` 클래스가 필요합니다 — 승인자의 정책에 이 클래스가 포함되어 있는지 확인하십시오.

### 감사 내보내기에 이벤트가 누락됨

- 필터(`actor_id`, `tool_id`, 시간 범위, 결정)가 지나치게 제한적이지 않은지 확인합니다.
- `export_audit_logs`는 `read` 실행 클래스로 분류됩니다.

### 정책이나 역할을 삭제할 수 없음

- 내장 ID(`builtin/read-only`, `builtin/write`, `builtin/admin`)는 삭제할 수 없습니다.
- 수정 가능한 변형이 필요하면 다른 ID로 사용자 지정 정책을 만듭니다.

### GUI에서 설정을 변경했지만 서버가 여전히 이전 값을 사용함

- MCP 서버 프로세스를 다시 시작합니다. 거버넌스는 시작 시 디스크에서 한 번 불러옵니다.
