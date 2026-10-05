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
4. **도구 + 클래스별 결정**: 도구 ID가 할당된 정책에 나열되어 있어야 하며, 그 정책이 호출의 실행 클래스를 Allow, Ask, Deny 중 하나로 결정합니다 (5절 참조).
5. **승인 경로**: Ask 결정은 호출을 대기 중인 실행으로 큐에 넣습니다. 사람이 DBFlux에서 승인하거나 거부하며, 승인된 호출은 에이전트가 같은 인수로 다시 호출할 때 한 번 실행됩니다.
6. **감사 기록**: 모든 결정은 통합 SQLite 데이터베이스의 `aud_audit_events`에 추가되며 조회와 내보내기가 가능합니다. 전체 이벤트 스키마는 `docs/AUDIT.md`를 참조하세요.

여섯 계층은 모두 `tools/call` 요청마다 서버 프로세스 내부에서 실행됩니다. 어떤 계층도 클라이언트 쪽에서 우회할 수 없습니다.

## 4. 표준 도구 집합 (v1)

| 그룹 | 도구 ID | 클래스 | 설명 |
|-------|---------|-------|--------------|
| 연결 | `list_connections` | metadata | 구성된 모든 데이터베이스 연결을 나열합니다 |
| 연결 | `connect` | metadata | 구성된 연결에 대해 세션을 엽니다. 드라이버에 데이터베이스 개념이 있으면 응답에 `current_database`와 서버에서 사용할 수 있는 `databases`가 포함되며, 다른 도구는 `database` 매개변수로 다른 데이터베이스를 지정합니다 |
| 연결 | `disconnect` | metadata | 열려 있는 세션을 닫습니다 |
| 연결 | `get_connection_info` | metadata | 드라이버 기능과 연결 메타데이터를 가져옵니다 |
| 스키마 | `list_databases` | metadata | 연결에서 접근할 수 있는 모든 데이터베이스를 나열합니다 |
| 스키마 | `list_schemas` | metadata | 데이터베이스 내의 스키마를 나열합니다 |
| 스키마 | `list_tables` | metadata | 스키마 내의 테이블과 뷰를 나열합니다. `names_only: true`를 전달하면 항목마다 객체 하나 대신 이름을 문자열로 반환합니다 |
| 스키마 | `list_collections` | metadata | MongoDB 컬렉션을 나열합니다. `list_tables`와 마찬가지로 `names_only`를 받습니다 |
| 스키마 | `describe_object` | metadata | 테이블의 열/필드 정의와 인덱스를 가져옵니다 |
| 읽기 | `select_data` | read | 테이블이나 컬렉션에 대해 구조화된 `SELECT`를 실행합니다. 다른 테이블과의 `joins`는 조인 지원을 선언한 드라이버에서 실행되며, 문서형, 키-값형 및 지원을 선언하지 않은 그 밖의 드라이버는 명시적인 오류를 반환합니다. `on` 조건은 `AND`로 연결된 컬럼 비교만 허용합니다. 조인을 사용할 때, 생성된 쿼리에서 스키마를 버리는 연결(SQLite, Turso)에서의 스키마 한정 테이블, `$ilike`를 선언하지 않은 드라이버에서의 `$ilike`, `asc`나 `desc`가 아닌 `order_by` 방향은 거부됩니다. SQL Server도 행 제한을 `OFFSET … FETCH`로 생성하여 조인을 실행합니다. 조인 테스트는 SQLite에서 실행됩니다 |
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
| 스크립트 | `list_scripts` | metadata | 스크립트 디렉터리에 저장된 스크립트를 나열합니다(외부 스크립트 폴더는 제외) |
| 스크립트 | `get_script` | read | 특정 저장 스크립트의 소스를 가져옵니다 |
| 스크립트 | `create_script` | write | 새 스크립트를 스크립트 디렉터리에 저장합니다 |
| 스크립트 | `update_script` | write | 기존 저장 스크립트를 덮어씁니다 |
| 스크립트 | `delete_script` | admin | 스크립트를 영구적으로 제거합니다 |
| 스크립트 | `execute_script` | computed | 연결에 대해 저장된 스크립트를 실행합니다. 분류는 스크립트 본문에서 도출됩니다 |
| 승인 | `request_execution` | admin | 사람의 승인을 받도록 호출을 큐에 넣습니다. 승인되면 같은 인수로 해당 도구를 직접 호출해 한 번 실행합니다 |
| 승인 | `list_pending_executions` | read | 승인 대기 중인 모든 실행을 봅니다 |
| 승인 | `get_pending_execution` | read | 특정 대기 중 실행의 세부 정보를 가져옵니다 |
| 승인 | `approve_execution` | — | MCP로는 항상 거부됩니다. 사람이 DBFlux에서 승인합니다 |
| 승인 | `reject_execution` | — | MCP로는 항상 거부됩니다. 사람이 DBFlux에서 거부합니다 |
| 감사 | `query_audit_logs` | read | 감사 기록을 검색하고 필터링합니다 |
| 감사 | `get_audit_entry` | read | ID로 단일 감사 로그 항목을 가져옵니다 |
| 감사 | `export_audit_logs` | read | 감사 로그 항목을 CSV 또는 JSON으로 내려받습니다 |

드라이버가 `select_data`, `count_records`, `aggregate_data` 또는 `describe_object` 호출을 실패시켰고 조회한 데이터베이스나 스키마의 메타데이터에 해당 테이블이나 컬렉션이 나열되어 있지 않으면, 오류는 이름을 어디에서 찾았는지 알려 주고 나열된 이름 중 가장 비슷한 것을 보여 줍니다. 테이블이 존재하지 않을 수도 있고 연결에 접근 권한이 없을 수도 있으므로, 오류 문구는 "is not listed"(나열되지 않음)입니다. 호출에 `database`를 전달하지 않았고 서버가 둘 이상의 데이터베이스를 나열하면, 오류는 테이블이 다른 데이터베이스에 있을 수 있다는 내용을 덧붙입니다. 컬렉션이 아닌 테이블의 경우, `count_records`, `aggregate_data`, 그리고 실행 전에 확인하지 않는 엔진의 `select_data`(아래 참조)는 `where`나 `order_by`에 지정된 열에 대해서도 같은 방식으로 동작합니다. 힌트에는 클라이언트가 나열할 수 있는 이름만 포함됩니다. 테이블 이름에는 `list_tables`, 열 이름에는 `describe_object`, 데이터베이스 정보에는 `list_databases` 권한이 필요합니다. 드라이버의 원래 오류 텍스트는 끝에 그대로 유지됩니다. 이 조회는 드라이버가 호출을 실패시킨 뒤에만 실행되며, 이 메타데이터를 제공하지 않는 드라이버는 원래 오류를 반환합니다.

반면 `select_data`는 알 수 없는 따옴표 식별자를 문자열로 읽는 엔진에서는 실행하기 전에 열을 확인합니다. SQLite와 Turso가 그런 엔진으로, 철자가 틀린 열은 오류 대신 행을 반환하지 않습니다. 관계형 테이블과 `joins`가 있는 모든 호출에서, `columns`, `where`, `order_by`에 있는 이름 중 한정자가 없거나 해당 테이블로 한정된 이름은 문자 구성과 관계없이 모두 테이블의 열 메타데이터와 비교되며, SQLite처럼 ASCII 문자만 대소문자를 구분하지 않습니다. 나열되지 않은 열은 같은 힌트와 "The query was not run." 문구와 함께 거부되며, 쿼리는 실행되지 않습니다. 테이블과 뷰 이름은 ASCII 대소문자를 구분하지 않고 해석되며, 생성 열, 가상 테이블의 숨겨진 열, 그리고 엔진이 허용하는 곳의 `rowid`, `oid`, `_rowid_`는 나열된 것으로 간주됩니다. `describe_object` 권한이 없으면 거부 메시지에 다른 열 이름이 포함되지 않습니다. 드라이버에 해당 테이블의 열 메타데이터가 없으면 확인을 건너뛰고 호출은 이전처럼 실행되며, 중첩 경로와 다른 테이블로 한정된 이름은 엔진이 거부하므로 확인하지 않습니다. 비용은 호출이 열을 지정한 테이블마다 열 조회 한 번입니다. PostgreSQL, MySQL, MariaDB, SQL Server, ClickHouse, Redshift는 알 수 없는 열에서 실패하므로 확인 없이 실행되며, 실패하면 위에서 설명한 힌트가 붙습니다.

드라이버가 해당 테이블에 대해 선언한 의사 열(SQLite의 `rowid`, MySQL의 `_rowid`, PostgreSQL의 `ctid`와 `xmin` 등)은 나열된 것으로 간주되며 제안되지 않습니다. `joins`가 없는 호출이라도 `columns`에 의사 열을 지정하면 조인과 같은 방식으로 생성된 SELECT로 실행되어 값이 반환됩니다. PostgreSQL은 `ctid`를 `(0,1)` 같은 텍스트로, 나머지 시스템 열은 정수로 반환합니다. 이 SELECT는 일반 호출보다 허용하는 범위가 좁습니다. `where`에는 조인에서 허용하는 연산자만, 이름은 단순한 ASCII 식별자만 사용할 수 있고, 각 열은 한 번만 지정할 수 있으며, 정렬 방향은 `asc` 또는 `desc`여야 합니다. 실행 전에 확인하지 않는 엔진에서는 `columns`의 항목이 호출이 읽은 행에 없을 때만 열 조회 한 번으로 의사 열인지 확인합니다.

보류된 도구 (v1에서는 요청 시점에 명시적으로 거부됨):

- `estimate_query_cost`
- `get_execution_status`

## 5. 실행 클래스

정책은 두 수준에서 도구를 통제합니다: 도구 ID 자체와 실행 분류입니다. 정책은 자신이 다루는 도구를 나열하고, 각 실행 클래스에 하나의 결정을 부여합니다:

| 결정 | 해당 클래스의 호출에 일어나는 일 |
|------|------------------------------|
| Allow | 즉시 실행됩니다 |
| Ask | 사람을 기다립니다: 호출은 대기 중인 실행으로 큐에 들어가며 승인된 후에만 실행됩니다 |
| Deny | 거부됩니다 |

| 클래스 | 포함 범위 |
|-------|---------------|
| `metadata` | 스키마 검사 — 데이터베이스와 테이블 나열, 개체 설명 |
| `read` | 읽기 전용 쿼리 실행, 데이터 가져오기, 읽기 전용 미리보기 |
| `write` | 데이터를 수정하는 삽입, 업데이트 또는 스크립트 실행 |
| `destructive` | DELETE, DROP, TRUNCATE 및 기타 되돌릴 수 없는 작업 |
| `admin_safe` | 추가 스키마 변경, 인덱스 생성 같은 안전한 DDL 작업 |
| `admin` | 위험한 DDL 작업, 감사 내보내기, 특권 작업 |
| `admin_destructive` | 스키마 개체 삭제나 자르기 같은 되돌릴 수 없는 관리 작업 |

`metadata`와 `read`는 읽기만 합니다. 나머지 다섯 클래스는 데이터나 스키마를 변경하며, 아래에서는 변경 클래스라고 부릅니다.

### 정책이 결합되는 방식

액터는 한 연결에서 직접 또는 역할을 통해 여러 정책을 가질 수 있습니다. 요청된 도구를 나열한 정책만 판단에 참여하며, 그중 가장 관대한 결정이 적용됩니다: Allow가 Ask보다, Ask가 Deny보다 우선합니다. 정책은 권한을 부여하는 것이며, Deny는 거부권이 아니라 권한이 없다는 뜻입니다. 따라서 어떤 정책이 한 클래스에 승인을 요구하더라도, 할당된 다른 정책이 이미 그 클래스를 허용한 액터는 막히지 않습니다. 한 클래스가 승인을 기다리게 하려면 액터에게 할당된 다른 어떤 정책도 그 클래스를 허용하지 않도록 하십시오.

### 승인 흐름

1. 에이전트가 정책이 Ask로 결정한 클래스의 도구를 호출합니다. 서버는 호출을 대기 중인 실행으로 큐에 넣고, outcome이 `pending`인 `mcp_authorize` 감사 이벤트를 기록하며, 데이터가 `{"code": "approval_required", "status": "pending", "pending_id": "..."}`인 JSON-RPC 오류로 응답합니다.
2. 사람이 DBFlux에서 호출을 승인하거나 거부합니다 (**워크스페이스 → 대기 중인 승인**). 서버와 앱은 `dbflux.db`를 통해 큐를 공유하므로, `dbflux mcp`가 큐에 넣은 호출이 앱에 나타납니다.
3. 에이전트가 같은 도구를 같은 인수로 다시 호출합니다. 서버는 액터, 연결, 도구, 인수가 일치하는 승인을 찾아 소비하고 호출을 실행합니다. 그 호출의 `mcp_authorize` 이벤트는 outcome이 `success`이며 `details_json.pending_execution_id`에 사용된 승인을 기록합니다.

승인 하나는 호출 하나를 실행합니다. 호출을 다시 반복하면 새 요청이 큐에 들어가며, 인수를 바꿔도 마찬가지입니다. 거부된 호출은 절대 실행되지 않습니다. 승인은 호출이 큐에 들어간 지 24시간 후에 만료됩니다.

`request_execution`은 호출을 명시적으로 큐에 넣으며, Ask 상태에서 도구를 호출한 것과 결과가 같습니다. `request_execution`, `list_pending_executions`, `get_pending_execution`은 큐 항목을 만들거나 읽기만 하므로, Ask 상태에서도 자신은 큐에 들어가지 않고 실행됩니다.

MCP 클라이언트는 절대 승인하거나 거부할 수 없습니다: `approve_execution`과 `reject_execution`은 정책과 관계없이 MCP로는 오류 코드 `self_approval_forbidden`과 함께 거부되며, 모든 시도가 감사됩니다. 대기 중인 실행은 DBFlux UI에서 사람만 처리합니다.

### 읽기 스크립트는 읽기 전용으로 실행됩니다

`execute_script`는 스크립트 본문에서 클래스를 도출합니다. `read` 또는 `metadata`로 분류된 스크립트는 읽기 전용 강제와 함께 실행됩니다: 드라이버는 데이터베이스 자체가 데이터 수정을 거부하는 세션에서 스크립트를 실행하며, 호출은 `read` 또는 `metadata`로 통제되고 감사됩니다.

| 드라이버 | 세션을 읽기 전용으로 만드는 방법 |
|----------|----------------------------------|
| PostgreSQL, Redshift | `BEGIN READ ONLY`, 실행 후 롤백 |
| MySQL, MariaDB | `START TRANSACTION READ ONLY`, 실행 후 롤백. 실행 가능한 주석(`/*! */`, `/*M! */`)과 `INTO`는 거부되며, 열린 트랜잭션이나 `LOCK TABLES` 잠금이 있을 수 있는 경우에도 스크립트가 거부됩니다 |
| SQLite | `PRAGMA query_only` |
| ClickHouse | 요청별 설정 `readonly = 2` |

SQL Server, Turso, 외부 IPC 드라이버, MongoDB, Redis, DynamoDB, CloudWatch, InfluxDB는 읽기 전용을 강제할 수 없습니다. 이러한 연결과, 세션에 이미 열린 트랜잭션이 있는 모든 연결에서는 스크립트가 `write`로 통제됩니다: 정책의 `write` 결정(Allow, Ask 또는 Deny)이 적용되고, 감사에는 `write`가 기록됩니다.

데이터베이스는 세션 안의 데이터 수정을 막을 뿐, 외부 효과가 있는 함수는 막지 않습니다. 예를 들어 PostgreSQL의 `dblink_exec`, `COPY ... TO PROGRAM`, `lo_export`, `pg_terminate_backend`, advisory lock, 그리고 MySQL의 `GET_LOCK`과 사용자 정의 함수가 그렇습니다. 읽기 전용 MCP 클라이언트의 경우 최소 권한 데이터베이스 자격 증명이 여전히 실제 경계입니다.

편집기의 자동 새로고침도 같은 강제를 사용합니다. 드라이버가 읽기 전용을 강제할 수 없는 연결에서는 자동 새로고침이 Manual로 돌아갑니다.

## 6. 내장 정책과 역할

세 개의 정책과 세 개의 역할은 변경할 수 없는 내장 항목으로 제공됩니다. 디스크에 무엇이 저장되어 있든 항상 존재하며, 삭제하거나 수정할 수 없습니다.

### 내장 정책

읽기는 기본으로 허용되며, 내장 정책이 부여하는 모든 변경 클래스는 승인을 요구합니다.

| ID | Allow | Ask | 범위 |
|----|-------|-----|-------|
| `builtin/read-only` | metadata, read | — | 모든 탐색 + 스키마 도구; 읽기 전용 쿼리와 미리보기 도구; 스크립트 나열/가져오기; 감사 읽기 도구 |
| `builtin/write` | metadata, read | write | 모든 읽기 전용 도구에 더해 쓰기 가능 스크립트와 요청/승인 제출 흐름 |
| `builtin/admin` | metadata, read | write, destructive, admin_safe, admin, admin_destructive | `approve_execution`과 `reject_execution`을 제외한 모든 표준 도구 |

내장 정책이 나열하지 않은 클래스는 거부됩니다.

### Ask 도입 이전에 만든 정책

Ask 결정이 생기기 전에는 정책이 클래스를 허용하는 것만 가능했으므로, 변경 클래스를 허용한 것이 승인 없이 실행하겠다는 명시적인 선택이었던 적은 없습니다. Ask를 도입한 저장소 마이그레이션(`034_cfg_tool_policy_approval_classes`)은 이에 맞춰 기존 사용자 지정 정책을 다시 씁니다: 허용되던 변경 클래스는 Ask가 되고, 허용되던 `metadata`나 `read` 클래스는 Allow로 유지되며, 허용되지 않던 클래스는 Deny로 유지됩니다. 에이전트가 다시 승인 없이 변경 호출을 실행하게 하려면 정책에서 **승인 없이 모두 허용**을 선택하십시오.

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
   - 도구를 선택하고 각 실행 클래스에 Allow, Ask, Deny 중 하나를 골라 사용자 지정 정책을 만듭니다. 키보드로는 클래스 행에서 `enter`를 누르면 다음 결정으로 바뀝니다.
   - **승인 없이 모두 허용**은 모든 변경 클래스를 Allow로 설정합니다. 이후 에이전트는 `DROP DATABASE`를 포함한 모든 변경 호출을 묻지 않고 실행할 수 있습니다.

4. **연결 관리자 → MCP 탭**
   - 대상 연결에 대해 MCP를 활성화합니다.
   - 채워진 드롭다운에서 이 연결의 액터(신뢰할 수 있는 클라이언트), 역할, 정책을 선택합니다.

5. **워크스페이스 → 대기 중인 승인**
   - 정책이 승인으로 보낸 호출을 검토하고 승인하거나 거부합니다. 대기 중인 실행은 이곳에서만 처리됩니다.
   - 대기 중인 호출은 제목 표시줄 종 아이콘의 알림 센터에도 표시되며, 호출이 대기하는 동안 종에 강조색 배지가 붙습니다. 해당 행의 **검토**는 이 탭에서 그 호출을 엽니다. 팝오버 자체는 승인하거나 거부하지 않습니다.
   - 승인된 호출은 에이전트가 같은 인수로 다시 호출할 때 실행됩니다. 모든 결정은 감사 로그에 기록됩니다.

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
        correlation_id: None,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code와 deny_reason이 이유를 설명합니다
}
```

`authorize_request`에는 승인 큐가 없습니다: Ask 결정은 `deny_code == Some("approval_required")`와 함께 허용되지 않은 결과로 돌아옵니다. MCP 서버는 대신 `McpRuntime::authorize_with_approval_mut`를 호출하며, 이 함수는 호출 인수를 전달해 Ask 결정을 큐에 넣거나 일치하는 승인을 소비해 호출을 실행시킵니다.

## 10. 통합 체크리스트

AI 클라이언트를 MCP 서버에 연결하기 전에:

- [ ] MCP 지원이 포함된 `dbflux` 빌드 (기본값으로 활성화되어 있거나 `--features mcp`로 활성화)
- [ ] DBFlux GUI에 신뢰할 수 있는 클라이언트가 등록되어 활성화되어 있음
- [ ] 바이너리에 전달한 `--client-id`가 등록된 클라이언트와 일치함
- [ ] 대상 연결에서 MCP가 활성화되어 있음
- [ ] 액터가 해당 연결에 대한 정책 할당을 보유함
- [ ] 정책이 에이전트가 사용할 도구를 포괄함
- [ ] 승인이 필요한 클래스를 Ask로 설정했고, 에이전트가 작업하는 동안 누군가 대기 중인 승인을 확인함

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
- 정책이 해당 실행 클래스를 Deny가 아닌 Allow 또는 Ask로 결정하는지 확인합니다.
- `builtin/read-only`를 사용하는 경우 쓰기 도구(`create_script` 등)는 설계상 제외됩니다.

### 호출이 `approval_required`로 응답됨

- 정책이 호출의 클래스를 Ask로 결정했습니다. **워크스페이스 → 대기 중인 승인**에서 `pending_id`가 가리키는 대기 중인 실행을 승인한 다음, 같은 인수로 호출을 반복하십시오.
- 다른 인수로 호출을 반복하면 승인을 사용하지 않고 새 요청이 큐에 들어갑니다.
- 에이전트는 자신의 호출을 승인할 수 없습니다: `approve_execution`과 `reject_execution`은 MCP로는 항상 거부됩니다 (`self_approval_forbidden`).

### 감사 내보내기에 이벤트가 누락됨

- 필터(`actor_id`, `tool_id`, 시간 범위, 결정)가 지나치게 제한적이지 않은지 확인합니다.
- `export_audit_logs`는 `read` 실행 클래스로 분류됩니다.

### 정책이나 역할을 삭제할 수 없음

- 내장 ID(`builtin/read-only`, `builtin/write`, `builtin/admin`)는 삭제할 수 없습니다.
- 수정 가능한 변형이 필요하면 다른 ID로 사용자 지정 정책을 만듭니다.

### GUI에서 설정을 변경했지만 서버가 여전히 이전 값을 사용함

- MCP 서버 프로세스를 다시 시작합니다. 거버넌스는 시작 시 디스크에서 한 번 불러옵니다.
