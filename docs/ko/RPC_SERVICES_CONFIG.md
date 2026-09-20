# RPC 서비스 UI 참조

이 파일은 DBFlux에서 RPC 서비스를 저장하고 관리하는 방법을 설명합니다.

DBFlux는 이제 `RpcServiceKind`를 통해 일급 RPC 서비스 기반을 영속 저장합니다:

- `Driver` — 런타임 데이터베이스 드라이버로 변환됩니다
- `AuthProvider` — 앱과 MCP 서버 양쪽의 런타임 인증 제공자 레지스트리로 변환됩니다

## 저장 방식

RPC 서비스는 JSON 파일이 아니라 SQLite의 `~/.local/share/dbflux/dbflux.db`에 저장됩니다.

**테이블:**

- `cfg_services` — 주요 서비스 레코드 (socket_id, service_kind, command, startup_timeout_ms, enabled)
- `cfg_services.api_family`, `cfg_services.api_major`, `cfg_services.api_minor` — 선택적 RPC API 계약 메타데이터
- `cfg_service_args` — 순서가 있는 프로세스 인수
- `cfg_service_env` — 환경 변수

## 스키마

```sql
-- 기본 테이블(마이그레이션 001). `service_kind`는 마이그레이션 005에서,
-- `api_family`/`api_major`/`api_minor`는 마이그레이션 006에서 추가됩니다.
-- 참조를 위해 여기에 인라인으로 보여주지만 기본 DDL의 일부는 아닙니다.
CREATE TABLE cfg_services (
    socket_id TEXT PRIMARY KEY,
    enabled INTEGER DEFAULT 1,
    command TEXT,
    startup_timeout_ms INTEGER,        -- SQL 수준 기본값 없음; 5000ms 폴백
                                       -- (DEFAULT_STARTUP_TIMEOUT_MS)은 앱
                                       -- 코드에서 적용됩니다
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    service_kind TEXT NOT NULL DEFAULT 'driver',  -- 마이그레이션 005에서 추가됨
    api_family TEXT,                              -- 마이그레이션 006에서 추가됨
    api_major INTEGER,                            -- 마이그레이션 006에서 추가됨
    api_minor INTEGER                             -- 마이그레이션 006에서 추가됨
);

CREATE TABLE cfg_service_args (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    position INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE cfg_service_env (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
```

## 서비스 관리

서비스는 파일을 직접 편집하는 방식이 아니라 설정 UI의 **RPC Services** 섹션을 통해 관리됩니다.

서비스를 추가하거나 편집하려면:
1. 설정을 열고 RPC Services로 이동합니다
2. 새 서비스를 추가하거나 기존 서비스를 선택합니다
3. 서비스 종류(`Driver` 또는 `Auth Provider`)를 선택합니다
4. 소켓 ID, command 경로, 인수, 환경 변수, 시간 초과를 구성합니다
5. 변경 사항을 저장합니다

참고:

- `Driver` 서비스는 런타임에서 활성 상태이며 기존 `rpc:<socket_id>` 드라이버 식별자를 유지합니다.
- `Auth Provider` 서비스는 런타임 인증 제공자 레지스트리에서만 활성 상태이며 드라이버로는 표시되지 않습니다.
- DBFlux는 드라이버 등록 ID에 대해 `rpc:<socket_id>` 형태의 호환성을 유지합니다.
- 기존 드라이버 행에 API 메타데이터가 없으면 DBFlux는 현재 `driver_rpc` 계약의 버전 `1.1`을 기본값으로 적용합니다.
- 인증 제공자 행에 API 메타데이터가 없으면 DBFlux는 현재 `auth_provider_rpc` 계약의 버전 `1.2`를 기본값으로 적용합니다.
- `api_family` / `api_major`는 DBFlux가 소켓을 프로브하기 전에 인증 제공자의 시작 프리플라이트로 사용됩니다.

## 동작 방식

- `socket_id`는 소켓 파일 이름으로 그대로 사용됩니다
- DBFlux는 내부적으로 각 서비스를 `rpc:<socket_id>`로 식별합니다
- DBFlux는 런타임 변환 전에 `service_kind`로 각 서비스를 분류합니다
- 드라이버 이름/아이콘/카테고리/폼은 설정이 아니라 서비스의 `Hello` 응답(`driver_metadata`, `form_definition`)에서 옵니다
- `service_kind='driver'` 서비스 중 시작 시 RPC 핸드셰이크(`Hello`)를 완료하지 못하면 등록되지 않습니다
- `service_kind='auth_provider'` 서비스는 호환성 검사를 통과하고 프로브에 성공하면 인증 제공자 레지스트리에 적재됩니다
- 드라이버 경로의 협상은 `Hello` 동안 서로 지원하는 호환 마이너 버전 중 가장 높은 것을 선택하고, 이후 모든 엔벨로프가 그 협상된 버전을 정확히 사용하도록 요구합니다
- 인증 제공자 협상은 `auth_provider_rpc` 아래에서 동일한 family/major/minor 방식을 따르며, 호환되지 않는 family 또는 major 버전은 등록 전에 건너뜁니다

## 필드

- `socket_id` (필수): DBFlux와 서비스가 사용하는 로컬 소켓 이름입니다.
  - 허용되는 문자: ASCII 영문자, 숫자, `.`, `_`, `-`
  - 경로 구분자, 공백, 기타 문장 부호는 거부됩니다.
  - 이 값은 플랫폼 소켓 네임스페이스에 그대로 전달되므로 짧고 안정적인 값으로 유지해야 합니다.
- `command` (선택): DBFlux가 서비스를 시작해야 할 때 실행하는 실행 파일입니다.
  - 생략되고 `args`도 비어 있으면 DBFlux는 서비스가 이미 실행 중인 것으로 간주하고 아무것도 생성하지 않습니다.
  - `driver`의 경우 생략되고 `args`가 비어 있지 않으면 DBFlux는 `dbflux-driver-host`를 실행합니다.
  - `auth_provider`의 경우 DBFlux가 서비스를 실행해야 한다면 `command`를 명시적으로 설정해야 합니다.
- `args` (선택): 프로세스 인수입니다.
- `env` (선택): 생성된 프로세스의 환경 변수입니다.
- `startup_timeout_ms` (선택): 생성 후 소켓이 준비되기를 기다리는 최대 시간입니다.
  - 기본값: `5000`

## 흔한 실수

- 서비스 구성과 서비스 인수 간의 소켓 이름 불일치
- DBFlux 프로세스 환경에서 해석되지 않는 상대 `command` 경로
- 설정 UI가 아니라 데이터베이스를 직접 편집하는 것
- 현재 RPC 프로토콜 버전에 필요한 `Hello` 필드를 구현하지 않는 서비스
- `command`를 생략하면서 일부만 `args`를 제공하는 것. DBFlux가 기본 호스트를 실행하게 하려면 `args`에 `--driver`와 `--socket`을 모두 포함해야 합니다.
- `command` 없이 `args`만으로 인증 제공자 서비스를 구성하는 것. DBFlux는 드라이버 호스트로 가정하는 대신 그 실행 구성을 거부합니다
