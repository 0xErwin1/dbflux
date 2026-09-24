# PostgreSQL

고급 오픈 소스 관계형 데이터베이스입니다.

## 한눈에 보기

- **분류** — 관계형
- **쿼리 언어** — SQL
- **기본 포트** — 5432
- **URI 스킴** — `postgresql`

## 기능

- SQL 쿼리 실행과 스키마 탐색을 갖춘 PostgreSQL 관계형 드라이버입니다.
- 스키마, 테이블, 뷰, 인덱스, 외래 키, CHECK 제약 조건, UNIQUE 제약 조건, 사용자 정의 타입을 지원합니다.
- 저장 루틴(함수, 프로시저, 집계 함수, 윈도우 함수)을 스키마 트리에서 읽기 전용 정의 뷰어와 함께 노출합니다.
- 인증, SSL, SSH 터널링, URI/수동 연결 모드를 지원합니다.
- PostgreSQL 취소 토큰을 통한 쿼리 취소를 지원합니다.
- CRUD, 인덱스, reindex, 외래 키, 타입 작업을 위한 PostgreSQL 전용 SQL/코드 생성을 포함합니다.
- 다중 문 스크립트(여러 개의 `;`로 구분된 문)는 단순 쿼리 프로토콜을 통해 배치로 실행되며 문마다 하나의 결과 집합을 반환합니다.
- 데이터 전송 엔진: 네이티브 다중 행 `INSERT` 대량 적재(`BULK_INSERT`), 원본 테이블의 열을 기반으로 한 드라이버 네이티브 `CREATE TABLE` DDL, `TRUNCATE TABLE` 지원, FK 안전 마이그레이션을 위한 참조 무결성 토글(`SET session_replication_role`).
- 검증된 1차원 배열을 포함해 `pgvector`의 `vector`, `halfvec`, `sparsevec` 값을 텍스트 결과로 표시합니다.
- 1차원 배열을 포함해 전문 검색의 `tsvector`와 `tsquery` 값을 PostgreSQL의 정규 텍스트 형태로 표시합니다.
- 1차원 `numeric[]` 배열을 포함해 `numeric` 값을 PostgreSQL이 저장한 정확한 십진수로 표시합니다.
- 연결 문자열이 이미 `application_name`을 설정하고 있지 않은 한, 클라이언트 신원을 서버에 `application_name=dbflux/<version>`으로 보고합니다. 설정되어 있다면 사용자가 지정한 값을 유지합니다.
- 연결 후 쓰기 권한을 프로브합니다: 복제본이거나 읽기 전용 트랜잭션 모드라면 권한과 무관하게 읽기 전용으로 확인되고, 그렇지 않으면 인증된 역할이 보이는 기본 테이블에 가진 `INSERT`/`UPDATE`/`DELETE` 권한이 결정합니다. 데이터베이스가 비어 있거나 프로브가 실패하면 판정 불가로 처리되어 프로필 자체의 변경 정책은 그대로 유지됩니다.

### 인스턴스 지표

PostgreSQL 시스템 뷰에서 가져온 엄선된 실시간 서버 지표 집합을 노출합니다:

- `pg.tps` — 초당 트랜잭션 수(`pg_stat_database`에서)
- `pg.cache_hit_ratio` — 버퍼 캐시 적중률(`pg_statio_user_tables`에서)
- `pg.active_connections` — 상태가 `'active'`인 연결
- `pg.idle_connections` — 상태가 `'idle'`인 연결
- `pg.blocks_read` — 디스크에서 읽은 블록(`pg_statio_user_tables`에서)
- `pg.stat_statements.mean_exec_ms` — 쿼리별 평균 실행 시간(`pg_stat_statements` 확장 필요)

각 지표는 실시간 차트 그리기를 위해 단일 `(timestamp_ms, value)` 행으로 반환됩니다.

### 인스턴스 검사기

실행 중인 서버 상태의 표 형태 스냅샷을 노출합니다:

- `pg.activity` — `pg_stat_activity`의 현재 세션(쿼리 텍스트, 상태, 대기 이벤트, 소요 시간)
- `pg.locks` — `pg_locks`를 `pg_class`와 조인한 활성 잠금

- 단일 문의 행 제한은 요청한 수의 행만 보관하고 생략된 행이 있는지 표시합니다. 실행은 끝까지 완료됩니다.
- 행 제한이 있는 다중 문 배치와 요청된 실행 시간 제한은 실행 전에 거부됩니다.

## 제한 사항

- 행 제한은 서버 작업, 네트워크 전송량, 실행 시간이 아닌 보관량만 제한합니다. 변경 작업은 모든 효과를 완료합니다.
- 인스턴스 지표와 검사기의 행 제한은 실행 전에 거부됩니다. 제한 없는 배치는 기존 동작을 유지합니다.

- 배치(다중 문) 결과 열에는 타입 메타데이터가 없습니다. 값은 텍스트로 반환되며 이에 대해서는 차트 자동 감지가 비활성화됩니다. 완전한 타입 정보를 가진 열이 필요하면 단일 문을 실행하세요.

- `pg.stat_statements.mean_exec_ms`는 `pg_stat_statements` 확장이 설치되어 있고 로드되어 있을 때만 사용할 수 있습니다. 드라이버는 카탈로그 생성 시점에 이 확장의 존재를 확인하고, 없으면 해당 지표를 `list_metrics()`에서 생략합니다.

- 인스턴스 지표는 호출당 하나의 데이터 포인트(현재 스냅샷)를 반환하며 과거 시계열이 아닙니다. UI는 설정된 새로고침 간격으로 폴링하여 실시간 차트를 만듭니다.

- SQL 전용 드라이버입니다. 문서 또는 키-값 API를 노출하지 않습니다.
- 드라이버가 디코딩할 수 없는 단일 문의 값(예: `infinity` 타임스탬프나 날짜)은 `NULL` 대신 지원되지 않는 타입으로 표시되고 결과에 표시가 남습니다. 서버 자체의 텍스트를 읽으려면 열을 `text`로 캐스팅하세요. 인스턴스 검사기는 그런 셀의 열과 타입을 로그에 기록합니다.
- `money` 값은 지원되지 않는 타입으로 표시됩니다. 전송 형식은 정수 금액을 담고 있고 그 소수 자릿수는 클라이언트가 볼 수 없는 서버의 `lc_monetary` 설정에서 정해집니다. 읽으려면 열을 `numeric` 또는 `text`로 캐스팅하세요.
- 집계 함수와 윈도우 함수의 루틴 정의는 `pg_get_functiondef`가 지원하지 않기 때문에 카탈로그 메타데이터로부터 합성됩니다.
- 루틴 편집과 실행은 지원되지 않습니다. 루틴 뷰어는 읽기 전용입니다.
- 취소는 최선 노력이며 취소 시점의 서버/세션 상태에 따라 달라집니다.
- 코드 생성은 지원되는 PostgreSQL 구문만 대상으로 합니다. 지원되지 않는 생성기 ID는 `NotSupported`를 반환합니다.

## DDL 지원 범위

### 트랜잭션 DDL

PostgreSQL은 **트랜잭션 DDL**을 지원합니다 — `CREATE INDEX CONCURRENTLY`를 제외한 모든 DDL 작업은 트랜잭션으로 감싸고 롤백할 수 있습니다:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- 변경 사항을 테스트해 봅니다
ROLLBACK;  -- 문제가 생기면 안전하게 롤백할 수 있습니다
```

**예외**: `CREATE INDEX CONCURRENTLY`와 `DROP INDEX CONCURRENTLY`는 트랜잭션 안에서 실행할 수 없습니다.

### ALTER TABLE 동작

**기본값과 함께 열 추가(PostgreSQL 11+)**:
- 빠름(메타데이터만 변경하는 작업)
- 테이블 재작성이 필요 없습니다
- 읽기/쓰기를 위해 테이블을 잠그지 않습니다

**기본값 없이 열 추가**:
- 빠름(재작성 없음)
- 기존 행은 새 열에서 `NULL`을 갖습니다

**열 타입 변경**:
- 테이블 재작성이 필요할 수 있습니다(테이블 잠금)
- 사용자 지정 변환에는 `USING` 절을 사용하세요: `ALTER COLUMN age TYPE integer USING age::integer`

**열 삭제**:
- 빠름(열을 삭제됨으로 표시, 재작성 없음)
- 데이터는 즉시 회수되지 않습니다(필요하면 `VACUUM FULL`을 사용)

**열 이름 바꾸기**:
- 빠름(메타데이터만 변경)
- 뷰, 트리거, 애플리케이션 코드가 깨질 수 있습니다

### 인덱스 작업

**CREATE INDEX**:
- 쓰기 작업에 대해 테이블을 잠급니다(읽기는 허용)
- 무중단 인덱스 생성에는 `CONCURRENTLY`를 사용하세요:
  ```sql
  CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
  ```

**DROP INDEX**:
- 쓰기 작업에 대해 테이블을 잠급니다(읽기는 허용)
- 무중단 인덱스 제거에는 `CONCURRENTLY`를 사용하세요:
  ```sql
  DROP INDEX CONCURRENTLY idx_users_email;
  ```

**REINDEX**:
- 읽기와 쓰기 모두에 대해 테이블을 잠급니다
- 무중단 reindex에는 `CONCURRENTLY`(PostgreSQL 12+)를 사용하세요

### 제약 조건

**제약 조건 추가**:
- `CHECK`와 `UNIQUE` 제약 조건은 테이블을 스캔합니다(대형 테이블에서는 시간이 걸릴 수 있음)
- 검증을 미루려면 `NOT VALID`를 사용하세요:
  ```sql
  ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0) NOT VALID;
  -- 나중에 잠금 없이 검증합니다:
  ALTER TABLE users VALIDATE CONSTRAINT age_check;
  ```

**외래 키**:
- 외래 키 추가는 두 테이블을 모두 스캔합니다
- 무중단 FK 생성에는 `NOT VALID` + `VALIDATE CONSTRAINT`를 사용하세요

### 사용자 정의 타입

**CREATE TYPE (enum)**:
- 빠름(메타데이터만 변경)
- 열거형 값을 추가하려면 `ALTER TYPE ... ADD VALUE`를 사용하세요:
  ```sql
  ALTER TYPE status_enum ADD VALUE 'archived';
  ```
  **참고**: 트랜잭션 안에서 롤백할 수 없습니다(즉시 커밋됨)

**DROP TYPE**:
- 테이블이 해당 타입을 사용 중이면 실패합니다
- 의존하는 열을 먼저 삭제해야 합니다

### 알려진 제한 사항

- `CREATE INDEX CONCURRENTLY`는 순간적으로 배타적 잠금이 필요합니다(트래픽이 많은 테이블에서는 차단될 수 있음)
- `ALTER TYPE ADD VALUE`는 롤백할 수 없습니다
- 열 삭제는 디스크 공간을 즉시 회수하지 않습니다(`VACUUM FULL` 필요)
