# MySQL과 MariaDB

널리 사용되는 오픈 소스 관계형 데이터베이스입니다.
## 한눈에 보기

- **분류** — 관계형
- **쿼리 언어** — SQL
- **기본 포트** — 3306
- **URI 스킴** — `mysql`

## 기능

- 하나의 크레이트에 MySQL과 MariaDB 관계형 드라이버 구현을 모두 포함합니다.
- SQL 실행, 스키마 탐색, 인덱스, 외래 키, CHECK 제약 조건, UNIQUE 제약 조건을 지원합니다.
- 인증, SSH 터널링, URI/수동 연결 모드를 지원합니다.
- 다섯 가지 고유 SSL 모드(`DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA`, `VERIFY_IDENTITY`)를 갖춘 TLS: `VERIFY_CA`는 호스트 이름 검증을 건너뛰면서 서버 인증서 체인을 검증하고, `VERIFY_IDENTITY`는 둘 다 검증합니다. 검증 모드에서는 사용자 지정 루트 CA가 시스템 신뢰 저장소를 대체하며, 클라이언트 인증서 + 키로 상호 TLS를 활성화할 수 있습니다. `rustls`/`aws-lc-rs` 백엔드를 사용합니다.
- 전용 취소 경로(`KILL QUERY` 흐름)를 통한 쿼리 취소를 지원합니다.
- CRUD, 인덱스, 외래 키, 테이블 DDL 작업을 위한 SQL/코드 생성을 포함합니다.
- 루틴 탐색: `information_schema.ROUTINES`에서 저장 프로시저와 사용자 정의 함수를 매개변수 타입 및 반환 타입 힌트와 함께 나열합니다(함수만 해당).
- 루틴 정의: `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE`로 전체 `CREATE FUNCTION` 또는 `CREATE PROCEDURE` 본문을 가져옵니다(읽기 전용. 뷰어에서 정의를 편집하거나 실행할 수 없습니다).
- 다중 문 스크립트(여러 개의 `;`로 구분된 문)는 문별로 분할 실행되며, 각 문은 타입이 지정된 prepared 경로로 실행되어 문마다 하나의 결과 집합을 반환합니다.
- 데이터 전송 엔진: 네이티브 다중 행 `INSERT` 대량 적재(`BULK_INSERT`), 원본 테이블의 열을 기반으로 한 드라이버 네이티브 `CREATE TABLE` DDL, `TRUNCATE TABLE` 지원, FK 안전 마이그레이션을 위한 참조 무결성 토글(`SET FOREIGN_KEY_CHECKS`). MySQL과 MariaDB 모두 이 기능을 지원합니다.
- `program_name` 연결 속성을 `dbflux/<version>`으로 전송하며, `performance_schema.session_connect_attrs`에서 확인할 수 있습니다.
- 쓰기 권한 프로브: 연결 후 현재 사용자에 대해 `@@read_only`/`@@super_read_only`와 `SHOW GRANTS`를 확인하여 읽기 전용 복제본 또는 `INSERT`/`UPDATE`/`DELETE` 권한이 없는 역할을 감지하고, 서버가 어차피 쓰기를 거부할 상황이라면 확인된 변경 정책을 읽기 전용으로 강화합니다(부작용 없음. `@@super_read_only`가 없는 MariaDB에서는 `@@read_only`만 확인합니다).

### 인스턴스 지표

`SHOW GLOBAL STATUS`에서 가져온 엄선된 실시간 서버 지표 집합을 노출합니다:

- `mysql.threads_connected` — 현재 열려 있는 연결 수
- `mysql.threads_running` — 현재 실행 중인 쿼리
- `mysql.queries_per_sec` — 초당 쿼리 수(누적 카운터)
- `mysql.innodb_buffer_pool_hit_ratio` — InnoDB 버퍼 풀 읽기 효율
- `mysql.innodb_rows_read` — InnoDB 스토리지 엔진에서 읽은 행
- `mysql.innodb_rows_inserted` — InnoDB에 삽입된 행
- `mysql.innodb_rows_updated` — InnoDB에서 업데이트된 행
- `mysql.innodb_rows_deleted` — InnoDB에서 삭제된 행
- `mysql.slow_queries` — 누적 슬로우 쿼리 수
- `mysql.table_locks_waited` — 테이블 수준 잠금 경합 카운터
- `mysql.bytes_sent` — 전송된 네트워크 바이트

각 지표는 실시간 차트 그리기를 위해 단일 `(timestamp_ms, value)` 행으로 반환됩니다.

### 인스턴스 검사기

실행 중인 서버 상태의 표 형태 스냅샷을 노출합니다:

- `mysql.processlist` — `information_schema.PROCESSLIST`의 활성 세션(user, host, db, command, time, state, info)

## 제한 사항

- SQL 전용 드라이버입니다. 문서 또는 키-값 API를 노출하지 않습니다.

- 인스턴스 지표는 호출당 하나의 데이터 포인트(`SHOW GLOBAL STATUS`의 현재 스냅샷)를 반환하며 과거 시계열이 아닙니다. 누적 카운터(예: `mysql.bytes_sent`)는 단조 증가하므로 절대적인 비율이 아니라 샘플 간 변화량으로 해석해야 합니다.

- `performance_schema` 가용성 프로브는 카탈로그 생성 시점에 한 번 실행됩니다. `performance_schema`가 없으면 performance_schema 전용 지표는 `list_metrics()`에서 생략됩니다. 정적 지표 집합(`SHOW GLOBAL STATUS` 기반)은 항상 사용할 수 있습니다.

- 다중 문 스크립트는 하나의 원자적 서버 측 배치가 아니라 각 문을 순차적으로 실행합니다. 문 분할은 텍스트 기반이므로 `;`를 포함하는 저장 프로그램 본문(예: `CREATE PROCEDURE ... BEGIN ... END`)을 잘못 분할할 수 있습니다.
- 취소는 `KILL QUERY`가 실행될 때의 서버 권한과 연결 상태에 따라 달라집니다.
- 코드 생성은 지원되는 MySQL/MariaDB 구문으로 한정됩니다. 지원되지 않는 생성기 ID는 `NotSupported`를 반환합니다.
- 루틴 목록은 FUNCTION과 PROCEDURE 타입만 다룹니다. MySQL 집계 함수(`CREATE AGGREGATE FUNCTION` UDF 플러그인으로 등록)와 윈도우 함수는 `information_schema.ROUTINES`에 나타나지 않으므로 목록에 포함되지 않습니다.
- `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE`는 `SHOW_ROUTINE` 권한(MySQL 8.0+) 또는 해당 루틴의 소유권이 필요합니다. 권한이 충분하지 않으면 정의 열이 `NULL`을 반환하고 뷰어는 소스 대신 알림을 표시합니다.

## DDL 지원 범위

### 비트랜잭션 DDL

**중요**: MySQL DDL 작업은 **트랜잭션으로 처리되지 않습니다** — 롤백할 수 없습니다:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- DDL은 즉시 커밋되며, ROLLBACK은 효과가 없습니다!
ROLLBACK;  -- 너무 늦었습니다. 열이 이미 추가되었습니다
```

**예외**: `RENAME TABLE`은 원자적이며(트랜잭션에서 사용해도 안전합니다).

### ALTER TABLE 동작

**테이블 재작성**:
- 대부분의 `ALTER TABLE` 작업은 전체 테이블을 재작성합니다(작업 동안 테이블 잠금)
- 온라인 DDL에는 `ALGORITHM=INPLACE`와 `LOCK=NONE`을 사용하세요(MySQL 5.6+):
  ```sql
  ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL, ALGORITHM=INPLACE, LOCK=NONE;
  ```

**열 추가**:
- 테이블 **끝에** 열 추가: 빠름(메타데이터만 변경)
- 테이블 **중간에** 열 추가: 테이블 재작성(테이블 잠금)
- `AFTER column_name`으로 위치를 지정할 수 있습니다

**기본값과 함께 열 추가**:
- 테이블 재작성(테이블 잠금)
- 기본값이 기존 모든 행에 기록됩니다

**열 타입 변경**:
- 항상 테이블 재작성이 필요합니다(테이블 잠금)
- 데이터 변환은 재작성 중에 이루어집니다

**열 삭제**:
- 테이블 재작성(테이블 잠금)
- 데이터는 즉시 삭제됩니다

**열 이름 바꾸기**:
- 테이블 재작성(테이블 잠금)
- 뷰, 트리거, 애플리케이션 코드가 깨질 수 있습니다

### 인덱스 작업

**CREATE INDEX**:
- 쓰기 작업에 대해 테이블을 잠급니다(읽기는 허용)
- 온라인 인덱스 생성에는 `ALGORITHM=INPLACE, LOCK=NONE`을 사용하세요:
  ```sql
  CREATE INDEX idx_users_email ON users(email) ALGORITHM=INPLACE, LOCK=NONE;
  ```

**DROP INDEX**:
- 쓰기 작업에 대해 테이블을 잠급니다(읽기는 허용)
- 온라인 인덱스 제거에는 `ALGORITHM=INPLACE, LOCK=NONE`을 사용하세요

### 제약 조건

**외래 키**:
- 외래 키 추가는 두 테이블을 모두 스캔합니다(둘 다 잠금)
- 가능하면 `ALGORITHM=INPLACE, LOCK=NONE`을 사용하세요

**UNIQUE 제약 조건**:
- 인덱스 생성이 필요합니다(테이블 잠금)

**CHECK 제약 조건** (MySQL 8.0.16+):
- 메타데이터만 변경(빠름)
- INSERT/UPDATE 시에만 검증됩니다

### 온라인 DDL (MySQL 5.6+)

**ALGORITHM 옵션**:
- `INPLACE` — 테이블을 제자리에서 수정(복사 없음)
- `COPY` — 새 테이블을 만들고 행을 복사(구버전 MySQL의 기본값)
- `INSTANT` — 메타데이터만 변경(MySQL 8.0+, 일부 작업만 지원)

**LOCK 옵션**:
- `NONE` — 동시 읽기와 쓰기 허용
- `SHARED` — 읽기 허용, 쓰기 차단
- `EXCLUSIVE` — 읽기와 쓰기 모두 차단

**예제**:
```sql
ALTER TABLE users 
  ADD COLUMN phone VARCHAR(20) NULL,
  ALGORITHM=INPLACE,
  LOCK=NONE;
```

### 알려진 제한 사항

- DDL은 트랜잭션으로 처리되지 않음(롤백 불가)
- 대부분의 `ALTER TABLE` 작업은 전체 테이블을 재작성함(테이블 잠금)
- 테이블 중간에 열을 추가하려면 재작성이 필요합니다
- 온라인 DDL 지원은 MySQL 버전에 따라 다릅니다
- 대형 테이블의 무중단 DDL에는 `pt-online-schema-change`(Percona Toolkit)를 사용하세요

### 모범 사례

1. **먼저 복사본에서 테스트** — DDL은 롤백할 수 없습니다
2. **온라인 DDL 사용** — 지원되는 경우 `ALGORITHM=INPLACE, LOCK=NONE`을 추가하세요
3. **유지보수 시간 확보** — 트래픽이 적은 시간에 DDL을 실행하세요
4. **테이블 크기 모니터링** — 큰 테이블은 재작성에 더 오래 걸립니다
5. **pt-online-schema-change 사용** — 프로덕션 테이블의 무중단 DDL에 사용하세요
