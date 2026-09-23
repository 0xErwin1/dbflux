# SQL Server

Microsoft SQL Server 관계형 데이터베이스입니다.
## 한눈에 보기

- **카테고리** — 관계형
- **쿼리 언어** — T-SQL
- **기본 포트** — 1433
- **URI 스킴** — `sqlserver`

DBFlux용 Microsoft SQL Server 드라이버로, [`tiberius`](https://crates.io/crates/tiberius) TDS 클라이언트를 기반으로 합니다.

## 주요 기능

- SQL 쿼리 실행과 스키마 검색을 지원하는 SQL Server / Azure SQL 관계형 드라이버입니다.
- SQL Server 로그인(사용자 이름 + 비밀번호)을 통한 인증을 지원합니다. URI 모드에서는 ADO, JDBC, `sqlserver://user:pass@host:port/db` 형식의 연결 문자열을 받습니다.
- 연결 문자열이나 URI가 이미 `Application Name`을 설정하고 있지 않는 한 `Application Name`을 `dbflux/<version>`으로 보고하며, 이미 설정되어 있다면 사용자가 지정한 값이 항상 우선합니다. 이를 위해 `sqlserver://`/`mssql://` URL 스킴은 `applicationname` 쿼리 파라미터를 받습니다.
- TLS 암호화 모드(`off`, `on`, `required`)는 tiberius `EncryptionLevel`을 통해 제공됩니다. 폼은 **SSL Mode** 드롭다운 하나만 노출하며, `TrustServerCertificate` 플래그는 자동으로 도출됩니다:
  - `off` — 암호화하지 않습니다(로그인 패킷은 TDS에 의해 여전히 암호화됩니다).
  - `on` — 암호화하며 자체 서명된 인증서를 허용합니다. 자동 생성 인증서를 쓰는 로컬/개발용 SQL Server에 가장 적합합니다.
  - `required` — 암호화하며 인증서 체인을 검증합니다. 실제 CA 서명 인증서를 사용하는 서버(Azure SQL 등)에 사용하세요.
  URI 모드에서는 `?trust=true|false`로 도출된 값을 명시적으로 재정의할 수 있으므로, 특수한 조합이 필요할 때 사용합니다(예: `?encrypt=required&trust=true`).
- 선택적인 SQL Server 명명된 인스턴스(`SQLEXPRESS`, `MSSQLSERVER2019` 등)는 연결 시점에 UDP 1434의 SQL Browser에 질의하여 확인됩니다(tiberius의 `sql-browser-tokio` 기능으로 활성화됩니다). 폼의 Instance 필드, URI 모드에서 SSMS 스타일의 `host\instance` 형식, `?instance=` URI 쿼리 파라미터는 모두 tiberius 설정의 동일한 `instance_name`을 설정합니다.
- 배스천 호스트를 경유한 연결을 위한 SSH 터널 지원(TCP 전용 터널을 통해서는 명명된 인스턴스 조회를 사용할 수 없습니다).
- `USE [database]`를 통한 탭별 데이터베이스 전환. 세션 상태(SET 옵션, 임시 테이블, 트랜잭션)는 동일한 연결의 `execute()` 호출 간에 유지됩니다.
- 다중 결과 집합 배치: 하나의 배치가 여러 결과 집합을 생성하는 경우(예: `SELECT 1; SELECT 2;` 또는 여러 `SELECT`를 포함하는 저장 프로시저), 드라이버는 **마지막** 비어 있지 않은 집합을 기본 `QueryResult`로 반환하고(과거의 "마지막 문이 이긴다" UX를 유지), 그보다 앞선 비어 있지 않은 집합 전부를 배치 순서대로 `QueryResult.additional_results`에 붙입니다. 순수한 준비 배치(`SET LOCK_TIMEOUT 5000`)는 여전히 비어 있는 단일 기본 집합으로 나타납니다. 모든 집합을 순회하려는 호출자는 `QueryResult::iter_result_sets()`를 사용합니다.
- 데이터 전송 엔진: 네이티브 다중 행 `INSERT` 벌크 로드(`BULK_INSERT`, T-SQL의 `VALUES` 행 제한에 따라 문당 최대 1000행으로 제한, `DriverLimits::max_bulk_insert_rows`로 노출)와 소스 테이블의 열로부터 만드는 드라이버 네이티브 `CREATE TABLE` DDL을 지원합니다(`TRUNCATE_TABLE`도 지원됩니다).
- 쓰기 권한 프로브: 연결 후 `DATABASEPROPERTYEX(DB_NAME(), 'Updateability')`로 데이터베이스가 읽기 전용인지(예: Always On 읽기 가능 보조 복제본) 확인하고, 그렇지 않으면 `HAS_PERMS_BY_NAME`을 통해 로그인이 표시되는 비시스템 기본 테이블 중 하나라도 `INSERT`/`UPDATE`/`DELETE` 권한을 갖는지 검사하여, 서버가 어차피 쓰기를 거부할 상황이라면 해석된 변경 정책을 읽기 전용으로 강화합니다(부작용 없음; 빈 데이터베이스는 판별 근거가 없으므로 정책을 변경하지 않습니다).
### 인스턴스 메트릭

`sys.dm_os_performance_counters`에서 가져온, 엄선된 실시간 서버 메트릭 집합을 노출합니다:

- `mssql.batch_requests_per_sec` — 초당 T-SQL 배치 요청 수
- `mssql.compilations_per_sec` — 초당 SQL 컴파일 수
- `mssql.recompilations_per_sec` — 초당 SQL 재컴파일 수
- `mssql.user_connections` — 현재 열려 있는 사용자 연결 수
- `mssql.lock_waits_per_sec` — 초당 잠금 대기 수(`_Total` 인스턴스)
- `mssql.page_reads_per_sec` — 초당 버퍼 풀 페이지 읽기 수
- `mssql.page_writes_per_sec` — 초당 버퍼 풀 페이지 쓰기 수
- `mssql.buffer_cache_hit_ratio` — 버퍼 캐시 적중률(백분율)
- `mssql.server_memory_kb` — KB 단위의 총 서버 메모리

각 메트릭은 실시간 차트를 위해 단일 `(timestamp_ms, value)` 행으로 반환됩니다.

`VIEW SERVER STATE` 서버 권한이 필요합니다. 권한이 없으면 `list_metrics()`는 오류 대신 빈 목록을 반환하고 경고가 로깅됩니다. 드라이버는 카탈로그 생성 시점에 이 권한을 한 번 검사합니다.

### 인스턴스 검사기

실행 중인 서버 상태의 표 형태 스냅샷을 노출합니다:

- `mssql.active_sessions` — `sys.dm_exec_sessions`와 `sys.dm_exec_requests`를 조인한 사용자 세션(세션 ID, 로그인 이름, 호스트 이름, 프로그램 이름, 상태, CPU 시간, 메모리 사용량, 명령, 요청 상태, 대기 유형, 대기 시간, 차단 세션 ID)

`VIEW SERVER STATE` 권한이 필요합니다.

### 쿼리 취소

- 취소는 새로운 사이드 채널 연결에서 `KILL <spid>`를 실행하는 방식으로 구현됩니다. tiberius는 현재 SSMS가 사용하는 TDS Attention 프리미티브를 노출하지 않으므로, 그다음으로 좋은 선택은 쿼리를 실행 중인 세션을 서버에 종료하도록 요청하는 것입니다.
- 연결 시점에 드라이버는 `@@SPID`를 캡처하고 tiberius `Config`의 복제본을 캐시합니다(로그인 정보가 이미 포함됨). 취소 핸들은 필요할 때 두 번째 연결을 열어 `KILL <spid>`를 실행하고 기본 연결을 손상된 것으로 표시합니다.
- 취소 후 `cleanup_after_cancel()`은 기본 tiberius 클라이언트를 다시 만들고 새 SPID를 캡처하며 이전의 `USE [db]`를 다시 실행하여 다음 쿼리가 동일한 데이터베이스에서 실행되도록 합니다. UI 관점에서는 연결이 유지되고 기반 세션 ID만 바뀝니다.
- 죽은 세션에서 발생한 오류(코드 596 / 233 / 6005)는 `DbError::Cancelled`로 변환되어, UI가 전송 수준 실패 대신 "쿼리가 취소되었습니다"를 표시하도록 합니다.
- 최신 SQL Server에서 세션 소유자는 `ALTER ANY CONNECTION` 권한 없이 자신의 SPID에 `KILL`을 실행할 수 있습니다. 오래되었거나 제한된 로그인에서는 KILL 자체가 권한 오류로 실패할 수 있으며, 드라이버는 이를 사용자에게 표시합니다.

### 스키마 검색

- 데이터베이스(`sys.databases`, 시스템 DB는 숨김).
- 데이터베이스별 테이블과 뷰(`sys.tables`, `sys.views`).
- 테이블별 열 + 기본 키 플래그, 인덱스, 외래 키.
- 테이블별 제약 조건: CHECK 제약 조건(정의 포함)과 UNIQUE 제약 조건(`sys.indexes.is_unique_constraint`를 통해).
- 스키마 브라우저 사이드바를 위한 전체 스키마 인덱스와 외래 키.
- 사용자 정의 타입(`sys.types where is_user_defined = 1`)을 `Domain`(별칭 타입) 또는 `Composite`(테이블 타입)로 분류.
- `view_details()`는 요청된 데이터베이스에 뷰가 존재하는지 검증합니다.
- **루틴:** 저장 프로시저(`P`), 스칼라 함수(`FN`), 인라인 테이블 반환 함수(`IF`), 다중 문 테이블 반환 함수(`TF`), CLR 집계(`AF`)가 `sys.objects`를 통해 스키마별로 나열됩니다. 소스 정의는 `OBJECT_DEFINITION(object_id)`로 가져옵니다.

### 충실한 CREATE TABLE (스키마 diff)

- 열 인트로스펙션은 **정확한 타입 차원**을 보고합니다: `nvarchar`/`nchar` 길이는 문자 단위(UTF-16 바이트를 2로 나눔, `-1`은 `MAX`로 렌더링), `varchar`/`char`/`binary`/`varbinary`는 바이트 길이, `decimal`/`numeric`는 정밀도와 스케일, `datetime2`/`datetimeoffset`/`time`는 스케일, `float(n)`는 정밀도를 그대로 유지합니다. identity는 의도적으로 `type_name`에 포함되지 않으며 구조화된 생성 메타데이터로 전달됩니다.
- `table_creation_metadata()`는 identity seed와 increment를 **서버가 변환한 정확한 텍스트**로 보고합니다(`numeric(38,0)` identity 값은 64비트 정수 범위를 넘을 수 있음), 기본 키 열을 선언된 키 순서대로 보고하며, 관측하지 못한 항목을 이름으로 알려주는 완전성 리포트와 생성기가 표현할 수 없는 생성 의미론에 대한 blocker를 제공합니다.
- `generate_code_with_creation_metadata("create_table", …)`는 **참조 측** 메타데이터로부터 충실한 `CREATE TABLE`을 렌더링합니다: 이스케이프된 대괄호 식별자, 정확한 identity 텍스트, 열별 nullability와 기본값, 선언된 순서의 기본 키. 메타데이터가 없으면 —이전 스냅샷은 딥 스냅샷으로 다시 캡처해야 합니다— 거부하고(이름 있는 `NotSupported`), 메타데이터가 불완전하거나 blocker가 있으면 역시 거부합니다. 레거시 `generate_code("create_table")` 시임은 설계상 거부합니다: 참조 메타데이터를 전달할 수 없습니다. **스키마 diff 전체 테이블 재생성**의 경우 지원은 전적으로 메타데이터 인식 `generate_code_with_creation_metadata` 코드 경로에 의존하며, 이 연산에 대해 `DdlCapabilities::supports_create_table`이 기술하는 바로 그 경로입니다; 그 재생성 외부에는 구조화된 DDL 지원이 평소처럼 존재하고, 레거시 `generate_code("create_table")` 시임은 참조 메타데이터가 없으면 항상 거부합니다.
- 스키마 diff 문서가 참조 측에 존재하는 테이블을 대상 측에 생성할 때 사용됩니다; 대상 연결이 생성하고 참조 연결은 메타데이터만 제공합니다.

### OUTPUT을 사용하는 CRUD

- 행에 대한 INSERT/UPDATE/DELETE는 SQL Server의 `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` 절을 사용하여 변경 후 행 데이터를 호출자에게 반환합니다(`CrudResult::success(row)`). Postgres 드라이버가 `RETURNING *`를 사용하는 방식과 동일합니다.
- `MutationCapabilities::supports_returning`은 `true`입니다.
- 행 식별자는 복합 기본 키여야 합니다(관계형 드라이버에 의미가 있는 유일한 `RecordIdentity` 변형입니다).

### 쿼리 계획

- `explain()`은 `SET SHOWPLAN_XML ON` 상태에서 쿼리를 실행하고 쿼리 계획을 XML로 반환합니다. 드라이버는 세션 상태가 새지 않도록 이후에 항상 `SET SHOWPLAN_XML OFF`를 실행합니다.
- `version_query()`는 `SELECT @@VERSION`을 반환합니다.

### 방언

- `]` 이스케이프가 있는 `[bracket]` 식별자 인용.
- `N'…'` 유니코드 문자열 리터럴; `0x…`(대문자) 이진 리터럴; 부울(`BIT`) 값에는 `1`/`0`.
- `OFFSET … ROWS FETCH NEXT … ROWS ONLY` 페이지 나누기(OFFSET 뒤에 ORDER BY가 없는 쿼리가 오류로 끝나지 않도록 대체 `ORDER BY 1` 포함).
- `SELECT TOP N`은 사용하지 않으며, OFFSET/FETCH가 표준 페이지 나누기 형식입니다.
- `UPSERT`는 의도적으로 생성하지 않습니다. SQL Server의 `MERGE`에는 알려진 버그가 있어 수동으로 작성해야 합니다.

### 오류 보고

- Tiberius `Server` 토큰 오류는 숫자 코드, 심각도 상태, 소스 줄을 `FormattedError`를 통해 노출합니다.
- 흔한 MSSQL 오류 번호는 일반적인 `QueryFailed` 대신 의미 있는 `DbError` 변형으로 매핑됩니다:

  | 코드                                          | DbError 변형            |
  | --------------------------------------------- | ----------------------- |
  | 4060, 18450, 18452, 18456, 18486, 18487, 18488 | `AuthFailed`            |
  | 229, 230, 262, 297, 916                       | `PermissionDenied`      |
  | 207, 208, 2812, 4902                          | `ObjectNotFound`        |
  | 245, 334, 515, 547, 2601, 2627, 8152          | `ConstraintViolation`   |
  | 102, 156, 8180                                | `SyntaxError`           |

- 제약 조건 위반 메시지는 파싱되어 `ErrorLocation`(스키마, 테이블, 열, 제약 조건 이름)을 채우므로, UI가 문제의 개체를 강조할 수 있습니다.

### 작업 및 제한

- 모든 작업은 `transactional_ddl: true`와 `supports_savepoints: true`를 선언합니다.
- 지원되는 격리 수준: ReadUncommitted, ReadCommitted, RepeatableRead, Serializable, Snapshot. 기본값은 ReadCommitted입니다.

## DDL 동작

- **트랜잭션 DDL.** SQL Server의 대부분의 DDL은 트랜잭션적입니다. `CREATE`, `ALTER`, `DROP TABLE`을 `BEGIN TRAN … COMMIT` / `ROLLBACK`으로 감싸는 것이 동작합니다. 예외: `CREATE DATABASE`, `DROP DATABASE`, `ALTER DATABASE`, `BACKUP`/`RESTORE`, `CREATE FULLTEXT INDEX`는 명시적 트랜잭션 안에서 실행할 수 없습니다.
- **ALTER TABLE 잠금.** `ALTER TABLE … ADD COLUMN <nullable>`은 빠릅니다(메타데이터 전용). 기본값이 있는 NOT NULL 열 추가는 모든 페이지에 기록하며 Sch-M 잠금을 겁니다. `ALTER TABLE … ALTER COLUMN`은 테이블을 다시 쓸 수 있으며 완료될 때까지 읽기와 쓰기를 차단합니다.
- **온라인 인덱스 작업**(Enterprise / Azure SQL): `CREATE INDEX … WITH (ONLINE = ON)`과 `ALTER INDEX … REBUILD WITH (ONLINE = ON)`는 동시 DML을 허용합니다. `ONLINE = ON`이 없으면 인덱스 생성은 Sch-M을 잡고 쓰기를 차단합니다(Standard/Express 에디션은 오프라인만 지원).
- **TRUNCATE TABLE.** 메타데이터 전용, 빠름, 트랜잭션적, 테이블에 대한 `ALTER` 권한 필요. 외래 키가 참조하는 테이블에는 사용할 수 없습니다(`DELETE`를 사용하거나 FK를 먼저 삭제하세요).
- **DROP TABLE / DROP VIEW.** 트랜잭션적입니다. `IF EXISTS`는 2016+에서 지원됩니다.
- **제약 조건.** `CHECK` / `UNIQUE` / `FOREIGN KEY` 제약 조건 추가는 기본적으로 기존의 모든 행을 검증합니다(짧게 Sch-M을 겁니다). 스캔 없이 제약 조건을 추가하려면 `WITH NOCHECK`를 사용하고, 나중에 여유가 될 때 `WITH CHECK CHECK CONSTRAINT`로 검증하세요 — Postgres의 `NOT VALID` + `VALIDATE CONSTRAINT`와 같은 패턴입니다.
## 제한 사항

- 충실한 `CREATE TABLE` 생성(스키마 diff)은 재현할 수 없는 생성 의미론을 가진 테이블을 평탄화하는 대신 거부합니다: 계산 열, 사용자 정의/CLR(및 별칭) 열 타입, sparse 열, `FILESTREAM`, `ROWGUIDCOL`, 메모리 최적화 테이블, system-versioned temporal 테이블, nonclustered 기본 키, row/page 압축, 기본 파일 그룹이 아닌 테이블. 인덱스(기본 키 제외), 외래 키, CHECK/UNIQUE 제약 조건은 `CREATE TABLE`로 전달되지 않으므로 별도로 적용해야 합니다.
- 생성된 기본 키 제약 조건 이름은 서버가 정합니다: 원래 제약 조건 이름은 캡처되지 않아 `PK_…`가 소스와 다릅니다.
- 스키마 diff는 shallow 테이블 목록에서 테이블 전체 생성을 감지합니다. 참조 생성 메타데이터는 **모든 라이브 참조 테이블에 대해 수집**되고(그것을 가진 모든 스냅샷 행에서 읽힘), 전체 테이블 추가(`TableAdded`)—대상에 새 테이블이 생성되는 경우—에만 **소비**됩니다. 기존 테이블의 identity나 기본 키 변경은 diff로 수집·적용되지 않으며 수동으로 처리해야 합니다.
- 생성 메타데이터 지원 이전(DBF-161 PR1)에 캡처된 딥 스냅샷은 메타데이터가 없습니다; diff 참조로 사용하면 생성이 거부되며 스냅샷을 다시 캡처해야 합니다. UI의 연결 시 자동 캡처는 현재 **shallow**이므로, **저장된** 스냅샷은 딥 스냅샷 API를 통해 프로그래매틱하게 캡처된 경우에만 생성 메타데이터를 가집니다; diff 참조가 **라이브 연결**이면 제네릭 `table_creation_metadata` 시임을 통해 메타데이터를 가져오므로 저장된 스냅샷 없이도 생성이 동작합니다.
- 문자 타입 열(`char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`)을 하나라도 가진 테이블은 항상 거부됩니다: `sys.columns.collation_name`은 모든 문자 열에 대해 non-null이며(소스 데이터베이스 기본값에서 암시적으로 상속된 경우에도 마찬가지) 생성 시점에는 target 데이터베이스의 기본 collation을 알 수 없어, 재생성된 열이 조용히 다르게 정렬·비교될 수 있습니다. collation이 없는 타입(정수, 소수, 날짜, 바이너리, 타입 없는 `xml`, …)만으로 구성된 테이블만 충실하게 생성할 수 있습니다.
- XML 스키마 컬렉션에 바인딩된 타입이 지정된 `xml` 열은 거부됩니다: 일반 `xml` 열을 생성하면 컬렉션 바인딩이 조용히 사라집니다. 타입이 없는 `xml` 열은 충실하게 생성됩니다.

- 인스턴스 메트릭과 검사기 기능은 `VIEW SERVER STATE` 서버 권한이 필요합니다. 권한이 없으면 `list_metrics()`와 `list_inspectors()` 모두 오류 대신 빈 목록을 반환합니다.

- 인스턴스 메트릭은 호출당 단일 데이터 포인트를 반환하며(`sys.dm_os_performance_counters`의 현재 값), 과거 시계열이 아닙니다. 비율 카운터(예: `mssql.batch_requests_per_sec`)는 드라이버가 계산한 델타가 아니라 DMV가 보고하는 서버 측 누적 평균을 나타냅니다.

- 지원되는 최소 SQL Server 버전은 2016(13.0)입니다. 드라이버는 `DROP INDEX IF EXISTS … ON …` 구문을 사용하며, 더 오래된 서버는 이를 구문 오류(102)로 거부합니다. Azure SQL Database와 Managed Instance는 문제없습니다.
- `INSTEAD OF` 트리거가 있는 테이블(또는 업데이트 가능한 뷰)에 대한 CRUD는 지원되지 않습니다. 드라이버는 `INTO` 절 없이 `OUTPUT INSERTED.*` / `OUTPUT DELETED.*`를 통해 변경 후 행을 반환하는데, SQL Server는 오류 334("문에 INTO 없는 OUTPUT 절이 포함된 경우 대상 테이블에는 활성화된 트리거가 있을 수 없습니다")로 이를 거부합니다. 이 오류는 `ConstraintViolation`으로 표시됩니다.
- SQL 전용 드라이버이며, 문서 또는 키-값 API를 노출하지 않습니다.
- 취소는 기반 세션을 종료하고 투명하게 다시 연결합니다. SSMS가 사용하는 수술적인 TDS-Attention 취소가 아닙니다(tiberius는 현재 해당 프리미티브를 노출하지 않습니다). 실제로 사용자에게 보이는 유일한 차이는 세션 로컬 상태(`SET` 옵션, 임시 테이블, 열린 트랜잭션)가 취소로 초기화된다는 점입니다. 활성 데이터베이스는 자동으로 복원됩니다.
- 취소 지연 시간은 SQL Server 스케줄러에 따라 달라집니다: CPU 바운드 쿼리는 보통 몇 밀리초, 잠금 대기자는 즉시입니다. 긴 롤백(예: 대규모 `DELETE`를 트랜잭션 도중에 취소)의 경우 드라이버가 이미 새 세션으로 이동한 후에도 *서버 측* SPID가 한동안 KILLED/ROLLBACK 상태로 남아 있을 수 있습니다.
- 매개변수 바인딩을 사용하지 않습니다 — 문은 `simple_query`로 전달됩니다. CRUD 헬퍼는 공유 `SqlQueryBuilder`와 방언 리터럴 포매터를 통해 값을 SQL 텍스트로 조립합니다. 큰 이진 또는 유니코드 페이로드는 `0x…` 또는 `N'…'` 리터럴로 인라인됩니다.
- 스트리밍: 결과 집합은 `Vec<Row>`로 구체화됩니다. `Connection::execute` 트레이트는 완전히 해석된 `QueryResult`를 반환하므로, 커서 스타일 스트리밍은 드라이버만의 수정이 아니라 워크스페이스 수준의 API 변경을 필요로 합니다.
- 다중 문 배치는 모든 비어 있지 않은 결과 집합을 `QueryResult.additional_results`를 통해 노출하지만, UI는 현재 기본(마지막) 집합만 렌더링합니다. 결과 탭 시스템이 `additional_results`를 읽기 전까지는 이전 집합들이 드라이버에 의해 캡처되지만 편집기에서는 보이지 않습니다.
- 배치 도중에 발생한 `PRINT`와 정보성 메시지는 버려집니다. 이를 표시하려면 `QueryStream::into_results()` 대신 tiberius의 더 낮은 수준인 `TokenStream`을 구동해야 합니다.
- `UPSERT`는 의도적으로 생성하지 않습니다. 필요할 때 `MERGE`를 수동으로 사용하세요.
- 명명된 인스턴스는 직접 연결할 때는 적용되지만(tiberius가 UDP 1434의 SQL Browser 서비스에 질의합니다), SSH 터널을 경유할 때는 적용되지 않습니다. libssh2는 TCP만 포워딩하기 때문입니다. 표준 해결 방법은 인스턴스에 고정 TCP 포트를 할당하고 해당 포트로 직접 연결하는 것입니다.
- 스키마 인트로스펙션은 `sys.*` 카탈로그 뷰를 사용합니다. 기본 `VIEW DEFINITION` 권한이 없는 사용자는 부분적인 메타데이터만 볼 수 있습니다. SQL Server의 메타데이터 가시성 규칙이 적용됩니다.
- CLR 루틴(CLR 스칼라 함수, CLR 테이블 반환 함수, CLR 저장 프로시저)과 `ENCRYPTION`으로 만든 모든 루틴은 `OBJECT_DEFINITION`이 `NULL`을 반환하며, 이 경우 드라이버는 오류 대신 짧은 대체 메시지를 표시합니다.
- 루틴에 대해 `parameter_types`가 채워지지 않습니다. 이 구현은 `sys.parameters`를 질의하지 않습니다.
- SQL Server에는 `sys.objects.type` 분류에 `Window` 함수 종류가 없으므로, 이 드라이버는 `RoutineKind::Window`를 절대 내보내지 않습니다.
- 데이터 전송 엔진의 마이그레이션 경로에 대한 참조 무결성 토글이 없습니다(`DriverCapabilities::DISABLE_FK_CHECKS`가 설정되지 않음; `Connection::set_referential_integrity`는 `NotSupported`를 반환합니다). SQL Server는 `ALTER TABLE ... NOCHECK CONSTRAINT`로 테이블별로 FK 검사를 비활성화하는데, 이는 엔진의 단일 전역 토글에 맞지 않습니다. 테이블별 변형은 향후 추가 가능성이 있습니다.
