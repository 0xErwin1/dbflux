# DBFlux 드라이버

이 문서는 DBFlux와 함께 제공되는 데이터베이스 드라이버들의 비교 개요입니다.
드라이버별 세부 사항은 각 드라이버 크레이트의 `README.md` 링크를 따르십시오.
내부 드라이버 아키텍처(트레이트, 등록, `DbDriver`/`Connection` 경계)에 대해서는
[`ARCHITECTURE.md`](../ARCHITECTURE.md)의 **드라이버 시스템** 절을 참조하십시오.
드라이버를 구현하려는 기여자는 [드라이버 작성 가이드](DRIVER_AUTHORING.md)부터
시작하십시오.
## 드라이버 추상화 방식

모든 드라이버는 `DriverMetadata` 값(`crates/dbflux_core/src/driver/capabilities.rs`에 정의)을 노출합니다. UI는 드라이버에 독립적이며 순수하게 이 메타데이터만으로 동작을 조정합니다. 관련 필드는 다음과 같습니다:

- **`DatabaseCategory`** — 뷰 모델과 용어를 선택합니다. 값: `Relational`, `Document`, `KeyValue`, `Graph`, `TimeSeries`, `WideColumn`, `LogStream`. (모든 값에 출시된 드라이버가 있는 것은 아닙니다.)
- **`QueryLanguage`** — 에디터 모드, 자리 표시자 텍스트, 쿼리 파싱을 결정합니다. 값에는 `Sql`, `MongoQuery`, `RedisCommands`, `Cypher`, `InfluxQuery`, `Flux`, `Cql`, `CloudWatchLogsInsightsQl`, `OpenSearchPpl`, `OpenSearchSql`, 스크립트 언어인 `Lua` / `Python` / `Bash`, 그리고 `Custom(String)`이 포함됩니다.
- **`DriverCapabilities`** — 지원되는 기능(트랜잭션, 페이지 나누기, 스키마, 키-값 작업 등)을 선언하는 `u64` 비트플래그 집합입니다. 편의용 베이스인 `RELATIONAL_BASE`, `DOCUMENT_BASE`, `KEYVALUE_BASE`는 각 카테고리의 공통 플래그를 묶어 줍니다.

아래에 나열된 기능 플래그는 각 드라이버의 `DriverMetadata`가 코드에서 설정하는 것들과 정확히 일치합니다. 추론되는 것은 없습니다.

## 비교

| 드라이버 | 카테고리 | 쿼리 언어 | 주요 기능 | 참고 / 제한 |
| --- | --- | --- | --- | --- |
| PostgreSQL | 관계형 | SQL | 관계형 베이스 + 스키마, SSH 터널, SSL, 인증, 외래 키, CHECK/UNIQUE 제약 조건, 사용자 정의 타입, `RETURNING`, 트랜잭션 DDL, 루틴, 다중 문 | 완전한 SQL 드라이버, 루틴 뷰어는 읽기 전용, `CREATE INDEX CONCURRENTLY`를 제외한 트랜잭션 DDL. |
| Amazon Redshift | 관계형 | SQL | 여러 데이터베이스, 스키마, 뷰, SSH 터널, SSL/클라이언트 인증서, 인증, 쿼리 취소, 프리페어드 스테이트먼트, 페이지 나누기, 정렬, 필터, CSV/JSON 내보내기 | PostgreSQL 와이어 프로토콜 기반 읽기 전용, 단일 문만 지원, Redshift 스토리지 힌트 제공, 쓰기/DDL, IAM/SSO, 인덱스 미지원. |
| MySQL | 관계형 | SQL | 관계형 베이스 + SSH 터널, SSL, 인증, 외래 키, CHECK/UNIQUE 제약 조건, 루틴, 다중 문 | DDL은 트랜잭션이 적용되지 않음, 다중 문 스크립트는 텍스트 기준으로 분할되어 순차 실행, 루틴 목록은 FUNCTION/PROCEDURE만 포함. |
| MariaDB | 관계형 | SQL | MySQL과 동일한 크레이트 및 기능 | MySQL 구현을 공유하는 별도의 `mariadb` 메타데이터로 등록됩니다. |
| SQLite | 관계형 | SQL | 뷰, 인덱스, 외래 키, CHECK/UNIQUE 제약 조건, 프리페어드 스테이트먼트, 삽입/업데이트/삭제, 페이지 나누기, 정렬, 필터, CSV/JSON 내보내기, 쿼리 취소, 트랜잭션 DDL, 다중 문 | 임베디드 파일 드라이버, 네트워크·SSH 터널·TLS 없음, 다중 스키마 네임스페이스 없음. |
| SQL Server | 관계형 | SQL | 관계형 베이스 + 스키마, SSH 터널, SSL, 인증, 외래 키, CHECK/UNIQUE 제약 조건, 트랜잭션 DDL, 루틴, 다중 문 | `tiberius` 기반, SSH 터널 경유 시 명명된 인스턴스 조회 불가, 다중 결과 집합 배치는 마지막 집합을 기본으로 반환. |
| MongoDB | 문서 | MongoQuery | 문서 베이스 + 집계, SSH 터널, 인덱스 | MongoDB 셸 스타일 문법만 지원(SQL 없음), 쿼리 취소 없음, 파서는 지원되는 명령 패턴으로 한정. |
| Redis | 키-값 | RedisCommands | 키-값 베이스 + 여러 데이터베이스, TTL, 키 타입, 값 크기, 이름 바꾸기, 벌크 조회, 스트림 범위/추가/삭제, 인증, SSH 터널, SSL | Redis 명령 문법만 지원(SQL 없음), 쿼리 취소 없음, URI 모드에서는 SSH 터널링 불가. |
| DynamoDB | 문서 | Custom("DynamoDB") | 인증, 페이지 나누기, 필터, 삽입/업데이트/삭제, 중첩 문서, 배열 | AWS 관리형, 네이티브 명령 엔벨로프(`scan`/`query`/`put`/`update`/`delete`), PartiQL/트랜잭션 없음, 쿼리 취소 없음, `update many+upsert` 미지원. |
| CloudWatch Logs | 로그 스트림 | Sql (메타데이터 기본값) | 인증 | AWS 관리형, 에디터가 관리하는 소스 컨텍스트를 통해 Logs Insights QL, OpenSearch PPL, OpenSearch SQL 실행, 아직 쿼리 취소 없음. |
| InfluxDB | 시계열 | InfluxQuery | 인증, 여러 데이터베이스, 페이지 나누기, CSV/JSON 내보내기 | v1과 v2를 하나의 크레이트에서 지원, 두 버전 모두 InfluxQL, Flux는 v2 전용, 읽기 전용(INSERT/UPDATE/DELETE 없음), 트랜잭션 없음. |
| ClickHouse | 관계형 | SQL | 여러 데이터베이스, 뷰, 인증, 페이지 나누기, 정렬, 필터, 그룹화, 조인, CTE, 윈도우, CSV/JSON 내보내기 | HTTP(S), ClickHouse Cloud 포함, 구조화된 변경, DDL, 트랜잭션, SSH 터널링, 쿼리 매개변수가 없는 읽기 중심 DBFlux 통합. |
| TursoDB | 관계형 | SQL | 인증 토큰, 뷰, 인덱스, 외래 키, CHECK/UNIQUE 제약 조건, 프리페어드 스테이트먼트, 삽입/업데이트/삭제, 페이지 나누기, 정렬, 필터, CSV/JSON 내보내기, 트랜잭션, 트랜잭션 DDL, 다중 문 | HTTP를 통한 원격 Turso / libSQL(`libsql://`, 로컬 `sqld`는 `http://`), 대화형 트랜잭션은 문서별 서버 스트림에서 실행, 쿼리 취소, SSH 터널, 복제본, 데이터베이스 전환 없음. |
| Amazon S3 | 개체 스토리지 | Custom("S3") | 인증(프로필/SSO 또는 정적 자격 증명, 사용자 지정 엔드포인트), 버킷 탐색, 페이지 나누기 기반 개체 탐색, 미리 보기, 전체 CRUD, 사전 서명된 URL | S3 호환(Cloudflare R2, MinIO), 멀티파트 업로드/전송 패널 없음, 내장 PDF 뷰어 없음, 수명 주기/ACL 관리 및 S3 Select 없음. |

## 드라이버별 요약

### PostgreSQL

스키마 탐색, 저장 루틴(읽기 전용 뷰어), SSL, SSH 터널링, 취소 토큰을 통한 쿼리 취소, 트랜잭션 DDL, PostgreSQL 전용 코드 생성을 갖춘 완전한 SQL 드라이버입니다. 다중 문 스크립트는 단순 쿼리 프로토콜을 통해 일괄 실행됩니다. 자세한 내용은 [`crates/dbflux_driver_postgres/README.md`](../crates/dbflux_driver_postgres/README.md)를 참조하세요.

### Amazon Redshift

PostgreSQL 와이어 프로토콜을 사용하는 읽기 전용 관계형 SQL 드라이버입니다. 스키마, 테이블, 뷰, 열 인트로스펙션, SSH 터널링, TLS와 클라이언트 인증서, 쿼리 취소, Redshift 배포/정렬 키 스토리지 힌트를 지원합니다. 쓰기나 DDL, IAM/SSO 인증, 다중 문 쿼리, 인덱스는 지원하지 않습니다. 자세한 내용은 [`crates/dbflux_driver_redshift/README.md`](../crates/dbflux_driver_redshift/README.md)를 참조하세요.

### MySQL / MariaDB

하나의 크레이트가 MySQL과 MariaDB를 모두 구현합니다. SQL 실행, 스키마 탐색, `KILL QUERY`를 통한 쿼리 취소, 코드 생성, 함수와 프로시저의 루틴 탐색을 지원합니다. DDL은 트랜잭션이 적용되지 않으며 다중 문 분할은 텍스트 기준으로 이루어집니다. 자세한 내용은 [`crates/dbflux_driver_mysql/README.md`](../crates/dbflux_driver_mysql/README.md)를 참조하세요.

### SQLite

스키마 탐색, 인터럽트 핸들을 통한 쿼리 취소, 트랜잭션 DDL, 코드 생성을 갖춘 임베디드 파일 기반 드라이버입니다. 네트워크 전송, SSH 터널링, TLS가 없으며 다중 스키마 네임스페이스도 없습니다. 자세한 내용은 [`crates/dbflux_driver_sqlite/README.md`](../crates/dbflux_driver_sqlite/README.md)를 참조하세요.

### SQL Server

`tiberius` TDS 클라이언트 기반으로 만들어졌습니다. SQL Server / Azure SQL, TLS 모드, 명명된 인스턴스(SQL Browser로 확인), SSH 터널링, 탭별 데이터베이스 전환, 다중 결과 집합 배치를 지원합니다. 자세한 내용은 [`crates/dbflux_driver_mssql/README.md`](../crates/dbflux_driver_mssql/README.md)를 참조하세요.

### MongoDB

컬렉션 탐색, 문서 CRUD, MongoDB 셸 스타일 쿼리 파싱, 집계, 문서 중심 스키마 메타데이터를 갖춘 문서 드라이버입니다. SQL은 지원하지 않으며 쿼리 취소도 사용할 수 없습니다. 자세한 내용은 [`crates/dbflux_driver_mongodb/README.md`](../crates/dbflux_driver_mongodb/README.md)를 참조하세요.

### Redis

문자열, 해시, 리스트, 셋, 정렬된 셋, 스트림을 다루는 키-값 드라이버입니다. 키 스캔, TTL 작업, 이름 바꾸기, 벌크 조회, 여러 논리 데이터베이스도 지원합니다. SQL은 지원하지 않으며 URI 모드에서는 SSH 터널링을 사용할 수 없습니다. 자세한 내용은 [`crates/dbflux_driver_redis/README.md`](../crates/dbflux_driver_redis/README.md)를 참조하세요.

### DynamoDB

리전/프로필/엔드포인트 구성과 함께 `aws-sdk-dynamodb` 기반으로 만들어진 AWS NoSQL 드라이버입니다. 테이블 탐색은 PK/SK와 GSI/LSI 메타데이터를 매핑하고, 실행은 네이티브 명령 엔벨로프(`scan`, `query`, `put`, `update`, `delete`)를 사용합니다. PartiQL과 DynamoDB 트랜잭션은 노출되지 않습니다. 자세한 내용은 [`crates/dbflux_driver_dynamodb/README.md`](../crates/dbflux_driver_dynamodb/README.md)를 참조하세요.

### CloudWatch Logs

에디터가 관리하는 시간 범위와 로그 그룹 소스 컨텍스트로 `StartQuery`를 통해 쿼리를 실행하는 AWS CloudWatch Logs 드라이버입니다. 쿼리 문서는 Logs Insights QL, OpenSearch PPL, OpenSearch SQL을 실행할 수 있으며, 스키마 탐색은 로그 그룹을 나열하고 로그 스트림을 이벤트 스트림 하위 항목으로 노출합니다. 실제 모드는 쿼리 문서별로 선택되지만 `DriverMetadata.query_language`는 기본 에디터 모드로 `Sql`로 설정됩니다. 자세한 내용은 [`crates/dbflux_driver_cloudwatch/README.md`](../crates/dbflux_driver_cloudwatch/README.md)를 참조하세요.

### InfluxDB

하나의 크레이트에서 InfluxDB v1과 v2를 모두 지원하는 시계열 드라이버입니다. InfluxQL은 두 버전 모두에서, Flux는 v2에서만 실행됩니다. 쿼리 API는 읽기 전용(INSERT/UPDATE/DELETE 없음, 트랜잭션 없음)이며, 선택적으로 기본 버킷/데이터베이스와 쿼리별 버킷 라우팅을 지원합니다. 자세한 내용은 [`crates/dbflux_driver_influxdb/README.md`](../crates/dbflux_driver_influxdb/README.md)를 참조하세요.

### ClickHouse

HTTP(S)를 통해 자체 호스팅 ClickHouse와 ClickHouse Cloud를 다루는 관계형 SQL 드라이버입니다. 데이터베이스, 테이블, 뷰, 열, 엔진 메타데이터를 탐색하며 페이지 나누기와 시각적 SELECT 생성이 있는 읽기 중심 SQL 워크플로를 지원합니다. 구조화된 변경, DDL, 트랜잭션, SSH 터널링, 일반 쿼리 매개변수는 이 초기 범위에서 지원하지 않습니다. 자세한 내용은 [`crates/dbflux_driver_clickhouse/README.md`](../crates/dbflux_driver_clickhouse/README.md)를 참조하세요.

### TursoDB

`turso_serverless` SDK 기반으로, HTTP를 통해 Turso Cloud와 자체 호스팅 `sqld`를 다루는 관계형 SQL 드라이버입니다. SQLite 방언을 사용하며 `sqlite_master`와 PRAGMA를 통해 테이블, 뷰, 열, 인덱스, 외래 키, 제약 조건을 탐색하고, 타입화된 CRUD, 바인딩된 매개변수, 일괄 처리된 스크립트, 에디터 실행당 하나의 제어 문을 갖는 문서별 서버 스트림 위의 대화형 트랜잭션을 지원합니다. 쿼리 취소, 저장점, SSH 터널링, 임베디드 복제본, 데이터베이스 전환은 지원하지 않습니다. 자세한 내용은 [`crates/dbflux_driver_turso/README.md`](../crates/dbflux_driver_turso/README.md)를 참조하세요.

### Amazon S3

AWS S3와 S3 호환 엔드포인트(Cloudflare R2, MinIO)를 위한 개체 스토리지 드라이버입니다. 엔드포인트 재정의와 경로 스타일 주소 지정과 함께 AWS 프로필/SSO 또는 정적 자격 증명으로 인증합니다. 연결 루트는 버킷 테이블을 열고, 버킷 탐색은 (AWS 콘솔 방식으로) 수준별 페이지 나누기를 하며 선택적으로 페이지 나누기가 없는 트리 모드를 제공합니다. 개체 미리 보기는 이미지를 기본 지원하고, 텍스트 유사 개체는 저장 후 되돌리기가 가능한 인라인 편집 버퍼로, PDF와 기타 바이너리 개체는 메타데이터와 다운로드/외부 열기를 제공합니다. 아카이브 스토리지 클래스(GLACIER, DEEP_ARCHIVE)는 본문 미리 보기를 완전히 건너뜁니다. 업로드, 삭제, 확인 입력이 필요한 재귀적 프리픽스/버킷 삭제, 폴더/버킷 생성, 이름 바꾸기(복사 후 삭제), 사전 서명된 URL을 지원합니다. 멀티파트 업로드, 전송 패널, 내장 PDF 뷰어, 수명 주기/ACL 관리, S3 Select는 지원하지 않습니다. 자세한 내용은 [`crates/dbflux_driver_s3/README.md`](../crates/dbflux_driver_s3/README.md)를 참조하세요.

## 외부 RPC 드라이버

DBFlux는 프로세스 외부에서 실행되고 로컬 IPC로 통신하는 드라이버를 로드할 수 있으며, `dbflux_driver_ipc`로 구현되고 `dbflux_driver_host`를 통해 호스팅됩니다. 이 드라이버들은 `rpc:<socket_id>` 형식의 합성 ID로 등록하고 카테고리, 쿼리 언어, 기능을 담은 자체 `DriverMetadata`를 와이어로 전달하므로, UI는 이를 내장 드라이버와 정확히 동일하게 취급합니다. 탐색 핸드셰이크, 서비스 수명 주기, 프로토콜 세부 사항은 [`docs/DRIVER_RPC_PROTOCOL.md`](DRIVER_RPC_PROTOCOL.md)를 참조하세요.
