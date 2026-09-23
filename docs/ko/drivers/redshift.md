# Amazon Redshift

와이어 프로토콜이 PostgreSQL과 호환되는 AWS 관리형 데이터 웨어하우스입니다. 읽기 전용입니다.
## 한눈에 보기

- **분류** — 관계형
- **쿼리 언어** — SQL
- **기본 포트** — 5439
- **URI 스킴** — `redshift`

Amazon Redshift 드라이버(읽기 전용 v1)로, `dbflux_driver_postgres` 위에 만들어지는 대신 [`postgres`](https://crates.io/crates/postgres) 와이어 클라이언트를 직접 기반으로 만들어졌습니다.

## 기능

- Redshift 클러스터 또는 Redshift Serverless 엔드포인트에 대해 PostgreSQL 와이어 프로토콜로 통신하는 관계형 드라이버(`DatabaseCategory::Relational`, `QueryLanguage::Sql`)입니다.
- 호스트, 포트(기본값 `5439`), 데이터베이스, 사용자, 비밀번호, SSL/`sslmode`(`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`), 연결 URI 모드(`redshift://...`, 내부적으로 `postgresql://...`로 정규화), SSH 터널링을 갖춘 연결 폼입니다.
- 사용자 지정 TLS 신뢰와 상호 TLS: TLS가 활성화된 모드에서는 고정된 private 루트 CA(PEM)가 시스템 루트 위에 신뢰 저장소에 추가되고, 클라이언트 인증서 + 개인 키(PEM/PKCS#8)가 상호 TLS를 활성화합니다. 인증서 자료는 폼에서 구성된 경로에서 연결별로 로드되며, 검증은 절대 약화되지 않습니다(`verify-ca`/`verify-full`은 여전히 신뢰할 수 없는 인증서를 거부합니다). 누락되었거나 읽을 수 없거나 형식이 잘못된 인증서/키 파일은 시스템 신뢰 저장소로 조용히 대체되는 대신 명확한 연결 오류로 표면화되며, 개인 키 내용은 절대 로깅되지 않습니다.
- 데이터베이스, 스키마, 테이블, 뷰, 열에 대한 `information_schema` 기반 스키마 인트로스펙션. 표준 PostgreSQL OID를 따르는 `ColumnKind` 분류(timestamp/integer/float/text)를 포함합니다.
- Redshift 전용 확장 타입(`SUPER`, `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`)은 `ColumnKind::Text`로 분류되어 텍스트로 렌더링됩니다. 그 외 인식되지 않는 OID는 패닉 대신 방어적인 UTF-8 텍스트 디코딩으로 대체됩니다.
- 테이블 상세 정보는 일반 `TableInfo.storage_hints` 연결 지점을 통해 Redshift 전용 스토리지 메타데이터를 표면화합니다(`SVV_TABLE_INFO`와 `PG_TABLE_DEF`에서 읽음): 배포 키(`KEY`/`EVEN`/`ALL`/`AUTO`, 해당하는 경우 키 열 포함)와 정렬 키(compound 또는 interleaved, 정렬된 열과 함께). 선언된 기본 키, 외래 키, UNIQUE 제약 조건은 여전히 표준 코어 메타데이터 형태를 통해 표면화되며, 각각 권고(advisory)/미강제로 표시됩니다 — Redshift는 이러한 제약 조건을 받아들이지만 절대 강제하지 않으며, 이로부터 인덱스 목록을 만들어내지 않습니다.
- 쿼리 실행(`SELECT`/탐색)은 표준 `Connection::execute` 경로를 통해 타입이 지정된 열과 함께 행을 반환합니다. 쿼리 취소는 와이어 클라이언트의 취소 토큰을 통해 지원됩니다.
- `RedshiftErrorFormatter`는 일반적인 연결 및 쿼리 실패(시간 초과, 연결 거부, 인증 실패, 접근할 수 없는 클러스터, `SQLSTATE`가 포함된 쿼리 오류)를 원시 디버그 출력 대신 명확한 드라이버 형식의 메시지로 매핑합니다.
- 연결 URI가 자체 `application_name` 쿼리 매개변수를 이미 설정하지 않는 한 모든 연결에서 `application_name`을 `dbflux/<version>`으로 보고합니다.

## 제한 사항

- 요청한 행 제한(0 포함)과 문 실행 시간 제한은 읽기 전용 검증 및 쿼리 준비 전에 거부됩니다. 이 드라이버는 실행 전에 이를 보장할 수 없습니다. 두 옵션이 없으면 기존 쿼리 동작이 유지되고 전체 결과가 버퍼링될 수 있습니다. 서버 작업량이나 메모리 사용량을 제한하지 않습니다. 조기 거부 회귀 테스트는 Redshift 클러스터가 아닌 일회용 PostgreSQL 16으로 와이어 프로토콜 호환 경로만 검증합니다.

- 읽기 전용: `DriverMetadata.capabilities`는 `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, `BULK_INSERT`, `TRUNCATE_TABLE`, 모든 DDL/트랜잭션 DDL 플래그를 생략합니다. 이 드라이버에는 인라인 그리드 편집과 변경/시각적 쿼리 빌더가 없습니다. `Connection::execute`는 추가로 와이어 계층에서 읽기가 아닌 모든 문을 명시적인 오류로 거부하므로, 쓰기 시도가 조용한 no-op이 되는 일은 없습니다.
- 단일 문 전용: `Connection::execute`는 한 번에 하나의 읽기 전용 문을 실행합니다. 다중 문 입력(예: `SELECT 1; SELECT 2`)은 와이어에 도달하기 전에 명시적인 오류로 거부됩니다. 선택적인 단일 후행 `;`은 허용되며, 문자열 리터럴, 인용된 식별자, 주석 내부의 `;`는 구분자로 취급되지 않습니다.
- `INDEXES` 기능 없음: Redshift에는 진짜 인덱스 구조가 없으므로 `TableDetails.indexes`는 (강제되지 않는) 기본 키에서 합성되는 대신 항상 `None`입니다.
- 트리거 없음: Redshift는 트리거를 지원하지 않으므로 트리거를 발견하거나 표면화하지 않습니다.
- IAM/SSO 기반 인증 없음. 사용자 이름/비밀번호(선택적으로 SSH 터널을 통해)만 지원되며, Redshift의 IAM 기반 `GetClusterCredentials`와 브라우저 SSO 흐름은 구현되지 않았습니다.
- 상호 TLS용 클라이언트 인증서는 PEM 인증서와 PKCS#8 PEM 개인 키로 제공해야 하며(둘은 별도의 파일 경로로 구성됨), 결합된 PKCS#12 번들은 받지 않습니다. 루트 CA / 클라이언트 인증서 로드 경로의 PEM 파싱과 오류 처리는 단위 테스트로 검증되지만, private CA가 앞에 있는 클러스터나 클라이언트 인증서를 요구하는 클러스터에 대한 엔드투엔드 TLS 핸드셰이크는 `#[ignore]`된 라이브 통합 테스트(`redshift_live_verify_full_with_private_ca_and_client_cert`)로만 검증됩니다. 로컬이나 Docker 기반 Redshift 엔진이 존재하지 않기 때문입니다.
- `COPY`/`UNLOAD` 지원이 없고 Redshift 전용 데이터 전송/대량 내보내기 통합도 없습니다.
- 쿼리 계획 시각화가 없습니다(`EXPLAIN` 출력은 파싱되거나 렌더링되지 않음).
- 인스턴스 메트릭과 인스턴스 검사기가 없습니다(`INSTANCE_METRICS`/`INSTANCE_INSPECTOR`가 선언되지 않음).
- 쓰기 권한 프로브 없음: `Connection::probe_write_privilege`는 트레이트 기본값(`WritePrivilege::Unknown`)에 그대로 머뭅니다. 드라이버가 연결된 역할의 실제 권한과 무관하게 모든 변경 문을 와이어 계층에서 이미 거부하기 때문입니다.
- `SUPER`/`VARBYTE`/`GEOMETRY`/`GEOGRAPHY`/`HLLSKETCH`에 사용되는 확장 타입 OID 값과 스토리지 힌트에 사용되는 정확한 `SVV_TABLE_INFO`/`PG_TABLE_DEF` 쿼리 형태는 `#[ignore]`된 라이브 통합 테스트(`crates/dbflux_driver_redshift/tests/live_integration.rs`)로만 검증됩니다. 로컬이나 Docker 기반 Redshift 엔진이 존재하지 않기 때문입니다. 실제 클러스터에 대해 `cargo nextest run -p dbflux_driver_redshift --run-ignored all`로 명시적으로 실행하세요.
- `NUMERIC`/`DECIMAL` 값은 디코딩됩니다: 드라이버는 PostgreSQL 바이너리 `NUMERIC` 와이어 형식을 정확한 `Value::Decimal` 문자열로 직접 파싱합니다(열에 선언된 scale까지의 정수/소수 재구성, `NaN`/±`Infinity` 포함). 형식이 잘못된 페이로드는 데이터를 손상시키는 대신 안전하게 대체됩니다. 바이너리 디코더는 합성 와이어 페이로드에 대한 단위 테스트로 검증되며, 엔드투엔드 충실도는 여전히 `#[ignore]`된 통합 테스트를 통해 라이브 클러스터에서만 검증됩니다.
