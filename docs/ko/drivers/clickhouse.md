# ClickHouse

HTTP를 통한 열 지향 분석 데이터베이스입니다.
## 한눈에 보기

- **분류** — 관계형
- **쿼리 언어** — SQL
- **기본 포트** — 8123
- **URI 스킴** — `http`

## 연결

이 드라이버는 네이티브 프로토콜이 아닌 ClickHouse의 HTTP 인터페이스로 통신하므로,
엔드포인트는 호스트/포트 쌍이 아니라 URL입니다.

| 필드 | 기본값 | 참고 |
|---|---|---|
| HTTP URL | `http://localhost:8123` | `https://` 엔드포인트는 rustls를 통해 처리됩니다 |
| 데이터베이스 | `default` | 스키마 검색과 한정자 없는 쿼리의 범위를 지정합니다 |
| 요청 시간 초과 | `30`초 | 0보다 커야 합니다 |
| 사용자 | `default` | |
| 비밀번호 | — | OS 키링에 저장되며 HTTP Basic 인증으로 전송됩니다 |

## 기능

- rustls 기반의 블로킹 HTTP(S) 전송 계층으로, HTTP Basic 인증을 사용합니다.
- 임의의 단일 문 SQL을 지원하며, 열 이름과 타입이 행과 함께 도착하도록 응답을 `JSONCompact`로 강제합니다.
- 모든 응답에서 행 너비를 선언된 열 개수와 대조해 검사하므로, 형식이 잘못된 응답은 열 사이에 값이 어긋나는 대신 명확한 오류로 실패합니다.
- `system.databases`, `system.tables`, `system.columns`를 통한 스키마 검색: 데이터베이스, 테이블, 뷰, 열, 엔진, 정렬 키와 파티션 키, 디스크상 크기, 압축.
- 스키마 로딩은 데이터베이스별로 지연 수행되므로, 데이터베이스가 많은 서버도 연결 시점에 모든 데이터베이스에 대한 비용을 치르지 않습니다.
- SQL은 수정 없이 HTTP로 전송됩니다. 기존 `offset` 요청 값은 HTTP URL 매개변수로 전송되며, 여기서는 서버에서의 동작을 보증하지 않습니다.
- ClickHouse의 식별자와 리터럴 인용 규칙을 사용하는 읽기 전용 시각적 SELECT 생성.
- 쿼리 결과로 차트 작성, CSV 및 JSON 내보내기.
- 모든 HTTP 요청은 `User-Agent` 헤더로 `dbflux/<version>`을 보고하며, 서버 측 요청 로그에서 확인할 수 있습니다.
- 위험한 쿼리 감지는 공유 `SqlLanguageService`를 사용합니다(ClickHouse 전용 재정의 없음): `ALTER TABLE ... DELETE WHERE ...`와 `ALTER TABLE ... UPDATE ... WHERE ...`는 이미 `Alter`로 감지되고(하위 명령과 무관하게 `ALTER`로 시작하는 모든 문이 표시됨), `TRUNCATE`/`DROP`도 평소와 같이 감지됩니다. `KILL QUERY`/`KILL MUTATION`과 `OPTIMIZE TABLE ... FINAL`은 표시되지 않습니다 — 둘 다 행을 삭제하거나 테이블 구조를 변경하지 않습니다 — 이는 다른 관계형 드라이버가 유사한 비파괴적 관리 문을 다루는 방식과 일치합니다.
- 쓰기 권한 검사: 연결 후 현재 사용자와 활성 세션 역할에 대해 `readonly` 서버 설정과 `system.grants`를 확인하여 읽기 전용 세션이나 `INSERT`/`ALTER UPDATE`/`ALTER DELETE` 권한이 없는 사용자를 감지하고, 서버가 어차피 쓰기를 거부할 경우 확인된 변경 정책을 읽기 전용으로 강화합니다(부작용 없음; 역할의 역할을 통해서만 도달 가능한 권한은 확인하지 않으며 정책을 변경하지 않습니다).
- 인스턴스 지표와 검사기(`INSTANCE_METRICS`/`INSTANCE_INSPECTOR`): `system.metrics`, `system.events`, `system.asynchronous_metrics`에서 가져온 게이지와 카운터의 엄선된 집합(활성 쿼리, 추적 중인 메모리, TCP/HTTP 연결, 선택/삽입된 행, 사용 가능한 OS 메모리)과 "Kill query" 행 작업이 있는 `system.processes` 실행 중인 쿼리 검사기. 이 kill 작업은 카탈로그를 만들 때 한 번 실행되는 `KILL QUERY` 권한 검사로 게이트되며, 해당 검사가 성공하지 않으면 작업이 숨겨집니다. 따라서 어떤 이유로든 검사가 실패하면 세션이 실행할 수 없는 파괴적 컨트롤을 UI에 제공하는 대신 노출하지 않습니다.

### 타입 처리

값은 재귀적으로 디코딩되므로, `Map(String, Array(Nullable(Decimal256)))`는
원시 텍스트가 아니라 완전한 구조로 도착합니다:

- 래퍼 — `Nullable`, `LowCardinality`
- 컨테이너 — `Array`, `Tuple`, `Map`, `Nested`
- 숫자 — `UInt256`까지의 정수, `Decimal256`, `BFloat16`, `Bool`
- 시간 — `Date32`, `DateTime64`
- 기타 — `Enum16`, `Nothing`

## 제한 사항

- SSH 터널을 지원하지 않습니다.
- 트랜잭션, 준비된 문(prepared statement), 쿼리 취소를 지원하지 않습니다. 명시적인 `QueryRequest` 행 제한(0 포함)과 문 시간 초과는 실행 전에 거부됩니다. 구성된 기본 HTTP 전송 시간 초과는 그대로 적용되지만 서버 작업이나 변경 작업의 중단을 보장하지 않습니다. 제한 없는 쿼리는 전체 결과를 메모리에 할당할 수 있습니다(HTTP 응답 본문 상한 이내). 드라이버는 잠금 시간 초과 지원을 보고하지 않습니다.
- 구조화된 `INSERT`, `UPDATE`, `DELETE`, DDL 또는 데이터 전송 기능을 지원하지 않습니다. 쓰기 SQL은 편집기에 직접 입력할 때만 실행되므로 그리드는 읽기 전용이며 이 드라이버는 전송 대상이 아닙니다.
- 요청당 하나의 SQL 문; 여러 문으로 이루어진 스크립트는 일괄 처리되지 않습니다.
- HTTP 응답 본문은 128 MiB로 제한됩니다.
- 이름이 지정된 ClickHouse 표준 시간대는 클라이언트 측에서 해석되지 않습니다. 오프셋이 있는 ISO 타임스탬프는 정확하게 처리되며, 오프셋이 없는 타임스탬프는 UTC로 취급됩니다.
