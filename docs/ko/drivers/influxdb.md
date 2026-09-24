# InfluxDB

InfluxQL과 Flux 쿼리를 지원하는 InfluxDB v1 및 v2 시계열 데이터베이스입니다.

## 한눈에 보기

- **카테고리** — 시계열
- **쿼리 언어** — InfluxQL / Flux
- **기본 포트** — 8086
- **URI 스킴** — `http`

DBFlux용 InfluxDB 드라이버입니다.

## 기능

- **시계열 카테고리** — `DatabaseCategory::TimeSeries`로 분류되며 기본 에디터 언어는 `QueryLanguage::InfluxQuery`입니다. 선언된 기능은 `AUTHENTICATION`, `MULTIPLE_DATABASES`, `PAGINATION`, `EXPORT_CSV`, `EXPORT_JSON`, `CHART_AUTHORING`, `INSTANCE_METRICS`, `INSTANCE_INSPECTOR`입니다. 연결은 기본 포트 8086에서 `http` URI 스킴을 사용하며, TLS는 rustls 기반 HTTP 클라이언트가 제공합니다.
- **InfluxDB v1과 v2** — 두 API 버전 모두 하나의 드라이버 크레이트에서 지원됩니다.
- **두 버전 모두에서 InfluxQL** — v1 쿼리 언어는 v1에서와 v2 호환성 엔드포인트를 통해 동작합니다.
- **v2에서 Flux** — 연결이 v2로 구성된 경우 Flux 쿼리를 사용할 수 있습니다.
- **선택적 기본 버킷** — 연결 프로필의 버킷(v2) 또는 데이터베이스(v1) 필드는 선택 사항입니다. v2 API 토큰은 조직의 모든 버킷에 접근할 수 있고, v1 사용자는 서버의 모든 데이터베이스에 접근할 수 있습니다. 필드를 비워 두면 사용자가 에디터의 소스 컨텍스트 드롭다운에서 쿼리별로 버킷을 선택할 수 있습니다. 값을 설정하면 다른 버킷에 대한 접근을 제한하지 않으면서 해당 버킷을 미리 선택합니다.
- **쿼리별 버킷 라우팅** — 각 InfluxQL 쿼리에 사용되는 버킷은 연결 프로필이 아니라 소스 컨텍스트 드롭다운 선택에서 옵니다. Flux 쿼리에서는 버킷이 쿼리 텍스트 자체에 포함됩니다(`from(bucket: "...")`).
- **버킷 없는 ping** — 연결 생존 확인에는 버킷이 필요하지 않습니다. v1은 내부 데이터베이스에 대해 `SHOW DATABASES`를 사용하고, v2는 `/api/v2/buckets?limit=1`을 가져옵니다.
- **시간 범위 매크로** — InfluxQL과 Flux 쿼리는 Grafana 호환 매크로 토큰을 지원하며, 이 토큰은 쿼리가 드라이버로 전송되기 전에 바인딩된 시간 범위 창으로 치환됩니다:

  | 토큰 | 언어 | 확장 |
  |---|---|---|
  | `$timeFilter` | InfluxQL | `time >= 'RFC3339_start' AND time <= 'RFC3339_end'` |
  | `$__from` | InfluxQL | `'RFC3339_start'` |
  | `$__to` | InfluxQL | `'RFC3339_end'` |
  | `v.timeRangeStart` | Flux | `'RFC3339_start'` |
  | `v.timeRangeStop` | Flux | `'RFC3339_end'` |

  이 토큰들은 Grafana의 변수 규칙과 일치합니다(InfluxQL은 `$timeFilter`, Flux는 `v.timeRangeStart`/`v.timeRangeStop`). Grafana에 익숙한 사용자에게는 이 문법이 직관적으로 느껴질 것입니다.

  RFC3339 형식: `YYYY-MM-DDTHH:MM:SSZ`(UTC, 초 단위 정밀도, Z 접미사).

  **InfluxQL 예제** — `$timeFilter` 사용:

  ```influxql
  -- 입력:
  SELECT mean(usage_user) FROM cpu WHERE $timeFilter GROUP BY time(1m)

  -- 실행됨 (window = 2026-05-20T00:00:00Z to 2026-05-22T23:59:00Z):
  SELECT mean(usage_user) FROM cpu WHERE time >= '2026-05-20T00:00:00Z' AND time <= '2026-05-22T23:59:00Z' GROUP BY time(1m)
  ```

  **Flux 예제** — `v.timeRangeStart` / `v.timeRangeStop` 사용:

  ```flux
  -- 입력:
  from(bucket: "telegraf")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "cpu")

  -- 실행됨 (같은 window):
  from(bucket: "telegraf")
    |> range(start: '2026-05-20T00:00:00Z', stop: '2026-05-22T23:59:00Z')
    |> filter(fn: (r) => r._measurement == "cpu")
  ```

  **매크로에는 바인딩된 창이 필요합니다** — 쿼리에 매크로 토큰이 있지만 시간 범위 창이 설정되어 있지 않으면(즉, 소스 컨텍스트 패널에 선택이 없으면) 매크로는 치환되지 않은 채 드라이버로 전달됩니다. `$timeFilter` 등은 유효한 InfluxQL/Flux 문법이 아니므로 InfluxDB는 파싱 오류를 반환합니다.

  **매크로가 있으면 자동 주입은 억제됩니다** — 쿼리에 인식되는 매크로 토큰이 하나라도 있으면 자동 시간 창 주입(아래 참조)은 억제됩니다. 매크로 치환이 사용자의 권위 있는 시간 경계로 취급됩니다.

  **v1의 알려진 제한(단순 부분 문자열 치환)** — 따옴표로 묶인 문자열 리터럴이나 주석 안의 매크로 토큰도 치환됩니다. v1에는 이스케이프 문법이 없습니다. Flux에서는 이름이 단지 `v.timeRangeStart`나 `v.timeRangeStop`으로 시작하는 변수(예: `v.timeRangeStartCustom`)도 치환됩니다. 적절한 토큰화는 향후 버전에서 계획되어 있습니다.

- **자동 시간 창 주입** — 소스 컨텍스트 패널을 통해 시간 범위가 설정되어 있고 쿼리에 이미 시간 조건자가 없는 경우(InfluxQL은 `time >=` 등, Flux는 `|> range(`), 드라이버가 경계를 자동으로 주입합니다. 쿼리에 명시적인 시간 범위 매크로 토큰이 있으면 이 동작은 억제됩니다.
- **구조화된 오류 메시지** — 서버 측 오류는 원시 HTTP 상태 코드로 표시되는 대신 JSON `{"error": "..."}` 필드에서 파싱됩니다.
- **CSV 및 JSON 내보내기** — 쿼리 결과는 표준 DBFlux 내보내기 파이프라인을 통해 내보낼 수 있습니다.
- **감사 이벤트 발행** — 모든 쿼리는 표준 DBFlux 감사 싱크(audit sink)를 통해 추적됩니다. `bucket_or_database` 메타데이터 필드는 프로필 기본값이 아니라 각 쿼리에 실제로 사용된 버킷을 기록합니다.
- **다중 문 InfluxQL** — 쿼리에 `;`로 구분된 여러 문이 있으면(예: `SHOW MEASUREMENTS; SHOW SERIES`), 모든 결과가 하나의 결과 집합으로 이어집니다. 서로 다른 문의 행을 구별하기 위해 합성 `statement_index` 정수 열이 앞에 붙습니다.
- **"Query Measurement" 상황에 맞는 메뉴** — 사이드바에서 measurement를 마우스 오른쪽 버튼으로 클릭하면 "Query Measurement"가 표시됩니다. 이 작업은 템플릿 쿼리(InfluxQL은 `SELECT * FROM ...`, Flux는 `from(bucket: ...) |> range(...)`)가 미리 채워진 새 코드 문서를 엽니다.
- **measurement를 열면 차트로 표시됩니다** — 사이드바에서 measurement를 열면 해당 measurement가 속한 버킷 또는 데이터베이스에 대해 InfluxQL로 `SELECT * FROM "<measurement>" ORDER BY time DESC LIMIT <n> OFFSET <m>`을 실행합니다(v2에서는 InfluxQL 호환 엔드포인트를 통해). `InfluxQueryGenerator::collection_browse_query`가 같은 문장을 반환하므로 데이터 그리드의 도구 모음과 상태 표시줄은 실제로 실행되는 InfluxQL 쿼리를 보여줍니다. 결과에는 타임스탬프 열과 타입이 지정된 필드 열이 있으므로 measurement는 Chart 뷰로 열리며, 행은 Data 뷰에서 한 번의 클릭으로 볼 수 있습니다.
- **버킷의 "New Query" 상황에 맞는 메뉴** — 버킷/데이터베이스 노드를 마우스 오른쪽 버튼으로 클릭하면 "New Query"가 표시되며, 연결이 활성화된 빈 코드 문서를 엽니다.
- **읽기 템플릿 생성** — `InfluxQueryGenerator`는 InfluxQL과 Flux 모두에 대해 전체 선택 및 measurement별 읽기 템플릿을 생성하며(상황에 맞는 메뉴 작업과 쿼리로 복사에서 사용), 연결에 구성된 버전과 기본 버킷에 따라 버전을 인식합니다.
- **클라이언트 식별** — 모든 HTTP 요청은 `User-Agent` 헤더로 `dbflux/<version>`을 보고하며, 서버 측 요청 로그에 표시됩니다.
- **방언을 인식하는 위험 쿼리 감지** — `InfluxLanguageService`는 쿼리 텍스트에서 InfluxQL과 Flux를 구분하고(Flux는 파이프라인 형태이거나 `import`로 시작), 실행 전에 InfluxQL의 `DROP DATABASE/MEASUREMENT/SERIES/RETENTION POLICY/SHARD`, `WHERE` 절 없는 `DELETE`, 그리고 Flux의 `delete()`/`influxdb.delete()` 호출에 표시를 합니다. `classify_execution`은 `SELECT`/`SHOW`를 읽기로, `DELETE`/`DROP`/Flux `delete()`를 파괴적으로 보고하므로 거버넌스 정책은 일반적인 쓰기 대신 정확한 영향도를 봅니다. `validate`는 전체 파서가 아니라 구문 수준의 온전성 검사(괄호와 따옴표의 균형)입니다.
- **쓰기 권한 프로브(v2 전용)** — 연결 후 `GET /api/v2/authorizations`를 가져와 연결 토큰 자체의 권한을 검사합니다. `buckets`에 대한 `write` 권한이 있으면 쓰기 가능으로 판정되고, `read` 권한만 있으면 읽기 전용으로 판정되며, 요청이 실패하거나 금지되면(토큰에는 대개 `read:authorizations`가 없음) 판정된 변경 정책이 그대로 유지됩니다. v1에는 동등한 토큰 범위 API가 없으므로 절대 프로브하지 않습니다.
- **인스턴스 지표와 검사기(v2 전용)** — `InstanceCatalog`는 `_monitoring` 버킷이 아니라 서버 자체의 `GET /metrics` 엔드포인트(Prometheus 텍스트 노출 형식)에서 차트 가능한 시계열과 표 형태 스냅샷을 가져옵니다. `_monitoring`은 Tasks 및 Checks 시스템이 쓰는 알림 검사/알림 레코드를 담고 있으며 기본 설치에서는 비어 있고, `/metrics`가 InfluxDB의 실제 텔레메트리 표면입니다. 지표는 Go 런타임 메모리, 고루틴과 OS 스레드, 서버 가동 시간과 버킷 수, BoltDB 읽기/쓰기 카운터, HTTP API 요청 수를 다룹니다. 레이블 조합으로 분할된 Prometheus 샘플(예: 핸들러/경로/상태별 `http_api_requests_total`)은 지표별로 하나의 값으로 합산됩니다. 두 개의 검사기가 노출됩니다. `influx.health`(`GET /health`의 스냅샷)와 `influx.metrics`(이름/레이블/값 표로 된 전체 Prometheus 스크랩)입니다. 엄선된 `DefaultInstanceDashboard`가 둘을 결합합니다.

## 제한 사항

- `execute()`는 요청한 행 제한(0 포함) 또는 명령문 시간 제한이 있으면 HTTP 요청이나 인스턴스 컨텍스트 디스패치 전에 `NotSupported`를 반환합니다. 보호 옵션이 없는 쿼리는 계속 실행할 수 있지만 기본 편집기는 이 보호를 보장하지 않습니다.

- **쿼리 취소 없음** — `cancel()`은 `NotSupported`를 반환하며, 진행 중인 쿼리는 UI에서 중단할 수 없습니다(`QUERY_CANCELLATION`이 선언되지 않음).
- **변경 생성 없음** — `QueryGenerator::generate_mutation`은 항상 `None`을 반환합니다. 읽기 전용 쿼리 API와 일치하도록 읽기 템플릿만 생성됩니다.
- **v1에서 Flux 미지원** — v1 연결에 대해 Flux 쿼리를 실행하려고 하면 HTTP 호출 없이 즉시 오류를 반환합니다.
- **INSERT/UPDATE/DELETE 없음** — InfluxDB의 쿼리 API는 읽기 전용입니다. 데이터 수집은 이 드라이버가 노출하지 않는 Line Protocol 쓰기 API를 사용합니다.
- **트랜잭션 없음** — InfluxDB는 트랜잭션을 지원하지 않습니다.
- **InfluxQL에는 버킷이 필요합니다** — InfluxQL 쿼리는 버킷을 URL에 포함합니다(`?db=<bucket>`). 소스 컨텍스트 드롭다운과 프로필 기본값 어느 쪽도 버킷을 제공하지 않으면, 사용자에게 버킷을 선택하라고 요청하는 명확한 오류와 함께 실행이 거부됩니다.
- **정규식 기반 시간 조건자 감지** — 드라이버는 쿼리에 이미 시간 조건자가 있는지 판단하는 데 정규식을 사용합니다. `time <`, `time >`, `|> range(`와 일치하는 텍스트가 우연히 들어 있는 따옴표로 묶인 문자열 리터럴에서 거짓 양성이 나올 수 있습니다.
- **다중 문의 열은 첫 번째 비어 있지 않은 문이 고정합니다** — 다중 문 쿼리가 서로 다른 형태의 결과를 반환할 때(예: `SHOW MEASUREMENTS; SHOW SERIES`), 열 배치는 첫 번째 비어 있지 않은 문이 결정합니다. 이후 문의 행은 그 배치에 매핑됩니다. 형태가 맞지 않으면 오류 대신 열이 어긋난 결과가 나옵니다.
- **Authorization 헤더를 통한 기본 인증** — v1 사용자 이름/비밀번호 자격 증명은 URL 쿼리 매개변수가 아니라 `Authorization: Basic <base64>` 헤더로 전송됩니다. 로그 위생 측면에서 더 깔끔하지만 일부 InfluxDB 클라이언트 라이브러리와는 다릅니다.
- **하위 호환 직렬화** — 이전의 필수 `bucket_or_database` 필드로 저장된 프로필도 계속 올바르게 로드됩니다. 이 필드는 serde 별칭을 통해 `default_bucket`으로 역직렬화됩니다. 이 변경 이후에 저장된 프로필은 `default_bucket` 키를 사용합니다.
- **인스턴스 지표와 검사기는 v2 전용입니다** — `instance_catalog()`는 v1 연결에서 `None`을 반환합니다. v1도 `/metrics`를 제공하지만 그 노출에는 이 카탈로그가 선언하는 v2 지표 이름이 없고, `/health`는 v2 엔드포인트입니다(v1은 `/ping`을 제공). `execute()`는 v1 연결에 대한 인스턴스 쿼리를, 카탈로그가 검증된 적 없는 표면에서 답하는 대신 `NotSupported`로 거부합니다. 이는 위의 v1 쓰기 권한 프로브 제한과 같은 방식입니다.
- **인스턴스 지표 행 작업은 지원되지 않습니다** — `InstanceCatalog::row_actions`는 트레이트 기본값(빈 목록)을 사용합니다. `/metrics`는 읽기 전용 텔레메트리 스크랩이며 행에서 트리거할 서버 측 작업이 없습니다.
- **measurement 조회는 필터 입력을 무시합니다** — `browse_collection`과 `count_collection`은 컬렉션 필터를 읽지 않으므로 데이터 그리드의 필터 상자에 입력한 텍스트는 행을 줄이지 않습니다.
- **measurement 조회에는 시간 창이 없습니다** — 조회는 전체 보존 기간에서 가장 최근 행을 읽으며, 차트의 시간 범위 프리셋은 measurement에 제공되지 않습니다. 시간 범위가 있는 쿼리에는 "Query Measurement"를 사용하세요.
- **결과에서 태그와 필드를 구분하지 않습니다** — 둘 다 일반 열로 전달됩니다. 차트는 첫 번째 텍스트 열로 그룹화하며, 이는 보통 태그이지만 문자열 필드일 수도 있습니다.
