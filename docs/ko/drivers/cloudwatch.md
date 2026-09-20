# CloudWatch Logs

편집기 관리 소스 컨텍스트를 사용하는 AWS CloudWatch Logs Insights 쿼리입니다.
## 한눈에 보기

- **분류** — 로그 스트림
- **쿼리 언어** — Logs Insights (SQL 편집기 모드)
- **URI 스킴** — `cloudwatch`

[`aws-sdk-cloudwatchlogs`](https://crates.io/crates/aws-sdk-cloudwatchlogs) SDK를 기반으로 구축된 DBFlux용 AWS CloudWatch Logs 드라이버입니다.

## 기능

- `DatabaseCategory::LogStream`으로 분류되는 로그 스트리밍 드라이버입니다. `deployment_class`는 `CloudManaged`이며, 선언된 기능은 `AUTHENTICATION`과 `METRIC_SERIES`입니다.
- 리전, 이름이 지정된 프로필, 선택적 엔드포인트 재정의를 통한 AWS 연결 구성으로, DynamoDB의 AWS 연결 흐름과 일치합니다.
- 대상 로그 그룹과 시간 범위를 공급하는 편집기 관리 소스 컨텍스트와 함께, `StartQuery` + 폴링 `GetQueryResults`(폴링 간격 500 ms, 최대 120회 시도)를 통한 쿼리 실행.
- 소스 컨텍스트의 "Syntax" 드롭다운에서 선택할 수 있는 세 가지 쿼리 문법:
  - CloudWatch Logs Insights QL (`cwli`, 기본값) — `QueryLanguage::CloudWatchLogsInsightsQl`.
  - OpenSearch PPL (`ppl`) — `QueryLanguage::OpenSearchPpl`.
  - OpenSearch SQL (`sql`) — `QueryLanguage::OpenSearchSql`.
  이들은 SDK의 `Cwli`, `Ppl`, `Sql` 쿼리 언어 값에 매핑됩니다.
- 소스 컨텍스트 스펙(`SourceContextSpec`)은 "Log groups" 대상 선택기와 Start/End 시간 범위 컨트롤을 노출하며, CWLI와 PPL 쿼리는 선택한 로그 그룹을 `set_log_group_names`를 통해 `StartQuery`에 전달합니다.
- 스키마 검색은 로그 그룹(`fetch_log_groups`)을 단일 논리 데이터베이스(`SchemaLoadingStrategy::SingleDatabase`, 기본 데이터베이스 `logs`)로 나열합니다.
- 로그 스트림은 페이지 나누기가 적용된 컬렉션 하위 항목(`fetch_log_stream_page` 위의 `collection_children`)으로 표시되며, 이벤트 스트림(`CollectionPresentation::EventStream`)으로 열립니다.
- `FilterLogEvents`로 뒷받침되는 이벤트 스트림 탐색(`browse_event_stream` / `EventStreamTarget`)은 기본 24시간 탐색 창을 제공하며, 필터 패턴, 스트림 이름 접두사, 명시적 스트림 이름, 최신 항목 토글을 지원합니다.
- Insights 열 이름은 차트 자동 감지를 위해 의미론적 `ColumnKind`로 분류됩니다(예: `@timestamp`, `@ingestionTime`을 타임스탬프로 인식).
- `GetMetricData`를 통한 CloudWatch 지표: 요청당 하나의 `MetricDataQuery`를 실행하고, 응답을 타임스탬프 오름차순으로 정렬된 2열(타임스탬프, 값) `QueryResult`에 매핑합니다. AWS의 초 단위 정밀도 타임스탬프는 밀리초로 변환됩니다. 여러 `MetricDataResult` 항목이 반환되면 다중 지표 피벗(와이드 형식)을 지원합니다.
- `ListMetrics` 페이지 나누기를 통해 CloudWatch 지표 카탈로그(네임스페이스와 네임스페이스별 차원 조합을 가진 지표)를 탐색합니다. 네임스페이스 나열은 필터 없이 `ListMetrics`를 전체 훑어 고유한 네임스페이스 문자열을 모아 만들어집니다. 결과는 `MetricCatalogCache`에 의해 세션 동안 캐시됩니다.
- 지표 카탈로그는 연결 사이드바 트리(Metrics > Namespace > Metric)에서 탐색할 수 있습니다. 지표 리프를 클릭하면 기본값(Average / 5분 주기 / 모든 차원에 걸친 집계)으로 미리 채워진 차트가 열리고 즉시 실행됩니다. 차트 문서의 피커 레일에서 차원, 주기, 통계를 다듬을 수 있습니다.
- 클라이언트 식별: 모든 요청은 AWS SDK 앱 이름으로 `dbflux-<version>`을 전달하며, CloudTrail의 `userAgent` 필드에서 확인할 수 있습니다.
- `CloudWatchLanguageService`(`language_service.rs`)는 세 가지 쿼리 표면이 모두 읽기 전용임을 정직하게 반영합니다: Logs Insights QL / PPL / OpenSearch SQL 텍스트에 SQL 위험 쿼리 휴리스틱이나 SQL 문법을 적용하는 대신, `detect_dangerous`는 항상 `None`을 반환하고 `classify_execution`은 항상 `Read`를 보고합니다(이 방언들 중 어느 것도 쿼리 형태의 변경·삭제 표면이 없으며, 로그 그룹/스트림 삭제는 쿼리가 아니라 관리 API 작업입니다).

## 제한 사항

- `profile` 필드(AWS 이름이 지정된 프로필)는 `AuthProfileRef` 폼 필드입니다. 일반 이식성 경계(`DbDriver::export_field_hint`)는 모든 `AuthProfileRef` 필드를 `RequiredOnImport`로 매핑하므로, 필드 값은 내보낸 번들에서 생략되며 수신자는 가져오는 시점에 일치하는 인증 프로필을 제공하거나 만들어야 합니다. 드라이버별 재정의는 필요하지 않습니다.
- 쿼리 취소는 구현되어 있지 않으며, `cancel()`은 `NotSupported`를 반환합니다.
- OpenSearch SQL 모드는 외부 로그 그룹을 받지 않습니다: CloudWatch API가 SQL 모드에 외부 로그 그룹 매개변수를 받아들이지 않기 때문에(CWLI와 PPL만 `set_log_group_names`를 받음), SQL 쿼리는 쿼리 대상 로그 그룹을 SQL 텍스트 안에서 선언해야 합니다.
- 편집기 구문 강조는 일반 형태를 유지합니다(메타데이터 수준에서 `query_language`는 `Sql`로 보고됨). 모드 선택은 모드별 강조가 아니라 실행 의미 체계와 완성 키워드를 결정합니다.
- 읽기 전용: 변경, DDL, 트랜잭션, 페이지 나누기 기능이 선언되지 않았으며(`query`, `mutation`, `ddl`, `transactions`, `limits`가 모두 `None`), `schema_features`는 비어 있습니다.
- SSL 폼이 없습니다(TLS는 AWS SDK 전송 계층이 처리).
- 지표 실행은 호출당 하나의 `MetricDataQuery`만 지원합니다.
- 네임스페이스 목록 생성(필터 없이 `ListMetrics`를 훑기)은 지표가 많은 대규모 AWS 계정에서 느릴 수 있으며, 완료되면 세션 동안 캐시됩니다. 이 훑기는 매우 큰 계정의 최악의 경우를 한정하기 위해 50페이지(약 25,000개 지표)로 제한됩니다. 제한에 도달하면 네임스페이스 목록이 조용히 잘리고 경고가 로그에 기록되며, 향후 변경에서 이 제한은 완전한 시간 초과 + 취소 인프라로 대체될 예정입니다.
- 지표에 대한 라이브 통합 테스트(`live_execute_cloudwatch_metric`)는 실제 AWS 자격 증명이 필요하며 기본적으로 `#[ignore]` 처리되어 있습니다. LocalStack Community는 CloudWatch Metrics API를 지원하지 않습니다.
- `tests/live_integration.rs`는 CI에서 LocalStack Community 컨테이너를 상대로 Logs 데이터 플레인(로그 그룹/로그 스트림 검색, 이벤트 탐색)을 실행합니다. `DashboardImporter`는 순수 JSON 파싱이며 같은 방식으로 검증됩니다. `DashboardSource`(Metrics 계열 `GetDashboard`/`ListDashboards` API를 사용)와 CloudWatch Logs Insights(`StartQuery`/`GetQueryResults`)는 LocalStack을 상대로 시도되지만 커뮤니티 티어가 호출을 거부하면 로그 메시지와 함께 건너뜁니다. 둘 다 완전한 엔드투엔드 검증에는 실제 AWS 계정(또는 LocalStack Pro)이 필요합니다.
- 쓰기 권한 검사 없음: `Connection::probe_write_privilege`는 트레이트 기본값(`WritePrivilege::Unknown`)에 의도적으로 머물러 있습니다. 신뢰할 수 있는 검사에는 `iam:SimulatePrincipalPolicy`가 필요한데, 이는 연결하는 역할이 일반적으로 가지고 있지 않은 권한입니다.
- 인스턴스 지표와 인스턴스 검사기 없음(`INSTANCE_METRICS`/`INSTANCE_INSPECTOR` 미선언): CloudWatch Logs 자체의 서버 측 지표는 CloudWatch Metrics에 속하므로, 드라이버별 `InstanceCatalog`는 표면을 추가하는 대신 중복하게 됩니다.
- `QueryGenerator` 없음: `Connection::query_generator()`는 트레이트 기본값(`None`)에 머둅니다. Logs Insights QL, PPL, OpenSearch SQL은 모두 미리 볼 변경/DDL 형태가 없는 읽기 전용 쿼리 표면입니다.
