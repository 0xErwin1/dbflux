# 대시보드 및 저장된 차트

DBFlux는 차트 구성을 **저장된 차트**로 유지하고 이를 **대시보드**로 묶습니다 — 시간 범위와 새로고침 정책을 공유하는 차트 패널(그리고 선택적 마크다운 구분선)로 이루어진 그리드입니다.

차트 엔진 내부(렌더링, 축, 데시메이션, 팔레트)는 [`CHARTS.md`](./CHARTS.md)를 참고하세요. SQLite 저장소 계층은 [`ARCHITECTURE.md`](../ARCHITECTURE.md#저장소-및-구성)를 참고하세요.
## 개요

- **SavedChart**는 차트 구성의 저장된 형태입니다: 데이터 소스 바인딩, 시리즈, Y축 바인딩,
  새로고침 정책, 시간 범위 프리셋.
- **대시보드(Dashboard)**는 이름이 붙은 패널 그리드입니다. 각 패널은 (`SavedChart`를 id로
  참조하는) `Chart` 슬롯이거나 (차트도 도구 모음도 없는 인라인 마크다운 헤더 스트립인)
  `Divider` 슬롯입니다.
- 대시보드는 공유 **시간 범위**와 **새로고침 정책**을 소유하며, 구독을 통해 로드된 모든
  차트 패널에 전파합니다.
- 원격 대시보드(예: CloudWatch)는 드라이버가 적절한 기능을 광고하는 경우 사이드바에서
  **탐색**하고 로컬 대시보드로 **가져올** 수 있습니다.

## 저장소 계층

모든 대시보드와 저장된 차트 데이터는 `~/.local/share/dbflux/dbflux.db`의 `viz_*` 테이블
접두사 아래에 위치합니다:

| 테이블 | 용도 |
|-------|---------|
| `viz_dashboards` | 대시보드 레코드 (`profile_id` nullable, `ON DELETE SET NULL`) |
| `viz_dashboard_panels` | 패널 슬롯: `panel_kind` 판별자 + 선택적 `divider_markdown` |
| `viz_saved_charts` | 저장된 차트 루트 (`SavedChartDto`) |
| `viz_saved_chart_series` | 시리즈별 설정 |
| `viz_saved_chart_binding_y` | Y축 바인딩 |
| `viz_saved_chart_source_metric_dimensions` | CloudWatch 메트릭 차원 |
| `viz_saved_chart_source_metric_series` | CloudWatch 메트릭 시리즈 사양 |

리포지토리는 `crates/dbflux_storage/src/repositories/viz_*.rs`에 위치하며 표준
`Repository` 트레이트(`all()`, `find_by_id()`, `upsert()`, `delete()`)를 구현합니다.

## 인메모리 관리자

두 관리자 모두 동기 읽기를 위해 인메모리 캐시로 SQLite 리포지토리를 감쌉니다. 쓰기는
먼저 리포지토리로 이루어지며, 캐시는 성공 시에만 갱신됩니다.

- **`DashboardManager`** (`crates/dbflux_ui_base/src/dashboard_manager.rs`) —
  도메인 타입 `Dashboard`, `DashboardPanel`, `DashboardPanelKind`
  (`Chart { saved_chart_id }` | `Divider { markdown }` |
  `Inspector { metric_id }`), `DashboardPanelDraft`. 새 대시보드는
  `grid_columns = 12`로 생성되고, 새 패널은 `grid_width = 12, grid_height = 2`로
  새 행의 `grid_column = 0`에 추가됩니다.
- **`SavedChartManager`** (`crates/dbflux_ui_base/src/saved_chart_manager.rs`)
  — `SavedChart` 수명 주기를 소유하며, 여기에는 `SavedChartRefreshPolicy`
  (`Off` / `Interval { every_secs }`)가 포함됩니다.
- **`RemoteDashboardCache`** (`crates/dbflux_app/src/remote_dashboard_cache.rs`)
  — 업스트림 대시보드 목록을 위한 세션 범위 인메모리 캐시.
  재시작 시 유지되지 않습니다.

## 문서 시스템 통합

대시보드는 `DashboardDocument`
(`crates/dbflux_ui_document/src/dashboard/`)로 열립니다:

- **중복 제거 키**: `DocumentKey::Dashboard { dashboard_id }` (영속화됨) 또는
  `DocumentKey::InstanceOverview { profile_id }` (자동 생성된 읽기 전용).
- **차트 패널**: 각 슬롯은 `ChartDocument` 엔티티
  (`Loaded`) 또는 삭제된 차트의 자리 표시자 (`Orphan`)를 감쌉니다.
- **검사기 패널**: 각 슬롯은 `DataGridPanel`을 호스팅하고 대시보드의 공유
  간격에 따라 새로 고치는 `InspectorPanel` 엔티티를 감쌉니다.
  드라이버가 제공하는 행 작업(예: 연결 종료, 쿼리 취소)은 행 상황에 맞는 메뉴에 나타납니다.
- **공유 도구 모음**: 단일 `TimeRangePanel`이 구독을 통해 창 변경을
  로드된 모든 패널에 전파합니다.
- **동시성**: 패널 재실행은 `PANEL_REEXEC_CAP`으로 제한되어
  동시 쿼리가 연결을 압도하지 않도록 합니다.
- **그리드**: 12열 표준 그리드; `dashboard/builder.rs`의
  `DragReorderState` / `DragResizeState`를 통한 드래그로 순서 변경 및 크기 조정.

독립 실행형 저장된 차트는 `DocumentKey::Chart { saved_chart_id }`를 키로 하는
`ChartDocument` (`crates/dbflux_ui_document/src/chart_document/`)로 열립니다.
`ChartDocument`는 독립적으로 렌더링되거나 `DashboardDocument` 패널 안에
내장됩니다.

## 인스턴스 개요와 검사기

`INSTANCE_METRICS` 또는 `INSTANCE_INSPECTOR`를 광고하는 드라이버의 연결은 읽기 전용
**인스턴스 개요** — 사용자가 유지하기로 선택하기 전까지는 저장소에 전혀 기록하지 않는,
실시간 서버 지표와 표 형태 검사기로 합성된 대시보드 — 를 제공합니다.

### 인스턴스 개요 열기

사이드바는 연결된 프로필 아래(*인스턴스 지표* 및 *인스턴스 검사기* 폴더 위)에 단일
**인스턴스 개요** 리프를 표시합니다. 클릭하거나 오른쪽 클릭 메뉴에서 **열기**를
선택하면 개요가 열립니다.

| 단계 | 세부 사항 |
|------|--------|
| 소스 | `InstanceCatalog::default_dashboard()`가 반환하는 드라이버의 `DefaultInstanceDashboard` 디스크립터(고정 12열 레이아웃) |
| 중복 제거 | `DocumentKey::InstanceOverview { profile_id }` — 연결당 하나의 탭; 다시 클릭하면 기존 탭에 포커스 |
| 영속성 | 없음. `DashboardDocument`는 열리는 시점에 메모리에서 빌드되며, `viz_*` 행은 기록되지 않음 |
| 모드 | 읽기 전용. 편집 모드 전환, *패널 추가*, 편집/보기 토글은 억제되며, 드래그 순서 변경과 드래그 크기 조정은 비활성화됨 |

### 편집 가능한 대시보드로 저장

읽기 전용 개요는 **편집 가능으로 저장** 버튼(도구 모음 오른쪽 그룹;
도구 설명 *"이 개요를 새 편집 가능 대시보드로 복제"*)을 표시합니다. 이 버튼은
합성된 레이아웃 — 정확한 패널 위치를 포함 — 을 `DashboardManager::append_panels`와
패널별 명시적 `DraftGridLayout`을 통해 새로 영속화되는 사용자 소유
`Dashboard`로 복제합니다. 복제본은 일반적인 편집 가능
`DocumentKey::Dashboard { dashboard_id }` 탭으로 열립니다. 원본 개요는
읽기 전용으로 유지되며 열 때마다 다시 합성됩니다.

### 검사기 패널

검사기는 `Chart` 및 `Divider`와 함께 세 번째 대시보드 패널 종류입니다:

| 패널 종류 | 백엔드 | 비고 |
|------------|---------|-------|
| `Chart` | `SavedChart` 참조 (`saved_chart_id`) | 시계열 차트 |
| `Divider` | 인라인 마크다운 | 헤더 스트립; 도구 모음 없음 |
| `Inspector` | `DashboardPanelKind::Inspector { metric_id }` | 표 형태 스냅샷; 공유 간격에 따라 새로 고침 |

`DashboardPanelKind::Inspector { metric_id }`
(`crates/dbflux_ui_base/src/dashboard_manager.rs`)는 차트 참조를 갖지 않습니다 —
검사기는 `metric_id`만으로 식별됩니다. 각 검사기 패널은
`InstanceCatalog::fetch_inspector_snapshot`이 반환한 현재 스냅샷을 표시하는
`DataGridPanel`을 호스팅합니다(예: PostgreSQL `pg_stat_activity`,
MySQL `PROCESSLIST`, MongoDB `currentOp`, Redis `CLIENT LIST`).

영속성: `Inspector` 값은 `viz_dashboard_panels.panel_kind`에, 검사기 키는
`inspector_metric_id`에 저장됩니다. 둘 다 **마이그레이션 014** (`014_viz_inspector_and_instance_metric`)가
추가했으며, 이 마이그레이션은 마이그레이션 013에서 `chart` / `divider`로
도입된 `panel_kind` CHECK를 확장하여 `inspector`도 허용합니다. (*인스턴스 검사기*
사이드바 폴더에서 직접 열리는 독립 실행형 검사기 탭은
`DocumentKey::InstanceInspector { profile_id, metric_id }`를 사용합니다.)

### 검사기 행 작업

검사기 행은 드라이버가 제공하는 행 작업(오른쪽 클릭 상황에 맞는 메뉴)을 노출할 수
있습니다. 예: *연결 종료* / *세션 종료*. 흐름:

1. 드라이버는 `InstanceCatalog::row_actions(metric_id)`에서 `InspectorRowAction`을 반환합니다. 가용성은 드라이버별 권한 프로브로 게이트되므로(드라이버 README 참조) 권한이 부족한 세션은 실행할 수 없는 작업을 볼 수 없습니다.
2. `is_destructive` 작업은 실행 전 확인 모달을 표시합니다.
3. 확인 시 연결은 클릭 시점이 아닌 실행 시점에 다시 확인되고 `InstanceCatalog::execute_row_action(metric_id, action_id, row_values)`가 실행됩니다.
4. 모든 시도는 감사 이벤트를 기록합니다. 실패는 `report_error_async` (`ErrorKind::Driver`의 `UserFacingError`)로 라우팅되므로 사용자는 감사 행으로 연결되는 상관 관계 ID가 포함된 토스트를 받습니다.

실행은
`crates/dbflux_ui_document/src/instance_inspector/mod.rs`에
있습니다.

### 새로고침 동작

대시보드, 독립 실행형 차트, 검사기의 새로고침 타이머는 모두 매 틱마다 패널의
프로필에 대해 `AppState::connections()`를 확인하고 연결이 닫혀 있으면
작업을 건너뜁니다. 타이머 자체는 유지되므로 다시 연결하면 재설정 없이
새로고침이 자동으로 재개됩니다.

### 내장 드라이버 지원

| 드라이버 | `INSTANCE_METRICS` | `INSTANCE_INSPECTOR` | 지표 / 검사기 목록 |
|--------|:---:|:---:|---|
| PostgreSQL | ✓ | ✓ | [README](../crates/dbflux_driver_postgres/README.md) |
| MySQL / MariaDB | ✓ | ✓ | [README](../crates/dbflux_driver_mysql/README.md) |
| MongoDB | ✓ | ✓ | [README](../crates/dbflux_driver_mongodb/README.md) |
| Redis | ✓ | ✓ | [README](../crates/dbflux_driver_redis/README.md) |
| SQL Server | ✓ | ✓ | [README](../crates/dbflux_driver_mssql/README.md) |
| ClickHouse | ✓ | ✓ | [README](../crates/dbflux_driver_clickhouse/README.md) |
| InfluxDB (v2 only) | ✓ | ✓ | [README](../crates/dbflux_driver_influxdb/README.md) |

각 드라이버 README는 노출하는 구체적인 지표, 검사기, 행 작업을
나열합니다; 이 문서는 이를 복제하지 않습니다.

## 드라이버 연결부

드라이버는 일반적인 코어 연결부(seam)를 통해 대시보드 상호 운용에 참여합니다 — UI는
드라이버 ID로 분기하지 않습니다.

### 대시보드 가져오기 (JSON → 로컬 대시보드)

- **트레이트**: `DashboardImporter`
  (`crates/dbflux_core/src/connection/dashboard_import.rs`)
- **기능**: `DriverCapabilities::DASHBOARD_IMPORT`
- **값 타입**:
  - `WidgetImportSpec` — 파싱된 위젯 사양
  - `MetricView::{TimeSeries, StackedArea, SingleValue}`
  - `ImportedMetricSeries` — 시리즈 + 차원
  - `WidgetLayout` — 로컬 그리드로 전달되는 네이티브 레이아웃 좌표

드라이버는 대시보드 JSON을 정규화된 위젯 집합으로 파싱하고, UI는 이를
`SavedChart`로 가져와 새 `Dashboard`에 배치합니다.

### 원격 대시보드 탐색 (사이드바)

- **트레이트**: `DashboardSource`
  (`crates/dbflux_core/src/connection/dashboard_source.rs`)
- **기능**: `DriverCapabilities::DASHBOARD_SYNC`
- **값 타입**: `RemoteDashboard`, `DashboardRef`
  (선택적 `last_modified: ISO8601`)

사이드바는 이 연결부를 통해 업스트림 대시보드를 나열합니다; 결과는
`RemoteDashboardCache`에 캐시됩니다. 원격 대시보드를 선택하면 `DashboardImporter`가
로컬에서 이를 구체화합니다.

### 인스턴스 지표와 검사기

- **트레이트**: `InstanceCatalog`
  (`crates/dbflux_core/src/connection/instance_catalog.rs`)
- **기능**: `DriverCapabilities::INSTANCE_METRICS` (시계열),
  `DriverCapabilities::INSTANCE_INSPECTOR` (표 형태 스냅샷)
- **값 타입**: `InstanceMetricDef`, `InstanceInspectorDef`,
  `DefaultInstanceDashboard`, `InspectorRowAction`

드라이버는 실시간 서버 지표(예: `pg.tps`,
`mysql.queries_per_sec`, `clickhouse.query`)와 표 형태 검사기
(예: `pg.activity`, `mysql.processlist`, `mongo.currentop`,
`redis.client_list`, `clickhouse.processes`)를 단일 카탈로그를 통해 노출합니다.
각 드라이버는 고정된 12열 레이아웃을 가진 `DefaultInstanceDashboard` 디스크립터도
게시합니다 — 워크스페이스는 이 디스크립터를 **읽기 전용 인스턴스 개요**
대시보드로 엽니다(중복 제거 키
`DocumentKey::InstanceOverview { profile_id }`). "편집 가능으로
저장" 작업은 이 레이아웃을 사용자가 소유하는 영속화된 대시보드로 복제합니다.

검사기 행은 `InspectorRowAction`(예: *연결
종료*)을 선언할 수 있습니다. 작업 가용성은 드라이버별 권한
프로브(PostgreSQL의 `pg_monitor` / `pg_signal_backend`, MySQL의 `PROCESS` /
`CONNECTION_ADMIN`, MongoDB의 `killOp`, Redis의 `CLIENT KILL`,
SQL Server의 `VIEW SERVER STATE` / `KILL`, ClickHouse의 `KILL QUERY`)로
게이트되므로 권한이 부족한 세션은 실행할 수 없는 작업을 볼 수 없습니다.

모든 새로고침 타이머(대시보드 틱, 독립 실행형 차트 틱, 검사기
틱)는 패널의 프로필에 대해 `AppState::connections()`를 확인하고
연결이 닫혀 있으면 작업을 건너뜁니다; 타이머는 유지되므로
다시 연결하면 새로고침이 자동으로 재개됩니다.

이 연결부의 사용자 대면 동작(인스턴스 개요가 열리는 방식,
*편집 가능으로 저장*, 검사기 패널과 행 작업)은 위의
[인스턴스 개요와 검사기](#인스턴스-개요와-검사기)를
참조하세요.

### CloudWatch 구현

`crates/dbflux_driver_cloudwatch/src/`는 다음을 제공합니다:

- `CloudWatchDashboardSource` — AWS SDK를 통해 CloudWatch 대시보드를 나열
- `CloudWatchDashboardImporter` — CloudWatch 대시보드 JSON을 메트릭 시리즈, 차원,
  통계 집계가 포함된 `WidgetImportSpec`으로 파싱

이것은 **읽기 전용 탐색 및 가져오기**이며 동기화 기능이 아닙니다. DBFlux는
CloudWatch 대시보드에 절대로 되돌려 쓰지 않습니다.
## 기능 매트릭스

| 기능 비트 | 의미 |
|---|---|
| `DASHBOARD_IMPORT` (51) | 드라이버가 대시보드 JSON을 위젯 사양으로 파싱할 수 있음 |
| `DASHBOARD_SYNC` (52) | 드라이버가 업스트림 대시보드를 나열할 수 있음 |

두 기능은 서로 독립적입니다: 드라이버는 가져오기 없이 동기화를 광고할 수도 있고
그 반대도 가능합니다.

## 새로운 대시보드 기능 드라이버 추가하기

1. 업스트림 대시보드를 나열하기 위해 드라이버 `Connection`에 `DashboardSource`를
   구현합니다. `DriverMetadata.capabilities`에 `DASHBOARD_SYNC`를 추가합니다.
2. 대시보드 페이로드를 `WidgetImportSpec`으로 파싱하기 위해 드라이버 `Connection`에
   `DashboardImporter`를 구현합니다. `DASHBOARD_IMPORT`를 추가합니다.
3. UI는 드라이버별 분기 없이도 사이드바에 대시보드 트리를 표시하고 가져오기를
   라우팅합니다.
