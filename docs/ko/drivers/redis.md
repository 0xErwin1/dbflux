# Redis

인메모리 키-값 데이터베이스입니다.
## 한눈에 보기

- **분류** — 키-값
- **쿼리 언어** — Redis 명령
- **기본 포트** — 6379
- **URI 스킴** — `redis`

[`redis`](https://crates.io/crates/redis) 크레이트를 기반으로 만들어진 DBFlux용 Redis 키-값 드라이버입니다.

## 기능

- `RedisCommands` 쿼리 언어와 함께 `DatabaseCategory::KeyValue`로 분류되는 키-값 드라이버입니다. 에디터는 SQL이 아니라 Redis 명령 구문을 사용합니다.
- 연결 모드: 수동(호스트/포트/사용자/비밀번호/데이터베이스)과 URI 모드. URI 모드는 `redis://`와 `rediss://` 연결 문자열을 받습니다.
- `SELECT <db>`를 통한 여러 논리 데이터베이스(`MULTIPLE_DATABASES`). 활성 데이터베이스 인덱스는 연결에서 추적됩니다.
- 선택적인 사용자 이름 + 비밀번호를 통한 인증(`AUTHENTICATION`).
- 연결 시 `CLIENT SETNAME`으로 서버에 클라이언트 식별자를 보고합니다(`dbflux/<version>`, `CLIENT LIST`에서 확인 가능). 일부 관리형 공급자가 `CLIENT` 명령을 제한하므로 최선의 시도로 수행됩니다.
- 연결 시 최선의 시도로 수행하는 쓰기 권한 프로브(`probe_write_privilege`): 먼저 `ACL WHOAMI` + `ACL DRYRUN`을 시도하고, `ACL`을 사용할 수 없으면(구버전 서버 또는 제한된 관리형 공급자) 짧은 TTL의 네임스페이스가 지정된 `SET ... NX` / `DEL`로 대체합니다. 복제본의 `READONLY` 응답이나 `NOPERM` 거부는 읽기 전용 연결로 해석됩니다.
- 세 가지 모드(`off`, `on`, `verify`)를 갖춘 TLS/SSL:
  - `off` — 일반 `redis://` 연결.
  - `on` — 인증서 체인 검증 없이 인증서를 신뢰하는 `rediss://` (안전하지 않음 표시).
  - `verify` — 제공된 루트 인증서와 선택적인 클라이언트 인증서/키를 사용하는 `rediss://`, `Client::build_with_tls`를 통해 구축됩니다.
- 배스천 호스트를 통해 Redis에 도달하기 위한 SSH 터널 지원(수동 모드 전용. 제한 사항 참조).
- 배포 토폴로지는 자동으로 감지하거나 명시적으로 설정할 수 있습니다(`standalone`, `cluster`, `sentinel`):
  - 자동 감지는 연결 시점에 `ROLE`과 `INFO cluster`를 프로브하여 standalone 또는 Cluster 처리로 라우팅합니다.
  - `cluster`는 감지를 건너뛰고 기본 호스트/포트와 설정된 추가 시드 노드를 사용해 `ClusterClient`로 직접 연결합니다. Cluster 연결은 항상 데이터베이스 0만 노출하며, Cluster 프로필에 0이 아닌 데이터베이스가 지정되면 db 0에 조용히 적용되는 대신 연결 시점에 거부됩니다.
  - `sentinel`은 `SentinelClient`를 통해 연결하며, 하나 이상의 Sentinel 노드(기본 호스트/포트와 설정된 추가 노드)에서 이름이 지정된 마스터를 해석합니다. 해석 후 드라이버는 `CLIENT SETNAME`, `PING`, 그리고 해석된 노드가 실제로 마스터인지 확인하는 `ROLE` 검사를 실행합니다.
  - Sentinel 장애 조치 복구: Sentinel 기반 연결에서 연결 클래스 장애(연결 끊김, IO 오류)가 발생하면 오류가 표면화되기 전에 Sentinel을 통한 재해석 한 번과 실패한 명령의 재시도 한 번이 정확히 한 차례 이루어집니다.
- 키 탐색 및 검색:
  - 커서 기반 키 스캔(`KV_SCAN`, `PaginationStyle::Cursor`). 한 번의 페이지 요청은 페이지가 요청한 개수의 키를 채우거나 스캔이 끝날 때까지 `SCAN`을 계속 실행하므로, 드물게 일치하는 `MATCH` 필터도 빈 페이지를 만들지 않습니다. 각 페이지는 `SCAN` 왕복 1000회와 500 ms로 제한되며, 제한에 도달하면 그때까지 찾은 키와 대기 중인 커서를 반환합니다. `SCAN`이 반복해서 돌려준 키는 페이지마다 한 번만 표시되며, 페이지는 요청한 크기를 최대 `SCAN` 배치 하나만큼 초과할 수 있습니다. 페이지의 키 타입은 하나의 파이프라인으로 가져옵니다. Cluster 연결에서는 일반 `SCAN`이 단일 노드 기준으로 의미가 없으므로 드라이버는 같은 페이지 제한 안에서 대기 중인 마스터를 차례로 스캔합니다: 각 노드의 커서는 독립적으로 추적되며, 집계된 커서는 `"<host>:<port>"`를 해당 노드의 대기 중인 `SCAN` 커서에 매핑하는 불투명한 JSON 개체로 왕복합니다. 전체 스캔은 모든 마스터가 커서 0을 보고하면 소진됩니다.
  - 키스페이스 전체 키 수(`key_count`): 선택한 데이터베이스에 대한 `DBSIZE`이며, Cluster 연결에서는 모든 마스터의 `DBSIZE` 합계입니다. 필터가 없을 때 키 브라우저가 페이지 키 수 옆에 표시합니다.
  - 키별 타입 검색(`KV_KEY_TYPES`): string, hash, list, set, sorted set, stream을 지원합니다.
  - TTL 검사(`KV_TTL`)와 값 크기 보고(`KV_VALUE_SIZE`).
  - 존재 확인(`KV_GET`/`KV_EXISTS`), 키 이름 바꾸기(`KV_RENAME`), 여러 키의 대량 가져오기(`KV_BULK_GET`).
- 값 타입 지원 범위: 문자열, 해시, 리스트, 셋, 정렬된 셋, 스트림. 스트림 범위 읽기, 스트림 항목 추가, 스트림 항목 삭제를 포함합니다(`KV_STREAM_RANGE`, `KV_STREAM_ADD`, `KV_STREAM_DELETE`).
- 연결 설정으로 노출되는 구성 가능한 스트림 미리 보기 제한.
- 변경 작업: 삽입, 업데이트, 삭제, 일괄 처리, 대량 삭제. `RedisCommandGenerator`는 미리 보기와 쿼리로 복사에서 사용하기 위해 set/delete, hash set/delete, list push/set/remove, set add/remove, sorted-set add/remove, stream add/delete에 대한 Redis 명령을 생성합니다.
- 결과의 JSON 내보내기(`EXPORT_JSON`).
- 전체 페이로드 읽기에 대한 크기 게이트: 요청이 바이트 예산을 갖는 경우 string/JSON 값은 `GET` 전에 `STRLEN`으로 프로브되고, 한도를 초과한 값은 페이로드를 전송하는 대신 실제 크기를 밝힌 자리 표시자를 반환합니다. 컬렉션 타입은 영향을 받지 않으며, 가져오기 상한에 도달한 스트림 읽기는 잘림(truncated)으로 스스로 보고합니다.
- 오프라인 RDB 덤프 분석(`DumpAnalyzer`): `.rdb` 파일을 서버에 연결하지 않고 키 단위로 스캔하며, I/O 속도로 파일을 스트리밍하고 평탄한 메모리를 유지합니다(키 값은 절대 디코딩되지 않고 키 이름과 값 타입만 다룹니다). 전체 키 개수, 타입별 분류, 가장 큰 500개의 키, 접두사별 크기 롤업을 보고합니다. 보고되는 크기는 각 키의 **디스크상 직렬화 크기**이며 라이브 Redis 메모리에서의 점유량이 아닙니다 — 할당자 오버헤드와 인메모리 인코딩 때문에 두 수치는 서로 달라집니다.

Cluster 연결에서의 스키마 인트로스펙션은 집계된 단일 `db0` 키스페이스를 보고합니다: 키 개수와 평균 TTL은 노드별로 보고되는 대신 모든 마스터의 `DBSIZE`/키스페이스 통계에 대해 합산/평균됩니다.

### 인스턴스 메트릭

`INFO` 명령 출력에서 가져온 라이브 서버 메트릭의 엄선된 집합을 노출합니다. Cluster 연결에서는 사용할 수 없습니다 — `INFO`를 샘플링할 단일 노드가 없으므로 `instance_catalog()`가 `None`을 반환하고 Cluster 프로필에 대해서는 인스턴스 개요/메트릭/검사기를 사용할 수 없습니다.

- `redis.connected_clients` — 현재 연결된 클라이언트
- `redis.blocked_clients` — 블로킹 명령을 기다리는 클라이언트
- `redis.used_memory` — Redis 할당자가 할당한 바이트
- `redis.used_memory_rss` — OS가 할당한 바이트(상주 집합 크기)
- `redis.total_commands_processed` — 누적으로 처리된 명령
- `redis.total_connections_received` — 누적으로 수락된 연결
- `redis.instantaneous_ops_per_sec` — 초당 처리된 명령(서버 측 비율)
- `redis.keyspace_hits` — 키 조회에 대한 캐시 히트
- `redis.keyspace_misses` — 키 조회에 대한 캐시 미스
- `redis.evicted_keys` — `maxmemory` 정책으로 인해 축출된 키
- `redis.expired_keys` — TTL로 만료된 키
- `redis.rdb_changes_since_last_save` — 마지막 RDB 스냅샷 이후의 변경
- `redis.connected_slaves` — 연결된 복제본 개수

각 메트릭은 라이브 차트 작성을 위해 단일 `(timestamp_ms, value)` 행으로 반환됩니다.

### 인스턴스 검사기

실행 중인 서버 상태의 표 형태 스냅샷을 노출합니다:

- `redis.client_list` — `CLIENT LIST`의 활성 클라이언트 (id, cmd, age, idle, flags, db, sub, multi)

민감한 필드(`addr`, `laddr`, `name`)는 클라이언트 IP 주소와 호스트 이름 노출을 피하기 위해 `[redacted]`로 마스킹됩니다.

## 제한 사항

- `execute()`는 요청한 행 제한(0 포함) 또는 명령문 시간 제한이 있으면 명령을 전송하기 전에 `NotSupported`를 반환합니다. 보호 옵션이 없는 명령은 계속 실행할 수 있지만 기본 편집기는 이 보호를 보장하지 않습니다.

- SQL은 지원되지 않습니다. 쿼리는 Redis 명령으로 작성해야 합니다.

- 인스턴스 메트릭은 과거 시계열이 아니라 호출당 단일 데이터 포인트(`INFO`의 현재 스냅샷)를 반환합니다. 누적 카운터(예: `redis.total_commands_processed`)는 단조 증가하므로 절대 비율이 아니라 샘플 간 델타로 해석해야 합니다.

- `CLIENT LIST` 검사기는 클라이언트 IP 주소와 사용자가 입력한 이름이 UI에 노출되지 않도록 모든 행에서 `addr`, `laddr`, `name` 필드를 마스킹합니다.

- 쿼리 취소는 지원되지 않습니다(`QUERY_CANCELLATION`이 설정되지 않음). UI에서 장기 실행 명령을 중단할 수 없습니다.
- 업서트가 없고(`supports_upsert: false`), `RETURNING`도 없으며, 대량 업데이트도 없습니다(`supports_bulk_update: false`).
- DDL 기능은 모두 비활성화되어 있습니다(테이블, 뷰, 인덱스, 스키마 없음) — 이것은 관계형이 아니라 키-값 저장소입니다.
- 트랜잭션은 기능 수준에서 광고되지만(`supports_transactions: true`) 격리 수준, 저장점, 중첩 트랜잭션, 읽기 전용, deferrable 지원이 없습니다.
- Pub/Sub은 노출되지 않습니다(`PUBSUB` 기능이 설정되지 않음).
- URI 모드가 활성화되어 있으면 SSH 터널링을 사용할 수 없습니다. 터널 경로는 수동 연결 모드에만 연결되어 있습니다. SSH 터널을 Cluster 또는 Sentinel 추가 시드 노드와 결합하는 것은 지원되지 않습니다: 터널은 기본 호스트/포트만 포워딩하므로 추가 노드는 터널을 통해 도달할 수 없습니다.
- 스트림 소비자 그룹은 모델링되지 않습니다. 범위 읽기, 항목 추가, 항목 삭제만 지원됩니다.
- Sentinel과 Cluster 추가 시드 노드는 항상 일반 `redis://`로 연락됩니다. 해당 추가 노드에 대한 노드별 TLS 구성은 지원되지 않습니다. 이번 반복에서 해석된 Sentinel 마스터 연결 자체도 일반(비 TLS)입니다.
- Sentinel 인증은 해석된 마스터 연결에만 적용됩니다(구성된 사용자 이름/비밀번호를 통해). Sentinel 노드 자체는 인증 없이 연락됩니다.
