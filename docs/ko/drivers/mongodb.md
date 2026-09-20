# MongoDB

현대적인 애플리케이션을 위한 문서 데이터베이스입니다.

## 한눈에 보기

- **카테고리** — 문서
- **쿼리 언어** — MongoDB 쿼리 문법
- **기본 포트** — 27017
- **URI 스킴** — `mongodb`

DBFlux용 MongoDB 문서 드라이버입니다.

## 기능

- `DatabaseCategory::Document`로 분류되고 `MongoQuery` 쿼리 언어를 사용하는 문서 드라이버입니다. 에디터는 SQL이 아닌 MongoDB 셸 문법을 사용합니다.
- 연결 모드: 수동(호스트/포트/자격 증명/데이터베이스)과 URI 모드. URI 모드는 `mongodb://`와 `mongodb+srv://` 연결 문자열을 받아 들이며, 복제 세트 탐색을 위해 SRV 레코드가 파싱됩니다.
- 여러 논리 데이터베이스(`MULTIPLE_DATABASES`)와 컬렉션 탐색, 문서 수 세기를 지원합니다.
- 인증(`AUTHENTICATION`)과 세 가지 모드(`off`, `on`, `verify`)의 TLS/SSL을 지원하며, 루트 인증서와 선택적 클라이언트 인증서를 사용할 수 있습니다.
- 배스천 호스트를 거쳐 MongoDB에 도달하기 위한 SSH 터널을 지원합니다.
- `db.collection.method(...)`와 `db.method(...)` 형태에 대한 셸 스타일 쿼리 파싱을 지원하며, 하위 호환을 위한 JSON 문서 폴백이 있습니다. 지원되는 메서드: `find`, `findOne`, `aggregate`, `count`/`countDocuments`, `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`. 파싱 오류는 에디터 진단을 위해 바이트 오프셋 위치를 담고 있습니다.
- 집계 파이프라인(`AGGREGATION`); 쿼리 기능은 order-by, group-by, having, limit, offset을 알립니다.
- WHERE 연산자: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, 논리 `And`/`Or`/`Not`.
- 커서 및 페이지 토큰 스타일의 페이지 나누기(`PaginationStyle::Cursor`, `PaginationStyle::PageToken`).
- 문서 중심 스키마 메타데이터: 컬렉션 필드와 인덱스(`INDEXES`), 중첩 문서와 배열은 문서 트리 뷰에 매핑됩니다(`NESTED_DOCUMENTS`, `ARRAYS`).
- 변경: 삽입, 업데이트(upsert 포함), 삭제(`supports_upsert: true`). `MongoShellGenerator`는 미리 보기와 쿼리로 복사를 위해 `insertOne`/`insertMany`, `updateOne`/`updateMany`(`{ upsert: true }` 포함), `deleteOne`/`deleteMany`를 만들어 냅니다.
- DDL: 데이터베이스 삭제, 컬렉션 삭제, 인덱스 생성, 인덱스 삭제.
- 결과의 JSON 내보내기(`EXPORT_JSON`).
- 연결 시 `appName=dbflux/<version>`으로 클라이언트 식별을 보고합니다(서버 로그와 `db.currentOp()`에 표시됨). 단, 연결 URI가 이미 `appName`을 설정했다면 그 값이 사용됩니다.
- 쓰기 권한 프로브: 연결 후 `connectionStatus`(`showPrivileges: true`)에서 쓰기를 부여하는 권한/역할을 검사하여 세션을 쓰기 가능, 읽기 전용, 알 수 없음으로 분류합니다. 쓰기 불가 노드(예: 세컨더리)에 직접 연결된 경우 `hello`가 판정을 읽기 전용으로 바꿉니다.
- **다중 문 JS 스크립트 실행(`SCRIPT_EXECUTION`)**: 단일 `db.` 호출이나 JSON 쿼리로 파싱되지 않는 버퍼는 샌드박스화된 QuickJS 엔진에서 실행되어 소스 순서대로 모든 문을 실행합니다. 예: `db.users.insertOne({...}); db.orders.find({...});`. 디스패치된 각 문은 자신의 결과 집합을 만들며, `find()`/`aggregate()`는 실제 유계(bounded) JS `Array`를 반환합니다(`.forEach`, `for...of`, `.map`, `.length`, `.toArray()`가 모두 기본 동작하며, 문서 10 000개로 제한됨). `print()` 출력은 기본 결과에 수집됩니다. 디스패치된 모든 작업은 소스 텍스트가 아니라 실제로 구성되는 지점에서 분류됩니다. 따라서 계산된 메서드 이름(`db.coll[name]()`)이나 루프/조건문 안에서만 도달하는 작업도 올바르게 분류됩니다. 읽기 전용임을 증명할 수 없는 스크립트는 사전 확인을 한 번 요구하고, 확인된 상한을 초과하는 분류의 작업이 있으면 서버에 도달하기 전에 스크립트를 중단합니다. 디스패치된 각 작업은 실행 전체를 공유하는 하나의 상관 관계 ID로 자신의 감사 행을 받습니다. 스크립트 도중 드라이버 오류(예: 중복 키 실패)가 발생하면 이미 성공한 문을 롤백하지 않고 해당 문에서 실행을 멈춥니다.

### 인스턴스 지표

MongoDB `serverStatus` 명령에서 가져온 엄선된 실시간 서버 지표를 노출합니다. 지표는 BSON 점 경로(dotted-path) 순회를 통해 추출됩니다:

- `mongo.connections_current` — 현재 열린 연결 수
- `mongo.connections_available` — 사용 가능한 연결 슬롯
- `mongo.opcounters_insert` — 시작 이후 삽입 작업 수
- `mongo.opcounters_query` — 시작 이후 쿼리 작업 수
- `mongo.opcounters_update` — 시작 이후 업데이트 작업 수
- `mongo.opcounters_delete` — 시작 이후 삭제 작업 수
- `mongo.opcounters_getmore` — 시작 이후 getMore 작업 수
- `mongo.mem_resident` — 상주 메모리(MB)
- `mongo.mem_virtual` — 가상 메모리(MB)
- `mongo.network_bytes_in` — 시작 이후 수신한 바이트 수

각 지표는 실시간 차트를 위해 단일 `(timestamp_ms, value)` 행으로 반환됩니다.

### 인스턴스 검사기

실행 중인 서버 상태의 표 형태 스냅샷을 노출합니다:

- `mongo.current_op` — `$currentOp` 집계 파이프라인에서 가져온 진행 중인 작업 (opid, type, ns, op, secs_running, wait_for_lock)

## 제한 사항

- SQL은 지원되지 않습니다. 쿼리는 MongoDB 셸 스타일 문법(또는 JSON 폴백)을 사용해야 합니다.

- 인스턴스 지표는 호출당 단일 데이터 포인트(`serverStatus`의 현재 스냅샷)를 반환하며, 과거 시계열이 아닙니다. 작업 카운터(예: `mongo.opcounters_insert`)는 단조 증가하므로 절대 비율이 아니라 샘플 간 델타로 해석해야 합니다.

- `$currentOp`는 Atlas 클러스터에서 `inprog` 권한이나 `clusterMonitor` 역할이 필요합니다. 권한이 충분하지 않으면 `fetch_inspector_snapshot("mongo.current_op")`는 빈 결과 집합을 반환합니다.
- 쿼리 취소는 지원되지 않습니다(`QUERY_CANCELLATION`이 설정되지 않음).
- `RETURNING`은 지원되지 않습니다. 변경 기능도 기능 수준에서는 배치, 벌크 업데이트, 벌크 삭제가 없다고 보고합니다(`supports_batch`, `supports_bulk_update`, `supports_bulk_delete`가 모두 `false`). 생성기가 `updateMany`/`deleteMany` 텍스트를 만들어 낼 수 있음에도 그렇습니다.
- 파서 범위는 전체 대화형 셸 언어가 아니라 위에 나열된 지원 메서드 집합으로 의도적으로 한정됩니다. `distinct`는 쿼리 기능으로 노출되지 않습니다(`supports_distinct: false`).
- 쿼리 기능 수준에서 조인, 서브쿼리, 유니온, CTE, 윈도우 함수, `EXPLAIN`이 없습니다.
- 트랜잭션은 기능 수준에서 알려집니다(`supports_transactions: true`). 하지만 격리 수준, 저장점, 중첩 트랜잭션, 읽기 전용, deferrable 지원은 없습니다.
- DDL은 트랜잭션으로 보호되지 않습니다(`transactional_ddl: false`). 데이터베이스 생성, 컬렉션 생성, alter, 뷰, 트리거는 지원되지 않습니다.

- 스크립트 엔진은 생략(omission)으로 샌드박스를 만듭니다. `require`, 모듈 로더, 파일시스템/네트워크/프로세스 생성 전역은 스크립트 코드에서 도달할 수 없습니다. 최상위 `await`, `require(...)`, `import` 문은 무엇이든 디스패치되기 전에 이름으로 거부됩니다.
- 샌드박스 리소스 제한: 스크립트 실행당 64 MiB 메모리, 512 KiB 스택, 30초 벽시계 데드라인. 데드라인은 JS 시간에만 적용됩니다. 진행 중인 데이터베이스 호출은 도중에 중단될 수 없으며, 서버 측 `maxTimeMS`와 연결 자체의 취소 플래그로 별도로 제한됩니다.
- 스크립트 안의 단일 `find()`/`aggregate()` 호출은 문서 10 000개로 제한됩니다. 한도를 초과하면 결과를 조용히 잘라내는 대신 한도를 명시하는 오류로 실패합니다. 지연/스트리밍 커서 의미론(`find()`/`aggregate()` 결과에 연결하는 `hasNext`, `next`, `limit`, `skip`, `sort`, `count`)는 v1 범위 밖이며 호출된 메서드 이름을 밝히며 예외를 던집니다. 대신 동일한 limit/skip/sort를 `find()`에 추가 인수로 디스패치하거나 유계 결과에 `.toArray()`를 사용하세요.
- 스크립트로 반환되는 문서는 canonical extJSON이 아니라 relaxed extJSON으로 변환됩니다. 일반 JSON 숫자는 숫자로 유지되므로(`doc.qty + 1`이 문자열 연결이 아니라 산술) JSON이 표현하지 못하는 타입(ObjectId, Date 등)은 감싸집니다(`{"$oid": ...}`, `{"$date": ...}`). 이 변환은 정확히 왕복되지 않습니다. `1.0`인 BSON `Double`은 JSON `1`이 됩니다. 읽은 문서를 검사하는 용도로는 괜찮지만, 읽기 결과를 그대로 쓰기로 되돌려 보내서는 안 된다는 뜻입니다.
- 스크립트 도중 서버에서 실패하는 문(예: 중복 키 오류)은 그 지점에서 실행을 중단합니다. 이미 실행된 문은 롤백되지 않고, 이후 문은 디스패치되지 않습니다.
