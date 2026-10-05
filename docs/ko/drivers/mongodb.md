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
- **데이터 그리드에서 컬렉션 탐색(`DocumentFeatures::QUERY_SLOTS`)**: 컬렉션 탐색은 필터 외에 프로젝션 문서와 정렬 문서를 받으며, `sample_collection_schema`는 무작위 표본(필터의 `$match` 뒤 `$sample`)을 읽어 필드 경로마다 존재율, 유형 분포(`String`, `Int32`, `Decimal128`, `Object`, `Array` 등), 값 요약을 보고합니다. 그리드는 이 표본으로 쿼리 바의 필드 경로 자동 완성, 열 머리글의 존재율 막대, 스키마 뷰를 제공합니다. 필터가 없는 개수는 `estimatedDocumentCount`에서 가져오며 추정치로 표시됩니다.
- **필드 편집(`DocumentFeatures::FIELD_PATCH`)**: `patch_document`는 변경된 경로만 담은 `$set` / `$unset`으로 `updateOne`을 보내고 BSON 유형을 유지합니다(소수는 `Decimal128`, 날짜는 `Date`, ObjectId는 `ObjectId`로 남고, 텍스트는 ObjectId로 해석되지 않습니다). `replace_document`는 `_id`를 건드리지 않고 `replaceOne`을 보내며, `fetch_document`는 `_id`로 문서 하나를 읽어 페이지를 불러온 뒤 변경되었는지 그리드가 판단할 수 있게 합니다. 셸 생성기는 서버 변경 확인에 실제로 보낼 쓰기를 보여 줍니다. 예: `db.products.updateOne({ _id: ObjectId("…") }, { $set: { "price.amount": Decimal128("119.00") } })`.
- **집계 뷰(`DocumentFeatures::AGGREGATE`)**: `aggregate_collection`은 JSON 스테이지로 된 파이프라인을 컬렉션에 실행하며, 요청이 요구하지 않으면 `allowDiskUse`는 꺼져 있습니다. 드라이버는 요청한 상한보다 하나 큰 `$limit`을 덧붙이고, 파이프라인이 더 많은 문서를 내면 결과를 잘림으로 표시합니다. `$out`이나 `$merge`로 끝나는 파이프라인은 그 스테이지를 마지막에 유지하며 문서를 반환하지 않습니다. 결과는 `browse_collection`과 같은 문서 형태로 돌아옵니다. 셸 생성기는 파이프라인을 `db.<collection>.aggregate([...])`로 보여 주고, 언어 서비스는 `$out` 또는 `$merge` 스테이지가 있는 파이프라인을 `MongoAggregateWrite`로 표시하므로 실행 전에 위험 쿼리 확인을 거칩니다.
- **시각적 쿼리 빌더(`DocumentFeatures::VISUAL_BUILDER`)**: `Connection::document_query_codec()`은 `DocumentQuerySpec`을 `filter` / `project` / `sort` / `limit` 칸, 집계 파이프라인(`$match`, `$count` / `$sum` / `$avg`를 쓰는 `$group`, `$sort`, `$skip`, `$limit`), 셸 미리보기 텍스트로 렌더링하고 칸을 다시 스펙으로 읽는 코덱을 반환합니다. `$expr`처럼 스펙에 담을 수 없는 절은 버려지지 않고 표현할 수 없는 텍스트로 반환되므로, 빌더는 이를 다시 쓰지 않고 동기화 충돌을 표시합니다. [문서 컬렉션](../QUERY_BUILDER.md#문서-컬렉션)을 참조하세요.
- **확장 JSON 날짜**: 쿼리 칸, 집계 파이프라인, 문서 쓰기에 있는 `{"$date": "<RFC 3339>"}`는 BSON `Date`로 디코딩됩니다. 이런 위치의 잘못된 날짜 문자열은 하위 문서로 저장되지 않고 오류로 실패합니다.
- 변경: 삽입, 업데이트(upsert 포함), 삭제(`supports_upsert: true`). `MongoShellGenerator`는 미리 보기와 쿼리로 복사를 위해 `insertOne`/`insertMany`, `updateOne`/`updateMany`(`{ upsert: true }` 포함), `deleteOne`/`deleteMany`를 만들어 냅니다.
- DDL: 데이터베이스 삭제, 컬렉션 삭제, 인덱스 생성, 인덱스 삭제.
- 결과의 JSON 내보내기(`EXPORT_JSON`).
- 연결 시 `appName=dbflux/<version>`으로 클라이언트 식별을 보고합니다(서버 로그와 `db.currentOp()`에 표시됨). 단, 연결 URI가 이미 `appName`을 설정했다면 그 값이 사용됩니다.
- 쓰기 권한 프로브: 연결 후 `connectionStatus`(`showPrivileges: true`)에서 쓰기를 부여하는 권한/역할을 검사하여 세션을 쓰기 가능, 읽기 전용, 알 수 없음으로 분류합니다. 쓰기 불가 노드(예: 세컨더리)에 직접 연결된 경우 `hello`가 판정을 읽기 전용으로 바꿉니다.
- **네이티브 명령 콘솔(`NATIVE_CONSOLE`)**: 컬렉션 탭에는 `Connection::execute`를 통해 컬렉션의 데이터베이스에 대해 셸 명령을 한 번에 하나씩 실행하는 콘솔이 붙고, 사이드바의 데이터베이스 메뉴에서는 이 콘솔을 별도 탭으로 열 수 있으며, 편집기가 쓰는 것과 같은 언어 서비스의 검증과 위험 명령 감지, 감사 행, 쿼리 기록을 사용합니다. 확인된 위험 명령은 확인된 상한을 함께 가지므로, 콘솔에 입력한 스크립트도 편집기에서 실행한 스크립트와 같은 상한 규칙을 따릅니다.
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

- 행 제한(0 포함)이나 명령문 시간 제한이 지정된 실행 요청은 MongoDB가 해당 보호를 보장할 수 없으므로 디스패치 전에 거부됩니다. 보호 옵션이 없는 요청은 기존 동작을 유지합니다.

- SQL은 지원되지 않습니다. 쿼리는 MongoDB 셸 스타일 문법(또는 JSON 폴백)을 사용해야 합니다.

- 인스턴스 지표는 호출당 단일 데이터 포인트(`serverStatus`의 현재 스냅샷)를 반환하며, 과거 시계열이 아닙니다. 작업 카운터(예: `mongo.opcounters_insert`)는 단조 증가하므로 절대 비율이 아니라 샘플 간 델타로 해석해야 합니다.

- `$currentOp`는 Atlas 클러스터에서 `inprog` 권한이나 `clusterMonitor` 역할이 필요합니다. 권한이 충분하지 않으면 `fetch_inspector_snapshot("mongo.current_op")`는 빈 결과 집합을 반환합니다.
- 쿼리 취소는 지원되지 않습니다(`QUERY_CANCELLATION`이 설정되지 않음).
- `RETURNING`은 지원되지 않습니다. 변경 기능도 기능 수준에서는 배치, 벌크 업데이트, 벌크 삭제가 없다고 보고합니다(`supports_batch`, `supports_bulk_update`, `supports_bulk_delete`가 모두 `false`). 생성기가 `updateMany`/`deleteMany` 텍스트를 만들어 낼 수 있음에도 그렇습니다.
- 파서 범위는 전체 대화형 셸 언어가 아니라 위에 나열된 지원 메서드 집합으로 의도적으로 한정됩니다. `distinct`는 쿼리 기능으로 노출되지 않습니다(`supports_distinct: false`).
- 쿼리 기능 수준에서 조인, 서브쿼리, 유니온, CTE, 윈도우 함수, `EXPLAIN`이 없습니다.
- 트랜잭션은 기능 수준에서 알려집니다(`supports_transactions: true`). 하지만 격리 수준, 저장점, 중첩 트랜잭션, deferrable 지원은 없습니다.
- MongoDB에는 읽기 전용 세션 모드가 없으므로 읽기 전용 강제는 서버가 아니라 DBFlux가 작업마다 수행합니다. DBFlux가 무인으로 읽기로 실행하는 요청(`Read` 또는 `Metadata`로 분류된 MCP `execute_script` 스크립트)은 자기 클래스의 상한으로 실행되며 `Read`를 넘지 않습니다: 스크립트든 단일 문이든 모든 작업은 전송 전에 분류되며 상한을 넘으면 거부됩니다. insert, update, replace, delete, drop, `createCollection`, 모든 `runCommand`/`adminCommand`, 그리고 어느 깊이에든 `$out` 또는 `$merge` 스테이지가 있는 `aggregate`는 거부됩니다. 파서가 인식하지 못하는 작업(`findOneAndUpdate`, `bulkWrite`, `mapReduce`, `createIndex`, `renameCollection`, `getSiblingDB`, `distinct`, `watch` 등)도 거부됩니다. 변경 스트림(`$changeStream`)을 여는 `aggregate`도 끝나지 않으므로 거부됩니다. 읽기 안의 서버 측 JavaScript(`$where`, `$function`)는 여전히 실행되며, 최소 권한 자격 증명이 여전히 실제 경계입니다.
- DDL은 트랜잭션으로 보호되지 않습니다(`transactional_ddl: false`). 데이터베이스 생성, 컬렉션 생성, alter, 뷰, 트리거는 지원되지 않습니다.

- 스크립트 엔진은 생략(omission)으로 샌드박스를 만듭니다. `require`, 모듈 로더, 파일시스템/네트워크/프로세스 생성 전역은 스크립트 코드에서 도달할 수 없습니다. 최상위 `await`, `require(...)`, `import` 문은 무엇이든 디스패치되기 전에 이름으로 거부됩니다.
- 샌드박스 리소스 제한: 스크립트 실행당 64 MiB 메모리, 512 KiB 스택, 30초 벽시계 데드라인. 데드라인은 JS 시간에만 적용됩니다. 진행 중인 데이터베이스 호출은 도중에 중단될 수 없으며, 서버 측 `maxTimeMS`와 연결 자체의 취소 플래그로 별도로 제한됩니다.
- 스크립트 안의 단일 `find()`/`aggregate()` 호출은 문서 10 000개로 제한됩니다. 한도를 초과하면 결과를 조용히 잘라내는 대신 한도를 명시하는 오류로 실패합니다. 지연/스트리밍 커서 의미론(`find()`/`aggregate()` 결과에 연결하는 `hasNext`, `next`, `limit`, `skip`, `sort`, `count`)는 v1 범위 밖이며 호출된 메서드 이름을 밝히며 예외를 던집니다. 대신 동일한 limit/skip/sort를 `find()`에 추가 인수로 디스패치하거나 유계 결과에 `.toArray()`를 사용하세요.
- 스크립트로 반환되는 문서는 canonical extJSON이 아니라 relaxed extJSON으로 변환됩니다. 일반 JSON 숫자는 숫자로 유지되므로(`doc.qty + 1`이 문자열 연결이 아니라 산술) JSON이 표현하지 못하는 타입(ObjectId, Date 등)은 감싸집니다(`{"$oid": ...}`, `{"$date": ...}`). 이 변환은 정확히 왕복되지 않습니다. `1.0`인 BSON `Double`은 JSON `1`이 됩니다. 읽은 문서를 검사하는 용도로는 괜찮지만, 읽기 결과를 그대로 쓰기로 되돌려 보내서는 안 된다는 뜻입니다.
- 스크립트 도중 서버에서 실패하는 문(예: 중복 키 오류)은 그 지점에서 실행을 중단합니다. 이미 실행된 문은 롤백되지 않고, 이후 문은 디스패치되지 않습니다.
- 집계 뷰는 실행당 결과 문서를 최대 1,000개까지 보여 줍니다. 서버에서 실패하는 스테이지(알 수 없는 연산자, 사용자가 쓸 수 없는 대상으로의 `$merge`)는 미리 검증되지 않고 드라이버 오류로 보고됩니다. 실행 전에는 스테이지 형태만 확인합니다. 각 요소가 `$` 연산자 하나를 지정하는 JSON 배열이어야 합니다.
- 포함된 문서의 필드는 저장 순서가 아니라 키 순서로 반환됩니다. 값 모델이 포함 문서를 정렬된 맵으로 보관하기 때문입니다. 최상위 필드는 문서 순서를 유지합니다.
- 그리드에서는 `null`을 담은 최상위 필드와 없는 최상위 필드가 똑같이 보입니다(탐색이 없는 최상위 필드를 `null`로 채움). 중첩 필드는 구분되어 `missing`으로 표시됩니다. 같은 이유로 서버 변경 확인은 최상위 null을 무시합니다.
- 그리드에서 쓰는 정수는 들어가면 `Int32`, 아니면 `Int64`로 저장되며 필드의 이전 폭과 무관합니다.
- 편집기의 셸 파서는 JSON 인수 안의 `NumberDecimal(...)`, `ISODate(...)` 같은 셸 생성자를 읽지 못합니다. 유형이 있는 쓰기는 그리드의 필드 편집을 사용하세요.
- 서버 변경 확인은 쓰기 직전에 문서를 다시 읽어 페이지의 사본과 비교합니다. 그 읽기와 쓰기 사이에 일어난 변경은 감지되지 않습니다.
- 쿼리 칸에서 16진수 24자리로 된 텍스트 값은 `ObjectId`로 실행되므로, 그런 문자열은 칸이나 시각적 빌더에서 텍스트로 검색할 수 없습니다.
- 빌더 조건의 소수 값은 double로 비교되므로 `Decimal128` 필드와 일치하지 않습니다.
- `{"$date": ...}`는 RFC 3339 문자열만 받습니다. 확장 JSON의 숫자 형식과 `{"$numberLong": ...}` 형식은 디코딩되지 않습니다.
