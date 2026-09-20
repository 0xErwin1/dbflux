# DynamoDB

AWS 관리형 NoSQL 키-값 및 문서 데이터베이스입니다.

## 한눈에 보기

- **카테고리** — 문서
- **쿼리 언어** — DynamoDB 표현식
- **URI 스킴** — `dynamodb`

[`aws-sdk-dynamodb`](https://crates.io/crates/aws-sdk-dynamodb) SDK를 기반으로 만들어진 DBFlux용 AWS DynamoDB 드라이버입니다.

## 기능

- `DatabaseCategory::Document`로 분류되고 `QueryLanguage::Custom("DynamoDB")` 명령 엔벨로프를 갖는 관리형 NoSQL 드라이버입니다. 에디터는 SQL이 아닌 DynamoDB 전용 문법을 사용합니다.
- 리전, 명명된 프로필, 선택적 엔드포인트 재정의(DynamoDB Local 또는 VPC 엔드포인트용)를 통한 AWS 연결 구성입니다. `deployment_class`는 `CloudManaged`입니다.
- 테이블 탐색은 `ListTables`와 `DescribeTable`을 사용하며, 파티션 키(PK), 정렬 키(SK), Global/Local Secondary Index(GSI/LSI) 키 메타데이터를 DBFlux 스키마 추상화에 매핑합니다.
- `scan`, `query`, `put`, `update`, `delete`를 위한 네이티브 명령 엔벨로프 실행입니다. 쿼리 생성기는 scan 형태의 미리 보기 엔벨로프를 만들어 내며, 필터가 테이블 키 스키마와 일치하면 실행이 `Query`로 최적화될 수 있음을 알려 줍니다.
- 인덱스 대상 지정, consistent-read 제어, 필터 변환 폴백 정책(서버 측 필터 대 클라이언트 측 폴백; 폴백 정책이 reject로 설정되면 클라이언트 필터링은 거부됨)을 위한 읽기 옵션입니다.
- 시맨틱 필터의 WHERE 연산자: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, 논리 `And`/`Or`(`Not`은 제한 사항 참조).
- 변경: 삽입(`put`), 업데이트, 삭제(`INSERT`/`UPDATE`/`DELETE`). 배치 쓰기는 최대 25개 항목(`max_insert_values: 25`, `supports_batch: true`)까지 지원하며, 처리되지 않은 배치 쓰기 항목에 대해 범위가 제한된 재시도를 수행합니다.
- 조건부 업데이트에 put 폴백을 결합한 단일 항목 upsert 지원입니다. 키 맵은 필터 또는 업데이트 페이로드에서 해석됩니다(파티션 키는 필수이며, 테이블이 정렬 키를 정의하면 정렬 키도 필수입니다).
- 공유 업데이트 표현식을 사용하는 다중 항목 업데이트 경로(`many=true`인 `update`)입니다.
- 중첩 문서와 배열을 문서 트리 뷰에 매핑합니다(`NESTED_DOCUMENTS`, `ARRAYS`).
- DDL: 테이블 삭제(`supports_drop_table: true`).
- 페이지 토큰 기반 페이지 나누기(`PaginationStyle::PageToken`).
- 클라이언트 식별: 모든 요청은 AWS SDK 앱 이름으로 `dbflux-<version>`을 전송하며, CloudTrail의 `userAgent` 필드에 표시됩니다.

## 제한 사항

- `profile` 필드(AWS 명명된 프로필)는 `AuthProfileRef` 폼 필드입니다. 일반 이식성 시임(`DbDriver::export_field_hint`)은 모든 `AuthProfileRef` 필드를 `RequiredOnImport`로 매핑하므로, 이 필드 값은 내보낸 번들에서 생략되며 받는 쪽에서 가져오기 시점에 일치하는 인증 프로필을 제공하거나 만들어야 합니다. 드라이버별 재정의는 필요하지 않습니다.
- 쿼리 취소는 지원되지 않습니다. 취소 요청에 대해 드라이버는 `NotSupported`를 반환합니다.
- 명령 엔벨로프 API는 PartiQL이나 DynamoDB 트랜잭션 작업을 노출하지 않습니다. 트랜잭션은 비활성화되어 있습니다(`supports_transactions: false`).
- 단일 항목 upsert는 지원됩니다(`supports_upsert: true`). `many=true`와 `upsert=true`를 함께 사용하는 `update`는 거부됩니다(`update_many_with_upsert`).
- 벌크 업데이트와 벌크 삭제는 지원되지 않고(`supports_bulk_update: false`, `supports_bulk_delete: false`), `RETURNING`도 지원되지 않습니다.
- 시맨틱 필터는 `NOT` 표현식이나 지원 집합 밖의 연산자를 지원하지 않습니다. 지원되지 않는 연산자는 `NotSupported`를 반환합니다.
- SSL 폼이 없고(TLS는 AWS SDK 전송 계층이 처리), 스키마도 없으며, 테이블 삭제 외의 DDL(테이블 생성/변경, 인덱스 생성)은 지원하지 않습니다.
- 시맨틱 플래너는 집계 요청을 지원하지 않습니다.
- 핵심 요청 계층의 컬렉션 탐색은 여전히 오프셋 기반이지만, 하위 API는 페이지 토큰 기반입니다.
- 쓰기 권한 프로브가 없습니다. `Connection::probe_write_privilege`는 의도적으로 트레이트 기본값(`WritePrivilege::Unknown`)을 유지합니다. 신뢰할 수 있는 확인에는 `iam:SimulatePrincipalPolicy` 권한이 필요한데, 연결 역할에는 대개 이 권한이 없기 때문입니다.
- 인스턴스 지표와 인스턴스 검사기가 없습니다(`INSTANCE_METRICS`/`INSTANCE_INSPECTOR`를 선언하지 않음): DynamoDB의 서버 측 지표는 이미 CloudWatch에 있으므로, 드라이버별 `InstanceCatalog`는 새로운 표면을 추가하기보다 그 표면을 중복하게 됩니다.
