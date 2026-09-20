# 드라이버 작성 가이드

이 가이드를 사용해 DBFlux 데이터베이스 드라이버 통합을 선택하고 구현하세요. 더 넓은 아키텍처나 RPC 프로토콜 참조 문서를 반복하지 않으면서 기여자 경로를 다룹니다.
## 통합 경로 선택

| 선택 기준 | 내장 Rust 드라이버 | 외부 RPC 드라이버 |
| --- | --- | --- |
| 가장 적합한 경우 | 드라이버를 DBFlux 워크스페이스와 프로세스에 포함해 배포하는 경우 | 드라이버를 프로세스 외부에서 실행하거나 독립적으로 개발·배포하는 경우 |
| 구현 | 핵심 Rust 계약을 구현하는 `crates/dbflux_driver_<name>/` 크레이트 | 드라이버 RPC 프로토콜을 구현하는 서비스 |
| 등록 | 컴파일 타임 기능 연결과 `AppState::build_builtin_drivers()` | 설정 -> RPC 서비스에서 `kind=driver`와 `socket_id` 지정 |
| 안정 키 | `builtin:<name>` | `rpc:<socket_id>` |
| 구성 | 드라이버가 소유한 `DriverFormDef`를 내장 `DbConfig` 변형으로 변환 | 핸드셰이크가 제공하여 `DbConfig::External`로 저장되는 폼 데이터 |

## 내장 드라이버: 기본 절차

1. 가장 가까운 기존 `crates/dbflux_driver_*/` 크레이트의 구조를 복사합니다.
2. 메타데이터, 폼/구성 변환, 연결 동작, 오류, 타입이 지정된 결과 열을 포함해 `DbDriver`와 `Connection`을 구현합니다.
3. 동작하는 구현이 뒷받침하는 기능만 선언합니다. 선택적 연결 지점은 드라이버가 지원할 때만 추가합니다.
4. 워크스페이스, 앱, 바이너리를 통해 크레이트와 기능을 연결하고 `build_builtin_drivers()`에 등록합니다.
5. 집중된 테스트와 크레이트 README를 추가하고 드라이버 지원 매트릭스를 업데이트합니다.

[상세 내장 체크리스트](#내장-드라이버-체크리스트)가 각 단계를 확장해서 설명합니다.

## 외부 RPC 드라이버: 기본 절차

1. [사용자 지정 드라이버 예제](../examples/custom_driver/README.md)를 출발점으로 삼아 표준 [Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md)을 구현합니다.
2. 서비스를 독립적으로 빌드하고 실행하거나 관리형 명령과 함께 실행합니다.
3. 설정 -> RPC 서비스에 `kind=driver`, 안정적인 `socket_id`, 선택적인 관리형 명령으로 추가합니다.
4. DBFlux를 다시 시작하고 핸드셰이크가 제공한 메타데이터와 폼이 연결 관리자에 나타나는지 확인합니다.

현재 구성 동작은 [RPC 서비스 구성 참조](RPC_SERVICES_CONFIG.md)를 참조하세요. 이 가이드에서 실행 플래그를 복사하지 마십시오. 프로토콜, 구성 참조, 예제가 권위 있는 출처입니다.

## 핵심 계약과 디커플링 규칙

1차 계약은 [`DbDriver`와 `Connection`](../crates/dbflux_core/src/core/traits.rs)입니다:

- `DbDriver`는 드라이버 메타데이터, 연결 폼 정의, 구성 생성과 추출, 연결 생성, 안정적인 `DriverKey`를 제공합니다.
- `Connection`은 런타임 쿼리, 스키마, 변경, 선택적인 기능별 동작을 제공합니다. 지원하지 않는 많은 작업에는 기본 구현이 존재하지만, 필수 메서드와 광고된 기능은 여전히 서로 일치해야 합니다.
- 내장 `driver_key()` 값은 `builtin:<name>` 형식을 사용합니다. 외부 드라이버는 `rpc:<socket_id>`를 사용합니다.

메타데이터와 적응은 [`DriverMetadata`, `DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`](../crates/dbflux_core/src/driver/capabilities.rs)로 정의되며, 일반적인 에디터 표현 메타데이터를 포함합니다. 런타임 소스와 표현 동작은 [`traits.rs`](../crates/dbflux_core/src/core/traits.rs)의 `Connection`에 있는 일반 연결 지점을 통해 노출됩니다.

**엄격한 규칙:** UI와 앱 워크플로 코드는 구체적인 드라이버 ID로 분기해서는 안 됩니다. 메타데이터, 카테고리, 쿼리 언어, 기능 플래그, 폼 정의, 일반 소스/표현 연결 지점에서 적응하세요. 새로운 UI 구분이 필요하다면 다른 드라이버도 구현할 수 있는 일반적인 핵심 계약을 추가하십시오.

결과 데이터는 [`ColumnMeta.kind`를 `ColumnKind`로](../crates/dbflux_core/src/query/types.rs) 채워야 합니다. 차트와 다른 소비자는 이 의미 타입을 사용하며 드라이버 ID나 `type_name`으로부터 추론하지 않습니다.

## 내장 드라이버 체크리스트

### 1. 크레이트와 계약

- [ ] `crates/dbflux_driver_<name>/Cargo.toml`, `src/lib.rs`, 구현 모듈, 테스트를 추가합니다. 모든 드라이버가 동일한 모듈을 갖는다고 가정하지 말고 가장 가까운 드라이버를 따르십시오.
- [ ] [`crates/dbflux_core/src/core/traits.rs`](../crates/dbflux_core/src/core/traits.rs)의 `DbDriver`와 스레드 안전한 `Connection`을 구현합니다.
- [ ] `builtin:<name>` 형식의 안정적인 `DriverKey`를 반환합니다.
- [ ] 데이터베이스 클라이언트 타입과 드라이버별 동작은 드라이버 크레이트 안에 두고, 핵심 계약을 통해서만 동작을 노출합니다.

### 2. 메타데이터와 기능

- [ ] 사실에 근거한 `DriverMetadata`를 정의합니다: 식별 정보, 표시 필드, `DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`, 연결 기본값, 해당하는 일반 기능 구조.
- [ ] UI 적응에는 메타데이터와 일반 표현/소스 연결 지점을 사용합니다. UI나 앱 워크플로에 드라이버 ID 조건문을 추가하지 않습니다.
- [ ] 해당 작업이나 선택적 연결 지점이 실제로 동작할 때만 기능을 광고합니다. 지원되는 동작뿐 아니라 부정적 주장도 확인합니다.

### 3. 폼과 구성

- [ ] 크레이트의 `DriverFormDef`를 정의하고 소유합니다. 연결 UI는 이를 일반적으로 렌더링합니다.
- [ ] `build_config()` 검증과 `extract_values()` 편집 왕복을 구현합니다.
- [ ] 비밀은 저장된 폼 값에 포함하지 말고 확립된 비밀 경로에 두십시오.
- [ ] URI 파싱/빌드나 내보내기 필드 재정의는 해당하는 경우에만 구현합니다.

### 4. 연결, 오류, 결과

- [ ] 필수 비밀 처리와 연결 테스트를 포함해 `DbDriver` 메서드를 통해 연결을 생성하고 테스트합니다.
- [ ] [`QueryErrorFormatter`와 `ConnectionErrorFormatter`](../crates/dbflux_core/src/core/error_formatter.rs)를 통해 구조화된 쿼리 및 연결 오류 서식을 구현합니다. 비밀을 노출하지 않으면서 유용한 데이터베이스 컨텍스트를 보존합니다.
- [ ] 스키마와 쿼리 데이터를 핵심 타입으로 반환합니다. 모든 결과 열에 올바른 `ColumnKind`를 사용한 `ColumnMeta.kind`를 포함합니다.
- [ ] 타입 매핑을 직접 테스트합니다. 소비자가 원시 `type_name` 문자열에서 의미를 도출하도록 의존하지 않습니다.

### 5. 선택적 연결 지점

데이터베이스가 지원할 때만 구현하고 기능 플래그를 구현과 동기화해 두십시오:

- [ ] 언어별 검증과 변경 분류를 위한 기본값이 아닌 `LanguageService`.
- [ ] 해당하는 경우 SQL 방언, 코드 생성기, 쿼리 생성기, 또는 의미 플래너 동작.
- [ ] 해당하는 경우 소스 컨텍스트, 지표 카탈로그, 대시보드 가져오기, 또는 대시보드 소스 동작.
- [ ] 해당하는 경우 지표나 검사기를 위한 인스턴스 카탈로그.
- [ ] 핵심 트레이트와 기능 플래그가 표현하는 기타 스키마, CRUD, 취소, 전송, 키-값 연결 지점.

### 6. 기능 연결 및 등록

- [ ] 루트 [`Cargo.toml`](../Cargo.toml)에 워크스페이스 멤버십과 워크스페이스 종속성을 추가합니다.
- [ ] [`crates/dbflux_app/Cargo.toml`](../crates/dbflux_app/Cargo.toml)에 선택적 종속성과 기능 릴레이를 추가합니다.
- [ ] [`crates/dbflux/Cargo.toml`](../crates/dbflux/Cargo.toml)에서 바이너리 기능을 포워딩합니다.
- [ ] [`AppState::build_builtin_drivers()`](../crates/dbflux_app/src/app_state/bootstrap.rs)에 기능 게이트가 적용된 임포트와 등록을 추가합니다.
- [ ] 활성화된 기능 빌드와 대표적인 기능 비활성화 빌드를 모두 확인하여 등록이 올바르게 게이트되는지 검증합니다.

### 7. 테스트와 문서

- [ ] 메타데이터, 기능 선언, 폼/구성 왕복, 오류, 연결 동작, 스키마 매핑, 쿼리 결과, 광고된 모든 선택적 연결 지점을 테스트합니다.
- [ ] 동작이 드라이버/핵심 경계를 넘는 곳에는 통합 테스트를 추가하고, 라이브 서비스 테스트는 기존 크레이트 관례에 따라 ignored 또는 게이트 처리를 유지합니다.
- [ ] 명확한 **Features**와 **Limitations** 섹션을 갖춘 `crates/dbflux_driver_<name>/README.md`를 추가합니다.
- [ ] [`docs/DRIVERS.md`](DRIVERS.md)를 업데이트하고 기능 주장이 크레이트 README와 구현과 일치하도록 유지합니다.

## 외부 RPC 드라이버 체크리스트

- [ ] [Driver RPC Protocol](DRIVER_RPC_PROTOCOL.md)에 맞춰 핸드셰이크, 폼, 세션, 쿼리, 지원되는 선택적 작업을 구현합니다.
- [ ] 메타데이터, 기능, 폼 정의를 프로토콜 핸드셰이크를 통해 제공하고, 모든 기능 주장이 구현된 RPC 작업과 일치하도록 유지합니다.
- [ ] 설정 -> RPC 서비스에서 안정적인 `socket_id`와 함께 `kind=driver`로 서비스를 구성합니다. DBFlux가 프로세스 수명 주기를 소유해야 할 때만 관리형 명령을 추가합니다.
- [ ] 런타임 키 `rpc:<socket_id>`와 `DbConfig::External`을 통한 일반 구성 저장을 예상합니다.
- [ ] 저장되는 구성과 수명 주기 의미는 [RPC 서비스 구성](RPC_SERVICES_CONFIG.md)을 따릅니다.
- [ ] [사용자 지정 드라이버 예제](../examples/custom_driver/README.md)에서 빌드하고 스모크 테스트한 다음, 다시 시작, 핸드셰이크 실패, 지원되지 않는 작업, 연결 폼 왕복을 테스트합니다.

외부 RPC 드라이버는 내장 Cargo 기능 연결이나 `build_builtin_drivers()` 등록 경로를 사용하지 않습니다.

## 검토 체크리스트

PR을 열기 전에 확인합니다:

- [ ] 선택한 내장 또는 RPC 경로를 일관되게 사용했으며 두 등록 경로를 섞지 않았습니다.
- [ ] UI나 앱 워크플로가 구체적인 드라이버 ID로 분기하지 않습니다.
- [ ] 메타데이터, 기능 플래그, 선택적 연결 지점, 테스트, 크레이트 README, `docs/DRIVERS.md`가 동일한 주장을 합니다.
- [ ] 폼/구성 편집 왕복이 동작하고 비밀이 예기치 않게 저장되거나 로깅되지 않습니다.
- [ ] 쿼리 결과가 `ColumnMeta.kind`를 올바르게 채웁니다.
- [ ] 연결 및 쿼리 실패가 구조화되고 유용하며 비밀이 노출되지 않는 오류를 만듭니다.
- [ ] 내장 기능 비활성화 및 활성화 빌드가 통과하거나, RPC 서비스가 핸드셰이크와 다시 시작 스모크 테스트를 완료합니다.
- [ ] [CONTRIBUTING.md](../CONTRIBUTING.md)의 리포지토리 검사가 통과합니다.
