# 쿼리 실행

`Ctrl+n` (macOS에서는 `Cmd+n`)으로 새 쿼리 탭을 열거나 `Ctrl+o`로 스크립트 파일을 엽니다. 편집기의 쿼리 언어(SQL, MongoDB 쿼리 구문, Redis 명령 등)는 활성 연결의 드라이버가 결정하며, 이 드라이버가 구문 강조와 자리 표시자 텍스트도 결정합니다. `.csv` 또는 `.tsv` 파일은 대신 테이블로 열립니다. [CSV 및 TSV 파일](CSV_FILES.md)을 참고하세요.

SQL 연결에서는 [시각적 쿼리 빌더](QUERY_BUILDER.md)로 SQL을 작성하지 않고
쿼리를 만들 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/editor/query-result-dark.webp">
  <img src="../images/editor/query-result-light.webp" alt="주문을 국가별로 묶는 SQL 쿼리와 결과 그리드가 있는 쿼리 탭">
</picture>

## 탭 저장 및 닫기

새 쿼리 탭(`Ctrl+n`)은 `Ctrl+o`로 연 스크립트와 마찬가지로 스크립트 폴더에 있는 실제 파일을 기반으로 합니다. 열려 있는 편집기는 설정된 간격으로 그 파일에 자동 저장하며, `Ctrl+s`와 **다른 이름으로 저장**도 같은 대기열을 거칩니다. 자동 저장과 탭 닫기는 DBFlux 밖에서 변경된 파일을 덮어쓰지 않습니다. 편집 중인 내용은 편집기에 그대로 남고 DBFlux는 거부된 쓰기를 보고합니다. `Ctrl+s`와 **다른 이름으로 저장**은 의도적인 동작이므로 이 경우에도 파일을 씁니다.

저장하지 않은 변경 사항이 있는 탭을 닫으면 먼저 저장한 뒤 닫습니다. 쓰기를 완료할 수 없으면(예: 파일이 DBFlux 밖에서 변경되었거나 읽기 전용인 경우) 변경 사항이 남은 채 탭이 열려 있고, DBFlux는 `Ctrl+s` / **다른 이름으로 저장**을 의도적인 덮어쓰기 수단으로 안내합니다. 아직 파일이 없는 버퍼는 예외입니다. 닫을 때 먼저 묻기 때문에 저장하거나, 저장하지 않고 닫거나, 취소할 수 있습니다. DBFlux를 종료할 때도 같은 방식으로 저장되지 않은 변경 사항을 먼저 저장합니다. 시작 시 스크립트 폴더를 만들지 못했다면 새 쿼리는 세션 저장소에 보관되고, 탭을 닫을 때 **다른 이름으로 저장**을 제안합니다.

## 실행

- `Ctrl+Enter` (`Cmd+Enter`) — **쿼리 실행**.
- `Ctrl+Shift+Enter` (`Cmd+Shift+Enter`) — **새 탭에서 쿼리 실행**.

비어 있지 않은 텍스트 선택이 있으면 선택한 텍스트만 실행합니다. 선택이 없으면 편집기 버퍼 전체를 사용합니다.

실행 중 실제로 행이 생략되면 편집기는 쿼리당 경고 하나를 표시하고 그리드는 해당 결과 집합을 표시합니다. 보존된 행이 없어도 표시됩니다. 행이 생략되지 않고 제한에 정확히 도달한 결과에는 경고가 표시되지 않습니다. 보존 행 수 제한은 저장할 행만 제한하며 바이트 및 시간 제한은 별도의 실행 제어입니다. 이 기능은 편집기에 기본 행 수 제한이 설정되어 있음을 의미하지 않습니다.

## 다중 문 스크립트

선택 없이 실행했을 때 버퍼에 `;`로 구분된 문이 여러 개 있고 활성 드라이버가 일괄 처리를 지원한다고 알리면, DBFlux는 실행 전에 확인 대화 상자(`Run entire script (N statements)?`)를 표시합니다. 확인하면 각 문의 결과 집합이 자체 결과 탭에 렌더링됩니다.

문 분할은 SQL 계열 언어에서 언어를 인식합니다. 문자열, 식별자, 행/블록 주석, PostgreSQL 달러 인용 본문 안의 구분자는 문 경계로 취급하지 않습니다. 비 SQL 언어는 단일 문으로 유지됩니다. 일괄 처리 지원은 드라이버별입니다. 내장 SQL 드라이버 중에서는 PostgreSQL, MySQL/MariaDB, SQLite, Microsoft SQL Server가 지원합니다. 선택 영역은 항상 그대로 실행되며 스크립트 확인을 유발하지 않습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/editor/multi-statement-dark.webp">
  <img src="../images/editor/multi-statement-light.webp" alt="세 개의 문을 스크립트로 실행해 문마다 결과 탭이 하나씩 있는 쿼리 탭">
</picture>

## 위험 쿼리 확인

DBFlux는 여러 언어에 걸친 위험한 작업을 감지합니다. SQL의 `DELETE`/`DROP`/`TRUNCATE`와 `WHERE` 없는 `DELETE`/`UPDATE`, MongoDB의 `deleteMany`/`drop`, Redis의 `FLUSHALL`/`FLUSHDB`/`KEYS`가 대상이며, 실행 전에 확인을 요청합니다. 이 동작은 설정의 영향을 받습니다. 위험 쿼리 확인은 끌 수 있고, `DELETE`/`UPDATE`에 `WHERE` 절을 요구하도록 할 수 있으며, Redis `FLUSHALL`/`FLUSHDB`는 완전히 비활성화할 수 있습니다(이 경우 해당 명령은 확인 대신 차단됩니다).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/editor/dangerous-query-dark.webp">
  <img src="../images/editor/dangerous-query-light.webp" alt="WHERE 절 없는 DELETE에 대한 위험 쿼리 확인 대화 상자">
</picture>

## 스크립트 (Lua / Python / Bash)

Lua, Python, Bash 문서는 데이터베이스 쿼리가 아니라 스크립트로 실행됩니다. 실행 중에는 출력이 문서의 출력 영역으로 실시간 스트리밍되고, 최종 출력은 텍스트 결과로 유지됩니다. 내장 Lua 런타임은 `docs/LUA.md`를 참조하세요.

## 저장된 쿼리와 기록

DBFlux는 완료된 쿼리의 기록을 유지하며 이름 있는 쿼리를 저장할 수 있게 해줍니다.

- `Alt+h` (편집기에서) 또는 도구 모음의 기록 버튼 — 편집기 옆의 쿼리 기록 패널을 열고 닫습니다.
- `Ctrl+s` (`Cmd+s`) — 현재 쿼리를 **저장**합니다.
- `Ctrl+Shift+s` (`Cmd+Shift+s`) — **파일을 다른 이름으로 저장**합니다.
- `Ctrl+p` (`Cmd+p`, 편집기에서) — 저장된 쿼리 브라우저를 엽니다.

기록 패널은 최근 쿼리와 저장된 쿼리를 보여 주며 편집하는 동안 열려 있습니다. 항목을 클릭하거나 `Enter`를 누르면 편집기로 불러옵니다. 패널에 포커스가 있을 때는 `Ctrl+j`/`Ctrl+k`(또는 방향 키)로 탐색하고, `Ctrl+s`로 최근 쿼리를 저장하며, 로컬 니모닉 `Ctrl+f`(즐겨찾기 토글), `Ctrl+r`(이름 바꾸기), `Ctrl+d`(삭제)를 사용할 수 있습니다. `/` 또는 패널 헤더의 검색 버튼은 검색 필드를 열고, `Esc`는 패널을 닫습니다.
