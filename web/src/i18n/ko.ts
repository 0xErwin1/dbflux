import type { Dictionary } from './index';

export const ko: Dictionary = {
  nav: {
    features: '기능',
    drivers: '드라이버',
    docs: '문서',
    about: '소개',
    github: 'GitHub',
    download: '다운로드',
    menu: '메뉴',
    language: '언어',
    theme: '테마',
    theme_system: '시스템',
    theme_light: '밝게',
    theme_dark: '어둡게',
    theme_mirage: '신기루',
  },
  footer: {
    product: '제품',
    features: '기능',
    drivers: '드라이버',
    releases: '릴리스',
    docs: '문서',
    usage: '사용 가이드',
    connecting: '연결',
    mcp: 'AI + MCP',
    project: '프로젝트',
    about: '소개',
    contributing: '기여하기',
    source: '소스 코드',
    trademark: '상표 정책',
    privacy: '개인정보 처리방침',
    tagline: '완전한 오픈 소스, 키보드 우선 데이터베이스 클라이언트를 공개적으로 개발합니다.',
    license: 'MIT 또는 Apache-2.0 중에서 선택할 수 있습니다.',
  },
  search: {
    placeholder: '문서 검색',
    move: '이동',
    open: '열기',
    close: '닫기',
    no_results: '“{query}”과(와) 일치하는 페이지가 없습니다.',
    unavailable: '현재 검색을 사용할 수 없습니다.',
    result_count_one: '{n}개의 결과',
    result_count_other: '{n}개의 결과',
  },
  versions: {
    label: '버전',
    index_tag_title: '해당 버전에는 이 페이지가 없습니다',
    index_tag: '인덱스',
    default_tag: '기본값',
  },
  docs_sections: {
    start: '시작하기',
    using: 'DBFlux 사용하기',
    configure: '설정',
    integrate: '통합',
    reference: '참조',
    drivers: '드라이버 참조',
    contribute: '기여하기',
  },
  docs_tree: {
    search_cta: '문서 검색',
    rail_toggle: '문서 메뉴',
    on_this_page: '이 페이지에서',
    crumb_docs: '문서',
    crumb_overview: '개요',
    edit_page: '이 페이지 편집',
    report_issue: '이슈 보고',
    not_translated: '이 페이지는 아직 번역되지 않았습니다. 영문 버전이 표시됩니다.',
    view_in_english: '영문으로 보기',
  },
  docs_index: {
    title: '문서',
    intro:
      '여기의 모든 페이지는 리포지토리의 <code>docs/</code> 디렉터리에 있는 마크다운에서 렌더링됩니다. 따라서 동작 변경과 이를 설명하는 문단이 같은 커밋에 함께 반영됩니다.',
    unfiled_title: '아직 분류되지 않음',
    unfiled_body:
      '이 페이지들은 이 버전에 존재하지만 <code>src/data/nav.ts</code>에 선언된 읽기 순서에는 포함되지 않습니다.',
  },
  landing: {
    title: '실행하는 모든 데이터베이스를 하나의 키보드 중심 창에서.',
    lede: '확장 가능한 키보드 우선 데이터 플랫폼. 12개의 내장 드라이버, 드라이버 중립적인 코어, 그리고 그 외 필요한 것은 RPC 드라이버 프로토콜로 지원합니다.',
    download_linux: 'Linux용 다운로드',
    download_macos: 'macOS용 다운로드',
    download_windows: 'Windows용 다운로드',
    view_source: '소스 보기',
    platforms_meta: 'Linux · macOS · Windows — MIT 또는 Apache-2.0',
    hero_caption: 'Main server — acta.documents',
    hero_alt:
      '연결 사이드바와 선택된 행의 세부 정보를 함께 표시하는 DBFlux의 acta.documents 쿼리 결과 화면.',
    drivers_eyebrow: '내장 드라이버',
    drivers_link: '기능 매트릭스 →',
    drivers_note:
      '관계형, 문서, 키-값, 시계열, 개체 저장소가 하나의 결과 그리드, 하나의 차트 엔진, 하나의 감사 로그를 공유합니다. 외부 드라이버는 포크 없이 RPC 프로토콜로 등록됩니다.',
    features_eyebrow: '주요 기능',
    feature: {
      editor: {
        title: '방언 인식 편집기',
        body: '자동 완성, 검증, 위험한 문장 감지는 공유된 추측이 아니라 드라이버에서 제공됩니다. WHERE 없는 DELETE는 실행되기 전에 잡아냅니다.',
      },
      grid: {
        title: '편집 가능한 결과 그리드',
        body: '결과가 하나의 테이블에 깔끔하게 대응될 때 셀을 그 자리에서 편집하고, 키셋 방식으로 수백만 행을 넘겨 보며, 선택한 범위를 네이티브 쿼리로 복사합니다.',
      },
      charts: {
        title: '차트와 대시보드',
        body: '어떤 결과든 차트로 만들어 저장하고, 같은 연결의 인스턴스 지표와 함께 대시보드에 고정합니다.',
      },
      hooks: {
        title: '연결 훅',
        body: '연결과 연결 끊기 전후로 명령, 스크립트 또는 인프로세스 Lua를 실행하고, 작업 패널에서 실시간 출력을 보며, 실패 정책은 직접 선택합니다.',
      },
      reach: {
        title: '어디에든 연결',
        body: 'SSH 터널, HTTP 프록시, AWS SSO가 일급으로 지원됩니다. 비밀은 OS 키링에 저장되며 프로필 파일에는 절대 저장되지 않습니다.',
      },
      audit: {
        title: '기본적으로 감사 가능',
        body: '쿼리, 훅, 스크립트, MCP 호출이 모두 같은 이벤트 로그에 기록되며, 마스킹과 보존 기간을 직접 제어합니다.',
      },
    },
    keyboard_eyebrow: '키보드 우선',
    keyboard_title: '마우스는 선택 사항이지 전제가 아닙니다.',
    keyboard_body:
      '모든 화면에는 단축키와 명령 팔레트 항목이 있습니다: 연결 열기, 문 실행, 테이블로 이동, 결과를 차트로 전환. 빈 상태는 첫날 필요한 네 가지를 알려줍니다.',
    keyboard_link: '전체 키보드 참조 →',
    shortcut: {
      new_query: '새 쿼리',
      command_palette: '명령 팔레트',
      open_script: '디스크에서 스크립트 열기',
      new_connection: '새 연결',
    },
    governance_eyebrow: '거버넌스',
    governance_title: 'AI 클라이언트에게는 데이터베이스가 아닌 연결을 제공하세요.',
    governance_body:
      'MCP 서버는 모든 작업(메타데이터, 읽기, 쓰기, 파괴적, 관리)을 분류하고 정책 엔진이 역할과 연결별로 판단합니다. 쓰기와 파괴적 호출은 사람의 승인 뒤에 둘 수 있습니다.',
    audit_eyebrow: '감사',
    audit_title: '모든 쿼리, 훅, 도구 호출이 기록에 남습니다.',
    audit_body:
      '이벤트는 카테고리, 심각도, 행위자, 결과와 함께 로컬 SQLite 로그에 기록됩니다. 쿼리 텍스트는 원문 대신 지문으로 저장되고, 민감한 값은 마스킹되며, 전체 로그는 JSON 또는 CSV로 내보낼 수 있습니다.',
    docs_eyebrow: '문서',
    docs_link: '모든 가이드 →',
    doc_card: {
      usage: {
        title: '사용 가이드',
        body: '첫 실행, 연결 만들기, 쿼리 실행, 결과 탐색, 차트 만들기, 내보내기.',
      },
      connecting: {
        title: '연결',
        body: '단순한 호스트와 포트가 아닌 모든 것을 위한 SSH 터널, 프록시, AWS SSO, 값 소스.',
      },
      mcp: {
        title: 'AI + MCP',
        body: 'AI 클라이언트를 DBFlux에 연결한 뒤, 정해진 경계 안에 머물도록 역할, 정책, 승인을 설정합니다.',
      },
    },
  },
  install: {
    all_downloads: '모든 다운로드 →',
    copy: '복사',
    copied: '복사됨',
    copy_fallback: 'ctrl+c 누르기',
    hint: {
      tarball:
        'sudo 없이 설치하고 싶으신가요? 홈 디렉터리 아래에 설치하려면 -s -- --prefix ~/.local을 추가하세요.',
      aur: '어떤 AUR 헬퍼든 작동합니다. yay -S dbflux와 동일합니다.',
      deb: 'ARM 머신에서는 amd64를 arm64로 바꾸세요. .rpm도 dnf로 같은 방식으로 설치됩니다.',
      appimage: '완전히 휴대 가능합니다. 홈 디렉터리 밖에는 아무것도 기록하지 않습니다.',
      nix: '기본 패키지는 미리 빌드된 바이너리입니다. 소스에서 빌드하려면 #dbflux-source를 사용하세요.',
      dmg: '빌드는 Apple 개발자 인증서로 서명되지 않았습니다. 대화 상자를 건너뛰려면: xattr -cr /Applications/DBFlux.app. macOS 11 Big Sur 이상이 필요합니다.',
      installer:
        '실행 파일은 Windows 코드 서명 인증서로 서명되지 않았습니다. x86_64에서는 Windows 10 이상이 필요하며, ARM64는 아직 지원되지 않습니다.',
      portable: '아무것도 설치되지 않고, 압축을 푼 폴더 밖에는 아무것도 기록되지 않습니다.',
    },
    steps: {
      dmg: [
        'Apple Silicon용 dbflux-macos-arm64.dmg 또는 Intel용 dbflux-macos-amd64.dmg를 다운로드하세요.',
        'DMG를 열어 DBFlux를 응용 프로그램 폴더로 드래그하세요.',
        '"확인되지 않은 개발자" 경고가 표시되면 시스템 설정 → 개인정보 보호 및 보안으로 이동한 뒤 그래도 열기를 클릭하세요.',
      ],
      installer: [
        'dbflux-windows-amd64-setup.exe를 다운로드하세요.',
        '실행한 뒤 마법사를 따르세요.',
        'SmartScreen이 경고하면 자세한 정보 → 실행을 선택하세요.',
      ],
      portable: [
        'dbflux-windows-amd64.zip을 다운로드하세요.',
        '원하는 위치에 압축을 푸세요.',
        'dbflux.exe를 실행하세요.',
      ],
    },
  },
  about: {
    page_title: 'DBFlux 소개',
    page_description: 'DBFlux가 존재하는 이유, 그 배후의 원칙, 그리고 코드베이스가 구성되는 방식.',
    h1: 'DBFlux가 존재하는 이유',
    intro_p1:
      '모든 데이터베이스 클라이언트는 결국 어느 한쪽을 고르라고 요구합니다: 하나의 엔진만 말하는 빠른 네이티브 클라이언트, 아니면 모든 엔진을 말하지만 기다리게 하는 범용 클라이언트. DBFlux는 세 번째 선택지를 택합니다 — 드라이버 중립적인 하나의 코어, 그 코어에 연결되는 드라이버, 그리고 어느 드라이버의 이름도 배우지 않는 UI.',
    intro_p2:
      '이 제약은 스타일 가이드가 아니라 코드에서 강제됩니다. 인터페이스는 기능 플래그와 메타데이터를 통해 적응하므로, 문서 저장소에는 문서 뷰가, 시계열 저장소에는 범위 선택기가 드라이버 이름을 검사하는 분기 하나 없이 제공됩니다. 데이터베이스를 추가한다는 것은 앱을 패치하는 것이 아니라 드라이버를 작성하는 것입니다.',
    intro_p3:
      '장기 목표는 README에 분명히 적혀 있습니다: 작업하는 모든 데이터베이스를 위한 완전한 오픈 소스 클라이언트 하나. Rust와 GPUI는 그것이 갈아타기에 충분히 빠르게 유지되는 방법입니다.',
    principles_eyebrow: '원칙',
    principle: {
      p01: {
        title: '포인터보다 키보드',
        body: '동작이 존재한다면 단축키와 명령 팔레트 항목이 있습니다. 마우스는 대체 수단이며 어떤 워크플로도 마우스에 의존하지 않습니다.',
      },
      p02: {
        title: 'UI는 드라이버의 이름을 모릅니다',
        body: '카테고리, 쿼리 언어, 기능 플래그가 무엇을 렌더링할지 결정합니다. 새 동작이 필요한 드라이버는 인터페이스에 특수 사례를 추가하는 대신 코어에 연결 지점을 추가합니다.',
      },
      p03: {
        title: '장식보다 밀도',
        body: '직각 모서리, 가는 테두리, 하나의 강조색, 전체적으로 고정폭 글꼴. 화면 공간은 데이터의 것입니다.',
      },
      p04: {
        title: '기록 없이 실행되는 것은 없습니다',
        body: '쿼리, 훅, 스크립트, AI 도구 호출이 모두 같은 감사 로그에 기록됩니다. 기본적으로 마스킹되고 사용자만 볼 수 있으며, 절대 이 머신 밖으로 나가지 않습니다.',
      },
    },
    layers_eyebrow: '구성 방식',
    layer: {
      ui: {
        detail: '6개의 크레이트, 드라이버 종속성 없음, 드라이버별 기능 플래그 없음.',
      },
      app: {
        detail: '드라이버를 등록하고, RPC 서비스를 해석하며, 연결 상태를 소유합니다.',
      },
      core: {
        detail:
          'DbDriver, Connection, capabilities, metadata, language services, query generators.',
      },
      drivers: {
        detail:
          '12개가 Rust 크레이트로 내장되어 있고, 그 외는 RPC 드라이버 프로토콜을 통해 연결합니다.',
      },
    },
    muted_links: {
      prefix: '전체 크레이트 맵과 크레이트 간 흐름은 ',
      architecture: '아키텍처 가이드',
      middle: '에 있습니다. 드라이버를 작성하고 싶다면 ',
      driver_authoring: '드라이버 작성 가이드',
      suffix: '부터 시작하세요.',
    },
    maintainer_title: '메인테이너',
    maintainer_body:
      'Ignacio Perez, Rust와 C로 작업하는 백엔드 및 시스템 개발자. DBFlux는 그의 프로젝트이며, 커밋의 대다수 역시 그렇습니다.',
    contribute_title: '기여',
    contribute_body:
      '이슈, 드라이버, 문서 모두 환영합니다. 기여 가이드는 풀 리퀘스트가 리뷰 전에 통과해야 하는 검사를 다룹니다.',
    contribute_link: '기여 가이드 읽기 →',
  },
  notfound: {
    title: '그 페이지는 존재하지 않습니다.',
    lede: '이름이 바뀌었거나, 지금 읽고 있는 버전이 아닌 다른 DBFlux 버전에 속한 페이지일 수 있습니다.',
    docs_button: '문서',
    home_button: '홈',
    versions_label: '버전별 문서:',
  },
  banner: {
    skip_link: '본문으로 건너뛰기',
  },
};
