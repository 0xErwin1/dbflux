import type { Dictionary } from './index';

export const zh_Hans: Dictionary = {
  nav: {
    features: '功能',
    drivers: '驱动',
    docs: '文档',
    about: '关于',
    github: 'GitHub',
    download: '下载',
    menu: '菜单',
    language: '语言',
  },
  footer: {
    product: '产品',
    features: '功能',
    drivers: '驱动',
    releases: '版本',
    docs: '文档',
    usage: '使用指南',
    connecting: '连接',
    mcp: 'AI + MCP',
    project: '项目',
    about: '关于',
    contributing: '如何贡献',
    source: '源代码',
    trademark: '商标政策',
    privacy: '隐私政策',
    tagline: '完全开源、以键盘为核心的数据库客户端，在开放中构建。',
    license: 'MIT 或 Apache-2.0，任你选择。',
  },
  search: {
    placeholder: '搜索文档',
    move: '移动',
    open: '打开',
    close: '关闭',
    no_results: '没有页面与“{query}”匹配。',
    unavailable: '搜索当前不可用。',
    result_count_one: '{n} 条结果',
    result_count_other: '{n} 条结果',
  },
  versions: {
    label: '版本',
    index_tag_title: '此页面在该版本中不存在',
    index_tag: '索引',
    default_tag: '默认',
  },
  docs_sections: {
    start: '从这里开始',
    using: '使用 DBFlux',
    configure: '配置',
    integrate: '集成',
    reference: '参考',
    drivers: '驱动参考',
    contribute: '如何贡献',
  },
  docs_tree: {
    search_cta: '搜索文档',
    rail_toggle: '文档菜单',
    on_this_page: '本页内容',
    crumb_docs: '文档',
    crumb_overview: '总览',
    edit_page: '编辑此页面',
    report_issue: '报告问题',
    not_translated: '此页面尚未翻译，当前显示英文版本。',
    view_in_english: '查看英文版本',
  },
  docs_index: {
    title: '文档',
    intro:
      '这里的每个页面都渲染自仓库 <code>docs/</code> 目录中的 markdown，因此行为变更与描述它的段落会在同一个提交中发布。',
    unfiled_title: '尚未归类',
    unfiled_body:
      '这些页面存在于该版本中，但在 <code>src/data/nav.ts</code> 声明的阅读顺序中没有位置。',
  },
  landing: {
    title: '把你使用的每一个数据库，装进一个由键盘驱动的窗口。',
    lede: '一个可扩展、以键盘为核心的数据平台。十二个内置驱动、一个对驱动中立的内核，其余一切需求都可以通过 RPC 驱动协议接入。',
    download_linux: '下载 Linux 版',
    download_macos: '下载 macOS 版',
    download_windows: '下载 Windows 版',
    view_source: '查看源代码',
    platforms_meta: 'Linux · macOS · Windows — MIT 或 Apache-2.0',
    hero_caption: '主服务器 — SELECT * FROM public.transactions (1.5s)',
    hero_alt:
      'DBFlux 打开了一个连接树，展示一台 PostgreSQL 服务器的数据库、schema、例程和实例指标。',
    drivers_eyebrow: '内置驱动',
    drivers_link: '能力矩阵 →',
    drivers_note:
      '关系型、文档型、键值型、时序和对象存储共享同一个结果网格、同一个图表引擎和同一份审计日志。外部驱动通过 RPC 协议注册，无需 fork。',
    features_eyebrow: '你能得到什么',
    feature: {
      editor: {
        title: '感知方言的编辑器',
        body: '补全、校验和危险语句检测来自驱动本身，而不是共同的猜测。不带 WHERE 的 DELETE 会在执行前被捕获。',
      },
      grid: {
        title: '可编辑的结果网格',
        body: '当结果能干净地映射回单张表时可直接编辑单元格，按 keyset 翻阅数百万行，并把任何选区复制为原生查询。',
      },
      charts: {
        title: '图表与仪表盘',
        body: '把任何结果变成图表、保存它，并连同同一连接的实例指标一起固定到仪表盘上。',
      },
      hooks: {
        title: '连接 Hook',
        body: '在连接与断开前后运行命令、脚本或进程内 Lua，实时输出显示在任务面板中，失败策略由你选择。',
      },
      reach: {
        title: '触达一切',
        body: 'SSH 隧道、HTTP 代理和 AWS SSO 都是一等公民。密钥保存在操作系统密钥环里，绝不写入配置文件。',
      },
      audit: {
        title: '默认可审计',
        body: '查询、Hook、脚本和 MCP 调用都写入同一份事件日志，脱敏与保留策略由你掌控。',
      },
    },
    keyboard_eyebrow: '键盘优先',
    keyboard_title: '鼠标是可选的，不是必需的。',
    keyboard_body:
      '每个界面都有键位绑定和命令面板入口：打开连接、执行语句、跳转到表、把结果变成图表。空状态会告诉你第一天需要的四个操作。',
    keyboard_link: '完整键盘参考 →',
    shortcut: {
      new_query: '新建查询',
      command_palette: '命令面板',
      open_script: '从磁盘打开脚本',
      new_connection: '新建连接',
    },
    governance_eyebrow: '治理',
    governance_title: '给 AI 客户端一个连接，而不是你的数据库。',
    governance_body:
      'MCP 服务器为每个操作分类 — 元数据、读取、写入、破坏性、管理 — 并由策略引擎按角色和按连接做出决定。写入与破坏性调用可以置于人工审批之后。',
    audit_eyebrow: '审计',
    audit_title: '每一条查询、Hook 和工具调用，都有据可查。',
    audit_body:
      '事件落入本地 SQLite 日志，带有类别、严重级别、执行者和结果。查询文本以指纹形式保存而非明文，敏感值会被脱敏，整份日志可导出为 JSON 或 CSV。',
    docs_eyebrow: '文档',
    docs_link: '全部指南 →',
    doc_card: {
      usage: {
        title: '使用指南',
        body: '首次启动、创建连接、执行查询、浏览结果、绘图与导出。',
      },
      connecting: {
        title: '连接',
        body: 'SSH 隧道、代理、AWS SSO，以及一切超出普通主机和端口之外的值来源。',
      },
      mcp: {
        title: 'AI + MCP',
        body: '把 AI 客户端接入 DBFlux，然后设置让它不越界的角色、策略与审批。',
      },
    },
  },
  install: {
    all_downloads: '全部下载 →',
    copy: '复制',
    copied: '已复制',
    copy_fallback: '按 ctrl+c',
    hint: {
      tarball: '不想用 sudo？追加 -s -- --prefix ~/.local 即可安装到你的用户目录。',
      aur: '任何 AUR 助手都可以。yay -S dbflux 与之等价。',
      deb: '在 ARM 机器上把 amd64 换成 arm64。.rpm 用 dnf 以同样的方式安装。',
      appimage: '完全便携。不会在你的用户目录之外写入任何内容。',
      nix: '默认包是预编译二进制。要自行从源码构建请使用 #dbflux-source。',
      dmg: '该构建未使用 Apple 开发者证书签名。跳过提示的方法：xattr -cr /Applications/DBFlux.app。需要 macOS 11 Big Sur 或更高版本。',
      installer:
        '可执行文件未使用 Windows 代码签名证书签名。需要 x86_64 上的 Windows 10 或更高版本；暂不支持 ARM64。',
      portable: '不安装任何内容，也不会在你解压的文件夹之外写入任何东西。',
    },
    steps: {
      dmg: [
        'Apple Silicon 下载 dbflux-macos-arm64.dmg，Intel 下载 dbflux-macos-amd64.dmg。',
        '打开 DMG 并把 DBFlux 拖入"应用程序"。',
        '在"未验证开发者"警告处，进入 系统设置 → 隐私与安全性，点击 仍要打开。',
      ],
      installer: [
        '下载 dbflux-windows-amd64-setup.exe。',
        '运行并按向导完成安装。',
        '如果 SmartScreen 提示，选择 更多信息 → 仍要运行。',
      ],
      portable: ['下载 dbflux-windows-amd64.zip。', '解压到任意位置。', '运行 dbflux.exe。'],
    },
  },
  about: {
    page_title: '关于 DBFlux',
    page_description: 'DBFlux 为什么存在、背后的原则，以及代码库是如何组织的。',
    h1: 'DBFlux 为什么存在',
    intro_p1:
      '每一个数据库客户端最终都会让你选边站：要么是只说一种引擎方言的快速原生客户端，要么是什么都会说却让你等待的通用客户端。DBFlux 选择第三条路 — 一个对驱动中立的内核、插入其中的驱动，以及一个永远不需要知道任何驱动名字的界面。',
    intro_p2:
      '这条约束写在代码里，而不是风格指南里。界面通过能力标志和元数据自适应，所以文档存储得到文档视图、时序存储得到范围选择器，而无需对驱动名做任何分支判断。添加一个数据库就是编写一个驱动，而不是给应用打补丁。',
    intro_p3:
      '长期目标在 README 中说得直白：为你使用的每一个数据库提供一个完全开源的客户端。Rust 和 GPUI 是它保持足够快、值得你切换过来的方式。',
    principles_eyebrow: '原则',
    principle: {
      p01: {
        title: '键盘先于指针',
        body: '只要一个操作存在，它就有键位绑定和命令面板入口。鼠标是后备，没有任何工作流依赖它。',
      },
      p02: {
        title: '界面永远不知道驱动的名字',
        body: '类别、查询语言和能力标志决定渲染什么。需要新行为的驱动为内核添加接缝，而不是为界面添加特例。',
      },
      p03: {
        title: '致密胜过装饰',
        body: '直角、发丝边框、一种强调色、全局等宽字体。屏幕空间属于你的数据。',
      },
      p04: {
        title: '没有无记录的执行',
        body: '查询、Hook、脚本和 AI 工具调用全部写入同一份审计日志，默认脱敏、只属于你 — 它永不离开这台机器。',
      },
    },
    layers_eyebrow: '它是如何组织的',
    layer: {
      ui: {
        detail: '六个 crate，零驱动依赖，零按驱动划分的 feature 标志。',
      },
      app: {
        detail: '注册驱动、解析 RPC 服务、持有连接状态。',
      },
      core: {
        detail:
          'DbDriver、Connection、capabilities、metadata、language services、query generators。',
      },
      drivers: {
        detail: '十二个以 Rust crate 形式内置；其余一切通过 RPC 驱动协议接入。',
      },
    },
    muted_links: {
      prefix: '完整的 crate 地图与跨 crate 流程见',
      architecture: '架构指南',
      middle: '。若想编写驱动，请从',
      driver_authoring: '驱动编写指南',
      suffix: '开始。',
    },
    maintainer_title: '维护者',
    maintainer_body:
      'Ignacio Perez，一位使用 Rust 和 C 工作的后端与系统开发者。DBFlux 属于他，其中绝大多数提交也是他完成的。',
    contribute_title: '贡献',
    contribute_body:
      '欢迎 issue、驱动和文档。贡献指南列出了一个 pull request 在评审前必须通过的检查。',
    contribute_link: '阅读贡献指南 →',
  },
  notfound: {
    title: '该页面不存在。',
    lede: '它可能已被重命名，也可能属于你所阅读版本之外的另一个 DBFlux 版本。',
    docs_button: '文档',
    home_button: '首页',
    versions_label: '按版本浏览文档：',
  },
  banner: {
    skip_link: '跳到主要内容',
  },
};
