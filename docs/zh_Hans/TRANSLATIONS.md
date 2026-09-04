# 贡献翻译

DBFlux 的翻译分布在三处，各自的贡献方式不同：应用程序界面通过 [Hosted Weblate](https://hosted.weblate.org/engage/dbflux/) 贡献，文档通过常规的拉取请求贡献，网站框架（导航、落地页、页脚等）则通过少量代码改动贡献。我们始终欢迎部分翻译——任何一处尚未翻译的内容都会回退显示英文，因此一门语言无需全部完成即可发布。

## 快速入口

| 要翻译的内容 | 所在位置 | 贡献方式 |
|---|---|---|
| 应用程序界面（菜单、对话框、设置） | `crates/dbflux_i18n/locales/<code>.yml` | 在 [Weblate](https://hosted.weblate.org/engage/dbflux/) 上翻译——无需在 DBFlux 项目注册账号，也无需使用 git |
| 文档（本站的指南与驱动程序页面） | `docs/<locale-dir>/` | 提交拉取请求，新增 Markdown 文件 |
| 网站框架（导航、落地页、页脚） | `web/src/i18n/<locale>.ts` | 提交拉取请求；如果你不写 TypeScript，也可提交 issue |

<a href="https://hosted.weblate.org/engage/dbflux/"><img src="https://hosted.weblate.org/widget/dbflux/multi-auto.svg" alt="翻译状态"></a>

## 应用程序界面——Weblate

应用程序内的字符串按语言分别存放在 `crates/dbflux_i18n/locales/` 下的 YAML 翻译文件中，每种语言一个文件。[Hosted Weblate](https://hosted.weblate.org/engage/dbflux/) 通过网页界面编辑这些文件，并将结果以拉取请求的形式送回本仓库，因此在那里翻译完全不需要搭建开发环境。

如果你更习惯使用 git，直接针对 YAML 文件提交拉取请求也可以。无论采用哪种方式：

- 英文（`en.yml`）是源语言，也是回退语言。你的语言中缺少的键在运行时会直接显示英文。
- 每个翻译文件都必须定义非空的 `language.native_name`——即该语言在其自身语言中的名称（例如 `Español`）。有一项契约测试会对此强制校验。
- 新增一门语言就是新增一个 `<code>.yml` 文件。应用程序在构建时自动发现这些翻译文件，因此无需修改任何 Rust 代码——该语言会自动出现在“设置”中。

### 术语

拿不准时，对于已成惯例的数据库与产品术语，应保留英文原文而不翻译：**Schema**、**MCP**、驱动程序名称、SQL 关键字。一个用户在别处从未见过的译名，反而比他们已经熟悉的英文原词更难理解。

## 文档——拉取请求

站点直接渲染仓库自身的 Markdown，因此翻译一个页面就是在该放的位置新增一个文件。每种语言在 `docs/` 下都有一个目录（`docs/es/`、`docs/zh_Hans/`），其结构与英文目录保持一致：

| 英文页面 | 翻译后的文件 |
|---|---|
| `docs/USAGE.md` | `docs/<locale-dir>/USAGE.md` |
| `docs/SETTINGS.md` | `docs/<locale-dir>/SETTINGS.md` |
| 驱动程序 README（`crates/dbflux_driver_postgres/README.md`） | `docs/<locale-dir>/drivers/postgres.md` |
| `ARCHITECTURE.md`、`CONTRIBUTING.md`、`SECURITY.md`、`TRADEMARK.md`、`PRIVACY.md`（仓库根目录） | `docs/<locale-dir>/ARCHITECTURE.md` 等 |

保证站点构建顺利的规则：

- 文件名必须与英文原文完全一致——站点按文件名配对页面。
- 保持与英文页面一致的 Markdown 结构：相同的标题、相同的相对链接、相同的 ```mermaid 围栏。构建时链接会被重写为站点路由，因此它们必须指向与英文页面相同的目标。
- 每个拉取请求翻译一个文件，或几个相关的文件。小的拉取请求评审更快。
- 尚未翻译的页面不成问题：站点会在你的语言 URL 下提供英文正文，并附“尚未翻译”提示，而绝不会返回 404。

英文文档变更时，译文页面会继续呈现上一次的状态——若因行为变化需要更新翻译，欢迎单独提交拉取请求。

## 网站框架——少量代码改动

导航、落地页、页脚以及搜索界面的字符串存放在 `web/src/i18n/` 下的带类型 TypeScript 词典中。为已有语言翻译这些字符串，就是编辑该语言的词典；而为网站**新增**一门语言则需要两处改动：

1. 在 `web/src/i18n/locale-registry.mjs` 中注册该区域设置。`id` 是公开标识——它会成为 URL 前缀（`/es/`、`/zh-Hans/`）以及 HTML 的 `lang`/`hreflang` 取值，因此必须是合法的 [BCP-47](https://en.wikipedia.org/wiki/IETF_language_tag) 标签（用连字符，绝不用下划线）。`docsDirectory` 指定 `docs/` 下的文件夹名称，可以与 `id` 不同。
2. 新增 `web/src/i18n/<locale>.ts`，导出一个 `Dictionary` 对象。该类型是穷尽的：缺少任何键都会导致编译错误，由 `pnpm check` 捕获。

如果你不熟悉 TypeScript，可以提交一个 issue 说明你的语言，由我们来接手代码部分——真正需要母语者投入时间的是翻译文件与文档翻译。

## 检查你的工作

| 改动内容 | 检查方式 |
|---|---|
| 应用程序 YAML 翻译文件 | `cargo test -p dbflux_i18n` |
| 文档 Markdown | 在 GitHub 上查阅——只要在那里渲染正常，站点上也会正常渲染 |
| 网站词典 | `cd web && pnpm check && pnpm build` |

通过 Weblate 的贡献无需做任何上述检查——它生成的拉取请求会像其他请求一样运行完整的 CI。
