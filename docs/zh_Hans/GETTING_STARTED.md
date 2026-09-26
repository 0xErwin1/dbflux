# 快速开始

本页带你从全新安装走到第一个查询结果。如果尚未安装 DBFlux，请先阅读[安装](INSTALL.md)。

DBFlux 以键盘操作为先。几乎所有操作都同时提供鼠标入口和键盘快捷键。这些页面列出的是应用程序默认的键位；可在 **设置 → 键盘快捷键** 中查看和修改当前生效的键位映射（参见[设置](SETTINGS.md#键盘快捷键)）。全部默认键位参见[键盘快捷键](KEYBOARD.md)。

## 首次启动

启动时，DBFlux 会恢复上一次的会话（打开的标签页）。全新安装时没有可恢复的内容，因此焦点默认落在侧边栏。

## 创建连接

按 `Ctrl+Shift+N`（macOS 上为 `Cmd+Shift+N`）打开连接管理器，选择驱动，填写其表单并连接。随后该连接的 Schema 会出现在侧边栏中。[连接到数据库](CONNECTIONS.md)介绍了打开连接管理器的其他方式、驱动选择器、访问标签页（SSH、代理、托管访问），以及连接失败时会发生什么。

## 执行第一个查询

使用 `Ctrl+n`（macOS 上为 `Cmd+n`）新建查询标签页，用当前活动连接的查询语言输入查询，然后按 `Ctrl+Enter`（`Cmd+Enter`）执行。结果会渲染在文档内的结果标签页中。

## 下一步

- [连接到数据库](CONNECTIONS.md) — 连接管理器、驱动、SSH 隧道、代理、AWS SSO 与取值来源。
- [浏览 Schema](SCHEMA_BROWSER.md) — 侧边栏、Schema 树、例程与 Schema 关系图。
- [执行查询](EDITOR.md) — 查询标签页、执行、脚本、危险查询确认与查询历史。
- [可视化查询构建器](QUERY_BUILDER.md) — 无需编写 SQL 即可构建 SELECT、UPDATE 与 DELETE。
- [处理结果](RESULTS.md) — 数据网格、记录视图、筛选、编辑与导出。
- [键值视图](KEY_VALUE.md) — 键、值、过期时间与命令控制台。
- [文档集合](DOCUMENTS.md) — 文档的表格、树与 JSON 视图。
- [图表](CHARTS.md)与[仪表盘](DASHBOARDS.md) — 为结果绘制图表并构建仪表盘。
- [键盘快捷键](KEYBOARD.md) — 全部默认键位，包括 Vim 模式。
- [设置](SETTINGS.md) — 每个设置项与连接 Hooks。
