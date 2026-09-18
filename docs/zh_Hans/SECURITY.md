# 安全策略

## 报告漏洞

请通过 GitHub Security Advisories 私下报告：

**https://github.com/0xErwin1/dbflux/security/advisories/new**

对于疑似漏洞，请不要提交公开的 issue。报告中如果能提供 DBFlux 版本、所在平台、涉及的驱动程序（如有），以及能复现该问题的最少步骤，会更有价值。

机器可读的联系方式发布在 [`/.well-known/security.txt`](https://dbflux.dev/.well-known/security.txt)。

## 受支持的版本

修复会落在当前的发布分支上。DBFlux 在 `main` 上开发，并为每个次版本切出 `release/vX.Y` 分支；该分支在达到生命周期结束之前，会持续接收从 `main` 拣选的修复，更早的次版本则不会。分支与渠道的运作方式参见[发布流程](docs/RELEASE.md)。

如果你运行的是更早的次版本，那么安全报告的答复将是：升级到当前次版本。

## 已知的设计限制

以下都是已记录在案的行为，而非漏洞。我们欢迎你以设计讨论的形式提出相关报告，但不会将其视为未披露的风险。

- **MCP 认证只代表进程身份。** 出示 `--client-id` 是唯一的认证凭据，因此任何知道该 client id 的本地进程都可以连接。它并非密码学上的保证；在没有额外认证层的情况下，不应把 MCP 服务器暴露到 localhost 之外。参见 [AI + MCP 集成](docs/MCP_AI_INTEGRATION.md)。
- **连接 Hook 与 Lua 脚本执行的是你自己配置的代码。** 它们按设计就以 DBFlux 进程的权限运行 —— 这正是 Hook 的意义所在。参见[设置与 Hooks](docs/SETTINGS.md)与 [Lua 脚本](docs/LUA.md)。
- **审计日志是本地的。** 它记录该机器上发生的事情，任何能读取你数据目录的程序都能读取它。参见[数据与隐私](docs/DATA_AND_PRIVACY.md)。

## 密钥存放在哪里

凭据保存在操作系统密钥环中，绝不会写进连接配置；审计日志保存的是查询文本的指纹，而非文本本身。[数据与隐私](docs/DATA_AND_PRIVACY.md)说明了哪些内容写在哪里，以及如何查看或删除它们。
