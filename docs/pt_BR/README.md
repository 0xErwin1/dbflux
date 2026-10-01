# DBFlux

[English](../../README.md) · [Español](../es/README.md) · [한국어](../ko/README.md) · [简体中文](../zh_Hans/README.md) · **Português (Brasil)**

Uma plataforma de dados extensível e voltada ao teclado, entregue como um cliente desktop em Rust + GPUI.

**[dbflux.dev](https://dbflux.dev)** &middot; [Documentação](https://docs.dbflux.dev/) &middot; [Instalar](https://docs.dbflux.dev/install/)

## Visão geral

O DBFlux é um cliente desktop de código aberto com drivers integrados para bancos de dados relacionais e não relacionais. Seus contratos centrais são neutros em relação ao driver, e drivers externos podem se integrar por RPC.

O cliente prioriza desempenho, uma UX limpa e fluxos de trabalho voltados ao teclado. O objetivo de longo prazo é ter um único cliente totalmente open source para todos os bancos de dados com que você trabalha.

![DBFlux](../../resources/dbflux.png)

## Documentação

Tudo abaixo é publicado em **[docs.dbflux.dev](https://docs.dbflux.dev/)**, renderizado a partir
destes mesmos arquivos, com busca e seletor de versão. Os links aqui apontam para a fonte; leia no
site se preferir. A documentação ainda está em inglês: os links levam às páginas originais.

Escolha o caminho que corresponde ao que você quer fazer.

### Comece aqui

| Objetivo                                    | Guia                                                                                                                                                                                                            |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Criar uma conexão                           | Comece pelo [Primeiros passos](../GETTING_STARTED.md). Para túneis SSH, proxies, AWS SSO e value sources, use [Conectando — configuração avançada](../CONNECTIONS.md).                                           |
| Executar consultas e seguir fluxos comuns   | Siga o [Guia de uso](../USAGE.md) para consultar, navegar pelos resultados, criar gráficos, exportar e usar a navegação por teclado.                                                                           |
| Ver eventos de auditoria                    | Abra o visualizador de auditoria com o [guia do visualizador de auditoria](../AUDIT.md#audit-viewer).                                                                                                          |
| Usar o MCP                                  | Siga o [Guia de integração de IA + MCP](../MCP_AI_INTEGRATION.md).                                                                                                                                             |
| Conferir o suporte e as limitações de drivers | Use a [Visão geral dos drivers](../DRIVERS.md), a visão canônica de recursos e limitações.                                                                                                                   |

### Mais guias do usuário

- [Configurações e hooks](../SETTINGS.md) — configurações, hooks de conexão e perfis de acesso
- [Dados e privacidade](../../PRIVACY.md#your-data-on-this-machine) — armazenamento de dados e segredos, backup e redefinição
- [Scripts em Lua](../LUA.md) — o runtime Lua embutido para hooks

### Contribuidores

- [Contribuindo](../../CONTRIBUTING.md) — configuração, verificações e fluxo de contribuição
- [Conceitos-chave](../CONCEPTS.md) — o modelo mental resumido de contratos e limites de subsistemas
- [Criação de drivers](../DRIVER_AUTHORING.md) — escolha e implemente um driver Rust integrado ou um driver externo via RPC
- [Arquitetura](../../ARCHITECTURE.md) — o mapa canônico de arquitetura e crates, incluindo limites entre crates e fluxos entre crates

### Traduções

O DBFlux é traduzido no [Hosted Weblate](https://hosted.weblate.org/engage/dbflux/).
Os catálogos ficam em `crates/dbflux_i18n/locales/`, um arquivo YAML por idioma, e
as atualizações de tradução chegam como pull requests vindos do Weblate.
[Contribuindo com traduções](../TRANSLATIONS.md) cobre todas as superfícies traduzíveis:
a interface do aplicativo, a documentação e o site.

<a href="https://hosted.weblate.org/engage/dbflux/"><img src="https://hosted.weblate.org/widget/dbflux/multi-auto.svg" alt="Translation status"></a>

### Referência

- [Gráficos](../CHARTS.md) — tipos de gráfico, tipos de coluna e detecção automática de eixos
- [Dashboards](../DASHBOARDS.md) — dashboards, gráficos salvos, métricas de instância e inspetores
- [Auditoria](../AUDIT.md) — esquema de eventos de auditoria e redação de dados sensíveis
- [Protocolo RPC de drivers](../DRIVER_RPC_PROTOCOL.md)
- [Configuração de serviços RPC](../RPC_SERVICES_CONFIG.md)
- [Processo de release](../RELEASE.md)
- [Estilo de código](../../CODE_STYLE.md)
- [Instruções para agentes](../../AGENTS.md)
- [Instruções para o Claude](../../CLAUDE.md)

## Instalação

```bash
# Linux — instalar em /usr/local
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash
```

Os pacotes para cada plataforma — tarball, AUR, `.deb`, `.rpm`, AppImage, Nix, DMG do macOS
e instalador do Windows — estão na página de [Releases](https://github.com/0xErwin1/dbflux/releases).
O guia completo, incluindo os passos do Gatekeeper e do SmartScreen para as versões
sem assinatura do macOS e do Windows, está em [Instalando o DBFlux](../INSTALL.md).

## Funcionalidades

### Suporte a bancos de dados

- **PostgreSQL** com modos SSL/TLS (Disable, Prefer, Require)
- **Amazon Redshift** com SQL somente leitura sobre o protocolo de rede do PostgreSQL, túnel SSH e certificados TLS/cliente
- **MySQL** / MariaDB
- **SQLite** para arquivos de banco de dados locais
- **Microsoft SQL Server** (TDS) com TLS, roteamento de instância nomeada via SQL Browser e introspecção de múltiplos schemas
- **MongoDB** com navegação de coleções, CRUD de documentos e geração de consultas de shell
- **Redis** com navegação de chaves de todos os tipos (String, Hash, List, Set, Sorted Set, Stream)
- **DynamoDB** com navegação de tabelas, CRUD de itens e autenticação AWS
- **InfluxDB** v1 e v2 (InfluxQL na v1, InfluxQL + Flux na v2)
- **ClickHouse** e ClickHouse Cloud sobre HTTP(S), com descoberta de bancos/tabelas, SELECTs visuais e execução explícita de SQL bruto
- **TursoDB** e libSQL (`sqld`) sobre HTTP, com descoberta de schema, CRUD tipado e transações interativas por aba do editor
- **CloudWatch Logs** com navegação de grupos/streams de log e streaming de eventos
- **Amazon S3** com navegação de buckets, pré-visualização/edição de objetos, CRUD completo e URLs pré-assinadas, incluindo endpoints compatíveis com S3 (Cloudflare R2, MinIO)
- **Drivers externos via RPC** (registre drivers fora de processo pelo [Protocolo RPC de drivers](../DRIVER_RPC_PROTOCOL.md))

Veja [docs/DRIVERS.md](../DRIVERS.md) para a matriz completa de recursos e as limitações por driver.

### Interface do usuário

- Workspace baseado em documentos com várias abas de resultados (como DBeaver/VS Code)
- Barra lateral recolhível e redimensionável com o comando ToggleSidebar (Ctrl+B)
- Navegador em árvore de schemas com carregamento sob demanda para bancos grandes
- Metadados em nível de schema: índices, chaves estrangeiras, restrições, tipos personalizados (PostgreSQL)
- Pasta de stored procedures / rotinas por schema (drivers que as expõem)
- Editor SQL com várias abas, realce de sintaxe e execução de múltiplas instruções (um conjunto de resultados por instrução, quando o driver oferece suporte)
- Tabela de dados virtualizada com redimensionamento de colunas, rolagem horizontal e ordenação
- Navegador de tabelas com filtros WHERE, LIMIT personalizado e paginação
- Painel lateral do workspace para detalhes de linha/documento
- Menu de contexto "Copy as Query" para copiar INSERT/UPDATE/DELETE como SQL, shell do MongoDB ou comandos do Redis
- Janela de pré-visualização de consulta com realce de sintaxe específico da linguagem
- Paleta de comandos com busca aproximada
- Sistema próprio de notificações toast com fechamento automático
- Painel de tarefas em segundo plano
- Restauração de sessão: as abas abertas são restauradas na inicialização, e um rascunho recuperado que difere do arquivo nunca o sobrescreve

### Construtor visual de consultas

- Construtor de SELECT no painel lateral direito: projeção, joins, uma árvore aninhada de predicados WHERE, ORDER BY e LIMIT/OFFSET, com pré-visualização do SQL parametrizado em tempo real
- GROUP BY com agregações (COUNT, SUM, AVG, MIN, MAX) e HAVING
- Construtor visual de UPDATE / DELETE com políticas de alteração (somente leitura / exige aprovação) e execução em blocos, cancelável
- Autocompletar com reconhecimento de schema nos campos do construtor e no filtro WHERE dos resultados
- Filtros relacionais na barra de filtros dos resultados por caminhos de chave estrangeira com pontos (p. ex. `created_by.email LIKE '%@acme.com'`)
- Edição inline de célula e exclusão de linha em resultados gerados pelo construtor quando eles correspondem 1:1 a uma única tabela
- Consultas visuais salvas por conexão
- Somente drivers SQL (SQLite, PostgreSQL, MySQL/MariaDB, SQL Server); neutro em relação ao driver por construção

### Gráficos e visualização

- Crie gráficos de qualquer resultado de consulta ou coleção: Line, Bar, Scatter, Area, Stacked Bar e Pie
- Detecção automática de eixos a partir dos tipos de coluna (eixo X de timestamp, séries Y numéricas) — sem heurísticas por driver
- Gráficos salvos que reabrem como uma aba de documento própria
- Dashboards: organize gráficos salvos, divisores e painéis de inspetor em uma grade de 12 colunas com um intervalo de tempo compartilhado
- Visão geral da instância, somente leitura, por conexão — métricas de servidor ao vivo e inspetores tabulares, com "Save as editable"; PostgreSQL, MySQL/MariaDB, MongoDB, Redis e SQL Server incluem catálogos de instância
- Navegue e importe dashboards de provedores externos (CloudWatch)
- Veja [docs/CHARTS.md](../CHARTS.md) e [docs/DASHBOARDS.md](../DASHBOARDS.md) para mais detalhes

### Conectividade e acesso

- Túneis SSH com autenticação por chave, senha e agente; perfis de túnel SSH reutilizáveis
- Túneis de proxy SOCKS5 / HTTP CONNECT com perfis de proxy reutilizáveis
- Provedores de acesso gerenciado (AWS SSM) para conectar sem expor portas
- Perfis de autenticação orientados por provedor (p. ex. AWS SSO/shared/static), com importação de `~/.aws/config`
- Hooks de conexão em PreConnect/PostConnect/PreDisconnect/PostDisconnect, executáveis como comando, script ou Lua em processo

### Integração com IA e MCP

- Servidor Model Context Protocol (MCP) integrado (`dbflux mcp`) para clientes de IA
- Camada de governança: classificação de operações, motor de funções/políticas, clientes confiáveis e fluxo de aprovação humana para operações de escrita/destrutivas
- Veja [docs/MCP_AI_INTEGRATION.md](../MCP_AI_INTEGRATION.md)

### Auditoria e scripts

- Log de auditoria em SQLite para consultas, conexões, hooks, scripts, MCP, governança e eventos de configuração, com ocultação de dados sensíveis e impressão digital de consultas — veja [docs/AUDIT.md](../AUDIT.md)
- Relato centralizado de erros para o usuário: as falhas aparecem como um toast com um ID de correlação e uma ação "View in Audit", acendem um selo de erro na barra de status e são correlacionadas com a linha de auditoria correspondente
- Scripts Lua, Python e Bash são executados como documentos com saída transmitida ao vivo — veja [docs/LUA.md](../LUA.md)

### Navegação por teclado

- Navegação estilo Vim (`j`/`k`/`h`/`l`) em todo o app
- Atalhos sensíveis ao contexto (Document, Sidebar, BackgroundTasks)
- Foco de documento com navegação interna entre editor e resultados
- Barra de ferramentas dos resultados: `f` para focar, `h`/`l` para navegar, `Enter` para editar/executar, `Esc` para sair
- Alternar a barra lateral com `Ctrl+B`
- Troca de abas (ordem MRU) com `Ctrl+Tab` / `Ctrl+Shift+Tab`

### Gerenciamento de consultas

- Histórico de consultas com data e hora
- Consultas salvas com favoritos
- Busca no histórico e nas consultas salvas

### Exportação

- Exportação baseada no formato do resultado: CSV, JSON (formatado/compacto), Text, Binary (raw/hex/base64)
- O formato de exportação é determinado pelo tipo do resultado (tabela, JSON, texto, binário)

## Desenvolvimento

### Pré-requisitos

No Linux, o linker `mold` é **obrigatório** para builds locais: o
`.cargo/config.toml` do repositório liga o target `x86_64-unknown-linux-gnu` com
`-fuse-ld=mold` para reduzir o tempo de linkedição e o uso de memória nos mais de 60
crates do workspace. O dev shell do Nix o fornece automaticamente; em
configurações sem Nix, instale-o pelo seu gerenciador de pacotes (incluído abaixo). Windows e
macOS usam o linker padrão e não são afetados.

**Ubuntu/Debian:**

```bash
sudo apt install pkg-config libssl-dev libdbus-1-dev libxkbcommon-dev mold
```

**Fedora:**

```bash
sudo dnf install pkg-config openssl-devel dbus-devel libxkbcommon-devel mold
```

**Arch:**

```bash
sudo pacman -S pkg-config openssl dbus libxkbcommon mold
```

**macOS:**

```bash
# Xcode Command Line Tools (obrigatório)
xcode-select --install
```

**Windows:**

```powershell
# Visual Studio Build Tools com o workload de C++ (obrigatório)
# Baixe em: https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

### Compilar

```bash
cargo build -p dbflux --release
```

### Executar

```bash
cargo run -p dbflux
```

### Comandos

```bash
cargo check --workspace                    # Verificação de tipos
python3 scripts/lint.py clippy             # Lint
python3 scripts/lint.py fmt                # Formatação
cargo test --workspace                     # Testes
```

### Testes mais rápidos com nextest

O [`cargo-nextest`](https://nexte.st) é o executor de testes recomendado para este
workspace: ele executa cada teste em seu próprio processo, em um pool global, o que é
bem mais rápido que o `cargo test` em um workspace deste tamanho. O dev shell do Nix
o fornece; caso contrário, instale-o a partir de <https://nexte.st/docs/installation>.

```bash
cargo nextest run --workspace              # testes unitários + de integração
cargo test --doc --workspace               # doctests (o nextest não os executa)
```

Os testes de integração ao vivo (normalmente marcados com `#[ignore]`) usam outra flag no nextest:

```bash
cargo nextest run -p dbflux_driver_sqlite --run-ignored all
```

### Site

O site em `web/` é um build estático do Astro. Ele lê `docs/`, os READMEs dos drivers,
`ARCHITECTURE.md` e `CONTRIBUTING.md` diretamente do git, um conjunto por versão publicada, então editar um
documento é tudo o que é preciso para mudar o que o site mostra.

```bash
cd web
pnpm install
pnpm dev          # servidor local
pnpm build        # saída estática em web/dist
pnpm check        # tipos
pnpm format       # prettier
```

As versões publicadas são declaradas em `web/versions.json`. Cada entrada nomeia uma git ref; a
versão do produto exibida para ela é lida do `Cargo.toml` dessa ref.

`DOCS_MODE` decide onde a documentação é servida: `embedded` (o padrão, tudo em uma
única origem sob `/docs/`), ou `site` e `docs` para uma implantação dividida entre dois hosts. O
desenvolvimento local usa o padrão, então um único comando ainda sobe o site inteiro.

### Dev shell do Nix

Se você usa Nix, pode entrar em um dev shell com todas as dependências:

```bash
# Com flakes
nix develop

# Tradicional
nix-shell
```

## Licença

MIT & Apache-2.0. O nome e o logotipo do DBFlux são cobertos pela [Política de marca](../../TRADEMARK.md), e não pela licença do código.

O DBFlux não coleta dados. Veja a [Política de privacidade](../../PRIVACY.md).

## Histórico de estrelas

[![Histórico de estrelas do DBFlux](https://api.star-history.com/svg?repos=0xErwin1/dbflux&type=Date)](https://star-history.com/#0xErwin1/dbflux)

## Colaboradores

[![Colaboradores do DBFlux](https://contrib.rocks/image?repo=0xErwin1/dbflux)](https://github.com/0xErwin1/dbflux/graphs/contributors)
