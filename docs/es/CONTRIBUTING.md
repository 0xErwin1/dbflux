# Contribuir a DBFlux

Gracias por considerar contribuir. Esta guía explica cómo reportar issues, abrir
pull requests y seguir las convenciones que DBFlux usa para releases y labels.

## Enlaces rápidos

- [Visión general de la arquitectura](ARCHITECTURE.md)
- [Guía de autoría de drivers](docs/DRIVER_AUTHORING.md)
- [Proceso de release y modelo de branching](docs/RELEASE.md)
- [Esquema de eventos de auditoría](docs/AUDIT.md)
- [Protocolo RPC de drivers](docs/DRIVER_RPC_PROTOCOL.md)
- [Scripting con Lua](docs/LUA.md)
- [Integración MCP / IA](docs/MCP_AI_INTEGRATION.md)

## Configuración del proyecto

DBFlux es un workspace de Rust que usa
[GPUI](https://github.com/zed-industries/zed) para la UI. El conjunto completo
de features requiere los feature flags de driver de base de datos:

```bash
cargo check --workspace
cargo build
cargo run
```

En Linux, el linker [`mold`](https://github.com/rui314/mold) es **obligatorio**
para builds locales: `.cargo/config.toml` enlaza el target
`x86_64-unknown-linux-gnu` con `-fuse-ld=mold` para reducir el tiempo de linkeo
y la memoria en todo el workspace. Instálalo con tu gestor de paquetes (p. ej.
`apt install mold`); el dev shell de Nix lo provee automáticamente. Windows y
macOS no se ven afectados.

Antes de abrir un PR ejecuta:

```bash
python3 scripts/lint.py fmt --check
python3 scripts/lint.py clippy
cargo test --workspace
```

`scripts/lint.py` solo cubre los crates propios bajo `crates/` y nunca analiza ni reformatea los crates de terceros bajo `vendor/`.

Los tests también pueden ejecutarse con [`cargo-nextest`](https://nexte.st) (más
rápido en este workspace, provisto por el dev shell de Nix). Ten en cuenta que
nextest no ejecuta doctests:

```bash
cargo nextest run --workspace
cargo test --doc --workspace
```

Hay un dev shell de Nix disponible: `nix develop`.

## Modelo de branching

DBFlux usa **trunk-based development con release branches de vida corta**:

- `main` es la única rama de vida larga. Todo el trabajo apunta a `main`.
- Las ramas `release/vX.Y` se cortan desde `main` solo cuando un minor necesita
  estabilizarse para una release estable. Aceptan únicamente fixes cherry-picked
  desde `main` — sin features nuevas.

Los contribuidores deben **siempre** apuntar sus PRs a `main`. Hacer backport a
una release branch es responsabilidad de los maintainers.

Las reglas completas (tags, bumps de versión, procedimiento de corte, disciplina
del CHANGELOG) viven en [`docs/RELEASE.md`](docs/RELEASE.md).

## Convención de commits

Usa [Conventional Commits](https://www.conventionalcommits.org/) donde encaje
naturalmente:

- `feat(scope): …` — nueva capacidad orientada al usuario
- `fix(scope): …` — corrección de bug
- `refactor(scope): …` — cambio interno sin cambio de comportamiento
- `perf(scope): …` — mejora de rendimiento
- `docs(scope): …` — solo documentación
- `test(scope): …` — solo tests
- `ci(scope): …` — cambios de CI / release workflow
- `chore(scope): …` — plomería del repositorio (deps, tooling, bumps de versión)

El scope es el área afectada: el nombre de un driver (`postgres`, `mongodb`),
`ui`, `mcp`, `audit`, `rpc`, `release`, etc. Mantén el subject bajo 70
caracteres; explica el *por qué* en el body cuando no sea obvio.

## Pull Requests

1. Crea la rama desde `main`. Mantén los PRs enfocados en una sola preocupación.
2. Completa el [template de PR](.github/pull_request_template.md): resumen, qué
   resuelve, cómo se resolvió, evidencia de validación y dónde se probó.
3. Enlaza el issue que cierra con `Resolves #N` en la descripción.
4. Aplica los labels que describen el cambio. Ver [Guía de
   etiquetas](#label-guide) más abajo.
5. Mantén los diffs revisables. Los PRs de más de ~400 líneas cambiadas deben
   dividirse en PRs apilados/encadenados salvo que el maintainer apruebe un
   `size:exception`.
6. CI debe pasar (`tests.yml`, `style.yml`). Vuelve a ejecutar localmente antes
   de hacer push si algo falla.
7. Un cambio de documentación viaja con sus traducciones. Cuando edites una
   página bajo `docs/`, un README de driver o un documento raíz que el sitio
   renderiza (`ARCHITECTURE.md`, `CONTRIBUTING.md`, `SECURITY.md`,
   `TRADEMARK.md`, `PRIVACY.md`), aplica el mismo cambio a cada contraparte
   existente bajo `docs/es/` y `docs/zh_Hans/` en el mismo PR. Una página que
   aún no tiene contraparte no necesita ninguna. Ver
   [Traducciones](TRANSLATIONS.md).


### Los mensajes de commit y el changelog

DBFlux mantiene dos artefactos sincronizados a partir del mismo cambio: el
`CHANGELOG.md` curado de este repositorio, y las release notes de GitHub que
[git-cliff](https://git-cliff.org) genera a partir del historial de git en CI.
Agrega tu entrada `## [Unreleased]` en el mismo commit del cambio; el type del
commit decide qué llevan las release notes generadas.

Reglas sobre qué aparece en las release notes generadas:

| Type                                                        | ¿Aparece en el changelog? |
| ----------------------------------------------------------- | ------------------------- |
| `feat`                                                      | Sí — bajo **Added**       |
| `fix`                                                       | Sí — bajo **Fixed**       |
| `perf`                                                      | Sí — bajo **Changed**     |
| `refactor`, `test`, `ci`, `chore`, `docs`, `style`, `build` | No — solo interno         |
| Cualquier type con scope `(security)` o footer `Security:`  | Sí — bajo **Security**    |

Los breaking changes (`feat!:`, `fix!:`, o un footer `BREAKING CHANGE:`) siempre
se muestran sin importar el type.

**Qué significa esto en la práctica:**

- Los cambios visibles para el usuario **deben** usar `feat`, `fix` o `perf`
  como type, y agregar su bullet bajo `## [Unreleased]` (`### Added`,
  `### Fixed` o `### Changed`). Un commit `chore` o `refactor` es invisible para
  los usuarios.
- Escribe un subject claro e imperativo — es el bullet de las release notes
  generadas, tal cual.
- Escribe el bullet de `[Unreleased]` para un usuario, no para un revisor: es lo
  que dice el changelog del repositorio.
- Si un solo PR contiene cambios internos y visibles al usuario a la vez,
  sepáralos en commits distintos con los types apropiados.
- Fixes de seguridad: usa `fix(security): ...` o añade un trailer `Security:
  ...` para que el cambio caiga bajo la sección Security.

## Cobertura de teclado

Toda acción de DBFlux debe poder alcanzarse con el teclado: un comando asignado en el contexto de teclas de la superficie, o una entrada de un menú que el teclado abre (el menú de acciones del panel, o el menú `m` de una tabla o un rail). Dos comprobaciones impiden que entre una acción que solo funciona con el mouse:

- `python3 scripts/lint.py mouse-down` rechaza los handlers `on_mouse_down` del botón izquierdo que no figuran en `scripts/mouse_down_allowlist.txt`. Las activaciones usan `on_click`.
- Los tests de cobertura de teclado dibujan cada superficie y comparan cada elemento que ejecuta una acción al hacer clic con el registro de cobertura de esa superficie (`dbflux_ui_base::keyboard_coverage`). Un elemento que el registro no lista hace fallar el test, y el fallo indica cómo corregirlo.

Cuando agregues un elemento interactivo:

1. Dale un `.id(...)` estable y actívalo con `.on_click(...)`.
2. Dale a su acción un camino de teclado: un `Command` asignado en el contexto de teclas de la superficie, o una entrada en sus acciones del panel o en su menú `m` que ejecute la misma acción. Dentro de un diálogo, un control que toma el foco se alcanza con Tab.
3. Registra el id en el registro de la superficie que lo dibuja: `crates/dbflux_ui_document/src/keyboard_coverage.rs` para los documentos, `crates/dbflux_ui_windows/src/keyboard_coverage.rs` para las ventanas de ajustes y del gestor de conexiones, `crates/dbflux_ui/src/ui/views/workspace/keyboard_coverage_tests.rs` para el marco del workspace, o el módulo de tests de un diálogo. Usa `KeyboardPath::MouseOnly("motivo")` solo para el marco de la ventana y los gestos del puntero. Un motivo que empieza con `gap:` registra un camino de teclado que falta, para que siga visible.

Ejecuta los tests de cobertura con:

```bash
cargo nextest run --workspace _covered
```

Una ejecución del workspace compila los crates de UI con las features con las que se distribuye la app, así que también comprueba las superficies MCP y el modo Lua de los hooks. Los tests propios del helper se ejecutan con `cargo nextest run -p dbflux_ui_base keyboard_coverage`.

## Issues

Antes de abrir un issue:

- Busca en los issues existentes para evitar duplicados.
- Reproduce contra un build reciente si puedes.

Incluye:

- Versión de DBFlux (`dbflux --version`), OS / display server (X11 vs Wayland en
  Linux), y motor de base de datos + versión.
- Pasos para reproducir.
- Comportamiento esperado vs. actual.
- Logs si son relevantes. Redacta los secretos.

Aplica los labels que describen el issue. Ver [Guía de etiquetas](#label-guide).

## Guía de etiquetas

El repositorio usa una taxonomía de labels estructurada. Aplica **un label de
cada eje aplicable** al abrir un issue o PR. Los maintainers pueden ajustarlos
durante el triage.

### Kind (uno de `*:bug` o `*:feature` por área afectada)

Áreas que tienen división bug/feature:

| Área     | Bug            | Feature            |
| -------- | -------------- | ------------------ |
| AWS      | `aws:bug`      | `aws:feature`      |
| Audit    | `audit:bug`    | `audit:feature`    |
| Driver   | `driver:bug`   | `driver:feature`   |
| MCP      | `mcp:bug`      | `mcp:feature`      |
| Pipeline | `pipeline:bug` | `pipeline:feature` |
| Proxy    | `proxy:bug`    | `proxy:feature`    |
| Query    | `query:bug`    | `query:feature`    |
| RPC      | `rpc:bug`      | `rpc:feature`      |
| SSH      | `ssh:bug`      | `ssh:feature`      |
| Storage  | `storage:bug`  | `storage:feature`  |
| UI       | `ui:bug`       | `ui:feature`       |

Además los genéricos por defecto de GitHub: `bug`, `documentation`, `question`,
`help wanted`, `good first issue`, `invalid`.

### Flags de subsistema (aplicar cuando sea relevante)

- `aws`, `proxy`, `ssh`, `query`, `driver`, `mcp`

### Driver (cuando el cambio es específico de un driver)

`driver:mongodb`, `driver:postgres`, `driver:sqlite`, `driver:mysql/mariadb`,
`driver:dynamodb`, `driver:redis`

### Kind del modelo de datos (para trabajo a nivel de store/driver)

`kind:sql`, `kind:document`, `kind:kv`, `kind:log`

### Platform / Arch (cuando el comportamiento es específico de la plataforma)

- Platform: `platform:linux`, `platform:macos`, `platform:windows`
- Arch: `arch:amd64`, `arch:arm64`

### Subtipo RPC (al tocar servicios respaldados por RPC)

`rpc:auth`, `rpc:driver` (además de `rpc:bug`/`rpc:feature`)

### Priority

`priority:high`, `priority:medium`, `priority:low` — normalmente aplicados por
los maintainers durante el triage.

### Status (aplicado por los maintainers)

`status:needs-review`, `status:approved`, `status:rejected`

### Ejemplos de combinaciones

- Un bug de query JSON de PostgreSQL en Linux: `driver:bug`, `driver:postgres`,
  `query:bug`, `platform:linux`, `kind:sql`
- Una nueva feature de pub/sub de Redis: `driver:feature`, `driver:redis`,
  `kind:kv`
- Una regresión del flujo de aprobación de MCP en Windows: `mcp:bug`,
  `platform:windows`
- Una mejora de UI del túnel SSH: `ui:feature`, `ssh:feature`, `ssh`

Si no estás seguro, etiqueta lo mejor que puedas — los maintainers lo refinarán
durante el triage.

## Seguridad

No reportes issues de seguridad públicamente. Envía un correo al maintainer o
usa un canal privado. Los logs y reproducciones deben tener los secretos
redactados (tokens, contraseñas, connection strings).

## Licencia

Al contribuir aceptas que tus contribuciones se licencian bajo la licencia dual
MIT / Apache-2.0 del proyecto. El nombre y el logo de DBFlux no forman parte de
esa licencia; consulta [TRADEMARK.md](TRADEMARK.md).
