# Primeros pasos

Esta página te lleva de una instalación nueva a tu primer resultado de query. Si
todavía no has instalado DBFlux, empieza por [Instalación](INSTALL.md).

DBFlux es keyboard-first. Casi todas las acciones tienen tanto un gesto de ratón
como un atajo de teclado. Los atajos listados en estas páginas son los valores
por defecto de la aplicación; puedes revisar y cambiar el keymap activo en
**Settings → Keybindings** (ver [Settings](SETTINGS.md#keybindings)). Todos los
atajos por defecto están en la [Referencia de teclado](KEYBOARD.md).

## Primer inicio

Al arrancar, DBFlux restaura tu sesión anterior (pestañas abiertas). En una
instalación nueva no hay nada que restaurar, así que el foco recae por defecto
en el sidebar.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/getting-started/first-launch-dark.webp">
  <img src="../images/getting-started/first-launch-light.webp" alt="La ventana principal de DBFlux en una instalación nueva, con el sidebar vacío y sin conexiones">
</picture>

## Crear una conexión

Pulsa `Ctrl+Shift+N` (`Cmd+Shift+N` en macOS) para abrir el Connection Manager,
elige un driver, completa su formulario y conecta. El schema de la conexión
aparece entonces en el sidebar. [Conectar a una base de datos](CONNECTIONS.md)
cubre las otras formas de abrir el Connection Manager, el selector de drivers,
la pestaña Access (SSH, proxy, acceso gestionado) y qué ocurre cuando una
conexión falla.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/getting-started/connection-manager-dark.webp">
  <img src="../images/getting-started/connection-manager-light.webp" alt="El Connection Manager con el formulario de PostgreSQL completado para una base de datos local">
</picture>

## Ejecutar tu primera query

Abre una nueva pestaña de query con `Ctrl+n` (`Cmd+n` en macOS), escribe una
query en el lenguaje de la conexión activa y pulsa `Ctrl+Enter` (`Cmd+Enter`)
para ejecutarla. El resultado se renderiza en una pestaña de resultado dentro
del documento.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/getting-started/first-query-dark.webp">
  <img src="../images/getting-started/first-query-light.webp" alt="Una pestaña de query en una conexión PostgreSQL con una consulta SELECT y su resultado debajo">
</picture>

## Siguientes pasos

- [Conectar a una base de datos](CONNECTIONS.md) — el Connection Manager, los
  drivers, túneles SSH, proxies, AWS SSO y fuentes de valores.
- [Explorar el schema](SCHEMA_BROWSER.md) — el sidebar, el árbol del schema,
  las rutinas y el diagrama de esquema.
- [Ejecutar queries](EDITOR.md) — pestañas de query, ejecución, scripts, la
  confirmación de queries peligrosas y el historial de queries.
- [Constructor visual de queries](QUERY_BUILDER.md) — componer SELECT, UPDATE y
  DELETE sin escribir SQL.
- [Trabajar con resultados](RESULTS.md) — el data grid, la vista de registro,
  el filtrado, la edición y la exportación.
- [Vista clave-valor](KEY_VALUE.md) — claves, valores, expiración y la consola
  de comandos.
- [Colecciones de documentos](DOCUMENTS.md) — vistas de tabla, árbol y JSON de
  los documentos.
- [Gráficos](CHARTS.md) y [Dashboards](DASHBOARDS.md) — graficar resultados y
  construir dashboards.
- [Referencia de teclado](KEYBOARD.md) — todos los atajos por defecto, incluido
  el modo Vim.
- [Settings](SETTINGS.md) — cada sección de Settings y los connection hooks.
