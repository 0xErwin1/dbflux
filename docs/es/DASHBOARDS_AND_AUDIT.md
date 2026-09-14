# Dashboards y Audit — Guía de uso

Cómo graficar y guardar queries, construir dashboards, ver métricas de instancia
en vivo y usar el visor de audit. Este es el complemento de *cómo se usa* de la
documentación interna: [Charts](CHARTS.md), [Dashboards](DASHBOARDS.md) y
[Audit](AUDIT.md).

---

## Saved charts

### Graficar el resultado de una query

1. Ejecuta una query que devuelva datos tabulares.
2. Haz clic derecho en la result grid y elige **Chart this query**.

Se abre un chart document, sembrado con tu query, y se ejecuta automáticamente.
La opción solo aparece cuando el resultado tiene una query original utilizable y
DBFlux puede autodetectar columnas graficables. Tipos de chart soportados: Line,
Bar, Scatter, Area, Stacked Bar, Pie. La detección de ejes usa el kind de cada
columna (time, numeric, text); ver [Charts](CHARTS.md) para las reglas.

### Guardar y reabrir

- En un chart document, pulsa **Save** y dale un nombre. Volver a guardar el
  mismo chart lo sobrescribe (sin duplicados).
- Reabre un chart guardado con **Open Chart…** en el command palette — lista los
  charts guardados de la conexión activa. (Si no hay ninguno: *"No saved charts
  for the current profile"*.)
- Los charts guardados también aparecen en el sidebar bajo **Saved Charts**,
  donde cada chart tiene **Open / Rename… / Duplicate / Delete…**.

Los charts se guardan por connection profile.

---

## Dashboards

Un dashboard es una cuadrícula con nombre de 12 columnas de panels que comparten
un rango de tiempo y una política de refresh.

### Crear uno

1. Ejecuta **New Dashboard…** desde el command palette (o **New Dashboard…** en
   la carpeta **Dashboards** del sidebar).
2. Ponle nombre. Se abre con una cuadrícula de 12 columnas y el refresh apagado.

Los dashboards nuevos se abren en modo **View**. La carpeta Dashboards del
sidebar lista los dashboards guardados con **Open / Rename… / Duplicate /
Delete…**.

### Edit vs. view

Alterna el botón **lápiz / ojo** en la toolbar:

- El modo **Edit** muestra tiradores de arrastre — arrastra panels para
  reordenarlos, arrastra los bordes o la esquina para redimensionar dentro de la
  cuadrícula de 12 columnas.
- El modo **View** es de solo lectura.

En modo Edit también puedes usar el teclado sobre un panel enfocado: `F2` para
renombrar, `Delete`/`Backspace` para eliminar, `Enter` para abrir su popover
Configure.

### Añadir panels

Haz clic en **+ Add Panel**. El selector tiene hasta tres pestañas:

| Pestaña    | Crea                                                                                                                         |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Saved**  | Un panel a partir de uno o más charts guardados existentes.                                                                  |
| **Query**  | Un panel nuevo a partir de un nombre + una query que escribes.                                                               |
| **Metric** | Un panel a partir de una metric del driver (solo se muestra cuando el driver de la conexión expone un catálogo de métricas). |

El menú kebab de cada panel (modo Edit) ofrece **Configure / Edit title / Remove
panel**. El popover **Configure** te permite cambiar el tipo de chart (Line,
Bar, Scatter, Area, Stacked, Pie), ajustar los axis bindings, ver **Stats** y
**Export PNG**.

Los dashboards también pueden contener tiras **Divider** — cabeceras markdown
que agrupan visualmente los panels y colapsan los panels debajo al hacer clic.

> Un panel **Chart** referencia un chart guardado. Si borras ese chart guardado,
> el panel se convierte en un placeholder ("Chart not found — saved chart was
> deleted") en lugar de desaparecer.

### Rango de tiempo y refresh

La toolbar tiene:

- Un desplegable de **rango de tiempo**: Last 15 min, Last 1 hour, Last 6 hours,
  Last 24 hours, Last 7 days, o **Custom** (que revela selectores de fecha y
  hora/minuto). El rango se aplica a todos los chart panels a la vez.
- Un botón split de **refresh**: haz clic para refrescar todos los panels ahora;
  el desplegable establece un intervalo de auto-refresh (o Off / refresh al
  abrir).

> **Las conexiones desconectadas se gestionan con elegancia.** Cuando la
> conexión de un panel se cierra, su tick de refresh se salta — el timer sigue
> vivo y se reanuda automáticamente al reconectar, sin necesidad de volver a
> abrir el dashboard.

---

## Instance Overview, metrics e inspectors

Para los drivers que lo soportan — **PostgreSQL, MySQL/MariaDB, MongoDB, Redis y
SQL Server** — un profile conectado muestra una entrada **Instance Overview** en
el sidebar, encima de las carpetas **Instance Metrics** e **Instance
Inspectors**.

- **Instance Overview** es un dashboard de solo lectura sintetizado a partir del
  layout por defecto del driver (una pestaña por conexión). No se puede editar
  ni añadirle panels, pero puedes pulsar **Save as editable** para clonar su
  layout en un dashboard nuevo, totalmente editable.
- **Instance Metrics** son charts de series temporales (p. ej. connections,
  throughput).
- **Instance Inspectors** son snapshots tabulares del estado en vivo del
  servidor (p. ej. `pg_stat_activity` de Postgres, la process list de MySQL, las
  operaciones actuales de MongoDB, la client list de Redis), refrescados en el
  intervalo compartido.

### Acciones por fila en los inspectors

Algunos inspectors ofrecen acciones por fila (por ejemplo **Kill connection** /
**Terminate session**). Estas son:

- **Reguladas por permisos** — una acción para la que no tienes privilegio se
  oculta, así nunca ves un botón que simplemente fallaría.
- **Confirmadas cuando son destructivas** — las acciones destructivas piden
  confirmación antes de ejecutarse.
- **Auditadas** — cada intento registra un audit event, y los fallos muestran un
  toast con un enlace a la fila de audit correspondiente.

---

## Dashboards remotos (CloudWatch)

Cuando un driver lo soporta (CloudWatch es la implementación de referencia),
DBFlux puede **explorar (browse)** e **importar** dashboards upstream.

- **Browse**: los dashboards upstream aparecen en el sidebar. Abrir uno obtiene
  y renderiza el dashboard como uno **en memoria, de solo lectura** — no se
  escribe nada de vuelta en la fuente, y nada se guarda localmente. Una acción
  **Refresh** vuelve a obtener el listado. El listado tiene el ámbito de la
  sesión y **no** se conserva entre reinicios.
- **Import**: el comando **Import Dashboard** (palette o sidebar) parsea la
  definición upstream en un dashboard **local** nuevo con charts importados.
  Solo está disponible cuando el driver de la conexión activa soporta la
  importación — de lo contrario verás *"The active connection does not support
  dashboard import."*

Los dashboards remotos son de exploración/importación de solo lectura; DBFlux
nunca modifica la fuente.

---

## Visor de audit

El visor de audit es el único lugar para revisar todo lo que DBFlux registró:
queries, conexiones, hooks, scripts, cambios de configuración y decisiones de
governance de AI/MCP.

### Abrirlo

- Teclado: **Ctrl+Shift+A** (**Cmd+Shift+A** en macOS).
- Command palette: **Open Audit Viewer**.

Hay una única pestaña de audit; reabrirla enfoca la existente.

### Qué se ve

Cada fila muestra un timestamp, un chip de **severity** (ERROR/WARN/INFO), un
chip de **category** y un summary. Expande una fila para ver **Category,
Outcome, Actor, Action, Duration y Summary**.

Las categorías de un vistazo:

| Categoría      | Cubre                                            |
| -------------- | ------------------------------------------------ |
| **Query**      | Ejecución de queries y scans.                    |
| **Connection** | Connect / disconnect / reconnect.                |
| **Hook**       | Ejecuciones de connection-hooks.                 |
| **Script**     | Ejecuciones de scripts Lua / Python / Bash.      |
| **Mcp**        | Llamadas a tools de un AI client.                |
| **Governance** | Decisiones de policy.                            |
| **Config**     | Cambios de profile y settings.                   |
| **System**     | Arranque, migraciones y eventos de log internos. |

### Filtrar

La toolbar ofrece **búsqueda** de texto libre, un **time range** (los mismos
presets que los dashboards, más Custom), un **timestamp mode** (Local / UTC), y
filtros multi-select para **Level** (Error/Warn/Info), **Category** y
**Outcome** (Success/Failure/Cancelled). **Clear** los restablece.

El menú contextual de una fila añade **Copy Row as CSV**, **Copy Summary** y —
cuando el evento tiene un correlation id — **Filter by Correlation**.

### Seguir un error hasta su fila de audit

Cuando algo que hiciste falla, DBFlux muestra un toast con una acción **View in
Audit**. Al hacer clic se abre el visor de audit filtrado a ese evento exacto
(emparejado por un correlation id compartido entre el toast y la fila de audit).
El **badge de error** en la status bar abre el visor pre-filtrado a los fallos
recientes orientados al usuario.

### Exportar

El botón **Export** escribe los eventos actualmente visibles a **CSV** o
**JSON** en tu carpeta `~/Downloads` (`audit_export.csv` / `audit_export.json`),
usando el schema extendido (todos los campos, incluidos los details
estructurados). Un toast de éxito informa de cuántos eventos se escribieron y
dónde.

### Retención

Los eventos antiguos se pueden purgar según una programación de retención cuando
está configurada (ver [Settings → Audit](SETTINGS.md#audit)). El audit log crece
con el uso salvo que se purgue; vive en el mismo `dbflux.db` que todo lo demás
([Data & Privacy](DATA_AND_PRIVACY.md#audit-and-privacy)).

---

## Relacionado

- [Charts](CHARTS.md) — tipos de chart, column kinds, autodetección de ejes.
- [Dashboards](DASHBOARDS.md) — modelo de almacenamiento y seams de driver.
- [Audit](AUDIT.md) — schema completo de eventos y redacción.
- [Settings & Hooks](SETTINGS.md) — audit y refresh settings.
