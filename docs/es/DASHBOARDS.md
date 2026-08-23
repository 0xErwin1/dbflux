# Dashboards y Saved Charts

DBFlux persiste configuraciones de chart como **Saved Charts** y las agrupa en
**Dashboards** — un grid de paneles de chart (y divisores markdown opcionales)
que comparten un rango de tiempo y una política de refresco.

Para los internals del motor de charts (renderizado, ejes, decimación,
paleta), ver [`CHARTS.md`](./CHARTS.md). Para la capa de almacenamiento
SQLite, ver [`ARCHITECTURE.md`](../ARCHITECTURE.md#storage--configuration).

## Visión general

- Un **SavedChart** es la forma persistida de una configuración de chart:
  binding de fuente de datos, series, bindings del eje Y, política de
  refresco y preset de rango de tiempo.
- Un **Dashboard** es un grid con nombre compuesto por paneles. Cada panel es
  o bien un slot `Chart` (referencia a un `SavedChart` por id) o bien un
  slot `Divider` (franja de encabezado markdown inline — sin chart, sin
  toolbar).
- Los dashboards tienen un **rango de tiempo** y una **política de refresco**
  compartidos que se propagan a cada panel de chart cargado mediante
  suscripciones.
- Los dashboards remotos (por ejemplo CloudWatch) se pueden **explorar** en
  la sidebar e **importar** a un Dashboard local cuando el driver anuncia las
  capacidades adecuadas.

## Capa de almacenamiento

Todos los datos de dashboard y saved-chart viven en
`~/.local/share/dbflux/dbflux.db`, bajo el prefijo de tabla `viz_*`:

| Tabla | Propósito |
|-------|---------|
| `viz_dashboards` | Registros de dashboard (`profile_id` nullable, `ON DELETE SET NULL`) |
| `viz_dashboard_panels` | Slots de panel: discriminador `panel_kind` + `divider_markdown` opcional |
| `viz_saved_charts` | Raíz de saved chart (`SavedChartDto`) |
| `viz_saved_chart_series` | Configuración por serie |
| `viz_saved_chart_binding_y` | Bindings del eje Y |
| `viz_saved_chart_source_metric_dimensions` | Dimensiones de métrica de CloudWatch |
| `viz_saved_chart_source_metric_series` | Spec de serie de métrica de CloudWatch |

Los repositorios viven en `crates/dbflux_storage/src/repositories/viz_*.rs` e
implementan el trait `Repository` estándar
(`all()`, `find_by_id()`, `upsert()`, `delete()`).

## Gestores en memoria

Ambos gestores envuelven los repositorios SQLite con cachés en memoria para
lecturas síncronas. Las escrituras van primero al repositorio; las cachés se
actualizan solo si tienen éxito.

- **`DashboardManager`** (`crates/dbflux_ui_base/src/dashboard_manager.rs`) —
  tipos de dominio `Dashboard`, `DashboardPanel`, `DashboardPanelKind`
  (`Chart { saved_chart_id }` | `Divider { markdown }` |
  `Inspector { metric_id }`), `DashboardPanelDraft`. Los dashboards nuevos se
  crean con `grid_columns = 12`; los paneles nuevos se añaden en
  `grid_column = 0` en una fila nueva con `grid_width = 12, grid_height = 2`.
- **`SavedChartManager`** (`crates/dbflux_ui_base/src/saved_chart_manager.rs`)
  — gestiona el ciclo de vida de `SavedChart`, incluyendo
  `SavedChartRefreshPolicy` (`Off` / `Interval { every_secs }`).
- **`RemoteDashboardCache`** (`crates/dbflux_app/src/remote_dashboard_cache.rs`)
  — caché en memoria con alcance de sesión para los listados de dashboards
  remotos. No se persiste entre reinicios.

## Integración con el sistema de documentos

Los dashboards se abren como un `DashboardDocument`
(`crates/dbflux_ui_document/src/dashboard/`):

- **Clave de dedup**: `DocumentKey::Dashboard { dashboard_id }` (persistido) o
  `DocumentKey::InstanceOverview { profile_id }` (auto-generado, de solo
  lectura).
- **Paneles de chart**: cada slot envuelve una entidad `ChartDocument`
  (`Loaded`) o un placeholder para un chart eliminado (`Orphan`).
- **Paneles de inspector**: cada slot envuelve una entidad `InspectorPanel`
  que aloja un `DataGridPanel` y se refresca en el intervalo compartido del
  dashboard. Las acciones de fila suministradas por el driver (por ejemplo,
  terminar conexión, cancelar query) aparecen en el menú contextual de la
  fila.
- **Toolbar compartida**: un único `TimeRangePanel` propaga los cambios de
  ventana a todos los paneles cargados mediante suscripciones.
- **Concurrencia**: la re-ejecución de paneles está acotada por
  `PANEL_REEXEC_CAP` para evitar saturar la conexión con queries
  concurrentes.
- **Grid**: grid canónico de 12 columnas; arrastrar para reordenar y
  arrastrar para redimensionar mediante `DragReorderState` /
  `DragResizeState` en `dashboard/builder.rs`.

Los saved charts independientes se abren como `ChartDocument`
(`crates/dbflux_ui_document/src/chart_document/`), con clave
`DocumentKey::Chart { saved_chart_id }`. `ChartDocument` se renderiza tanto de
forma independiente como embebido dentro de un panel de `DashboardDocument`.

## Instance Overview e inspectores

Las conexiones cuyo driver anuncia `INSTANCE_METRICS` o `INSTANCE_INSPECTOR`
exponen un **Instance Overview** de solo lectura — un dashboard sintetizado
con métricas de servidor en vivo e inspectores tabulares que nunca toca el
almacenamiento hasta que el usuario decide conservarlo.

### Abrir el Instance Overview

La sidebar muestra una única hoja **Instance Overview** bajo un perfil
conectado (encima de las carpetas *Instance Metrics* e *Instance
Inspectors*). Al hacer clic en ella — o al elegir **Open** desde su menú de
clic derecho — se abre el overview.

| Paso | Detalle |
|------|--------|
| Fuente | El descriptor `DefaultInstanceDashboard` del driver (layout fijo de 12 columnas) devuelto por `InstanceCatalog::default_dashboard()` |
| Dedup | `DocumentKey::InstanceOverview { profile_id }` — una pestaña por conexión; hacer clic de nuevo enfoca la pestaña existente |
| Persistencia | Ninguna. El `DashboardDocument` se construye en memoria en el momento de la apertura; no se escribe ninguna fila `viz_*` |
| Modo | Solo lectura. Las transiciones a modo edición, *Add Panel* y el toggle Edit/View se suprimen; el arrastrar-para-reordenar y arrastrar-para-redimensionar están deshabilitados |

### Guardarlo como dashboard editable

Un overview de solo lectura muestra un botón **Save as editable** (toolbar,
grupo derecho; tooltip *"Clone this overview into a new editable
dashboard"*). Clona el layout sintetizado — incluyendo las posiciones exactas
de los paneles — en un nuevo `Dashboard` persistido y propiedad del usuario,
vía `DashboardManager::append_panels` con un `DraftGridLayout` explícito por
panel. El clon se abre como una pestaña editable normal
`DocumentKey::Dashboard { dashboard_id }`. El overview original permanece de
solo lectura y se re-sintetiza en cada apertura.

### Paneles de inspector

Los inspectores son el tercer tipo de panel de dashboard, junto a `Chart` y
`Divider`:

| Tipo de panel | Respaldo | Notas |
|------------|---------|-------|
| `Chart` | Referencia a `SavedChart` (`saved_chart_id`) | Chart de serie temporal |
| `Divider` | Markdown inline | Franja de encabezado; sin toolbar |
| `Inspector` | `DashboardPanelKind::Inspector { metric_id }` | Snapshot tabular; se refresca en el intervalo compartido |

`DashboardPanelKind::Inspector { metric_id }`
(`crates/dbflux_ui_base/src/dashboard_manager.rs`) no lleva ninguna
referencia a chart — el inspector se identifica únicamente por `metric_id`.
Cada panel de inspector aloja un `DataGridPanel` que muestra el snapshot
actual devuelto por `InstanceCatalog::fetch_inspector_snapshot` (por ejemplo
`pg_stat_activity` de PostgreSQL, `PROCESSLIST` de MySQL, `currentOp` de
MongoDB, `CLIENT LIST` de Redis).

Persistencia: el valor `Inspector` se almacena en
`viz_dashboard_panels.panel_kind`, con la clave del inspector en
`inspector_metric_id`. Ambos fueron añadidos por la **migración 014**
(`014_viz_inspector_and_instance_metric`), que extiende el CHECK de
`panel_kind` — introducido primero en la migración 013 con `chart` /
`divider` — para aceptar también `inspector`. (Las pestañas de inspector
independientes abiertas directamente desde la carpeta de sidebar *Instance
Inspectors* usan `DocumentKey::InstanceInspector { profile_id, metric_id }`.)

### Acciones de fila del inspector

Las filas del inspector pueden exponer acciones de fila suministradas por el
driver (menú contextual de clic derecho), por ejemplo *Kill connection* /
*Terminate session*. El flujo:

1. El driver devuelve `InspectorRowAction`s desde `InstanceCatalog::row_actions(metric_id)`. La disponibilidad está condicionada por sondas de privilegio propias de cada driver (ver los README de driver), de modo que una sesión con privilegios insuficientes nunca ve una acción que no puede ejecutar.
2. Las acciones `is_destructive` piden confirmación en un modal antes de ejecutarse.
3. Al confirmar, la conexión se re-resuelve en el momento de la ejecución (no en el momento del clic) y se ejecuta `InstanceCatalog::execute_row_action(metric_id, action_id, row_values)`.
4. Cada intento registra un evento de auditoría. Los fallos se enrutan a través de `report_error_async` (`UserFacingError` de `ErrorKind::Driver`), así que el usuario recibe un toast con un id de correlación que enlaza a la fila de auditoría.

La ejecución vive en
`crates/dbflux_ui_document/src/instance_inspector/mod.rs`.

### Comportamiento de refresco

Los timers de refresco de dashboard, chart independiente e inspector
comprueban `AppState::connections()` para el perfil del panel antes de cada
tick y omiten su trabajo cuando la conexión está cerrada. El timer en sí
permanece activo, así que el refresco se reanuda automáticamente al
reconectar sin necesidad de rearmarlo.

### Cobertura de drivers integrados

| Driver | `INSTANCE_METRICS` | `INSTANCE_INSPECTOR` | Lista de métricas / inspectores |
|--------|:---:|:---:|---|
| PostgreSQL | ✓ | ✓ | [README](../crates/dbflux_driver_postgres/README.md) |
| MySQL / MariaDB | ✓ | ✓ | [README](../crates/dbflux_driver_mysql/README.md) |
| MongoDB | ✓ | ✓ | [README](../crates/dbflux_driver_mongodb/README.md) |
| Redis | ✓ | ✓ | [README](../crates/dbflux_driver_redis/README.md) |
| SQL Server | ✓ | ✓ | [README](../crates/dbflux_driver_mssql/README.md) |

El README de cada driver lista las métricas, inspectores y acciones de fila
concretas que expone; este documento no las duplica.

## Seams de driver

Los drivers se integran con los dashboards a través de seams genéricos del
core — la UI nunca bifurca según IDs de driver.

### Importar dashboards (JSON → Dashboard local)

- **Trait**: `DashboardImporter`
  (`crates/dbflux_core/src/connection/dashboard_import.rs`)
- **Capacidad**: `DriverCapabilities::DASHBOARD_IMPORT`
- **Tipos de valor**:
  - `WidgetImportSpec` — spec de widget parseado
  - `MetricView::{TimeSeries, StackedArea, SingleValue}`
  - `ImportedMetricSeries` — serie + dimensiones
  - `WidgetLayout` — coordenadas de layout nativas trasladadas al grid local

Los drivers parsean el JSON del dashboard en un conjunto normalizado de
widgets que la UI importa como `SavedChart`s y coloca en un nuevo
`Dashboard`.

### Explorar dashboards remotos (sidebar)

- **Trait**: `DashboardSource`
  (`crates/dbflux_core/src/connection/dashboard_source.rs`)
- **Capacidad**: `DriverCapabilities::DASHBOARD_SYNC`
- **Tipos de valor**: `RemoteDashboard`, `DashboardRef`
  (`last_modified: ISO8601` opcional)

La sidebar lista los dashboards remotos a través de este seam; los
resultados se cachean en `RemoteDashboardCache`. Seleccionar un dashboard
remoto dispara `DashboardImporter` para materializarlo localmente.

### Métricas e inspectores de instancia

- **Trait**: `InstanceCatalog`
  (`crates/dbflux_core/src/connection/instance_catalog.rs`)
- **Capacidades**: `DriverCapabilities::INSTANCE_METRICS` (series temporales),
  `DriverCapabilities::INSTANCE_INSPECTOR` (snapshots tabulares)
- **Tipos de valor**: `InstanceMetricDef`, `InstanceInspectorDef`,
  `DefaultInstanceDashboard`, `InspectorRowAction`

Los drivers exponen métricas de servidor en vivo (por ejemplo `pg.tps`,
`mysql.queries_per_sec`) e inspectores tabulares (por ejemplo `pg.activity`,
`mysql.processlist`, `mongo.currentop`, `redis.client_list`) a través de un
único catálogo. Cada driver también publica un descriptor
`DefaultInstanceDashboard` con un layout fijo de 12 columnas — el workspace
abre este descriptor como un dashboard **Instance Overview de solo lectura**
(clave de dedup `DocumentKey::InstanceOverview { profile_id }`). La acción
"Save as editable" clona el layout en un dashboard persistido propiedad del
usuario.

Las filas de inspector pueden declarar `InspectorRowAction`s (por ejemplo
*Terminate connection*). La disponibilidad de las acciones está condicionada
por sondas de privilegio propias de cada driver (`pg_monitor` /
`pg_signal_backend` para PostgreSQL, `PROCESS` / `CONNECTION_ADMIN` para
MySQL, `killOp` para MongoDB, `CLIENT KILL` para Redis, `VIEW SERVER STATE` /
`KILL` para SQL Server), de modo que una sesión con privilegios insuficientes
nunca ve acciones que no podría ejecutar.

Cada timer de refresco (tick de dashboard, tick de chart independiente, tick
de inspector) comprueba `AppState::connections()` para el perfil del panel y
omite su trabajo cuando la conexión está cerrada; el timer permanece activo
así que el refresco se reanuda automáticamente al reconectar.

Para el comportamiento de cara al usuario de este seam (cómo se abre el
Instance Overview, *Save as editable*, paneles de inspector y acciones de
fila), ver [Instance Overview e inspectores](#instance-overview-e-inspectores)
más arriba.

### Implementación de CloudWatch

`crates/dbflux_driver_cloudwatch/src/` provee:

- `CloudWatchDashboardSource` — lista los dashboards de CloudWatch a través
  del SDK de AWS
- `CloudWatchDashboardImporter` — parsea el JSON de dashboard de CloudWatch
  en `WidgetImportSpec`s con series de métrica, dimensiones y agregaciones
  estadísticas

Esto es **exploración e importación de solo lectura**, no una función de
sincronización. DBFlux nunca escribe de vuelta en los dashboards de
CloudWatch.

## Matriz de capacidades

| Bit de capacidad | Significado |
|---|---|
| `DASHBOARD_IMPORT` (51) | El driver puede parsear JSON de dashboard en specs de widget |
| `DASHBOARD_SYNC` (52) | El driver puede listar dashboards remotos |

Ambas capacidades son independientes: un driver puede anunciar sync sin
import, o viceversa.

## Añadir un nuevo driver compatible con dashboards

1. Implementa `DashboardSource` en el `Connection` del driver para listar los
   dashboards remotos. Añade `DASHBOARD_SYNC` a `DriverMetadata.capabilities`.
2. Implementa `DashboardImporter` en el `Connection` del driver para parsear
   un payload de dashboard en `WidgetImportSpec`s. Añade `DASHBOARD_IMPORT`.
3. La UI mostrará el árbol de dashboards en la sidebar y enrutará las
   importaciones sin ninguna bifurcación específica de driver.
