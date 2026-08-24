# Gráficos en DBFlux

DBFlux puede convertir el resultado de una query en un chart. El motor de
charting es completamente agnóstico al driver: solo inspecciona los metadatos de
columna estructurados que cada driver rellena, nunca un identificador de driver
ni una cadena de tipo específica de la base de datos. Este documento describe
los tipos de chart soportados, cómo el motor auto-detecta los ejes, cómo se
persisten los charts y cómo se crea un chart desde la UI.

Para dashboards (grids de saved charts con un rango de tiempo compartido), las
tablas de almacenamiento de visualización (`viz_*`) y los seams de driver para
importar/explorar dashboards remotos, ver [`DASHBOARDS.md`](./DASHBOARDS.md).

## Visión general

El motor de charts vive en el crate `dbflux_components`, bajo
`crates/dbflux_components/src/chart/`. Su `mod.rs` describe el pipeline
completo:

1. `detect` — auto-detecta columnas adecuadas a partir de un `QueryResult`
   usando únicamente la semántica de `ColumnKind`.
2. `spec` — tipos de especificación de chart y series, más constructores para
   selección de columnas guiada por detección y manual.
3. `decimate` — downsampling LTTB (Largest-Triangle-Three-Buckets) para mantener
   el pintado rápido en datasets grandes.
4. `axis` — generación de ticks y formateo de etiquetas para ejes numéricos y de
   tiempo.
5. `legend` — factory de elementos para la fila de leyenda.
6. `engine` — `ChartView`, la entidad GPUI que posee el estado del chart y
   renderiza el canvas.

La UI del documento de chart independiente vive en
`crates/dbflux_ui_document/src/chart_document/` (`mod.rs`, `render.rs`,
`pane.rs`). Un `ChartDocument` posee una query, una conexión, un chart spec y un
`ChartShell`, y aloja su renderizado a través del chrome compartido
`ResultPanel` en `crates/dbflux_components/src/result_panel/`.

## Tipos de chart

Los tipos de chart se definen en el enum `ChartKind` en
`crates/dbflux_components/src/chart/spec.rs`:

| Variante     | Descripción                                                                                                                                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Line`       | Chart de línea. El tipo por defecto (`#[default]`); también el tipo elegido por todos los constructores de `ChartSpec`.                                                                                             |
| `Bar`        | Chart de barras.                                                                                                                                                                                                    |
| `Scatter`    | Chart de dispersión.                                                                                                                                                                                                |
| `Area`       | Chart de línea con relleno; el área entre la línea de la serie y la base se sombrea. Comparte la geometría y el comportamiento de hover de Line.                                                                    |
| `StackedBar` | Barras verticales apiladas. Cada posición X muestra una barra por serie, apiladas de forma acumulativa en lugar de agrupadas lado a lado. El eje Y se reescala en tiempo de renderizado a la suma máxima del stack. |
| `Pie`        | Chart de tarta. Sin ejes X/Y; cada serie visible se convierte en una porción cuyo tamaño es la suma de los valores Y de esa serie.                                                                                  |

`ChartKind` lleva la semántica `#[serde(default)]` en el campo contenedor
`ChartSpec.kind`, así que los chart specs serializados que son anteriores al
campo `kind` se deserializan como `Line`.

## ColumnKind y auto-detección de ejes

### ColumnKind

La auto-detección se rige enteramente por el enum `ColumnKind` definido en
`crates/dbflux_core/src/query/types.rs`:

| Variante    | Significado                                |
| ----------- | ------------------------------------------ |
| `Timestamp` | Una columna de fecha/hora o timestamp.     |
| `Float`     | Una columna numérica de punto flotante.    |
| `Integer`   | Una columna numérica entera.               |
| `Text`      | Una columna de texto/string.               |
| `Unknown`   | El driver no pudo clasificar esta columna. |

Cada driver es responsable de fijar `ColumnMeta::kind` en cada columna que
devuelve (ver las reglas de "Adding a New Driver" en `CLAUDE.md`). Las columnas
que quedan como `Unknown` nunca se usan como ejes ni series de chart.

### Reglas de auto-detección

`detect_chart_columns` en `crates/dbflux_components/src/chart/detect.rs` aplica
estas reglas, en orden, a un `QueryResult`:

1. Si el resultado tiene cero filas, devuelve `EmptyResult`.
2. Elige la columna más a la izquierda con `kind == Timestamp` como eje X. Si no
   existe ninguna, devuelve `NoTimeColumn`.
3. Recolecta cada otra columna con `kind == Float` o `kind == Integer`, en orden
   de columna, como las series Y numéricas. Si no queda ninguna, devuelve
   `NoNumericSeries`.
4. En caso contrario, devuelve `Ok { time_col, numeric_cols }`.

El resultado es el enum `ChartDetection`, cuyas variantes son `Ok`,
`NoTimeColumn`, `NoNumericSeries` y `EmptyResult`.

### Por qué nunca se inspeccionan `type_name` ni los IDs de driver

La documentación a nivel de módulo en `detect.rs` indica que el módulo de
detección es el límite entre el modelo de resultado de query y el motor de
charts, y que inspecciona valores de `ColumnKind` — nunca strings de `type_name`
ni identificadores de driver. La función `detect_chart_columns` solo lee
`column.kind`; nunca lee `column.type_name`, `column.name`, ni ningún ID de
driver. Esto mantiene el motor completamente desacoplado de drivers específicos,
en línea con la regla de desacoplamiento driver/UI de `CLAUDE.md`: un driver
hace que sus columnas sean graficables simplemente clasificándolas con el
`ColumnKind` correcto.

Como `Unknown` no es ni `Timestamp` ni `Float`/`Integer`, una columna sin
clasificar no puede ser ni un eje X ni una serie auto-detectados. Esto es
intencional: obliga a los drivers a clasificar las columnas en lugar de dejar
que el motor adivine a partir de strings de tipo.

### Inferencia del tipo de eje

Cuando se construye un `ChartSpec`, el tipo del eje X se infiere del
`ColumnKind` de la columna X: `Timestamp` se mapea a `AxisKind::Time` (ticks
formateados como fechas/horas), todo lo demás se mapea a `AxisKind::Numeric`
(ticks decimales). El campo `AxisSpec.unit` es actualmente siempre `None`; es un
seam de compatibilidad futura para metadatos de unidad que algún driver podría
suministrar más adelante.

### Extracción de valores numéricos

Cuando el motor extrae un valor numérico de una celda (`extract_f64` en
`engine.rs`), maneja varias formas de `Value`:

- `Value::Int` → se convierte a `f64`.
- `Value::Float` → se usa directamente cuando es finito; los valores no finitos
  se descartan.
- `Value::Decimal` (almacenado como string para preservar precisión) → se parsea
  de forma tolerante a `f64`, descartando valores no finitos o no parseables.
  Los drivers que clasifican columnas `NUMERIC`/`DECIMAL` como
  `ColumnKind::Float` (por ejemplo `NUMERIC` de PostgreSQL, `DECIMAL` de MSSQL)
  pasan por este camino.
- `Value::Bool` → `true` se mapea a `1.0`, `false` a `0.0`, así que las columnas
  `BIT`/`BOOLEAN` que algunos drivers clasifican como `Integer` (por ejemplo
  `BIT` de MSSQL) siguen siendo graficables.
- `Value::Text` solo se parsea para el eje de tiempo, como un timestamp RFC
  3339.
- `Value::Null` y el resto de formas no producen ningún valor.

## Saved charts

Un chart persistido es un registro `SavedChart`, definido en
`crates/dbflux_components/src/saved_chart.rs`. Los saved charts se almacenan en
la base de datos SQLite unificada — la tabla `viz_saved_charts` y sus tablas
relacionadas `viz_saved_chart_*` — a través de `SavedChartsRepository`, con una
caché en memoria gestionada por `SavedChartManager`
(`crates/dbflux_ui_base/src/saved_chart_manager.rs`). Las escrituras van primero
al repositorio; la caché se actualiza solo si tienen éxito.

Un `SavedChart` persiste:

- `id`, `name`, `profile_id` — identidad, nombre visible y el perfil de conexión
  propietario.
- `source` — un `SavedChartSource`, ya sea `Query { query }` (un string de query
  ejecutado dentro de un `ChartDocument`) o `Collection { collection_ref,
  time_window }` (una fuente de exploración de colección).
- `chart_spec` y `bindings` — la configuración de renderizado completa
  (`ChartSpec` y `BindingSpec`).
- `time_range_preset`, `refresh_policy`, `created_at`, `updated_at`.

Solo se persiste el string de la query (o la referencia a la colección); los
datos de resultado en crudo nunca se almacenan.

### Abrir un saved chart

`Workspace::open_saved_chart` (en
`crates/dbflux_ui/src/ui/views/workspace/actions.rs`) enruta según el tipo de
fuente:

- Las fuentes `Query` abren un `ChartDocument` independiente vía
  `ChartDocument::from_saved`. `from_saved` y `validate_saved_source` rechazan
  las fuentes `Collection`; el workspace valida la fuente antes de asignar la
  entidad.
- Las fuentes `Collection` no abren un `ChartDocument`; en su lugar reabren el
  `DataDocument` subyacente en modo chart a través de
  `open_collection_document`.

### Deduplicación

Los documentos de chart abiertos se deduplican a través de la variante
`DocumentKey::Chart { saved_chart_id: Uuid }` en
`crates/dbflux_ui_document/src/dedup.rs`. Antes de abrir un saved chart,
`open_saved_chart` llama a `tab_manager.find_by_key(&DocumentKey::Chart { ...
})` y activa la pestaña existente en lugar de abrir un duplicado. Un documento
de chart creado desde una acción ad-hoc "Chart this query" aún no está vinculado
a un ID guardado y, por tanto, no se deduplica hasta que se guarda.

## Crear un chart en la UI

Hay dos puntos de entrada.

### Chart this query

El menú contextual de un data grid ofrece un elemento "Chart this query". El
elemento está condicionado por `can_chart_from_context_menu` en
`crates/dbflux_ui_document/src/data_grid_panel/context_menu.rs`, que requiere
ambas condiciones:

1. La fuente del panel es un `QueryResult` con una query original no vacía, y
2. `detect_chart_columns` sobre el resultado actual devuelve `Ok`.

Seleccionar el elemento llama a `Workspace::open_chart_from_query`, que
construye un `ChartDocument::new` sembrado con la query y la conexión, lo
envuelve en un `PaneHandle` a través de `ChartDocument::into_pane`, y lo abre
como una nueva pestaña. Una query no vacía hace que el documento se auto-ejecute
en su primer render.

### Open chart...

El comando "Open chart..." lista los saved charts (construidos por
`build_saved_chart_palette_items`) para el perfil activo, y abre el chart
seleccionado a través de `open_saved_chart` como se describió arriba.

### Guardar

Dentro de un `ChartDocument`, el botón Save de la toolbar abre un prompt de
nombre y luego llama a `confirm_save`, que construye un `ChartSpec` a partir del
último resultado (usando `detect_chart_columns` / `ChartSpec::from_detection`
cuando la detección tiene éxito) y hace upsert de un `SavedChart` en el gestor
`saved_charts` del estado de la app. Guardar reutiliza el `saved_chart_id`
existente cuando está presente, así que el registro se sobrescribe en lugar de
duplicarse.

```mermaid
flowchart TD
    QR[QueryResult with ColumnMeta.kind] --&gt; DET[detect_chart_columns]
    DET --&gt;|Ok time_col, numeric_cols| SPEC[ChartSpec::from_detection]
    DET --&gt;|NoTimeColumn / NoNumericSeries / EmptyResult| NA[Chart this query unavailable]
    SPEC --&gt; CD[ChartDocument + ChartShell]
    CD --&gt; CV[ChartView render]
    CD --&gt;|Save| SC[SavedChart in saved_charts.json]
    SC --&gt;|Open chart...| CD
```

## Limitaciones

Estas limitaciones están fundamentadas en el código actual, no son suposiciones:

- La auto-detección requiere al menos una columna `Timestamp` para elegir un eje
  X; sin ella, `detect_chart_columns` devuelve `NoTimeColumn` y "Chart this
  query" no está disponible. (La selección manual a través de `BindingSpec` /
  `ChartSpec::from_bindings` puede usar una columna X que no sea de timestamp,
  que entonces se clasifica como un eje `AxisKind::Numeric`.)
- Las columnas con `ColumnKind::Unknown` quedan excluidas por completo de la
  auto-detección.
- Los saved charts de fuente `Collection` no se pueden abrir como un
  `ChartDocument`; en su lugar reabren el `DataDocument` subyacente en modo
  chart. Pasar una fuente `Collection` a `ChartDocument::from_saved` devuelve un
  error.
- `AxisSpec.unit` siempre es `None` en esta versión; los drivers todavía no
  suministran metadatos de unidad.
- Todos los constructores de `ChartSpec` (`from_detection`, `from_bindings`,
  `from_manual_selection`) producen un spec con `kind = ChartKind::Line`; los
  demás tipos de chart se seleccionan después de la construcción.
- La decimación de series usa un umbral LTTB cuyo valor por defecto es 10.000
  puntos (`default_decimation_threshold`).
