# CloudWatch Logs

Queries de AWS CloudWatch Logs Insights con source context gestionado por el
editor.

## De un vistazo

- **Categoría** — Log stream
- **Lenguaje de query** — Logs Insights (modo editor SQL)
- **Esquema de URI** — `cloudwatch`

Driver de AWS CloudWatch Logs para DBFlux, construido sobre el SDK
[`aws-sdk-cloudwatchlogs`](https://crates.io/crates/aws-sdk-cloudwatchlogs).

## Funcionalidades

- Driver de log-streaming clasificado como `DatabaseCategory::LogStream`;
  `deployment_class` es `CloudManaged`. Las capacidades declaradas son
  `AUTHENTICATION` y `METRIC_SERIES`.
- Configuración de conexión AWS vía región, perfil con nombre y override de
  endpoint opcional, alineado con el flujo de conexión AWS de DynamoDB.
- Ejecución de queries a través de `StartQuery` + polling de `GetQueryResults`
  (intervalo de polling de 500 ms, hasta 120 intentos), con un source context
  gestionado por el editor que provee los log groups objetivo y el rango de
  tiempo.
- Tres sintaxis de query seleccionables desde el desplegable "Syntax" del source
  context:
  - CloudWatch Logs Insights QL (`cwli`, la opción por defecto) —
    `QueryLanguage::CloudWatchLogsInsightsQl`.
  - OpenSearch PPL (`ppl`) — `QueryLanguage::OpenSearchPpl`.
  - OpenSearch SQL (`sql`) — `QueryLanguage::OpenSearchSql`. Estas se mapean a
    los valores de lenguaje de query `Cwli`, `Ppl` y `Sql` del SDK.
- El spec de source context (`SourceContextSpec`) expone un selector de objetivo
  "Log groups" y controles de rango de tiempo Start/End; las queries CWLI y PPL
  pasan los log groups seleccionados a `StartQuery` vía `set_log_group_names`.
- El descubrimiento de schema enumera log groups (`fetch_log_groups`) como la
  única database lógica (`SchemaLoadingStrategy::SingleDatabase`, database por
  defecto `logs`).
- Los log streams se exponen como hijos de collection paginados
  (`collection_children` sobre `fetch_log_stream_page`) y se abren como event
  streams (`CollectionPresentation::EventStream`).
- Browsing de event streams (`browse_event_stream` / `EventStreamTarget`)
  respaldado por `FilterLogEvents`, con una ventana de browse por defecto de 24
  horas y soporte para patrón de filtro, prefijo de nombre de stream, nombres de
  stream explícitos y un toggle de "más reciente".
- Los nombres de columna de Insights se clasifican en `ColumnKind`s semánticos
  (p. ej. `@timestamp`, `@ingestionTime` reconocidos como timestamps) para la
  auto-detección de gráficos.
- CloudWatch Metrics vía `GetMetricData`: ejecuta un único `MetricDataQuery` por
  request, mapea la respuesta a un `QueryResult` de dos columnas (timestamp,
  value) ordenado ascendentemente por timestamp. Los timestamps de AWS
  (precisión de segundos) se convierten a milisegundos. Se soporta el pivot
  multi-métrica a formato ancho cuando se devuelven múltiples entradas
  `MetricDataResult`.
- Explorar el catálogo de métricas de CloudWatch (namespaces y métricas por
  namespace con combinaciones de dimensiones) vía paginación de `ListMetrics`.
  El listado de namespaces se sintetiza barriendo `ListMetrics` sin filtro y
  recolectando cadenas de namespace distintas. Los resultados se cachean en la
  sesión mediante `MetricCatalogCache`.
- El catálogo de métricas se puede explorar desde el árbol de la barra lateral
  de conexión (Metrics > Namespace > Metric). Hacer clic en una métrica hoja
  abre un gráfico pre-poblado con valores por defecto (Average / período de 5
  min / agregado entre todas las dimensiones) y lo ejecuta de inmediato. El
  panel lateral (picker rail) en el documento de gráfico permite refinar
  dimensiones, período y estadística.

## Limitaciones

- El campo `profile` (perfil con nombre de AWS) es un campo de formulario
  `AuthProfileRef`. El seam genérico de portabilidad
  (`DbDriver::export_field_hint`) mapea todos los campos `AuthProfileRef` a
  `RequiredOnImport`, así que el valor del campo se omite de cualquier bundle
  exportado y los destinatarios deben suministrar o crear un auth profile
  coincidente al momento de importar. No se requiere ningún override específico
  del driver.
- La cancelación de query no está implementada; `cancel()` devuelve
  `NotSupported`.
- El modo OpenSearch SQL no recibe log groups externos: las queries SQL deben
  declarar sus log groups consultados en el propio texto SQL, porque la API de
  CloudWatch no acepta parámetros de log group externos para el modo SQL (solo
  CWLI y PPL reciben `set_log_group_names`).
- El resaltado de sintaxis del editor permanece genérico (`query_language` se
  reporta como `Sql` a nivel de metadata); la selección de modo dirige la
  semántica de ejecución y las palabras clave de completado en lugar del
  resaltado por modo.
- Solo lectura: no se declaran capacidades de mutación, DDL, transacción ni
  paginación (`query`, `mutation`, `ddl`, `transactions`, `limits` son todos
  `None`); `schema_features` está vacío.
- Sin formulario SSL (TLS es manejado por el transporte del SDK de AWS).
- La ejecución de métricas soporta un único `MetricDataQuery` por request por
  llamada.
- La síntesis del listado de namespaces (barriendo `ListMetrics` sin filtro)
  puede ser lenta para cuentas de AWS grandes con muchas métricas; se cachea
  para la sesión una vez completada. El barrido está limitado a 50 páginas
  (~25.000 métricas) para acotar el peor caso en cuentas muy grandes. Cuando se
  alcanza el límite, el listado de namespaces se trunca en silencio y se
  registra una advertencia; un cambio futuro reemplazará el límite con
  infraestructura completa de timeout + cancelación.
- Las pruebas de integración en vivo para métricas
  (`live_execute_cloudwatch_metric`) requieren credenciales reales de AWS y
  están marcadas `#[ignore]` por defecto. LocalStack Community no soporta la API
  de CloudWatch Metrics.
