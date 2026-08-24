# InfluxDB

Base de datos de series temporales InfluxDB v1 y v2, con soporte de queries
InfluxQL y Flux.

## De un vistazo

- **Categoría** — Series temporales
- **Lenguaje de query** — InfluxQL / Flux
- **Puerto por defecto** — 8086
- **Esquema de URI** — `http`

Driver de InfluxDB para DBFlux.

## Funcionalidades

- **Categoría de series temporales** — clasificado como
  `DatabaseCategory::TimeSeries` con `QueryLanguage::InfluxQuery` como lenguaje
  de editor por defecto. Las capacidades declaradas son `AUTHENTICATION`,
  `MULTIPLE_DATABASES`, `PAGINATION`, `EXPORT_CSV` y `EXPORT_JSON`. Las
  conexiones usan el esquema de URI `http` en el puerto por defecto 8086, con
  TLS provisto por el cliente HTTP basado en rustls.
- **InfluxDB v1 y v2** — ambas versiones de la API se soportan en un único crate
  de driver.
- **InfluxQL en ambas versiones** — el lenguaje de query de v1 funciona en v1 y
  a través del endpoint de compatibilidad de v2.
- **Flux en v2** — las queries Flux están disponibles cuando la conexión está
  configurada para v2.
- **Bucket por defecto opcional** — el campo bucket (v2) o database (v1) del
  perfil de conexión es opcional. Un token de API de v2 da acceso a todos los
  buckets de la organización; un usuario de v1 da acceso a todas las databases
  del servidor. Dejar el campo vacío permite al usuario seleccionar un bucket
  por query desde el desplegable de source-context en el editor. Configurarlo
  pre-selecciona ese bucket sin restringir el acceso a los demás.
- **Enrutamiento de bucket por query** — el bucket usado en cada query InfluxQL
  viene de la selección del desplegable de source-context, no del perfil de
  conexión. Para queries Flux, el bucket va incrustado en el propio texto de la
  query (`from(bucket: "...")`).
- **Ping sin bucket** — la verificación de disponibilidad de la conexión no
  requiere un bucket: v1 usa `SHOW DATABASES` contra la database interna; v2
  obtiene `/api/v2/buckets?limit=1`.
- **Macros de rango de tiempo** — las queries InfluxQL y Flux soportan tokens de
  macro compatibles con Grafana que se sustituyen por la ventana de rango de
  tiempo vinculada antes de enviar la query al driver:

  | Token              | Lenguaje | Expansión                                                 |
  | ------------------ | -------- | --------------------------------------------------------- |
  | `$timeFilter`      | InfluxQL | `time &gt;= 'RFC3339_start' AND time &lt;= 'RFC3339_end'` |
  | `$__from`          | InfluxQL | `'RFC3339_start'`                                         |
  | `$__to`            | InfluxQL | `'RFC3339_end'`                                           |
  | `v.timeRangeStart` | Flux     | `'RFC3339_start'`                                         |
  | `v.timeRangeStop`  | Flux     | `'RFC3339_end'`                                           |

  Estos tokens coinciden con las convenciones de variables de Grafana
  (`$timeFilter` para InfluxQL, `v.timeRangeStart`/`v.timeRangeStop` para Flux).
  Los usuarios familiarizados con Grafana deberían encontrar la sintaxis
  intuitiva.

  Formato RFC3339: `YYYY-MM-DDTHH:MM:SSZ` (UTC, precisión de segundos, sufijo
  Z).

  **Ejemplo InfluxQL** — usando `$timeFilter`:

  ```influxql
  -- Typed:
  SELECT mean(usage_user) FROM cpu WHERE $timeFilter GROUP BY time(1m)

  -- Executed (window = 2026-05-20T00:00:00Z to 2026-05-22T23:59:00Z):
  SELECT mean(usage_user) FROM cpu WHERE time &gt;= '2026-05-20T00:00:00Z' AND time &lt;= '2026-05-22T23:59:00Z' GROUP BY time(1m)
  ```

  **Ejemplo Flux** — usando `v.timeRangeStart` / `v.timeRangeStop`:

  ```flux
  -- Typed:
  from(bucket: "telegraf")
    |&gt; range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |&gt; filter(fn: (r) =&gt; r._measurement == "cpu")

  -- Executed (same window):
  from(bucket: "telegraf")
    |&gt; range(start: '2026-05-20T00:00:00Z', stop: '2026-05-22T23:59:00Z')
    |&gt; filter(fn: (r) =&gt; r._measurement == "cpu")
  ```

  **Las macros requieren una ventana vinculada** — si la query contiene tokens
  de macro pero no hay un rango de tiempo definido (es decir, el panel de
  source-context no tiene selección), las macros pasan al driver sin sustituir.
  InfluxDB devolverá un error de parseo, ya que `$timeFilter`, etc. no son
  sintaxis InfluxQL/Flux válida.

  **Las macros suprimen la inyección automática** — cuando una query contiene
  alguno de los tokens de macro reconocidos, la inyección automática de ventana
  de tiempo (ver abajo) se suprime. La sustitución de macro se trata como el
  límite de tiempo autoritativo del usuario.

  **Limitación conocida en v1 (sustitución de subcadena ingenua)** — los tokens
  de macro dentro de literales de cadena entrecomillados o comentarios también
  se sustituyen. No hay sintaxis de escape en v1. Para Flux, una variable cuyo
  nombre simplemente comience con `v.timeRangeStart` o `v.timeRangeStop` (p. ej.
  `v.timeRangeStartCustom`) también se sustituirá. Se planea una tokenización
  adecuada para una versión futura.

- **Inyección automática de ventana de tiempo** — cuando se establece un rango
  de tiempo a través del panel de source context y la query aún no contiene un
  predicado de tiempo (`time >=`, etc. para InfluxQL, `|> range(` para Flux), el
  driver inyecta los límites automáticamente. Este comportamiento se suprime
  cuando la query contiene tokens de macro de rango de tiempo explícitos.
- **Mensajes de error estructurados** — los errores del lado del servidor se
  parsean desde el campo JSON `{"error": "..."}` en lugar de mostrarse como
  códigos de estado HTTP crudos.
- **Exportación a CSV y JSON** — los resultados de las queries se pueden
  exportar a través del pipeline de exportación estándar de DBFlux.
- **Emisión de auditoría** — todas las queries se rastrean a través del sink de
  auditoría estándar de DBFlux. El campo de metadata `bucket_or_database`
  registra el bucket real usado en cada query, no el valor por defecto del
  perfil.
- **InfluxQL multi-statement** — cuando una query contiene múltiples statements
  separados por `;` (p. ej. `SHOW MEASUREMENTS; SHOW SERIES`), todos los
  resultados se concatenan en un único result set. Se antepone una columna
  entera sintética `statement_index` para distinguir las filas de los distintos
  statements.
- **Menú contextual "Query Measurement"** — hacer clic derecho en un measurement
  en la barra lateral muestra "Query Measurement". La acción abre un nuevo
  documento de código pre-poblado con una query de plantilla (`SELECT * FROM
  ...` para InfluxQL, `from(bucket: ...) |> range(...)` para Flux).
- **Menú contextual "New Query" en buckets** — hacer clic derecho en un nodo de
  bucket/database muestra "New Query", abriendo un documento de código en blanco
  con la conexión activada.
- **Generación de plantillas de lectura** — `InfluxQueryGenerator` produce
  plantillas de lectura select-all y por measurement tanto para InfluxQL como
  para Flux (usadas por las acciones del menú contextual y copy-as-query),
  sensibles a la versión según la versión configurada de la conexión y el bucket
  por defecto.

## Limitaciones

- **Sin cancelación de query** — `cancel()` devuelve `NotSupported`; las queries
  en curso no se pueden abortar desde la UI (`QUERY_CANCELLATION` no está
  declarada).
- **Sin generación de mutaciones** — `QueryGenerator::generate_mutation` siempre
  devuelve `None`; solo se generan plantillas de lectura, en consonancia con la
  API de queries de solo lectura.
- **Flux no soportado en v1** — intentar ejecutar una query Flux contra una
  conexión v1 devuelve un error de inmediato, sin realizar una llamada HTTP.
- **Sin INSERT/UPDATE/DELETE** — la API de queries de InfluxDB es de solo
  lectura. La ingesta de datos usa la Line Protocol write API, que este driver
  no expone.
- **Sin transacciones** — InfluxDB no soporta transacciones.
- **InfluxQL requiere un bucket** — las queries InfluxQL incrustan el bucket en
  la URL (`?db=<bucket>`). Si ni el desplegable de source-context ni el valor
  por defecto del perfil proveen un bucket, la ejecución se rechaza con un error
  claro que pide al usuario seleccionar uno.
- **Detección de predicado de tiempo basada en regex** — el driver usa
  expresiones regulares para determinar si una query ya contiene un predicado de
  tiempo. Esto puede dar falsos positivos con literales de cadena
  entrecomillados que contengan texto coincidente con `time <`, `time >` o `|>
  range(`.
- **Las columnas multi-statement quedan fijadas por el primer statement no
  vacío** — cuando una query multi-statement devuelve resultados con formas
  distintas (p. ej. `SHOW MEASUREMENTS; SHOW SERIES`), el diseño de columnas
  queda determinado por el primer statement no vacío. Las filas de los
  statements siguientes se mapean a ese diseño. Formas no coincidentes producen
  columnas desalineadas en lugar de un error.
- **Autenticación básica vía cabecera Authorization** — las credenciales de
  usuario/contraseña de v1 se envían como una cabecera `Authorization: Basic
  <base64>` en lugar de mediante parámetros de query en la URL. Esto es más
  limpio para la higiene de logs pero difiere de algunas bibliotecas cliente de
  InfluxDB.
- **Serialización retrocompatible** — los perfiles guardados con el antiguo
  campo obligatorio `bucket_or_database` se siguen cargando correctamente. El
  campo se deserializa como `default_bucket` mediante un alias de serde. Los
  perfiles guardados tras este cambio usan la clave `default_bucket`.
