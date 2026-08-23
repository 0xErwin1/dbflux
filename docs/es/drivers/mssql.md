# SQL Server

Base de datos relacional Microsoft SQL Server.

## De un vistazo

- **Categoría** — Relacional
- **Query language** — T-SQL
- **Puerto por defecto** — 1433
- **Esquema de URI** — `sqlserver`

Driver de Microsoft SQL Server para DBFlux, construido sobre el cliente TDS
[`tiberius`](https://crates.io/crates/tiberius).

## Funcionalidades

- Driver relacional para SQL Server / Azure SQL con ejecución de queries SQL y
  descubrimiento de schema.
- Autenticación mediante logins de SQL Server (usuario + contraseña); el modo
  URI acepta connection strings ADO, JDBC y `sqlserver://user:pass@host:port/db`.
- Modos de encriptación TLS (`off`, `on`, `required`) vía el `EncryptionLevel`
  de tiberius. El formulario expone un único desplegable **SSL Mode**; el flag
  `TrustServerCertificate` se deriva automáticamente:
  - `off` — sin encriptación (el paquete de login sigue encriptado por TDS).
  - `on` — encriptado, acepta certificados autofirmados. Ideal para SQL Server
    local/de desarrollo con su certificado autogenerado.
  - `required` — encriptado, valida la cadena del certificado. Úsalo contra
    servidores con un certificado firmado por una CA real (Azure SQL, etc.).
  En modo URI, `?trust=true|false` sobrescribe explícitamente el valor
  derivado si necesitas una combinación inusual (p. ej.
  `?encrypt=required&trust=true`).
- Soporte opcional de instancias con nombre de SQL Server (`SQLEXPRESS`,
  `MSSQLSERVER2019`, etc.) resueltas en el momento de conectar consultando
  SQL Browser sobre UDP 1434 (habilitado vía el feature `sql-browser-tokio`
  de tiberius). El campo Instance del formulario, la forma `host\instance` al
  estilo SSMS en modo URI, y el parámetro de query `?instance=` en la URI fijan
  todos el mismo `instance_name` en la configuración de tiberius.
- Soporte de túnel SSH para conectar a través de bastion hosts (la búsqueda de
  instancia con nombre no está disponible a través de un túnel solo-TCP).
- Cambio de base de datos por pestaña vía `USE [database]`; el estado de sesión
  (opciones SET, tablas temporales, transacciones) persiste entre llamadas a
  `execute()` sobre la misma conexión.
- Lotes con múltiples result sets: cuando un lote produce varios result sets
  (p. ej. `SELECT 1; SELECT 2;` o un stored procedure con múltiples `SELECT`),
  el driver devuelve el **último** set no vacío como el `QueryResult`
  primario (preservando la UX histórica de "gana la última sentencia") y
  adjunta cada set anterior no vacío a `QueryResult.additional_results` en el
  orden del lote. Los lotes de pura preparación (`SET LOCK_TIMEOUT 5000`)
  siguen mostrándose como un único primario vacío. Los callers que quieran
  recorrer cada set usan `QueryResult::iter_result_sets()`.
- Motor de transferencia de datos: carga masiva nativa multi-fila con
  `INSERT` (`BULK_INSERT`, con un tope de 1000 filas por sentencia según el
  límite de filas de `VALUES` de T-SQL, expuesto vía
  `DriverLimits::max_bulk_insert_rows`) y DDL `CREATE TABLE` nativo del driver
  a partir de las columnas de una tabla origen (`TRUNCATE_TABLE` también
  está soportado).

### Instance Metrics

Expone un conjunto curado de métricas de servidor en vivo obtenidas de
`sys.dm_os_performance_counters`:

- `mssql.batch_requests_per_sec` — batch requests de T-SQL por segundo
- `mssql.compilations_per_sec` — compilaciones SQL por segundo
- `mssql.recompilations_per_sec` — recompilaciones SQL por segundo
- `mssql.user_connections` — conexiones de usuario abiertas actualmente
- `mssql.lock_waits_per_sec` — esperas de lock por segundo (instancia `_Total`)
- `mssql.page_reads_per_sec` — lecturas de página del buffer pool por segundo
- `mssql.page_writes_per_sec` — escrituras de página del buffer pool por
  segundo
- `mssql.buffer_cache_hit_ratio` — ratio de aciertos del buffer cache
  (porcentaje)
- `mssql.server_memory_kb` — memoria total del servidor en KB

Cada métrica se devuelve como una única fila `(timestamp_ms, value)` para
graficado en vivo.

Requiere el permiso de servidor `VIEW SERVER STATE`. Sin él, `list_metrics()`
devuelve una lista vacía y se registra una advertencia. El driver sondea este
permiso una vez al construir el catálogo.

### Instance Inspector

Expone snapshots tabulares del estado del servidor en ejecución:

- `mssql.active_sessions` — sesiones de usuario de `sys.dm_exec_sessions`
  unidas con `sys.dm_exec_requests` (session id, login name, host name,
  program name, status, tiempo de CPU, uso de memoria, command, request
  status, wait type, wait time, blocking session id)

Requiere el permiso `VIEW SERVER STATE`.

### Cancelación de queries

- La cancelación se implementa como `KILL <spid>` emitido desde una conexión
  side-channel nueva. tiberius actualmente no expone la primitiva TDS
  Attention que usa SSMS, así que la siguiente mejor opción es pedirle al
  servidor que termine la sesión que ejecuta la query.
- Al conectar, el driver captura `@@SPID` y cachea un clon de la `Config` de
  tiberius (con el login ya incorporado). El handle de cancelación abre una
  segunda conexión bajo demanda, ejecuta `KILL <spid>`, y marca la conexión
  primaria como envenenada.
- Tras la cancelación, `cleanup_after_cancel()` reconstruye el cliente
  tiberius primario, captura el nuevo SPID, y reemite el `USE [db]` anterior
  para que la siguiente query se ejecute en la misma base de datos. Desde la
  perspectiva de la UI, la conexión sigue conectada; solo cambia el id de
  sesión subyacente.
- Los errores lanzados en la sesión eliminada (códigos 596 / 233 / 6005) se
  traducen a `DbError::Cancelled` para que la UI muestre "query cancelled" en
  vez de un fallo a nivel de transporte.
- El propietario de la sesión puede hacer `KILL` de su propio SPID en SQL
  Server moderno sin el permiso `ALTER ANY CONNECTION`. En logins más
  antiguos o restringidos, el propio KILL puede fallar con un error de
  permisos; el driver lo muestra al usuario.

### Descubrimiento de schema

- Bases de datos (`sys.databases`, oculta las bases de datos de sistema).
- Tablas y vistas por base de datos (`sys.tables`, `sys.views`).
- Columnas por tabla + flag de primary key, índices, foreign keys.
- Constraints por tabla: constraints CHECK (con su definición) y constraints
  UNIQUE (vía `sys.indexes.is_unique_constraint`).
- Índices y foreign keys de todo el schema para el panel lateral de
  navegación de schema.
- Tipos definidos por el usuario (`sys.types where is_user_defined = 1`)
  clasificados como `Domain` (tipos alias) o `Composite` (table types).
- `view_details()` verifica que la vista existe en la base de datos
  solicitada.
- **Routines:** stored procedures (`P`), scalar functions (`FN`), inline
  table-valued functions (`IF`), multi-statement table-valued functions
  (`TF`), y CLR aggregates (`AF`) se listan por schema vía `sys.objects`. Las
  definiciones fuente se obtienen con `OBJECT_DEFINITION(object_id)`.

### CRUD con OUTPUT

- INSERT/UPDATE/DELETE sobre una fila usan la cláusula `OUTPUT INSERTED.*` /
  `OUTPUT DELETED.*` de SQL Server para que los datos de la fila
  post-mutación se devuelvan al caller (`CrudResult::success(row)`), de la
  misma forma que el driver de Postgres usa `RETURNING *`.
- `MutationCapabilities::supports_returning` es `true`.
- La identidad de fila debe ser una primary key compuesta (la única variante
  de `RecordIdentity` que tiene sentido para un driver relacional).

### Planificación de queries

- `explain()` ejecuta la query bajo `SET SHOWPLAN_XML ON` y devuelve el plan
  de query como XML. El driver siempre ejecuta `SET SHOWPLAN_XML OFF`
  después para que el estado de sesión no se filtre.
- `version_query()` devuelve `SELECT @@VERSION`.

### Dialecto

- Comillas de identificador con `[corchetes]` con escape de `]`.
- Literales de string Unicode `N'…'`; literales binarios `0x…` (en
  mayúsculas); `1`/`0` para valores booleanos (`BIT`).
- Paginación `OFFSET … ROWS FETCH NEXT … ROWS ONLY` (con un fallback
  `ORDER BY 1` para que las queries con OFFSET sin ORDER BY no den error).
- `SELECT TOP N` no se usa; OFFSET/FETCH es la forma canónica de paginación.
- `UPSERT` intencionalmente no se genera; `MERGE` en SQL Server tiene bugs
  conocidos y debería escribirse a mano.

### Reporte de errores

- Los errores de token `Server` de tiberius muestran su código numérico,
  severity state, y línea de origen a través de `FormattedError`.
- Los números de error comunes de MSSQL se mapean a variantes semánticas de
  `DbError` en vez del `QueryFailed` genérico:

  | Código(s)                                       | Variante de DbError      |
  | --------------------------------------------- | ----------------------- |
  | 4060, 18450, 18452, 18456, 18486, 18487, 18488 | `AuthFailed`            |
  | 229, 230, 262, 297, 916                       | `PermissionDenied`      |
  | 207, 208, 2812, 4902                          | `ObjectNotFound`        |
  | 245, 334, 515, 547, 2601, 2627, 8152          | `ConstraintViolation`   |
  | 102, 156, 8180                                | `SyntaxError`           |

- Los mensajes de violación de constraint se parsean para poblar
  `ErrorLocation` (schema, tabla, columna, nombre de constraint) para que la
  UI pueda resaltar el objeto en cuestión.

### Operaciones y límites

- Todas las operaciones declaran `transactional_ddl: true` y
  `supports_savepoints: true`.
- Niveles de aislamiento soportados: ReadUncommitted, ReadCommitted,
  RepeatableRead, Serializable, Snapshot. El valor por defecto es
  ReadCommitted.

## Comportamiento de DDL

- **DDL transaccional.** La mayoría del DDL en SQL Server es transaccional.
  Envolver `CREATE`, `ALTER`, o `DROP TABLE` dentro de
  `BEGIN TRAN … COMMIT` / `ROLLBACK` funciona. Excepciones: `CREATE DATABASE`,
  `DROP DATABASE`, `ALTER DATABASE`, `BACKUP`/`RESTORE`, y
  `CREATE FULLTEXT INDEX` no pueden ejecutarse dentro de una transacción
  explícita.
- **Locking de ALTER TABLE.** `ALTER TABLE … ADD COLUMN <nullable>` es rápido
  (solo metadata). Agregar una columna NOT NULL con un default escribe en
  cada página y toma un lock Sch-M. `ALTER TABLE … ALTER COLUMN` puede
  reescribir la tabla y bloquea lecturas y escrituras hasta que termina.
- **Operaciones de índice online** (Enterprise / Azure SQL):
  `CREATE INDEX … WITH (ONLINE = ON)` y
  `ALTER INDEX … REBUILD WITH (ONLINE = ON)` permiten DML concurrente. Sin
  `ONLINE = ON`, la construcción de índices toma un lock Sch-M y bloquea
  escrituras (Standard/Express solo soportan modo offline).
- **TRUNCATE TABLE.** Solo metadata, rápido, transaccional, requiere el
  permiso `ALTER` sobre la tabla. No puede usarse en tablas referenciadas por
  una foreign key (usa `DELETE` o elimina la FK primero).
- **DROP TABLE / DROP VIEW.** Transaccional. `IF EXISTS` está soportado desde
  2016+.
- **Constraints.** Agregar constraints `CHECK` / `UNIQUE` / `FOREIGN KEY`
  valida todas las filas existentes por defecto (toma un Sch-M brevemente).
  Usa `WITH NOCHECK` para agregar el constraint sin escanear, luego
  `WITH CHECK CHECK CONSTRAINT` más tarde para validar cuando quieras — el
  mismo patrón que `NOT VALID` + `VALIDATE CONSTRAINT` en Postgres.

## Limitaciones

- Las funcionalidades de Instance Metrics e Instance Inspector requieren el
  permiso de servidor `VIEW SERVER STATE`. Sin él, tanto `list_metrics()`
  como `list_inspectors()` devuelven listas vacías en vez de un error.

- Instance Metrics devuelve un único dato por llamada (valor actual de
  `sys.dm_os_performance_counters`), no una serie temporal histórica. Los
  contadores de tasa (p. ej. `mssql.batch_requests_per_sec`) representan el
  promedio en ejecución que reporta la DMV del lado del servidor, no un
  delta calculado por el driver.

- SQL Server mínimo soportado: 2016 (13.0). El driver usa la sintaxis
  `DROP INDEX IF EXISTS … ON …`, que los servidores más antiguos rechazan
  con un error de sintaxis (102). Azure SQL Database y Managed Instance
  funcionan bien.
- CRUD sobre tablas (o vistas actualizables) con triggers `INSTEAD OF` no
  está soportado. El driver devuelve la fila post-mutación vía
  `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` sin una cláusula `INTO`, que SQL
  Server rechaza con el error 334 ("the target table cannot have any
  enabled triggers if the statement contains an OUTPUT clause without
  INTO"). El error se muestra como `ConstraintViolation`.
- Driver solo SQL; no expone APIs de documentos ni de key-value.
- La cancelación elimina la sesión subyacente y reconecta de forma
  transparente; no es la cancelación quirúrgica vía TDS Attention que usa
  SSMS (tiberius actualmente no expone esa primitiva). En la práctica, la
  única diferencia visible para el usuario es que todo el estado local de
  sesión (opciones `SET`, tablas temporales, transacciones abiertas) se
  reinicia con la cancelación. La base de datos activa se restaura
  automáticamente.
- La latencia de cancelación depende del scheduler de SQL Server:
  típicamente unos pocos milisegundos para queries limitadas por CPU,
  inmediata para las que esperan un lock. Los rollbacks largos (p. ej.
  cancelar un `DELETE` grande a mitad de transacción) pueden mantener el
  SPID *del lado del servidor* en estado KILLED/ROLLBACK durante un rato
  después de que el driver ya pasó a una sesión nueva.
- No se usa parameter binding — las sentencias se despachan vía
  `simple_query`. Los helpers CRUD componen valores dentro del texto SQL a
  través del `SqlQueryBuilder` compartido y los formatters de literales del
  dialecto. Los payloads binarios o Unicode grandes se insertan como
  literales `0x…` o `N'…'`.
- Streaming: los result sets se materializan en `Vec<Row>`. El trait
  `Connection::execute` devuelve un `QueryResult` totalmente resuelto, así
  que el streaming al estilo cursor requeriría un cambio de API a nivel de
  workspace, no solo del driver.
- Los lotes multi-sentencia muestran cada result set no vacío vía
  `QueryResult.additional_results`, pero la UI actualmente solo renderiza el
  set primario (el último). Hasta que el sistema de pestañas de resultados
  lea `additional_results`, los sets anteriores son capturados por el driver
  pero invisibles en el editor.
- Los mensajes `PRINT` e informativos emitidos durante un lote se descartan.
  Mostrarlos requeriría manejar el `TokenStream` de bajo nivel de tiberius
  en vez de `QueryStream::into_results()`.
- `UPSERT` intencionalmente no se genera. Usa `MERGE` manualmente cuando lo
  necesites.
- Las instancias con nombre se respetan al conectar directamente (tiberius
  consulta el servicio SQL Browser sobre UDP 1434) pero no al pasar por un
  túnel SSH, ya que libssh2 solo hace forward de TCP. El workaround estándar
  es asignar un puerto TCP estático a la instancia y conectar directamente a
  ese puerto.
- La introspección de schema usa las vistas de catálogo `sys.*`; los
  usuarios sin el permiso `VIEW DEFINITION` por defecto verán metadata
  parcial. Aplican las reglas de visibilidad de metadata de SQL Server.
- Las rutinas CLR (funciones escalares CLR, funciones table-valued CLR,
  stored procedures CLR) y cualquier rutina creada con `ENCRYPTION` devuelven
  `NULL` desde `OBJECT_DEFINITION`, en cuyo caso el driver muestra un mensaje
  de fallback corto en vez de un error.
- `parameter_types` no se pobla para rutinas; `sys.parameters` no se
  consulta en esta implementación.
- SQL Server no tiene un tipo `Window` en la taxonomía de
  `sys.objects.type`; este driver nunca emite `RoutineKind::Window`.
- Sin toggle de integridad referencial para el flujo de migración del motor
  de transferencia de datos (`DriverCapabilities::DISABLE_FK_CHECKS` no está
  fijado; `Connection::set_referential_integrity` devuelve `NotSupported`).
  SQL Server deshabilita la comprobación de FK por tabla vía
  `ALTER TABLE ... NOCHECK CONSTRAINT`, lo cual no encaja con el toggle
  global único del motor; una variante por tabla es una posible mejora
  futura.

