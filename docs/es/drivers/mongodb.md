# MongoDB

Base de datos de documentos para aplicaciones modernas.

## De un vistazo

- **Categoría** — Documento
- **Lenguaje de query** — Sintaxis de query de MongoDB
- **Puerto por defecto** — 27017
- **Esquema de URI** — `mongodb`

Driver de documentos MongoDB para DBFlux.

## Funcionalidades

- Driver de documentos clasificado como `DatabaseCategory::Document` con el
  lenguaje de query `MongoQuery`; el editor usa sintaxis de shell de MongoDB, no
  SQL.
- Modos de conexión: manual (host/port/credenciales/database) y modo URI. El
  modo URI acepta cadenas de conexión `mongodb://` y `mongodb+srv://` (los
  registros SRV se parsean para el descubrimiento de replica-set).
- Múltiples databases lógicas (`MULTIPLE_DATABASES`) con browsing de collections
  y conteo de documentos.
- Autenticación (`AUTHENTICATION`) y TLS/SSL con tres modos (`off`, `on`,
  `verify`), soportando un certificado raíz y un certificado de cliente
  opcional.
- Soporte de túnel SSH para llegar a MongoDB a través de un bastion host.
- Parseo de queries en estilo shell para las formas `db.collection.method(...)`
  y `db.method(...)`, con un fallback de documento JSON para
  retrocompatibilidad. Métodos soportados: `find`, `findOne`, `aggregate`,
  `count`/`countDocuments`, `insertOne`, `insertMany`, `updateOne`,
  `updateMany`, `deleteOne`, `deleteMany`. Los errores de parseo llevan
  posiciones de byte-offset para los diagnósticos del editor.
- Pipelines de agregación (`AGGREGATION`); las capacidades de query anuncian
  order-by, group-by, having, limit y offset.
- Operadores WHERE: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, y los
  lógicos `And`/`Or`/`Not`.
- Paginación mediante los estilos cursor y page-token
  (`PaginationStyle::Cursor`, `PaginationStyle::PageToken`).
- Metadata de schema centrada en documentos: campos e índices de collection
  (`INDEXES`), con documentos anidados y arrays mapeados a la vista de árbol de
  documentos (`NESTED_DOCUMENTS`, `ARRAYS`).
- Mutaciones: insert, update (incluyendo upsert) y delete (`supports_upsert:
  true`). `MongoShellGenerator` emite `insertOne`/`insertMany`,
  `updateOne`/`updateMany` (con `{ upsert: true }`), y `deleteOne`/`deleteMany`
  para vistas previas y copy-as-query.
- DDL: drop database, drop collection, create index y drop index.
- Exportación de resultados a JSON (`EXPORT_JSON`).
- Reporta la identidad de cliente como `appName=dbflux/<version>` al conectar
  (visible en los logs del servidor y en `db.currentOp()`), salvo que la URI de
  conexión ya defina un `appName`.

### Instance Metrics

Expone un conjunto seleccionado de métricas de servidor en vivo tomadas del
comando `serverStatus` de MongoDB. Las métricas se extraen mediante recorrido de
rutas con puntos en BSON:

- `mongo.connections_current` — conexiones abiertas actualmente
- `mongo.connections_available` — slots de conexión disponibles
- `mongo.opcounters_insert` — operaciones insert desde el arranque
- `mongo.opcounters_query` — operaciones query desde el arranque
- `mongo.opcounters_update` — operaciones update desde el arranque
- `mongo.opcounters_delete` — operaciones delete desde el arranque
- `mongo.opcounters_getmore` — operaciones getMore desde el arranque
- `mongo.mem_resident` — memoria residente en MB
- `mongo.mem_virtual` — memoria virtual en MB
- `mongo.network_bytes_in` — bytes recibidos desde el arranque

Cada métrica se devuelve como una única fila `(timestamp_ms, value)` para
graficado en vivo.

### Instance Inspector

Expone snapshots tabulares del estado del servidor en ejecución:

- `mongo.current_op` — operaciones en curso desde el pipeline de agregación
  `$currentOp` (opid, type, ns, op, secs_running, wait_for_lock)

## Limitaciones

- SQL no está soportado; las queries deben usar sintaxis estilo shell de MongoDB
  (o el fallback JSON).

- Las métricas de instancia devuelven un único punto de datos por llamada
  (snapshot actual de `serverStatus`), no una serie temporal histórica. Los
  contadores de operaciones (p. ej. `mongo.opcounters_insert`) crecen de forma
  monótona — interprétalos como deltas entre muestras en lugar de tasas
  absolutas.

- `$currentOp` requiere el privilegio `inprog` o el rol `clusterMonitor` en
  clusters de Atlas. Sin privilegios suficientes,
  `fetch_inspector_snapshot("mongo.current_op")` devuelve un result set vacío.
- La cancelación de query no está soportada (`QUERY_CANCELLATION` no está
  establecida).
- `RETURNING` no está soportado; las capacidades de mutación también reportan
  sin batch, sin update masivo y sin delete masivo a nivel de capacidad
  (`supports_batch`, `supports_bulk_update`, `supports_bulk_delete` son todos
  `false`), aunque el generador pueda emitir texto `updateMany`/`deleteMany`.
- La cobertura del parser está intencionalmente acotada al conjunto de métodos
  soportado arriba, no al lenguaje completo del shell interactivo; `distinct` no
  se expone como capacidad de query (`supports_distinct: false`).
- Sin joins, subqueries, uniones, CTEs, funciones de ventana ni `EXPLAIN` a
  nivel de capacidad de query.
- Las transacciones se anuncian a nivel de capacidad (`supports_transactions:
  true`) pero sin niveles de aislamiento, savepoints, transacciones anidadas,
  read-only ni soporte deferrable.
- El DDL no es transaccional (`transactional_ddl: false`); create-database,
  create-collection, alter, views y triggers no están soportados.
