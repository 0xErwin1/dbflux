# Redis

Base de datos clave-valor en memoria.

## De un vistazo

- **Categoría** — Clave-valor
- **Lenguaje de query** — Comandos Redis
- **Puerto por defecto** — 6379
- **Esquema de URI** — `redis`

Driver de clave-valor Redis para DBFlux, construido sobre el crate
[`redis`](https://crates.io/crates/redis).

## Funcionalidades

- Driver clave-valor clasificado como `DatabaseCategory::KeyValue` con el
  lenguaje de query `RedisCommands`; el editor usa sintaxis de comandos Redis,
  no SQL.
- Modos de conexión: manual (host/port/user/password/database) y modo URI. El
  modo URI acepta cadenas de conexión `redis://` y `rediss://`.
- Múltiples databases lógicas mediante `SELECT <db>` (`MULTIPLE_DATABASES`). El
  índice de la database activa se rastrea en la conexión.
- Autenticación con username + password opcionales (`AUTHENTICATION`).
- Reporta su identidad de cliente al servidor vía `CLIENT SETNAME` al conectar
  (`dbflux/<version>`, visible en `CLIENT LIST`); best-effort, ya que algunos
  proveedores managed restringen los comandos `CLIENT`.
- TLS/SSL con tres modos (`off`, `on`, `verify`):
  - `off` — conexión `redis://` plana.
  - `on` — `rediss://` con el certificado confiado sin validación de cadena
    (marcador inseguro).
  - `verify` — `rediss://` con un certificado raíz suministrado y
    certificado/clave de cliente opcionales, construido a través de
    `Client::build_with_tls`.
- Soporte de túnel SSH para llegar a Redis a través de un bastion host (solo en
  modo manual; ver Limitaciones).
- Exploración y descubrimiento de claves:
  - Escaneo de claves basado en cursor (`KV_SCAN`, `PaginationStyle::Cursor`).
  - Descubrimiento de tipo por clave (`KV_KEY_TYPES`) entre string, hash, list,
    set, sorted set y stream.
  - Inspección de TTL (`KV_TTL`) y reporte de tamaño de valor (`KV_VALUE_SIZE`).
  - Comprobaciones de existencia (`KV_GET`/`KV_EXISTS`), renombrado de claves
    (`KV_RENAME`) y obtención masiva de múltiples claves (`KV_BULK_GET`).
- Cobertura de tipos de valor: strings, hashes, lists, sets, sorted sets y
  streams, incluyendo lecturas de rango de stream, adición de entradas de stream
  y eliminación de entradas de stream (`KV_STREAM_RANGE`, `KV_STREAM_ADD`,
  `KV_STREAM_DELETE`).
- Límite de vista previa de stream configurable, expuesto como ajuste de
  conexión.
- Mutaciones: insert, update, delete, operaciones por lotes y eliminación
  masiva. `RedisCommandGenerator` emite comandos Redis para set/delete, hash
  set/delete, list push/set/remove, set add/remove, sorted-set add/remove y
  stream add/delete, para su uso en vistas previas y copy-as-command.
- Exportación de resultados a JSON (`EXPORT_JSON`).
- Gate de tamaño en lecturas de payload completo: cuando el request trae un
  presupuesto de bytes, los valores string/JSON se sondean con `STRLEN` antes
  del `GET` y los valores que lo exceden devuelven un placeholder con el tamaño
  real en lugar de transferir el payload; los tipos de colección no se ven
  afectados, y las lecturas de stream que alcanzan el tope de fetch se reportan
  como truncadas.

### Instance Metrics

Expone un conjunto seleccionado de métricas de servidor en vivo tomadas de la
salida del comando `INFO`:

- `redis.connected_clients` — clientes actualmente conectados
- `redis.blocked_clients` — clientes esperando en un comando bloqueante
- `redis.used_memory` — bytes asignados por el allocator de Redis
- `redis.used_memory_rss` — bytes asignados por el sistema operativo (resident
  set size)
- `redis.total_commands_processed` — comandos procesados acumulados
- `redis.total_connections_received` — conexiones aceptadas acumuladas
- `redis.instantaneous_ops_per_sec` — comandos procesados por segundo (tasa del
  lado del servidor)
- `redis.keyspace_hits` — aciertos de caché en búsquedas de claves
- `redis.keyspace_misses` — fallos de caché en búsquedas de claves
- `redis.evicted_keys` — claves desalojadas por la política `maxmemory`
- `redis.expired_keys` — claves expiradas por TTL
- `redis.rdb_changes_since_last_save` — cambios desde el último snapshot RDB
- `redis.connected_slaves` — cantidad de réplicas conectadas

Cada métrica se devuelve como una única fila `(timestamp_ms, value)` para
graficado en vivo.

### Instance Inspector

Expone snapshots tabulares del estado del servidor en ejecución:

- `redis.client_list` — clientes activos desde `CLIENT LIST` (id, cmd, age,
  idle, flags, db, sub, multi)

Los campos sensibles (`addr`, `laddr`, `name`) se redactan a `[redacted]` para
evitar exponer direcciones IP y hostnames de clientes.

## Limitaciones

- SQL no está soportado; las queries deben escribirse como comandos Redis.

- Las métricas de instancia devuelven un único punto de datos por llamada
  (snapshot actual de `INFO`), no una serie temporal histórica. Los contadores
  acumulativos (p. ej. `redis.total_commands_processed`) crecen de forma
  monótona — interprétalos como deltas entre muestras en lugar de tasas
  absolutas.

- El inspector `CLIENT LIST` redacta los campos `addr`, `laddr` y `name` en cada
  fila para evitar exponer direcciones IP y nombres suministrados por el usuario
  a la UI.

- La cancelación de query no está soportada (`QUERY_CANCELLATION` no está
  establecida); los comandos de larga duración no se pueden abortar desde la UI.
- Sin upsert (`supports_upsert: false`), sin `RETURNING` y sin update masivo
  (`supports_bulk_update: false`).
- Las capacidades DDL están todas deshabilitadas (sin tables, views, indexes,
  schemas) — esto es un almacén clave-valor, no relacional.
- Las transacciones se anuncian a nivel de capacidad (`supports_transactions:
  true`) pero sin niveles de aislamiento, savepoints, transacciones anidadas,
  read-only ni soporte deferrable.
- Pub/Sub no está expuesto (la capacidad `PUBSUB` no está establecida).
- El túnel SSH no está disponible cuando el modo URI está habilitado; la ruta
  del túnel solo está conectada para el modo de conexión manual.
- Los grupos de consumidores de stream no están modelados; solo se soportan
  lecturas de rango, adición de entradas y eliminación de entradas.
