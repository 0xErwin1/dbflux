# PostgreSQL

Base de datos relacional open-source avanzada.

## De un vistazo

- **Categoría** — Relacional
- **Query language** — SQL
- **Puerto por defecto** — 5432
- **Esquema de URI** — `postgresql`

## Funcionalidades

- Driver relacional de PostgreSQL con ejecución de queries SQL y descubrimiento
  de schema.
- Soporta schemas, tablas, vistas, índices, foreign keys, constraints CHECK,
  constraints UNIQUE, y tipos personalizados.
- Expone routines almacenadas (funciones, procedures, agregados, funciones
  window) en el árbol de schema con un visor de definición de solo lectura.
- Soporta autenticación, SSL, túnel SSH, y modos de conexión URI/manual.
- Soporta cancelación de queries a través de los cancel tokens de PostgreSQL.
- Incluye generación de SQL/código específica de PostgreSQL para CRUD, índices,
  reindex, foreign keys, y operaciones de tipos.
- Los scripts multi-sentencia (varias sentencias separadas por `;`) se ejecutan
  como un lote vía el simple query protocol, devolviendo un result set por
  sentencia.
- Motor de transferencia de datos: carga masiva nativa multi-fila con `INSERT`
  (`BULK_INSERT`), DDL `CREATE TABLE` nativo del driver a partir de las columnas
  de una tabla origen, soporte de `TRUNCATE TABLE`, y un toggle de integridad
  referencial (`SET session_replication_role`) para migraciones seguras con FK.
- Muestra valores `vector`, `halfvec`, y `sparsevec` de `pgvector`, incluyendo
  arrays unidimensionales verificados, como resultados textuales.
- Muestra valores `tsvector` y `tsquery` de búsqueda de texto completo,
  incluyendo arrays unidimensionales, en la forma de texto canónica de
  PostgreSQL.

### Instance Metrics

Expone un conjunto curado de métricas de servidor en vivo obtenidas de las
vistas de sistema de PostgreSQL:

- `pg.tps` — transacciones por segundo (de `pg_stat_database`)
- `pg.cache_hit_ratio` — ratio de aciertos del buffer cache (de
  `pg_statio_user_tables`)
- `pg.active_connections` — conexiones en estado `'active'`
- `pg.idle_connections` — conexiones en estado `'idle'`
- `pg.blocks_read` — bloques leídos desde disco (de `pg_statio_user_tables`)
- `pg.stat_statements.mean_exec_ms` — tiempo de ejecución medio por query
  (requiere la extensión `pg_stat_statements`)

Cada métrica se devuelve como una única fila `(timestamp_ms, value)` para
graficado en vivo.

### Instance Inspector

Expone snapshots tabulares del estado del servidor en ejecución:

- `pg.activity` — sesiones actuales de `pg_stat_activity` (texto de query,
  state, wait event, duración)
- `pg.locks` — locks activos de `pg_locks` unidos con `pg_class`

## Limitaciones

- Las columnas de resultados en lote (multi-sentencia) no llevan metadata de
  tipo; los valores se devuelven como texto y la auto-detección de gráficos está
  deshabilitada para ellas. Ejecuta una única sentencia para obtener columnas
  completamente tipadas.

- `pg.stat_statements.mean_exec_ms` solo está disponible cuando la extensión
  `pg_stat_statements` está instalada y cargada. El driver sondea su presencia
  al construir el catálogo; cuando está ausente, la métrica se omite de
  `list_metrics()`.

- Instance Metrics devuelve un único dato por llamada (snapshot actual), no una
  serie temporal histórica. La UI hace polling en el intervalo de refresco
  configurado para construir el gráfico en vivo.

- Driver solo SQL; no expone APIs de documentos ni de key-value.
- Las definiciones de routines para funciones agregadas y window se sintetizan a
  partir de metadata del catálogo porque `pg_get_functiondef` no las soporta.
- La edición y ejecución de routines no están soportadas; el visor de routines
  es de solo lectura.
- La cancelación es best effort y depende del estado del servidor/sesión en el
  momento de la cancelación.
- La generación de código apunta solo a construcciones de PostgreSQL soportadas;
  los IDs de generador no soportados devuelven `NotSupported`.

## Capacidades de DDL

### DDL transaccional

PostgreSQL soporta **DDL transaccional** — todas las operaciones de DDL (excepto
`CREATE INDEX CONCURRENTLY`) pueden envolverse en transacciones y revertirse con
rollback:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- Prueba el cambio
ROLLBACK;  -- Seguro de revertir si algo sale mal
```

**Excepción**: `CREATE INDEX CONCURRENTLY` y `DROP INDEX CONCURRENTLY` no pueden
ejecutarse dentro de una transacción.

### Comportamiento de ALTER TABLE

**Agregar columnas con defaults (PostgreSQL 11+)**:
- Rápido (operación solo de metadata)
- No requiere reescritura de tabla
- No bloquea la tabla para lecturas/escrituras

**Agregar columnas sin defaults**:
- Rápido (sin reescritura)
- Las filas existentes reciben `NULL` para la columna nueva

**Cambiar tipos de columna**:
- Puede requerir reescritura de tabla (bloquea la tabla)
- Usa la cláusula `USING` para conversión personalizada: `ALTER COLUMN age TYPE
  integer USING age::integer`

**Eliminar columnas**:
- Rápido (marca la columna como eliminada, sin reescritura)
- Los datos no se liberan inmediatamente (usa `VACUUM FULL` si es necesario)

**Renombrar columnas**:
- Rápido (solo metadata)
- Puede romper vistas, triggers, y código de la aplicación

### Operaciones de índice

**CREATE INDEX**:
- Bloquea la tabla para escrituras (lecturas permitidas)
- Usa `CONCURRENTLY` para creación de índices sin downtime:
  ```sql
  CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
  ```

**DROP INDEX**:
- Bloquea la tabla para escrituras (lecturas permitidas)
- Usa `CONCURRENTLY` para eliminación de índices sin downtime:
  ```sql
  DROP INDEX CONCURRENTLY idx_users_email;
  ```

**REINDEX**:
- Bloquea la tabla para lecturas y escrituras
- Usa `CONCURRENTLY` (PostgreSQL 12+) para reindex sin downtime

### Constraints

**Agregar constraints**:
- Los constraints `CHECK` y `UNIQUE` escanean la tabla (puede tomar tiempo en
  tablas grandes)
- Usa `NOT VALID` para diferir la validación:
  ```sql
  ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0) NOT VALID;
  -- Más tarde, valida sin bloquear:
  ALTER TABLE users VALIDATE CONSTRAINT age_check;
  ```

**Foreign keys**:
- Agregar foreign keys escanea ambas tablas
- Usa `NOT VALID` + `VALIDATE CONSTRAINT` para creación de FK sin downtime

### Tipos personalizados

**CREATE TYPE (enum)**:
- Rápido (solo metadata)
- Usa `ALTER TYPE ... ADD VALUE` para agregar valores enum:
  ```sql
  ALTER TYPE status_enum ADD VALUE 'archived';
  ```
  **Nota**: no se puede revertir con rollback dentro de una transacción (se
  confirma inmediatamente)

**DROP TYPE**:
- Falla si el tipo está en uso por tablas
- Debes eliminar primero las columnas dependientes

### Limitaciones conocidas

- `CREATE INDEX CONCURRENTLY` requiere un lock exclusivo momentáneamente (puede
  bloquear en tablas de alto tráfico)
- `ALTER TYPE ADD VALUE` no se puede revertir con rollback
- Eliminar columnas no libera el espacio en disco inmediatamente (requiere
  `VACUUM FULL`)
