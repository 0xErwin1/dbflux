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
- Muestra valores `numeric`, incluyendo arrays unidimensionales `numeric[]`,
  como el decimal exacto que guarda PostgreSQL.
- Muestra fechas y timestamps `infinity` y `-infinity`, y sus arrays
  unidimensionales, con el texto propio de PostgreSQL.
- Reporta su identidad de cliente al servidor como
  `application_name=dbflux/<version>`, salvo que la connection string ya defina
  `application_name`, en cuyo caso se conserva el valor del usuario.
- Verifica el privilegio de escritura al conectar: una réplica o una
  transacción en modo solo lectura resuelve a solo lectura sin importar los
  grants, y en caso contrario decide según los privilegios `INSERT`/`UPDATE`/
  `DELETE` del rol autenticado sobre las tablas base visibles; una base vacía
  o un fallo en la verificación es inconcluyente y deja intacta la política de
  mutación del perfil.

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

- Los límites de filas transmiten los resultados, retienen como máximo la cantidad solicitada en todo el pedido e informan por cada result set si omitió filas; la ejecución siempre termina por completo.
- Un lote multi-sentencia con límite de filas se ejecuta sentencia por sentencia por la misma ruta tipada de streaming, con un único presupuesto de filas compartido por todo el pedido. Las sentencias posteriores al agotamiento del presupuesto se siguen ejecutando, los result sets conservan el orden sin límite (el primero es el resultado principal) y el lote se detiene en el primer fallo.
- Un lote con límite de filas conserva los límites de transacción que tiene el mismo script cuando se ejecuta sin límite como una única simple query. Las sentencias fuera de una transacción del usuario se ejecutan dentro de una transacción que el driver abre y confirma, o revierte si hay un fallo, así que un fallo no deja ninguna de ellas aplicada. Un `BEGIN` en el script adopta las sentencias anteriores y un `COMMIT` o `ROLLBACK` la termina, como hace el bloque de transacción implícito de PostgreSQL. Dentro de una transacción de sesión abierta, las sentencias se suman a ella y un fallo la deja abortada.
- Los tiempos de espera solicitados se rechazan antes de ejecutar.

## Limitaciones

- El límite de filas restringe la retención, no el trabajo del servidor, el tráfico ni el tiempo; las mutaciones completan todos sus efectos.
- Los límites de filas en métricas e inspectores de instancia se rechazan antes del despacho. Los lotes sin límite conservan su comportamiento previo.
- Un lote con límite de filas rechaza, antes de ejecutar cualquier sentencia, `PREPARE TRANSACTION`, y un `SAVEPOINT`, `RELEASE`, `ROLLBACK TO` o `COMMIT`/`ROLLBACK` encadenado que sigue a sentencias fuera de una transacción explícita. PostgreSQL los rechaza dentro de su bloque de transacción implícito, mientras que la transacción que abre el driver los aceptaría.
- Las sentencias que no pueden ejecutarse dentro de un bloque de transacción, como `VACUUM` o `CREATE INDEX CONCURRENTLY`, fallan dentro de un lote con límite de filas con el error propio de PostgreSQL, igual que en un lote sin límite.
- Un lote con límite de filas se divide con el divisor de sentencias del editor SQL, que no reconoce los cuerpos de función `BEGIN ATOMIC` del estándar SQL. Un lote que contiene uno falla con un error de sintaxis y se revierte en lugar de ejecutarse; la misma función sola en un pedido se ejecuta normalmente.

- Las columnas de resultados en lote sin límite (multi-sentencia) no llevan metadata de
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
- Un valor no `NULL` de una sentencia única que el driver no puede
  decodificar, como un valor de un tipo sin decodificador o una fecha fuera del
  rango que puede representar, se muestra como tipo no soportado y queda
  marcado en el resultado; un `NULL` real sigue apareciendo como `NULL`.
  Convierte la columna a `text` para leer el texto del propio servidor. Los
  inspectores de instancia registran en el log la columna y el tipo de esa
  celda.
- Los valores `money` se muestran como tipo no soportado: el formato de
  transmisión lleva un importe entero cuya escala decimal proviene de la
  configuración `lc_monetary` del servidor, que el cliente no puede ver.
  Convierte la columna a `numeric` o `text` para leerla.
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
