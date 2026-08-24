# MySQL y MariaDB

Base de datos relacional open-source popular.

## De un vistazo

- **Categoría** — Relacional
- **Query language** — SQL
- **Puerto por defecto** — 3306
- **Esquema de URI** — `mysql`

## Funcionalidades

- Implementaciones de driver relacional para MySQL y MariaDB en un solo crate.
- Soporta ejecución de SQL, descubrimiento de schema, índices, foreign keys,
  constraints CHECK, y constraints UNIQUE.
- Soporta autenticación, túnel SSH, y modos de conexión URI/manual.
- TLS con los cinco modos SSL nativos (`DISABLED`, `PREFERRED`, `REQUIRED`,
  `VERIFY_CA`, `VERIFY_IDENTITY`): `VERIFY_CA` verifica la cadena del servidor
  sin validar el hostname y `VERIFY_IDENTITY` verifica ambos. Una CA raíz
  personalizada reemplaza el trust store del sistema en los modos que verifican,
  y un certificado de cliente + clave habilita mutual TLS. Usa el backend
  `rustls`/`aws-lc-rs`.
- Soporta cancelación de queries a través de un camino de cancelación dedicado
  (flujo `KILL QUERY`).
- Incluye generación de SQL/código para CRUD, índices, foreign keys, y
  operaciones de DDL de tabla.
- Descubrimiento de routines: lista stored procedures y funciones definidas por
  el usuario desde `information_schema.ROUTINES` incluyendo tipos de parámetros
  y hints de tipo de retorno (solo Functions).
- Definición de routines: obtiene el cuerpo completo de `CREATE FUNCTION` o
  `CREATE PROCEDURE` vía `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` (de solo
  lectura; la definición no es editable ni ejecutable en el visor).
- Los scripts multi-sentencia (varias sentencias separadas por `;`) se dividen y
  ejecutan sentencia por sentencia, cada una a través del camino preparado
  tipado, devolviendo un result set por sentencia.
- Motor de transferencia de datos: carga masiva nativa multi-fila con `INSERT`
  (`BULK_INSERT`), DDL `CREATE TABLE` nativo del driver a partir de las columnas
  de una tabla origen, soporte de `TRUNCATE TABLE`, y un toggle de integridad
  referencial (`SET FOREIGN_KEY_CHECKS`) para migraciones seguras con FK. Tanto
  MySQL como MariaDB comparten este soporte.

### Instance Metrics

Expone un conjunto curado de métricas de servidor en vivo obtenidas de `SHOW
GLOBAL STATUS`:

- `mysql.threads_connected` — conexiones abiertas actualmente
- `mysql.threads_running` — queries en ejecución actualmente
- `mysql.queries_per_sec` — queries por segundo (contador acumulativo)
- `mysql.innodb_buffer_pool_hit_ratio` — eficiencia de lectura del buffer pool
  de InnoDB
- `mysql.innodb_rows_read` — filas leídas del storage engine InnoDB
- `mysql.innodb_rows_inserted` — filas insertadas en InnoDB
- `mysql.innodb_rows_updated` — filas actualizadas en InnoDB
- `mysql.innodb_rows_deleted` — filas eliminadas en InnoDB
- `mysql.slow_queries` — conteo acumulativo de slow queries
- `mysql.table_locks_waited` — contador de contención de locks a nivel de tabla
- `mysql.bytes_sent` — bytes de red enviados

Cada métrica se devuelve como una única fila `(timestamp_ms, value)` para
graficado en vivo.

### Instance Inspector

Expone snapshots tabulares del estado del servidor en ejecución:

- `mysql.processlist` — sesiones activas de `information_schema.PROCESSLIST`
  (user, host, db, command, time, state, info)

## Limitaciones

- Driver solo SQL; no expone APIs de documentos ni de key-value.

- Instance Metrics devuelve un único dato por llamada (snapshot actual de `SHOW
  GLOBAL STATUS`), no una serie temporal histórica. Los contadores acumulativos
  (p. ej. `mysql.bytes_sent`) crecen de forma monótona — interprétalos como
  deltas entre muestras en vez de tasas absolutas.

- El sondeo de disponibilidad de `performance_schema` se ejecuta una vez al
  construir el catálogo. Cuando `performance_schema` no está presente, las
  métricas específicas de performance schema se omiten de `list_metrics()`. El
  conjunto de métricas estáticas (basado en `SHOW GLOBAL STATUS`) siempre está
  disponible.

- Un script multi-sentencia ejecuta cada sentencia secuencialmente en vez de
  como un único lote atómico del lado del servidor; la división de sentencias es
  basada en texto y puede dividir incorrectamente cuerpos de stored programs que
  contienen `;` (p. ej. `CREATE PROCEDURE ... BEGIN ... END`).
- La cancelación depende de los permisos del servidor y el estado de la conexión
  cuando se emite `KILL QUERY`.
- La generación de código está limitada a las construcciones MySQL/MariaDB
  soportadas; los IDs de generador no soportados devuelven `NotSupported`.
- El listado de routines cubre solo los tipos FUNCTION y PROCEDURE. Las
  funciones agregadas de MySQL (registradas vía el plugin UDF `CREATE AGGREGATE
  FUNCTION`) y las funciones window no aparecen en `information_schema.ROUTINES`
  y por lo tanto no se listan.
- `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` requiere el privilegio
  `SHOW_ROUTINE` (MySQL 8.0+) o ser propietario de la rutina; sin privilegios
  suficientes la columna de definición devuelve `NULL` y el visor muestra un
  aviso en vez del código fuente.

## Capacidades de DDL

### DDL no transaccional

**CRÍTICO**: las operaciones de DDL en MySQL **NO son transaccionales** — no se
pueden revertir con rollback:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- El DDL se confirma inmediatamente, ¡ROLLBACK no tiene efecto!
ROLLBACK;  -- Demasiado tarde, la columna ya fue agregada
```

**Excepción**: `RENAME TABLE` es atómico (seguro de usar dentro de
transacciones).

### Comportamiento de ALTER TABLE

**Reescrituras de tabla**:
- La mayoría de las operaciones `ALTER TABLE` reescriben toda la tabla (bloquea
  la tabla durante la operación)
- Usa `ALGORITHM=INPLACE` y `LOCK=NONE` para DDL online (MySQL 5.6+):
  ```sql
  ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL, ALGORITHM=INPLACE, LOCK=NONE;
  ```

**Agregar columnas**:
- Agregar columna al **final de la tabla**: rápido (solo metadata)
- Agregar columna en **medio de la tabla**: reescritura de tabla (bloquea la
  tabla)
- Usa `AFTER column_name` para controlar la posición

**Agregar columnas con defaults**:
- Reescritura de tabla (bloquea la tabla)
- El valor por defecto se escribe en todas las filas existentes

**Cambiar tipos de columna**:
- Siempre requiere reescritura de tabla (bloquea la tabla)
- La conversión de datos ocurre durante la reescritura

**Eliminar columnas**:
- Reescritura de tabla (bloquea la tabla)
- Los datos se eliminan inmediatamente

**Renombrar columnas**:
- Reescritura de tabla (bloquea la tabla)
- Puede romper vistas, triggers, y código de la aplicación

### Operaciones de índice

**CREATE INDEX**:
- Bloquea la tabla para escrituras (lecturas permitidas)
- Usa `ALGORITHM=INPLACE, LOCK=NONE` para creación de índices online:
  ```sql
  CREATE INDEX idx_users_email ON users(email) ALGORITHM=INPLACE, LOCK=NONE;
  ```

**DROP INDEX**:
- Bloquea la tabla para escrituras (lecturas permitidas)
- Usa `ALGORITHM=INPLACE, LOCK=NONE` para eliminación de índices online

### Constraints

**Foreign keys**:
- Agregar foreign keys escanea ambas tablas (bloquea ambas)
- Usa `ALGORITHM=INPLACE, LOCK=NONE` cuando sea posible

**Constraints UNIQUE**:
- Requiere creación de índice (bloquea la tabla)

**Constraints CHECK** (MySQL 8.0.16+):
- Solo metadata (rápido)
- Validado solo en INSERT/UPDATE

### DDL online (MySQL 5.6+)

**Opciones de ALGORITHM**:
- `INPLACE` — modifica la tabla en su lugar (sin copia)
- `COPY` — crea una nueva tabla y copia las filas (por defecto en versiones
  antiguas de MySQL)
- `INSTANT` — solo metadata (MySQL 8.0+, operaciones limitadas)

**Opciones de LOCK**:
- `NONE` — permite lecturas y escrituras concurrentes
- `SHARED` — permite lecturas, bloquea escrituras
- `EXCLUSIVE` — bloquea lecturas y escrituras

**Ejemplo**:
```sql
ALTER TABLE users 
  ADD COLUMN phone VARCHAR(20) NULL,
  ALGORITHM=INPLACE,
  LOCK=NONE;
```

### Limitaciones conocidas

- El DDL no es transaccional (no se puede revertir con rollback)
- La mayoría de las operaciones `ALTER TABLE` reescriben toda la tabla (bloquea
  la tabla)
- Agregar una columna en medio de la tabla requiere reescritura
- El soporte de DDL online varía según la versión de MySQL
- Usa `pt-online-schema-change` (Percona Toolkit) para DDL sin downtime en
  tablas grandes

### Buenas prácticas

1. **Prueba primero en una copia** — el DDL no se puede revertir con rollback
2. **Usa DDL online** — agrega `ALGORITHM=INPLACE, LOCK=NONE` cuando esté
   soportado
3. **Planifica ventanas de mantenimiento** — ejecuta el DDL en periodos de bajo
   tráfico
4. **Monitorea el tamaño de la tabla** — las tablas grandes tardan más en
   reescribirse
5. **Usa pt-online-schema-change** — para DDL sin downtime en tablas de
   producción
