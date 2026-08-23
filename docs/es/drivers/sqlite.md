# SQLite

Base de datos embebida basada en archivos.

## De un vistazo

- **Categoría** — Relacional
- **Query language** — SQL
- **Esquema de URI** — `sqlite`

## Funcionalidades

- Driver relacional SQLite embebido usando rutas de base de datos basadas en
  archivos.
- Soporta ejecución de SQL, descubrimiento de schema, vistas, índices, foreign
  keys, constraints CHECK, y constraints UNIQUE.
- Soporta cancelación de queries vía los handles de interrupt de SQLite.
- Incluye generación de SQL/código para CRUD, índices, reindex, create table, y
  drop table.
- Los scripts multi-sentencia (varias sentencias separadas por `;`) se dividen y
  ejecutan sentencia por sentencia, cada una a través del camino preparado
  tipado, devolviendo un result set por sentencia. (`rusqlite::prepare` solo
  parsea la primera sentencia de un string, así que un script debe dividirse.)
- Motor de transferencia de datos: carga masiva nativa multi-fila con `INSERT`
  (`BULK_INSERT`), DDL `CREATE TABLE` nativo del driver a partir de las columnas
  de una tabla origen, y un toggle de integridad referencial por conexión
  (`PRAGMA foreign_keys`) para migraciones seguras con FK.

## Limitaciones

- Driver solo de archivo local; sin transporte de red, túnel SSH, ni modo
  TLS/SSL.
- Driver solo SQL; no expone APIs de documentos ni de key-value.
- El modelo de schema de SQLite no tiene un equivalente de namespace
  multi-schema del lado del servidor.
- No existe la sentencia `TRUNCATE TABLE`; la opción de carga Truncate del motor
  de transferencia de datos no está disponible para destinos SQLite
  (`DriverCapabilities::TRUNCATE_TABLE` no está fijado).

## Capacidades de DDL

### DDL transaccional

SQLite soporta **DDL transaccional** — todas las operaciones de DDL pueden
envolverse en transacciones y revertirse con rollback:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone TEXT NULL;
-- Prueba el cambio
ROLLBACK;  -- Seguro de revertir si algo sale mal
```

### Limitaciones de ALTER TABLE

**CRÍTICO**: SQLite tiene soporte **muy limitado** de `ALTER TABLE`:

**Operaciones soportadas**:
- `ADD COLUMN` (solo al final de la tabla)
- `RENAME COLUMN` (SQLite 3.25.0+)
- `RENAME TABLE`

**NO soportadas**:
- `DROP COLUMN` (requiere recreación de la tabla)
- `ALTER COLUMN` (el cambio de tipo requiere recreación de la tabla)
- `ADD COLUMN` en medio de la tabla (requiere recreación de la tabla)

### Patrón de recreación de tabla

Para operaciones de `ALTER TABLE` no soportadas, usa el patrón de recreación de
tabla:

```sql
BEGIN;

-- 1. Crea una tabla nueva con el schema deseado
CREATE TABLE users_new (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT,
  -- columna phone eliminada, columna age agregada
  age INTEGER
);

-- 2. Copia los datos de la tabla vieja
INSERT INTO users_new (id, email, name, age)
  SELECT id, email, name, NULL FROM users;

-- 3. Elimina la tabla vieja
DROP TABLE users;

-- 4. Renombra la tabla nueva
ALTER TABLE users_new RENAME TO users;

COMMIT;
```

**IMPORTANTE**: este patrón pierde:
- Las referencias de foreign key desde otras tablas
- Los triggers en la tabla original
- Los índices en la tabla original (deben recrearse)

### Operaciones de índice

**CREATE INDEX**:
- Bloquea la base de datos durante la operación (bloquea escrituras)
- Sin opción concurrente (a diferencia de PostgreSQL)

**DROP INDEX**:
- Rápido (solo metadata)

**REINDEX**:
- Reconstruye el índice (bloquea la base de datos)

### Constraints

**Agregar constraints**:
- SQLite valida los constraints en el momento de `INSERT`/`UPDATE`
- No se pueden agregar constraints a tablas existentes (requiere recreación de
  la tabla)

**Foreign keys**:
- Deshabilitadas por defecto (deben habilitarse con `PRAGMA foreign_keys = ON`)
- No se pueden agregar a tablas existentes (requiere recreación de la tabla)

### Limitaciones conocidas

- Sin `DROP COLUMN` (requiere recreación de la tabla)
- Sin `ALTER COLUMN` (requiere recreación de la tabla)
- No se pueden agregar constraints a tablas existentes
- Sin creación de índices concurrente (bloquea la base de datos)
- Tipado dinámico (los tipos de columna son solo indicativos)

### Buenas prácticas

1. **Usa transacciones** — el DDL es transaccional, envuelve siempre en
   `BEGIN`/`COMMIT`
2. **Planifica el schema con anticipación** — es difícil de modificar después
3. **Usa el patrón de recreación de tabla** — para operaciones de `ALTER TABLE`
   no soportadas
4. **Recrea índices y triggers** — después de recrear la tabla
5. **Prueba primero en una copia** — especialmente para el patrón de recreación
   de tabla
6. **Habilita foreign keys** — `PRAGMA foreign_keys = ON` antes de alterar el
   schema
7. **Usa VACUUM** — para liberar espacio en disco después de `DROP TABLE` o de
   recrear una tabla
