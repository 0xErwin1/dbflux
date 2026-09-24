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
- Aplica el límite de filas solicitado en toda sentencia única que produce filas
  (`SELECT`, `PRAGMA`, `EXPLAIN`, `WITH ... SELECT`, `VALUES` y DML con
  `RETURNING`) reteniendo solo las filas pedidas durante la iteración: la iteración drena hasta el final, así que
  los efectos de una mutación siempre se completan, un error tardío por fila
  aún se propaga, y el resultado informa cuando se omitieron filas.
- Ejecuta un lote multi-sentencia con límite de filas sentencia por sentencia,
  con un único presupuesto de filas compartido por todo el pedido. El lote se
  divide con el lexer propio de SQLite, así que una barra invertida no es un
  escape y los punto y coma dentro de literales, comentarios y cuerpos de
  triggers no dividen una sentencia; los comentarios o punto y coma finales no
  crean una sentencia extra. Cada sentencia que produce filas retiene filas solo
  mientras quede presupuesto, las sentencias posteriores al agotamiento del
  presupuesto se siguen ejecutando, y el lote se detiene en el primer fallo
  exactamente como un lote sin límite.
- Rechaza, antes de cualquier preparación o ejecución, los pedidos que no puede
  acotar de forma segura: un pedido con límite de filas dirigido a instance
  metrics o inspectors, y un statement timeout solicitado.
- Motor de transferencia de datos: carga masiva nativa multi-fila con `INSERT`
  (`BULK_INSERT`), DDL `CREATE TABLE` nativo del driver a partir de las columnas
  de una tabla origen, y un toggle de integridad referencial por conexión
  (`PRAGMA foreign_keys`) para migraciones seguras con FK.

## Limitaciones

- Driver solo de archivo local; sin transporte de red, túnel SSH, ni modo
  TLS/SSL.
- El límite de filas solicitado es un tope de retención, no un límite del
  motor: la sentencia sigue ejecutándose hasta el final dentro del motor
  embebido, todas las filas que pasan el tope se observan y descartan, y no se
  aplica ningún presupuesto de bytes, memoria ni tiempo — las asignaciones del
  sorter y de `RETURNING` no están acotadas por el tope. Una mutación con
  límite de filas completa todos sus efectos.
- Dividir un lote con límite de filas no prepara nada, pero consulta el texto de
  la sentencia con el lexer de SQLite en cada `;`, lo que es cuadrático en la
  longitud de una sentencia con muchos punto y coma dentro de literales,
  comentarios o cuerpos de triggers. Los lotes sin límite mantienen el
  comportamiento previo de división y ejecución.
- Sin límite de filas, `WITH ... SELECT`, `VALUES` y DML con `RETURNING`
  mantienen el comportamiento previo: reportan un error de ejecución no
  soportada después de que la sentencia ya se ejecutó.
- Un pedido con límite de filas dirigido a las instance metrics o inspectors
  del driver se rechaza antes del lock de conexión o de cualquier dispatch — el
  tipo público de pedido puede llevar esos contextos aunque el driver no
  anuncie instance catalog — mientras que los pedidos sin tope mantienen el
  comportamiento existente.
- Un statement timeout solicitado (`QueryRequest::statement_timeout`) no está
  soportado y se rechaza antes de la ejecución. Los queries normales sin tope
  siguen siendo cancelables por la vía de interrupción existente.
- Driver solo SQL; no expone APIs de documentos ni de key-value.
- El modelo de schema de SQLite no tiene un equivalente de namespace
  multi-schema del lado del servidor.
- No existe la sentencia `TRUNCATE TABLE`; la opción de carga Truncate del motor
  de transferencia de datos no está disponible para destinos SQLite
  (`DriverCapabilities::TRUNCATE_TABLE` no está fijado).

## Capacidades de DDL

### DDL transaccional

SQLite soporta **DDL transaccional** — las operaciones manuales de DDL pueden
envolverse en transacciones y revertirse con rollback:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone TEXT NULL;
-- Prueba el cambio
ROLLBACK;  -- Seguro de revertir si algo sale mal
```

Las alteraciones administradas de tablas son diferentes: su planificador requiere
una conexión en autocommit porque el driver controla la transacción por tabla.

### ALTER TABLE administrado

Para una solicitud que solo contiene `DROP COLUMN`, DBFlux usa primero la ruta
nativa `DROP COLUMN` de SQLite cuando la versión de SQLite enlazada la soporta.
Los drops nativos conservan las reglas de aceptación de SQLite; no están
limitados por la gramática de reconstrucción. DBFlux sigue verificando
previamente las dependencias conocidas y SQLite vuelve a validar la solicitud al
ejecutarse.

Para cambios seleccionados de tipo, nulabilidad o valor predeterminado, y para
un drop aislado cuando la versión de SQLite enlazada no dispone de `DROP COLUMN`
nativo, DBFlux usa una reconstrucción conservadora de una tabla del esquema
`main`. Una solicitud de reconstrucción también puede combinar esos cambios de
columna con drops.

La reconstrucción copia las filas retenidas sin casts, conversión ni backfill.
Preserva exactamente la identidad de fila comprobada y los valores almacenados;
un valor predeterminado modificado solo se aplica a inserciones futuras. Por ello,
hacer una columna obligatoria puede fallar si las filas existentes son
incompatibles.

La preparación es de solo lectura. La vista previa expuesta mediante la UI
genérica y MCP es una descripción inmutable e ilustrativa del ciclo de vida, no
SQL ejecutable para aplicar. El driver administra la tabla de reemplazo privada,
la copia y comparación exacta, el reemplazo de la tabla origen, la restauración
de índices explícitos comprobados y la validación final.

Cada reconstrucción es atómica para una única tabla. Ante un fallo, el driver
verifica el rollback y la restauración de los ajustes de conexión. Los fallos de
limpieza siguen siendo visibles; un estado de transacción incierto o un fallo al
restaurar los ajustes pone la conexión en cuarentena hasta reconectarla. No existe
atomicidad entre tablas.

#### Alcance de la reconstrucción

La ruta de reconstrucción solo acepta formas de tabla que puede demostrar seguras
en el esquema `main`. Rechaza, por ejemplo, constraints CHECK, columnas
generadas, `AUTOINCREMENT`, `STRICT`, `WITHOUT ROWID`, vistas o triggers,
índices de expresión o parciales, esquemas adjuntos y transacciones activas del
llamador. Estos límites solo se aplican a las reconstrucciones: un drop nativo
aislado que sea aplicable no se rechaza solo por estar fuera de la gramática de
reconstrucción.

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

- El planificador administrado de alteración de tablas cambia tipos, nulabilidad, valores predeterminados y drops seleccionados; no agrega constraints a una tabla existente.
- Las reconstrucciones se limitan deliberadamente a las formas de tabla comprobadas descritas antes.
- Sin creación de índices concurrente (bloquea la base de datos)
- Tipado dinámico (los tipos de columna son solo indicativos)

### Buenas prácticas

1. **Usa transacciones para DDL manual** — las alteraciones administradas de tablas requieren autocommit.
2. **Planifica el schema con anticipación** — es difícil de modificar después.
3. **Revisa la vista previa de ALTER administrado** — es descriptiva; aplícala mediante DBFlux en lugar de ejecutar sus sentencias.
4. **Planifica por separado las formas no admitidas** — la reconstrucción rechaza vistas, triggers y otras formas no comprobadas.
5. **Prueba primero en una copia** — especialmente antes de una reconstrucción administrada.
6. **Habilita foreign keys** — `PRAGMA foreign_keys = ON` antes de alterar el schema.
7. **Usa VACUUM** — para liberar espacio en disco después de `DROP TABLE` o de recrear una tabla.
