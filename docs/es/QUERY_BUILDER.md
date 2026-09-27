# Constructor visual de queries

Para conexiones SQL puedes componer queries sin escribir SQL. Desde la toolbar
del data grid de una tabla, haz clic en **Builder** para abrir un panel en el
rail derecho. Las colecciones de documentos tienen su propio constructor,
descrito en [Colecciones de documentos](#colecciones-de-documentos).

El panel tiene un selector de modo en la parte superior — **SELECT**,
**UPDATE**, **DELETE** — y una vista previa de SQL en vivo que se regenera con
cada cambio. La vista previa siempre es visible. Pulsa **Run** para ejecutar, o
(en modo SELECT) **Open in Editor** para volcar el SQL generado en un editor de
query normal. La cabecera tiene **Save**, **Reset** y un botón de cierre que
oculta el panel y conserva lo construido; **Builder** lo vuelve a mostrar.

| Teclas                             | Acción                           |
| ---------------------------------- | -------------------------------- |
| `Cmd+Enter` / `Ctrl+Enter`         | Ejecutar                         |
| `Cmd+E` / `Ctrl+E`                 | Abrir en el editor (modo SELECT) |
| `Cmd+S` / `Ctrl+S`                 | Guardar                          |
| `Cmd+Shift+S` / `Ctrl+Shift+S`     | Guardar como                     |
| `Cmd+Backspace` / `Ctrl+Backspace` | Reiniciar                        |

## Construir un SELECT

El cuerpo del SELECT tiene secciones que rellenas de arriba a abajo:

- **Columns** — la proyección (qué columnas seleccionar).
- **Filters** — un árbol de predicados `WHERE`. Los predicados se pueden anidar
  en grupos AND/OR, así que puedes construir condiciones complejas de forma
  visual.
- **Joins** — tablas adicionales con un alias y una condición `ON`.
- **Group By / Aggregates** — ver más abajo.
- **Sort and limit** — entradas de `ORDER BY` y los límites de paginación. Cada
  fila de orden es una columna elegida en un desplegable (columnas de la tabla
  origen y luego las de las tablas unidas) y un selector ASC/DESC; la primera
  fila también lleva el límite. La última línea añade otra columna de orden y
  lleva el desplazamiento. Los drivers que no pueden ordenar muestran solo el
  límite y el desplazamiento.

La vista previa de SQL está parametrizada: los valores literales se emiten como
placeholders para el dialecto activo (SQLite, PostgreSQL, MySQL/MariaDB o SQL
Server).

## GROUP BY y agregados

Añade columnas de agrupación y agregados en la sección **Group By /
Aggregates**. Las funciones de agregado soportadas son `COUNT`, `COUNT(*)`,
`COUNT(DISTINCT)`, `SUM`, `AVG`, `MIN` y `MAX`. Cada agregado obtiene un alias
editable que se genera automáticamente a partir de la función y la columna.

Una vez agrupada la query:

- La sección **Columns** se reemplaza por una vista previa de solo lectura del
  `SELECT` efectivo (columnas de agrupación seguidas de los alias de los
  agregados).
- Aparece una sección **Having**, que usa el mismo editor de predicados que
  Filters pero aplicado a `HAVING`.
- Los desplegables de **Sort** ofrecen solo las columnas de agrupación y los
  alias de los agregados.

Cómo se comportan los resultados agrupados en el data grid se describe en
[Resultados agregados](RESULTS.md#resultados-agregados).

## Autocompletado consciente del schema

Los inputs de una sola línea del builder (filtro, columnas proyectadas,
la tabla destino del join, y ambos lados de un `ON` de join) ofrecen sugerencias
inline obtenidas del schema en vivo y de la propia especificación del builder:
columnas de la tabla origen, alias de join declarados, y columnas de la tabla
unida (obtenidas de forma diferida en segundo plano). Escribir `<alias>.`
restringe las sugerencias solo a las columnas de ese alias. La coincidencia es
solo por prefijo.

| Teclas                    | Acción                            |
| ------------------------- | --------------------------------- |
| `Up` / `Down`             | Moverse entre sugerencias         |
| `Tab` / `Enter`           | Confirmar la sugerencia resaltada |
| `Esc` (o pérdida de foco) | Descartar                         |

El mismo autocompletado está disponible en el input de filtro `WHERE` del data
grid (ver [Filtrar resultados](RESULTS.md#filtrar-resultados)).

## Queries guardadas

Los builders se pueden guardar por perfil de conexión y reabrir más tarde. Las
queries guardadas están acotadas al perfil, con nombres únicos. Una query
guardada también se puede importar a otra conexión; al importar, DBFlux verifica
que las tablas referenciadas existan en la conexión destino antes de cargarla.

## UPDATE y DELETE visuales

Cambia el selector de modo a **UPDATE** o **DELETE** para construir una
mutación. Ambos modos reutilizan el mismo editor de filtros para la cláusula
`WHERE`; UPDATE añade una sección de asignaciones para las columnas del `SET`
(incluyendo asignaciones de expresión en bruto). La vista previa de SQL
permanece visible todo el tiempo.

Las mutaciones están sujetas a una política que combina el estado de solo
lectura de la conexión y el contexto del actor:

| Policy            | Efecto                                                                |
| ----------------- | --------------------------------------------------------------------- |
| Allowed           | La mutación puede ejecutarse.                                         |
| Read-only         | La ejecución está bloqueada (por ejemplo, un perfil de solo lectura). |
| Approval required | La mutación debe aprobarse antes de ejecutarse.                       |

**Modo de ejecución.** La sección **Execution** ofrece tres modos, con uno por
defecto sugerido automáticamente a partir de la estimación de número de filas,
el soporte de transacciones del driver, y la disponibilidad de una primary key.
Anular la sugerencia muestra un modal de tradeoffs.

| Modo           | Comportamiento                                                                                                                                                                                                                                                        |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Single TX**  | Una única transacción para todo el cambio.                                                                                                                                                                                                                            |
| **Chunked TX** | Chunks paginados por keyset sobre la primary key de la tabla (tamaño de chunk acotado entre 1000 y 10000, por defecto 5000). Cada chunk es su propia transacción, aparece como una entrada del panel Tasks, se puede cancelar entre chunks, y hace rollback si falla. |
| **Direct**     | Sin envoltorio de transacción (autocommit). Se usa cuando el driver no soporta transacciones.                                                                                                                                                                         |

**Gate de query peligrosa.** Un `UPDATE` o `DELETE` sin `WHERE` pasa por la
confirmación de queries peligrosas (ver [Confirmación de queries
peligrosas](EDITOR.md#confirmación-de-queries-peligrosas)) antes de ejecutarse.

## Colecciones de documentos

Las colecciones de los drivers que lo ofrecen (MongoDB) tienen un constructor
visual para consultas find y agregaciones sencillas. Haz clic en
**Constructor** en la cabecera de la colección para abrirlo en el rail derecho;
vuelve a hacer clic, o usa el botón de cierre del rail, para ocultarlo. El
borrador se conserva mientras la pestaña está abierta. En un driver de
documentos sin constructor, como DynamoDB, el botón está deshabilitado y su
tooltip explica el motivo; los campos de consulta siguen funcionando.

El rail tiene un selector de modo **Find** / **Aggregate**, las tarjetas que se
describen abajo y una vista previa de la consulta en la sintaxis propia del
driver, fijada encima del pie. El pie ejecuta la consulta (**Buscar** en modo
Find, **Ejecutar pipeline** en modo Aggregate) o abre la vista previa en un
editor de consultas (**Abrir en el editor**).

### Modo Find

| Tarjeta                    | Qué construye                                                                                                 |
| -------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Filtro**                 | Condiciones combinadas como *todas* (`$and`) o *alguna* (`$or`), con grupos anidados.                         |
| **Proyección**             | Campos a incluir o a excluir. `_id` siempre se devuelve, así que las filas del resultado siguen siendo editables. |
| **Orden, límite y salto**  | Claves de orden, cada una ascendente o descendente, y después un límite y un salto.                           |

Una condición es un campo, un operador y un valor. Los operadores ofrecidos
dependen del tipo del campo en la muestra del esquema, entre `$eq`, `$ne`,
`$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$regex`, `$exists`, `$elemMatch`,
`$size` y `$all`. Un campo muestreado con más de un tipo se marca, y sus
condiciones ofrecen los operadores de cada tipo. El input del valor depende del
operador:

| Operador o tipo                              | Input del valor                                                         |
| -------------------------------------------- | ----------------------------------------------------------------------- |
| `$in`, `$nin`, `$all`                        | Una lista de valores, un chip por valor (escribe un valor y pulsa `Enter`). |
| `$exists`, y `$eq` / `$ne` sobre un booleano | Un interruptor true / false.                                            |
| `$regex`                                     | Un patrón, `/patrón/flags` o un patrón sin delimitadores.               |
| `$size`                                      | Un número entero de elementos.                                          |
| `$elemMatch`                                 | Condiciones que debe cumplir un elemento del arreglo.                   |
| Campo de fecha                               | `YYYY-MM-DD`, `YYYY-MM-DD HH:MM` (UTC) o una marca de tiempo RFC 3339.  |
| Campo ObjectId                               | 24 dígitos hexadecimales.                                               |

Los campos se eligen en un selector alimentado por la muestra de la vista
Esquema: rutas anidadas, una etiqueta de tipo para cada una y con qué
frecuencia aparece el campo. Una ruta que la muestra no vio se puede escribir y
usar con `Enter`; se marca como sin muestrear.

**Buscar** ejecuta la consulta a través de la barra de consulta, igual que los
campos: los resultados son documentos normales, editables, contados y
guardados en el historial de consultas.

### Sincronización con la barra de consulta

Mientras el rail está abierto, el constructor y los campos `filter`, `project`,
`sort` y `limit` se sincronizan en ambos sentidos: una edición en el
constructor reescribe los campos que cambia, y una edición en un campo
recarga el constructor, lo que descarta una condición sin terminar. Los campos
que cambiaron mientras el rail estaba cerrado, o mientras el constructor estaba
en modo Aggregate, se vuelven a leer cuando el rail se reabre o el constructor
vuelve a Find: una parte que el constructor no cambió mientras tanto toma la
consulta del campo. Ejecutar una consulta del historial también recarga el
constructor y borra su salto.

El salto no tiene campo; se aplica mientras el rail está abierto, y la
paginación cuenta desde él: retroceder una página se detiene en el salto, y un nuevo
tamaño de página vuelve a empezar allí.

Cuando un campo contiene una cláusula que el constructor no puede mostrar,
como `$expr`, el constructor muestra las partes que entiende, deja el resto en
solo lectura y muestra una tarjeta con dos opciones. Las ediciones del
constructor que cambiarían ese campo esperan hasta que elijas, y **Buscar**
queda deshabilitado mientras haya una edición en espera. Sin ediciones en
espera, **Buscar** ejecuta los campos tal como están escritos.

| Opción                               | Efecto                                                                                          |
| ------------------------------------ | ----------------------------------------------------------------------------------------------- |
| **Conservar el texto**               | Descarta las ediciones pendientes del constructor y conserva el campo tal como está escrito.    |
| **Reescribir desde el constructor**  | Reemplaza los campos con la consulta del constructor y descarta las cláusulas que no pudo mostrar. |

### Modo Aggregate

Añadir una etapa de grupo en la tarjeta **Grupo** cambia a **Aggregate**, y
quitarla vuelve a **Find**. El modo Aggregate necesita un driver que ejecute
pipelines de agregación.

- **Agrupar por** admite cero o más campos; sin ninguno, todos los documentos
  forman un solo grupo.
- Los **Acumuladores** son `$count`, `$sum` y `$avg`, cada uno con un nombre de
  salida. `$sum` y `$avg` toman un campo numérico.
- El filtro pasa a ser una etapa `$match`, que se muestra como un resumen con
  **Editar**.
- La **Proyección** no se usa: los campos de salida son la clave de grupo y los
  acumuladores.
- Las claves de orden solo pueden ser claves de grupo o acumuladores.

**Ejecutar pipeline** escribe el pipeline en la vista Agregación de la
colección y lo ejecuta allí. Las filas agrupadas se calculan, así que no tienen
un `_id` que editar: los resultados son de solo lectura y llevan un aviso que
lo indica. Mientras el constructor está en modo Aggregate, la barra de
consulta y la vista Agregación muestran un resumen del pipeline en lugar de los
campos y del editor de pipeline. Mientras la vista Agregación ejecuta un
pipeline o espera una confirmación, **Ejecutar pipeline** no la toca e indica
que no se ejecutó nada.

### Consultas de documentos guardadas

Pon un nombre a la consulta en la cabecera del rail y haz clic en **Guardar
consulta**. Las consultas guardadas pertenecen a la colección: su perfil de
conexión, su base de datos y su nombre. Guardar con un nombre que ya existe
reemplaza esa consulta. **Consultas guardadas** las lista; al abrir una se
carga en el modo en que se guardó. Abrir un find guardado reemplaza los cuatro
campos, incluidas las cláusulas que el constructor no puede mostrar.

### Limitaciones

- Un valor de texto de 24 dígitos hexadecimales se ejecuta como ObjectId,
  porque el driver convierte esas cadenas. El constructor pide introducirlo
  como ObjectId, así que un campo de texto que contenga una cadena así no se
  puede buscar como texto.
- Los valores decimales se comparan como double, así que no coinciden con
  campos `Decimal128`.
- En los campos de consulta, `{"$date": "..."}` solo acepta una marca de tiempo
  RFC 3339, como `2024-03-09T14:30:05Z`.
- El constructor no tiene etapas de escritura (`$out`, `$merge`) y no construye
  actualizaciones ni borrados.
- Borrar una consulta guardada no pide confirmación.
