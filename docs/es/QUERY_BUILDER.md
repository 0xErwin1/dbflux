# Constructor visual de queries

Para conexiones SQL puedes componer queries sin escribir SQL. Desde la toolbar
del data grid de una tabla, haz clic en **Builder** para abrir un panel en el
rail derecho. El builder solo está disponible en drivers SQL; las conexiones no
SQL no lo muestran.

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
