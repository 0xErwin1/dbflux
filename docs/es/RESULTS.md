# Trabajar con resultados

Los resultados se renderizan en pestañas de resultado dentro del documento. Cada
tabla o colección se abre en la vista que su driver declara para ese objeto, y
ofrece las demás vistas que declara:

- **Vista de tabla** para tablas relacionales y resultados de query.
- **Vista de árbol** para colecciones de documentos (por ejemplo MongoDB,
  DynamoDB), con vistas de **Tabla** y **JSON** de la misma página (ver
  [Colecciones de documentos](DOCUMENTS.md)).
- **Vista de tabla** con una vista de **Árbol** de las mismas filas para una
  tabla con columnas JSON (ver
  [Columnas JSON en una tabla](DOCUMENTS.md#columnas-json-en-una-tabla)).
- **Gráfico sobre la cuadrícula** para una serie temporal, como un measurement
  de InfluxDB o una hypertable de TimescaleDB, cuando sus filas se pueden
  graficar (ver
  [Tablas y colecciones de series temporales](CHARTS.md#tablas-y-colecciones-de-series-temporales)).
- **Vista clave-valor** para Redis (ver [Vista clave-valor](KEY_VALUE.md)).

Una tabla con una columna vectorial también lista las filas más
[similares](#filas-similares) a la seleccionada.

Los contenedores de tipo event-stream se abren como event streams cuando el
driver declara esa presentación.

## Resultados junto al editor

En una pestaña de consulta los resultados están debajo del editor. En una
pantalla ancha, muévelos a la derecha del editor con el botón de panel de la
cabecera de resultados, la entrada **Mostrar los resultados a la derecha** de
las acciones del panel o **Mover los resultados junto al editor o debajo** en la
paleta de comandos. El mismo control los vuelve a poner debajo. Arrastra el
divisor para cambiar el tamaño de cada lado. Las pestañas de consulta nuevas se
abren con la última posición elegida.

En cualquiera de las dos posiciones, `Ctrl+j` lleva el teclado del editor a los
resultados y `Ctrl+k` lo devuelve.

## Navegar el data grid

Cuando el panel de resultados tiene el foco:

- `j`/`k` (o `Down`/`Up`) — moverse entre filas.
- `h`/`l` (o `Left`/`Right`) — moverse entre columnas.
- `g`/`Shift+g` (o `Home`/`End`) — primera / última fila.
- `Ctrl+d`/`Ctrl+u` (o `PageDown`/`PageUp`) — recorrer filas por páginas.
- `[` / `]` — página anterior / siguiente de resultados (paginación).
- `f` enfoca la toolbar; `/` enfoca la búsqueda/filtro.
- `z` alterna el colapso del panel.
- `m` (o `Shift+F10`) abre el menú contextual de fila/celda.

## Vista de registro

Pulsa `i`, o usa el conmutador Registro en la barra de estado del
resultado, para mostrar la fila activa como una lista de Nombre / Valor que
ocupa toda el área de resultados. La cabecera indica la posición de la fila en
el resultado. Los campos se editan igual que las celdas de la cuadrícula, así
que los cambios sin guardar, Guardar fila y revertir funcionan igual en ambos
modos; `Up`/`Down` recorren los campos y `Left`/`Right` cambian de fila. Pulsa
`i` de nuevo para volver a la cuadrícula.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/results/record-view-dark.webp">
  <img src="../images/results/record-view-light.webp" alt="La vista de registro con una fila de cliente como lista de campos y valores">
</picture>

## Menú de la cabecera de columna

Haz clic derecho en la cabecera de una columna para abrir un menú limitado a
esa columna: ordenar ascendente o descendente, quitar el orden y todos los
operadores de filtro, en una sola lista plana. El clic izquierdo en la
cabecera sigue alternando el orden.

## Panel de valor

Haz clic derecho en una celda y elige **Ver valor**, o pulsa `v`, para abrir la
celda en el panel inspector de la derecha. Muestra el valor como JSON, XML o
texto plano — detectado a partir del contenido, y solo cuando realmente se
analiza — con formato legible, compacto y ajuste de línea. Se puede editar allí:
**Guardar** confirma la fila directamente y **Revertir** descarta el cambio. El
panel sigue la celda seleccionada al moverte por la cuadrícula, salvo mientras
tenga un cambio sin guardar.

## Inspector de fila

Pulsa `Ctrl+Space`, o haz clic derecho en una fila y elige **Inspeccionar
fila**, para abrir la fila seleccionada en el panel inspector de la derecha.
Lista cada columna de la fila con su valor y marca las columnas de clave
primaria y foránea. El inspector sigue la fila seleccionada al moverte por la
cuadrícula; el botón de fijar de su cabecera lo mantiene en la fila actual.
**Editar**, **Duplicar** y **Eliminar**, al pie, actúan sobre la fila
inspeccionada cuando el resultado es editable. Pulsa `Ctrl+Space` de nuevo, o
el botón de cerrar, para ocultarlo.

Bajo **Referencias** el inspector lista las claves foráneas de la fila en ambas
direcciones:

- **Salientes** — una clave foránea de una sola columna de esta fila apunta a
  otra tabla. La entrada nombra esa tabla y la fila a la que apunta, por su
  valor legible una vez resuelto («María López · #8»), o por su clave hasta
  entonces. Haz clic en ella para abrir la tabla referenciada en esa fila:
  DBFlux reutiliza la pestaña existente de la tabla — abriendo una si no la
  hay — y reemplaza su filtro por una igualdad de clave primaria, de modo que
  la fila referenciada sea la visible.
- **Entrantes** — otras tablas tienen claves foráneas que apuntan a esta fila.
  Cada entrada nombra la tabla y cuenta cuántas de sus filas la referencian;
  los conteos se cargan en segundo plano. Estas entradas son solo
  informativas.

Las celdas de clave foránea de la cuadrícula pueden mostrar el mismo valor del
registro referenciado junto a su id: activa **Mostrar el valor del registro
referenciado en celdas de clave foránea** en [Configuración](SETTINGS.md). El
inspector de fila resuelve las referencias independientemente de ese ajuste.

## Filtrar resultados

La toolbar del data grid tiene un input de filtro `WHERE` que vuelve a ejecutar
la query con la condición que escribas. Para conexiones SQL soporta dos estilos:

- **`WHERE` en bruto** — escribe una condición plana (por ejemplo `status =
  'active'`). Este es el comportamiento por defecto.
- **Rutas relacionales (estilo ORM)** — escribe una ruta con puntos que recorre
  foreign keys, por ejemplo `created_by.email LIKE '%@acme.com'` o
  `created_by.organization.name = 'Acme'`. DBFlux resuelve la ruta contra los
  metadatos de foreign key de la tabla y hace los joins hasta la tabla
  referenciada por ti; no hace falta escribir los JOINs a mano.

Cuando un filtro relacional se resuelve, un chip muestra cuántos joins añadió.
Si un segmento es ambiguo o no se puede resolver, aparece un error inline con un
enlace **Open in builder** que abre el constructor visual de queries precargado
con los joins resueltos hasta ese punto. La entrada sin puntos siempre mantiene
el comportamiento de `WHERE` en bruto.

El input de filtro también ofrece autocompletado consciente del schema (misma
navegación que el builder — ver [Autocompletado consciente del
schema](QUERY_BUILDER.md#autocompletado-consciente-del-schema)).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/results/filtering-dark.webp">
  <img src="../images/results/filtering-light.webp" alt="La tabla customers filtrada con una condición WHERE escrita en la barra de filtro">
</picture>

## Filas similares

En una tabla con una columna vectorial (hoy, una tabla PostgreSQL con una
columna `vector` de pgvector), pulsa `Shift+s` o elige **Buscar filas
similares** en el menú de la fila para listar las filas más cercanas a la fila
seleccionada. La entrada del menú indica qué columna lee y cuántas filas lista,
o por qué no puede ejecutarse: la fila no tiene valor en esa columna, o la
cuadrícula tiene cambios sin guardar, que hay que guardar o revertir antes.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/results/similar-rows-dark.webp">
  <img src="../images/results/similar-rows-light.webp" alt="La tabla products tras Buscar filas similares: la fila seleccionada fijada arriba como ancla, sus diez filas más cercanas ordenadas por distancia L2 y la barra con la columna vectorial, la distancia y K">
</picture>

La cuadrícula muestra entonces la fila seleccionada fijada arriba, marcada como
`referencia`, y debajo sus 10 filas más cercanas, de la más cercana a la más
lejana. La primera columna contiene la distancia, bajo una cabecera que la
nombra. Una barra reemplaza la fila de filtro:

- El selector de columna elige la columna vectorial. Con el cursor sobre una
  columna vectorial, la búsqueda empieza en esa columna; si no, en la primera de
  la tabla.
- **L2**, **Coseno** y **Producto interno** cambian la distancia. Producto
  interno muestra el producto en sí, de mayor a menor.
- **K** fija cuántas filas listar, de 1 a 1000.
- **Volver a la tabla**, o `Esc`, regresa a la página de la tabla con su filtro.

La fila seleccionada queda fuera de sus vecinas por su clave primaria, las filas
sin vector no se listan y las filas similares son de solo lectura. Una distancia
que no es un número se muestra como un guion. Las columnas `halfvec` y
`sparsevec` no ofrecen la búsqueda.

## Editar y CRUD

En el data grid:

- `o` — añadir una fila.
- `x` — eliminar la fila seleccionada.
- `r` — renombrar / editar (según el contexto).
- `y` — copiar la fila seleccionada.
- `Ctrl+c` (`Cmd+c`) — copiar la(s) celda(s) seleccionada(s) al portapapeles.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/results/editing-dark.webp">
  <img src="../images/results/editing-light.webp" alt="La tabla customers con una celda editada aún sin guardar y los botones Save y Revert habilitados">
</picture>

### Cuándo los resultados son editables

Los browses de tabla planos son editables cuando la tabla tiene una primary key.
Los resultados producidos por el **constructor visual de queries** (modo SELECT)
también son editables, pero solo cuando están demostrablemente vinculados a una
única tabla: el resultado mapea 1:1 a una tabla subyacente y cada columna de
primary key de esa tabla está proyectada con su nombre original. Las ediciones y
eliminaciones construyen entonces su `WHERE` a partir de los valores de primary
key proyectados.

Los JOINs están permitidos: las columnas de la tabla origen son editables,
mientras que las columnas unidas son de solo lectura.

Un resultado del builder recae en **solo lectura** — con una pista en la toolbar
explicando por qué — cuando se cumple alguna de estas condiciones:

- La query agrega o usa `GROUP BY` / `HAVING`.
- La proyección es un wildcard a través de un JOIN.
- Falta una columna de primary key o está proyectada bajo un alias.
- Las keys de la tabla aún no se han cargado desde la caché del schema (el grid
  se actualiza a editable en cuanto llegan las keys).

El SQL de forma libre escrito en el editor sigue siendo de solo lectura; la
edición inline solo aplica a browses de tabla planos y a SELECTs generados por
el builder.

### Resultados agregados

Cuando un resultado proviene de una query agrupada (`GROUP BY`), las filas
muestran la salida agregada y la edición está deshabilitada — añadir fila,
eliminar fila, editar celda e inspeccionar fila no están disponibles, con
tooltips explicativos. La paginación cuenta las filas agrupadas (no las filas
subyacentes), así que el total de páginas es correcto. Las columnas de agregado
conservan el tipo de columna correcto, así que graficar sigue funcionando.

## Copiar como query

El menú contextual de resultados incluye **Copy as Query**, que genera una
sentencia de mutación específica del driver (o un envelope, para drivers no SQL)
a partir de la fila seleccionada usando el generador de queries propio del
driver.

## Exportar

Pulsa `Ctrl+e` (`Cmd+e`) en el panel de resultados, o ejecuta **Export results**
desde el command palette. Los formatos disponibles dependen de la forma del
resultado e incluyen:

- **CSV**
- **JSON (pretty)** y **JSON (compact)**
- **Text**
- **Binary** (para resultados con forma binaria)

Las secciones **Guardar filas cargadas como archivo** y **Copiar filas cargadas
al portapapeles** del menú escriben las filas que tiene la cuadrícula, que en una
tabla son la página actual, y el aviso indica cuántas filas fueron al archivo o
al portapapeles.

Cuando la cuadrícula muestra una tabla o una colección leída página a página, el
menú añade **Exportar todas las filas a un archivo**, en CSV o en JSON con un
objeto por línea. Vuelve a leer la tabla o la colección completa con el filtro,
el orden y la selección de columnas actuales de la cuadrícula, una página a la
vez, y la escribe en el único archivo que elijas, de modo que los datos nunca
tienen que caber en memoria. Una tabla mostrada mediante el constructor de
consultas o un filtro relacional se lee con esa misma consulta, desde su
desplazamiento y hasta su límite cuando lo define. La exportación se ejecuta
como una tarea en el panel de tareas, que muestra su progreso y permite
cancelarla; el aviso indica el total de filas y la ruta.

Una colección de documentos exportada como JSON escribe cada documento
completo, un objeto por línea, con todos sus campos. Como CSV se escribe con
las columnas que la cuadrícula muestra para la primera página, incluidos los
objetos expandidos. Los documentos de una colección no tienen por qué
compartir sus campos, así que en un CSV un campo que aparece por primera vez en
una página posterior no tiene columna, porque el encabezado ya está escrito; en
ese caso la exportación nombra esos campos en una advertencia.

Un resultado de consulta ofrece solo las filas cargadas, porque exportarlas
todas implicaría volver a ejecutar la consulta. Lo mismo ocurre con una búsqueda
de vecinos más cercanos: devuelve un número fijo de las filas más próximas, y la
cuadrícula ya las tiene todas.
