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
esa columna: copiar el nombre de la columna, ordenar ascendente o descendente,
quitar el orden y todos los operadores de filtro, en una sola lista plana. El clic izquierdo en la
cabecera sigue alternando el orden.
El menú de la tabla (`m`) también ofrece **Copiar nombre de columna** para la
columna de la celda activa, así que el teclado también llega a él.

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

### Expresiones SQL en celdas

Una celda normalmente guarda un literal: lo que escribís se guarda como valor
entre comillas, así que escribir `now()` guarda el texto `now()`. Para guardar
una expresión SQL que la base de datos evalúa (`now()`, `gen_random_uuid()`),
marcá la celda como expresión de forma explícita. El texto nunca se detecta
como expresión.

- **Establecer valor** en el menú de la celda abre un submenú con **Valor por
  defecto de la columna** (deshabilitado, con el motivo, cuando la columna no
  tiene valor por defecto), los valores que el dialecto de la conexión ofrece
  para el tipo de la columna, **NULL** y **Expresión…**.
- En el editor en línea, `Ctrl+Shift+I` alterna entre modo literal y modo
  expresión, y abre el editor en modo expresión cuando no hay ninguno abierto.
  En modo expresión el editor muestra un marcador `ƒx` y lista los valores del
  tipo bajo la celda; `Arriba` / `Abajo` ponen uno en la entrada y `Enter` lo
  guarda.
- **Poner en el valor por defecto** (`Ctrl+Shift+0`) prepara la expresión por
  defecto de la columna.

Una celda de expresión se dibuja con `ƒx` en el color de acento, y copiarla
copia el texto de la expresión. Un guardado que escribe una expresión pide
confirmación antes, porque la expresión se coloca en la sentencia tal como
está en lugar de enlazarse como valor. Rechazar no guarda esa expresión y
mantiene sus ediciones preparadas. En un guardado por lotes, las filas
anteriores ya se han escrito y las posteriores no. Solo las tablas SQL ofrecen
expresiones; las grillas de documentos no.

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
menú añade **Exportar todas las filas a un archivo**, en CSV, JSON (un array
con un objeto por línea) o JSON Lines (un objeto por línea, `.jsonl`), o como tabla HTML, tabla
Markdown o XML para leer y compartir. Vuelve a leer la tabla o la colección completa con el filtro,
el orden y la selección de columnas actuales de la cuadrícula, una página a la
vez, y la escribe en el único archivo que elijas, de modo que los datos nunca
tienen que caber en memoria. Una tabla mostrada mediante el constructor de
consultas o un filtro relacional se lee con esa misma consulta, desde su
desplazamiento y hasta su límite cuando lo define. La exportación se ejecuta
como una tarea en el panel de tareas, que muestra su progreso y permite
cancelarla; el aviso indica el total de filas y la ruta.

Los archivos HTML, Markdown y XML son para leer, no para importar: al importar
una carpeta que contenga uno de ellos, la importación se detiene en esa tabla,
que falla con un mensaje que indica que su formato no se puede importar, y las
tablas siguientes no se intentan. XML escribe un `<row>` por fila y un
`<field name="...">` por columna, con `null="true"` para un NULL, de modo que se
distingue de un valor vacío.

El asistente de exportación (**Export Table…** en la barra lateral) escribe cada
tabla seleccionada en su propio archivo dentro de una carpeta nueva, junto a un
`manifest.json`. Su paso Format ofrece las opciones que usa el formato y oculta
las demás:

- **Delimitador** (solo CSV): coma, punto y coma, tabulador o barra vertical.
- **Codificación** (solo CSV): UTF-8, UTF-8 con BOM, UTF-16LE (con BOM) o
  Windows-1252. Un carácter que Windows-1252 no puede representar detiene la
  exportación con un error que lo nombra, en lugar de sustituirlo.
- **Texto para NULL** (CSV, HTML y Markdown): el texto que se escribe para NULL,
  vacío por defecto. JSON y JSON Lines siempre escriben `null`, y XML marca un
  NULL con `null="true"`. Un valor cuyo texto es igual a un texto para NULL no vacío se
  leería como NULL, así que la exportación se detiene con un error que nombra el
  valor y la columna; elige otro texto para NULL. Con el texto para NULL vacío
  por defecto, un string vacío y NULL se escriben igual, así que al importar un
  CSV ambos vuelven como NULL; usa un texto para NULL no vacío, como `\N`, para
  conservar los strings vacíos.
- **Fechas** (todos los formatos excepto SQL): RFC 3339 (por defecto), `YYYY-MM-DD HH:MM:SS`
  en UTC o segundos epoch, que JSON y JSON Lines escriben como número. Una fecha
  se escribe como `YYYY-MM-DD` con los dos primeros y como su medianoche (UTC)
  con epoch, y una hora como `HH:MM:SS` con `YYYY-MM-DD HH:MM:SS` y como
  segundos desde medianoche con epoch.
- **Filas por INSERT** (solo SQL): cuántas filas entran en cada sentencia
  `INSERT`, 500 por defecto y como mínimo 1. Un driver que limita las filas por
  sentencia, como SQL Server con 1000, acota el valor.

SQL se ofrece en el asistente de exportación solo cuando el driver de la
conexión de origen puede generar sentencias `INSERT` masivas. Escribe un archivo
`<esquema>.<tabla>.sql` por tabla con sentencias `INSERT` en el dialecto de esa
conexión: cada sentencia contiene un lote de filas, nombra la tabla como lo hace
el dialecto y termina con `;`. El script se reproduce en una copia vacía de la
tabla; ejecutarlo dos veces falla por claves únicas o duplica las filas. Una
columna `GENERATED ALWAYS AS IDENTITY` de PostgreSQL se escribe con
`OVERRIDING SYSTEM VALUE`, y una tabla de SQL Server con una columna `IDENTITY`
se envuelve en `SET IDENTITY_INSERT ... ON;` y `OFF;`, de modo que se conservan
los ids originales. Tras la última sentencia de una tabla de PostgreSQL, el
script mueve la secuencia de cada columna de identidad o serial más allá de los
valores exportados (`setval`): al mayor si la secuencia es ascendente y al menor
si es descendente, de modo que una inserción por defecto posterior a la
reproducción no reutilice un id; SQL Server avanza por sí mismo la semilla de
identidad al insertar valores explícitos. Las columnas que calcula la base de datos (columnas
generadas de PostgreSQL y MySQL/MariaDB, columnas calculadas, `rowversion` y columnas
de período temporal `GENERATED ALWAYS AS ROW START/END` de SQL Server) se
omiten de las sentencias para que la base de datos las calcule de nuevo al
reproducir el script. Una tabla sin filas escribe un archivo vacío. El dialecto decide cómo se escriben NULL y las fechas, así que el texto
para NULL, el delimitador, la codificación y las opciones de fecha no se aplican
y se ocultan. Los archivos SQL aún no se pueden importar: al importar una
carpeta que contiene uno, esa tabla falla con el mensaje de que el formato no se
puede importar. **Exportar todas las filas a un archivo** no ofrece SQL.

Cuando la conexión de origen no puede escribir SQL, el paso Formato no lo
lista y explica el motivo bajo el selector de formato: su driver no puede
generar sentencias `INSERT`, así que exporta como CSV o JSON. Cada estado que
desactiva **Continuar** explica por qué. El pie nombra qué corregir (**Filas por
INSERT**, **Tamaño de bloque**, la carpeta o una selección de tablas vacía), y
un campo no válido muestra un marco de peligro, un icono de alerta y una
pista. Cuando termina la ejecución, el diálogo lista una fila por tabla con su
resultado. Una tabla que la ejecución no alcanzó tras un fallo lo indica, y una
ejecución fallida dice qué hacer a continuación: los archivos ya escritos
permanecen en la carpeta, una ejecución fallida no escribe `manifest.json`, así
que la carpeta no se puede importar completa, y cada archivo CSV o JSON
terminado todavía se puede importar con **Elegir archivo…**.

Los valores por defecto escriben exactamente lo que escribían las versiones
anteriores. El manifiesto registra las opciones que dieron forma a un archivo,
de modo que importar la carpeta lo lee igual; un manifiesto sin ellas significa
los valores por defecto. **Exportar todas las filas a un archivo** usa siempre
los valores por defecto.

Una colección de documentos exportada como JSON o JSON Lines escribe cada
documento completo, un objeto por línea, con todos sus campos. Como CSV, HTML,
Markdown o XML se escribe con las columnas que la cuadrícula muestra para la
primera página, incluidos los objetos expandidos. Los documentos de una
colección no tienen por qué compartir sus campos, así que en cualquiera de estos
archivos un campo que aparece por primera vez en una página posterior no tiene
columna, porque el encabezado ya está escrito; en
ese caso la exportación nombra esos campos en una advertencia.

Un resultado de consulta ofrece solo las filas cargadas, porque exportarlas
todas implicaría volver a ejecutar la consulta. Lo mismo ocurre con una búsqueda
de vecinos más cercanos: devuelve un número fijo de las filas más próximas, y la
cuadrícula ya las tiene todas.

## Importar un archivo

El asistente de importación también lee un archivo suelto. Elige **Elegir
archivo…** en su primer paso, junto al selector de carpeta, y selecciona un
archivo `.csv`, `.json`, `.jsonl`, `.xml` o de hoja de cálculo (`.xlsx`, `.xlsm`, `.xls` o `.ods`); los archivos HTML y Markdown no se
ofrecen, y un archivo `.sql` en una conexión SQL se ejecuta como script (consulta
[Ejecutar un script SQL](#ejecutar-un-script-sql)). El asistente lee las columnas de la fila de encabezado del CSV, de
las claves de los primeros 100 registros JSON, del `name` de los elementos
`<field>` de las primeras 100 filas XML (todas las claves vistas, en el
orden en que aparecen), o de la fila 1 de una hoja de cálculo. Una clave que aparece por primera vez después de esas
filas detiene la importación con un error que indica la clave y su fila. Un objeto o array anidado se importa como texto JSON.

La tabla de destino es por defecto el nombre del archivo sin extensión; escribe
otro nombre y elige **Usar tabla** (o pulsa Enter) para apuntar la importación a
una tabla existente. El mapeo de columnas y los modos de tabla (crear,
existente, recrear, truncar, omitir) son los mismos que para una carpeta. Un
archivo no trae tipos de columna, así que los valores que van a una tabla
existente se leen según los tipos de esa tabla, y una tabla nueva se crea con
una columna de texto por cada campo (`TEXT`, o el tipo de texto amplio propio del
motor, como `NVARCHAR(MAX)` en SQL Server o `LONGTEXT` en MySQL), de modo que los valores se guardan
como texto.

Un archivo CSV ofrece el delimitador, la codificación y el texto que representa
NULL, las mismas opciones que el asistente de exportación; JSON, JSON Lines y XML no
tienen opciones. Un archivo XML debe tener la forma que escribe la exportación:
una raíz `<rows>` con elementos `<row>`, cada uno con un
`<field name="...">texto</field>` por columna. Un campo escrito como
`<field name="..." null="true"/>` es NULL, un `<field>` vacío es una cadena
vacía y las referencias de carácter como `&#10;` se decodifican. Cualquier otra
forma de XML se rechaza con un mensaje que indica la esperada. Cambiar el delimitador o la codificación vuelve a leer el
archivo. Todo el archivo se lee una vez antes de escribir nada, así que una fila
o registro mal formado detiene la importación sin tocar el destino.

Una hoja de cálculo se importa una hoja cada vez. El paso Configurar añade un
desplegable **Hoja** que lista las hojas visibles del libro y empieza en la
primera; las hojas ocultas y las hojas de gráfico no aparecen, y al elegir otra
hoja el archivo se lee de nuevo al instante y se rehace el mapeo de columnas. Un
libro con una sola hoja visible la muestra con el desplegable desactivado e indica
que no hay nada que elegir. La fila 1 de la hoja es el encabezado, así que cada
columna necesita un nombre único y no vacío; una hoja de cálculo no tiene opciones de
delimitador, codificación ni NULL. Los números, booleanos, fechas, fechas con hora
y textos se leen como valores de esos tipos, una celda vacía es NULL y una
fórmula se lee como el último resultado guardado en el archivo. Los libros con el
sistema de fechas 1904 leen bien sus fechas, pero una hora del día en uno de
ellos llega como una marca de tiempo del 1904-01-01 en lugar de una hora. La hoja
entera se lee en memoria, así que se rechaza una hoja de más de 10.000.000 de
celdas (filas × columnas, contadas desde la celda A1). Una hoja que no se puede
importar mantiene el archivo elegido en Configurar y muestra un bloque de peligro
con qué hacer a continuación: la hoja está vacía, la fila 1 tiene un nombre vacío
o repetido (el bloque lista las columnas), una celda contiene un valor de error
como `#DIV/0!` (el bloque indica la hoja, la fila y la columna; cuenta también una celda de la fila 1, así que un valor de error nunca se toma por un nombre de columna), o la hoja supera
el límite (expórtala como CSV e importa ese CSV, o elige una hoja más pequeña).
Elegir otra hoja es una salida en cada caso, y cuando el libro no tiene otra hoja
visible el bloque indica en su lugar que corrijas la hoja en la aplicación de hojas
de cálculo, guardes y vuelvas a elegir el archivo. El pie explica por qué **Continuar**
está desactivado.

El diálogo de importación usa el mismo indicador de pasos superior y el mismo
pie que el asistente de exportación: **Cancelar** en los dos primeros pasos,
**Atrás** y **Cancelar** en la confirmación, y **Continuar** como botón
principal. Un archivo o una carpeta que no se puede leer muestra un único bloque
de peligro con el error y qué hacer a continuación. Un CSV cuyo encabezado no se
decodifica se queda en Configurar, donde otro delimitador o codificación lo lee
de nuevo; un JSON roto se queda en el primer paso hasta que se elige otro
archivo; y una consulta fallida de la tabla de destino lo indica y se reintenta
con **Usar tabla**. El pie explica por qué **Continuar** está desactivado (se
está leyendo un archivo o no se pudo leer). JSON y JSON Lines muestran una línea
que dice que no tienen opciones, y en una conexión que no puede truncar tablas
una línea explica por qué Truncate no aparece. En los campos de texto, `Escape`
sale del campo y devuelve las teclas al diálogo; un segundo `Escape` cierra el
asistente. Cuando termina la ejecución, una fila por tabla lista su resultado, y
una ejecución fallida dice qué hacer a continuación.

### Ejecutar un script SQL

En una conexión cuyo lenguaje de consulta es SQL, **Elegir archivo…** también
ofrece archivos `.sql`; las demás conexiones nunca los ven, y un archivo `.sql`
que se cuele de todos modos se rechaza con un mensaje que dice que la conexión no
es SQL. Al elegir uno se abre el flujo **Ejecutar script SQL** en el mismo
diálogo. El script se lee como UTF-8 y se divide en sentencias con el mismo
divisor del editor, que respeta comillas, comentarios y cuerpos entre dólares, y
no tiene opciones de delimitador ni de codificación. Un script de más de 100 MB,
un archivo que no es UTF-8 o no se puede leer, y un script sin sentencias se
quedan en el primer paso con el motivo y qué hacer a continuación. Lo mismo
ocurre con un script que usa algo que solo ejecuta un cliente de línea de
comandos: una línea `DELIMITER` o un separador de lotes `GO`, un cuerpo de rutina
(procedimiento, función o disparador) cuyas sentencias terminan con `;` y que no
tiene delimitadores de cliente alrededor, un bloque de datos `COPY … FROM stdin`
o un metacomando de psql con barra invertida como `\connect`, o una sentencia que
desactiva autocommit (`SET autocommit = 0`) o activa las transacciones
implícitas, lo que dejaría la conexión en modo de confirmación manual. Cada uno se
rechaza antes de ejecutar nada, con la línea y qué hacer a continuación: ejecutar
el script con el cliente de línea de comandos de la propia base de datos o
reescribirlo (psql para un bloque `COPY … FROM stdin` o un metacomando de psql: quitar una línea `\connect` ejecutaría el resto en esta conexión, que puede no ser la base de datos que selecciona). Un script de más de 100 MB
también se puede dividir en archivos más pequeños, pero cada archivo se ejecuta
por separado: un archivo posterior que falle no puede deshacer uno anterior.

**Configurar** muestra el número de sentencias y el tamaño, cuántas sentencias
hay en cada clasificación (las clases que usan el editor y el visor de auditoría,
incluida Administración) y las seis primeras sentencias con su clasificación; un
`DELETE` o `UPDATE` sin `WHERE` aparece marcado. Las sentencias que cambian una
secuencia (`setval`, `nextval`, `ALTER SEQUENCE`) cuentan como Escritura, así que
una conexión de solo lectura las rechaza. Tiene dos opciones:

- **Ante un error**: **Detener en el primer error** (por defecto) o **Continuar
  tras los errores**, que ejecuta todas las sentencias y lista las que fallaron.
- **Ejecutar en una sola transacción**: activada por defecto cuando la conexión
  admite transacciones. Si una sentencia falla, los cambios hechos en la
  transacción se deshacen y no se guarda nada hasta que la última sentencia tenga
  éxito. Está desactivada, con el motivo, cuando se elige **Continuar tras los
  errores** (una sentencia fallida aborta la transacción en algunos motores), y
  no se ofrece, con el motivo, en una conexión sin transacciones. Un script que
  gestiona sus propias transacciones (`BEGIN`, `START TRANSACTION`, `COMMIT`,
  `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `SET autocommit`) también la
  desactiva, indicando la sentencia y su línea: el script se ejecuta tal cual y
  su propio `COMMIT` guarda los cambios anteriores. Una transacción que el script
  abre y nunca termina se deshace al acabar el script, y el diálogo lo dice:
  añade un `COMMIT` al final para guardar sus cambios. Si ese rollback falla, el
  diálogo dice que puede seguir abierta una transacción y que hay que
  desconectarse y volver a conectar. Un script con un comentario condicional por
  versión que controla transacciones se rechaza, porque que se ejecute o no
  depende del servidor. Un script que usa `SAVEPOINT` fuera de una transacción
  recibe un `ROLLBACK` preventivo al terminar, por si quedó una abierta; el
  diálogo lo dice, y los cambios que la base de datos ya había confirmado se
  mantienen.

En una conexión que confirma por sí sola las sentencias de esquema (MySQL y
MariaDB), `CREATE`, `ALTER` y `DROP` se guardan al ejecutarse y también guardan
todas las sentencias anteriores, así que un rollback solo deshace los cambios
hechos después de la última. Los cambios de secuencias (`setval`, `nextval`)
nunca se deshacen con un rollback. Una conexión de solo lectura por la
configuración de su perfil rechaza un script que cambia datos, estructura o permisos, dice
cuántas sentencias lo hacen y mantiene **Continuar** desactivado; un script que
solo lee puede ejecutarse.

Todo script pasa por **Confirmar**, sea cual sea su contenido. Un script sin
sentencias destructivas muestra cuántas sentencias hay en cada clasificación y
ofrece **Ejecutar script**. Un script con sentencias destructivas muestra un
bloque de peligro que las cuenta por los tipos de consulta peligrosa del editor,
lista las tres primeras con su número en el archivo y ofrece **Sí, ejecutar
script**. **Atrás** vuelve a Configurar.

**Ejecutando** muestra "Ejecutando la sentencia N de M", la sentencia en curso y
una entrada en el panel de tareas. **Cancelar ejecución** se detiene tras la
sentencia en curso y pide al controlador que la interrumpa. En una transacción
los cambios se deshacen; sin ella, las sentencias ya ejecutadas se conservan. El
pie dice cuál de los dos casos es antes de pulsar.

El script se ejecuta en la conexión del perfil, así que sus sentencias y su
transacción comparten esa conexión con cualquier otra cosa iniciada desde el
perfil. Deja que termine el resto del trabajo en esa conexión (el editor, la
cuadrícula, otra importación) antes de ejecutar un script y no inicies trabajo
nuevo en ella hasta que acabe la ejecución: una consulta que se ejecute en
medio de la transacción del script puede deshacerse o guardarse con ella. En un
controlador que ofrece sesiones separadas, el script se ejecuta en su propia
sesión: el resto del trabajo no puede unirse a la transacción del script y
**Cancelar ejecución** solo detiene el script. Otra sesión aún puede bloquear o
cambiar los objetos que usa el script, así que este puede esperarla o entrar en
conflicto con ella. Una importación de carpeta o de archivo tiene la misma
propiedad en una conexión sin sesiones separadas.

**Terminado** dice qué ocurrió. Una ejecución que se detiene nombra la sentencia
(número, línea y texto), muestra el error de la base de datos, dice qué pasó con
las sentencias anteriores y qué hacer a continuación; una ejecución que continuó
tras los errores lista cada sentencia que falló. Sin transacción, el consejo es
conciliar los cambios que ya hicieron las sentencias anteriores y conservar o
restaurar los ajustes de sesión que necesita el script (como `SET search_path`)
antes de ejecutar el resto; tras los errores es el mismo. Si falla el propio commit o
rollback, el diálogo dice que el resultado no está claro y que se compruebe el
estado de la base de datos antes de ejecutar nada más. Cada fallo se informa
además una vez como notificación con **Ver en Auditoría**, y cada ejecución
registra un evento de auditoría `Query` (más uno por sentencia fallida, hasta 50)
que solo lleva la primera línea de la sentencia, ocultada según los ajustes de
auditoría.
