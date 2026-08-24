# Guía de uso de DBFlux

Una introducción práctica, orientada al usuario final, para trabajar con DBFlux:
conectar a una base de datos, explorar su schema, ejecutar queries, trabajar con
resultados, graficar, y el modelo de teclado.

DBFlux es keyboard-first. Casi todas las acciones tienen tanto un gesto de ratón
como un atajo de teclado. Los atajos listados en esta guía son los valores por
defecto de la aplicación; puedes revisar el keymap activo completo en **Settings
→ Keybindings** (un visor de solo lectura — ver el [Resumen de
Settings](#8-settings-overview)).

---

## 1. Primer inicio y creación de una conexión

Al arrancar, DBFlux restaura tu sesión anterior (pestañas abiertas). En una
instalación nueva no hay nada que restaurar, así que el foco recae por defecto
en el sidebar.

### Abrir el Connection Manager

Abre el Connection Manager para crear o editar conexiones:

- Desde el sidebar, pulsa `c`.
- O usa el command palette (`Ctrl+Shift+P` / `Cmd+Shift+P` en macOS) y ejecuta
  **Open Connection Manager**.

### Elegir un driver

El Connection Manager muestra un selector de drivers. Los drivers disponibles
dependen de los features con los que se compiló el binario; el build estándar
incluye SQLite, PostgreSQL, MySQL/MariaDB, MongoDB, Redis, DynamoDB, Microsoft
SQL Server, e integraciones respaldadas por AWS. Los drivers RPC registrados
externamente también aparecen aquí cuando están configurados (ver
`docs/RPC_SERVICES_CONFIG.md`).

Usa `/` para filtrar la lista de drivers, `j`/`k` (o las flechas) para moverte,
y `Enter` para seleccionar.

### Modo formulario vs. URI directa

Cada driver ofrece su propio formulario de conexión. El formulario es dinámico:
solo muestra los campos que ese driver realmente necesita. La mayoría de los
drivers relacionales soportan dos formas de indicar los detalles de conexión:

- **Basado en formulario**: campos individuales (host, port, database, user,
  etc.).
- **URI directa**: un único campo con la cadena de conexión.

Los drivers respaldados por archivo, como SQLite, usan en su lugar un formulario
de ruta de archivo.

### Pestaña Access: direct, SSH, proxy, managed

La pestaña **Access** controla cómo llega DBFlux a la base de datos:

- **Direct** — conecta directamente al host desde los campos de la pestaña Main.
  El modo Direct puede seguir resolviendo fuentes de valores
  Secret/Parameter/Auth para campos individuales.
- **SSH** — encamina la conexión a través de un host SSH. Los perfiles de túnel
  SSH se gestionan de forma centralizada en Settings y se seleccionan por
  conexión.
- **Proxy** — encamina a través de un proxy SOCKS5 o HTTP CONNECT. Proxy y SSH
  son mutuamente excluyentes para una misma conexión.
- **Managed** — acceso gestionado por un provider (por ejemplo `aws-ssm`), donde
  DBFlux abre el acceso a través de un provider externo antes de conectar.

Al conectar, DBFlux ejecuta un pipeline pre-connect: autenticación y validación
de sesión, resolución dinámica de valores, y luego la configuración del acceso
managed/direct, seguido del connect del driver y una carga inicial del schema.
Los hooks de conexión (si están configurados) se ejecutan en las fases
PreConnect, PostConnect, PreDisconnect y PostDisconnect. Ver el resumen de
Settings para dónde se definen los hooks.

---

## 2. Explorar el schema

El sidebar tiene dos pestañas:

- **Connections** — el árbol del schema (bases de datos, schemas,
  tablas/collections, columnas, índices y — donde el driver lo soporte — una
  carpeta Routines).
- **Scripts** — gestión de archivos y carpetas para archivos de query guardados,
  script hooks y otros archivos de usuario.

Cambia entre las dos pestañas con `q` o `e`.

### Navegar el árbol

- `j`/`k` (o `Down`/`Up`) — mueve la selección.
- `h` colapsa, `l` expande el nodo actual. `Space` alterna expandir/colapsar.
- `g` salta al primer elemento, `Shift+g` al último; `Home`/`End` hacen lo
  mismo.
- `Ctrl+d`/`Ctrl+u` (o `PageDown`/`PageUp`) — recorre listas largas por páginas.
- `/` enfoca la búsqueda/filtro del sidebar.
- `Enter` abre el elemento seleccionado (por ejemplo, una tabla abre un data
  grid).
- `r` refresca el schema; `d` desconecta la conexión activa.
- `m` abre el menú contextual del elemento seleccionado.

### Carga diferida (lazy loading)

El schema se carga de forma diferida. Al conectar, DBFlux obtiene metadatos
superficiales (nombres). Los metadatos detallados — columnas, índices y
similares — se obtienen bajo demanda al expandir un nodo. Esto mantiene rápida
la conexión inicial en bases de datos grandes.

### Rutinas / procedimientos almacenados

Para los drivers que declaran soporte de rutinas (PostgreSQL es la primera
implementación), el árbol del schema incluye una carpeta **Routines** con
funciones, procedimientos, agregados y rutinas de ventana. Abrir una rutina abre
un documento de código de solo lectura que muestra su definición. El documento
no es editable, pero puedes seleccionar y copiar su texto; los controles de
ejecución y mutación están ocultos.

---

## 3. Ejecutar queries

Abre una nueva pestaña de query con `Ctrl+n` (`Cmd+n` en macOS), o abre un
archivo de script con `Ctrl+o`. El lenguaje de query del editor (SQL, sintaxis
de queries de MongoDB, comandos de Redis, etc.) lo determina el driver de la
conexión activa, que también controla el resaltado de sintaxis y el texto de
placeholder.

### Ejecutar

- `Ctrl+Enter` (`Cmd+Enter`) — **Run Query**.
- `Ctrl+Shift+Enter` (`Cmd+Shift+Enter`) — **Run Query in New Tab**.

Si existe una selección de texto no vacía, solo se ejecuta el texto
seleccionado. Sin selección, se usa el buffer completo del editor.

### Scripts multi-statement

Cuando ejecutas sin selección y el buffer contiene varias sentencias separadas
por `;`, y el driver activo declara soporte de batch, DBFlux muestra un diálogo
de confirmación (`Run entire script (N statements)?`) antes de ejecutar. Al
confirmar, el conjunto de resultados de cada sentencia se renderiza en su propia
pestaña de resultado.

La división en sentencias es consciente del lenguaje para los lenguajes de la
familia SQL: los separadores dentro de strings, identificadores, comentarios de
línea/bloque y los cuerpos dollar-quoted de PostgreSQL no se tratan como límites
de sentencia. Los lenguajes no SQL siguen siendo de sentencia única. El soporte
de batch es por driver — entre los drivers SQL integrados, PostgreSQL,
MySQL/MariaDB, SQLite y Microsoft SQL Server lo soportan. Una selección siempre
se ejecuta tal cual y nunca dispara la confirmación de script.

### Confirmación de queries peligrosas

DBFlux detecta operaciones peligrosas entre lenguajes — `DELETE`/`DROP`/
`TRUNCATE` de SQL y `DELETE`/`UPDATE` sin `WHERE`, `deleteMany`/`drop` de
MongoDB, `FLUSHALL`/`FLUSHDB`/`KEYS` de Redis — y pide confirmación antes de
ejecutar. Este comportamiento se controla desde settings: la confirmación de
queries peligrosas se puede desactivar, se puede requerir una cláusula `WHERE`
para `DELETE`/`UPDATE`, y `FLUSHALL`/`FLUSHDB` de Redis se puede deshabilitar
por completo (en cuyo caso esos comandos quedan bloqueados en lugar de
confirmados).

### Scripts (Lua / Python / Bash)

Los documentos Lua, Python y Bash se ejecutan como scripts en lugar de queries
de base de datos. Su salida se transmite en vivo al área de salida del documento
mientras se ejecutan, y la salida final se conserva como un resultado de texto.
Ver `docs/LUA.md` para el runtime de Lua embebido.

### Constructor visual de queries

Para conexiones SQL puedes componer queries sin escribir SQL. Desde la toolbar
del data grid de una tabla, haz clic en **Builder** para abrir un panel en el
rail derecho. El builder solo está disponible en drivers SQL; las conexiones no
SQL no lo muestran.

El panel tiene un selector de modo en la parte superior — **SELECT**,
**UPDATE**, **DELETE** — y una vista previa de SQL en vivo que se regenera con
cada cambio. La vista previa siempre es visible. Pulsa **Run** para ejecutar, o
(en modo SELECT) **Open in Editor** para volcar el SQL generado en un editor de
query normal. La cabecera tiene **Save** y **Reset**.

| Teclas                             | Acción                           |
| ---------------------------------- | -------------------------------- |
| `Cmd+Enter` / `Ctrl+Enter`         | Ejecutar                         |
| `Cmd+E` / `Ctrl+E`                 | Abrir en el editor (modo SELECT) |
| `Cmd+S` / `Ctrl+S`                 | Guardar                          |
| `Cmd+Shift+S` / `Ctrl+Shift+S`     | Guardar como                     |
| `Cmd+Backspace` / `Ctrl+Backspace` | Reiniciar                        |

#### Construir un SELECT

El cuerpo del SELECT tiene secciones que rellenas de arriba a abajo:

- **Columns** — la proyección (qué columnas seleccionar).
- **Filters** — un árbol de predicados `WHERE`. Los predicados se pueden anidar
  en grupos AND/OR, así que puedes construir condiciones complejas de forma
  visual.
- **Joins** — tablas adicionales con un alias y una condición `ON`.
- **Group By / Aggregates** — ver más abajo.
- **Sort** — entradas de `ORDER BY`.
- **Limit & Offset** — límites de paginación.

La vista previa de SQL está parametrizada: los valores literales se emiten como
placeholders para el dialecto activo (SQLite, PostgreSQL, MySQL/MariaDB o SQL
Server).

#### GROUP BY y agregados

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
- Las entradas de **Sort** quedan restringidas a las columnas de agrupación y
  los alias de los agregados; las entradas inválidas se rechazan con un error
  visible.

Cómo se comportan los resultados agrupados en el data grid se describe en
[Resultados agregados](#aggregated-results).

#### Autocompletado consciente del schema

Los inputs de una sola línea del builder (filtro, orden, columnas proyectadas,
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
grid (ver [Filtrar resultados](#filtering-results)).

#### Queries guardadas

Los builders se pueden guardar por perfil de conexión y reabrir más tarde. Las
queries guardadas están acotadas al perfil, con nombres únicos. Una query
guardada también se puede importar a otra conexión; al importar, DBFlux verifica
que las tablas referenciadas existan en la conexión destino antes de cargarla.

#### UPDATE y DELETE visuales

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
peligrosas](#dangerous-query-confirmation)) antes de ejecutarse.

---

## 4. Trabajar con resultados

Los resultados se renderizan en pestañas de resultado dentro del documento. El
modo de vista se elige automáticamente según la categoría de la base de datos:

- **Vista de tabla** para bases de datos relacionales.
- **Vista de árbol de documentos** para bases de datos de documentos (por
  ejemplo MongoDB, DynamoDB).
- **Vista clave-valor** para Redis.

Los contenedores de tipo event-stream se abren como event streams cuando el
driver declara esa presentación.

### Navegar el data grid

Cuando el panel de resultados tiene el foco:

- `j`/`k` (o `Down`/`Up`) — moverse entre filas.
- `h`/`l` (o `Left`/`Right`) — moverse entre columnas.
- `g`/`Shift+g` (o `Home`/`End`) — primera / última fila.
- `Ctrl+d`/`Ctrl+u` (o `PageDown`/`PageUp`) — recorrer filas por páginas.
- `[` / `]` — página anterior / siguiente de resultados (paginación).
- `f` enfoca la toolbar; `/` enfoca la búsqueda/filtro.
- `z` alterna el colapso del panel.
- `m` (o `Shift+F10`) abre el menú contextual de fila/celda.

### Filtrar resultados

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
schema](#schema-aware-autocomplete)).

### Editar y CRUD

En el data grid:

- `o` — añadir una fila.
- `x` — eliminar la fila seleccionada.
- `r` — renombrar / editar (según el contexto).
- `y` — copiar la fila seleccionada.
- `Ctrl+c` (`Cmd+c`) — copiar la(s) celda(s) seleccionada(s) al portapapeles.

#### Cuándo los resultados son editables

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

#### Resultados agregados

Cuando un resultado proviene de una query agrupada (`GROUP BY`), las filas
muestran la salida agregada y la edición está deshabilitada — añadir fila,
eliminar fila, editar celda e inspeccionar fila no están disponibles, con
tooltips explicativos. La paginación cuenta las filas agrupadas (no las filas
subyacentes), así que el total de páginas es correcto. Las columnas de agregado
conservan el tipo de columna correcto, así que graficar sigue funcionando.

### Copiar como query

El menú contextual de resultados incluye **Copy as Query**, que genera una
sentencia de mutación específica del driver (o un envelope, para drivers no SQL)
a partir de la fila seleccionada usando el generador de queries propio del
driver.

### Exportar

Pulsa `Ctrl+e` (`Cmd+e`) en el panel de resultados, o ejecuta **Export Results**
desde el command palette. Los formatos disponibles dependen de la forma del
resultado e incluyen:

- **CSV**
- **JSON (pretty)** y **JSON (compact)**
- **Text**
- **Binary** (para resultados con forma binaria)

---

## 5. Graficar resultados

Cualquier query que produzca resultados tabulares se puede graficar. En la
toolbar del editor de queries, haz clic en el botón de gráfico (tooltip: "Open
current query in a chart document") para abrir la query actual en un documento
de gráfico.

Los gráficos usan los metadatos de tipo de columna que aporta el driver para
autodetectar los ejes (columnas de tiempo, columnas numéricas, etc.). Los tipos
de gráfico soportados son:

- **Line**
- **Bar**
- **Scatter**
- **Area**
- **Stacked Bar**
- **Pie**

Los gráficos se pueden guardar por perfil de conexión. Para reabrir un gráfico
guardado, ejecuta **Open Chart...** desde el command palette (`OpenSavedChart`),
que lista los gráficos guardados del perfil actual en un overlay de búsqueda
difusa.

---

## 6. Queries guardadas e historial

DBFlux mantiene un historial de las queries completadas y te permite guardar
queries con nombre.

- `Alt+h` (en el editor) alterna el desplegable de historial de queries.
- `Ctrl+s` (`Cmd+s`) — **Save** la query actual.
- `Ctrl+Shift+s` (`Cmd+Shift+s`) — **Save File As**.
- `Ctrl+p` (`Cmd+p`, en el editor) — abre el explorador de queries guardadas.

Dentro del modal de historial puedes navegar con `Ctrl+j`/`Ctrl+k` (o las
flechas), abrir una entrada con `Enter`, y usar los mnemónicos locales `Ctrl+f`
(marcar como favorito), `Ctrl+r` (renombrar) y `Ctrl+d` (eliminar). `/` enfoca
la búsqueda del modal.

---

## 7. Referencia de teclado

DBFlux usa un keymap por capas, sensible al contexto. La capa activa depende de
qué panel tiene el foco. Los atajos escritos con el modificador **primary** usan
`Cmd` en macOS y `Ctrl` en el resto de plataformas; los atajos escritos con
`Ctrl` literal se mantienen como `Ctrl` en todas las plataformas (para evitar
conflictos con los atajos del sistema en macOS).

### Global (disponible sin importar el foco)

| Teclas                                    | Acción                                 |
| ----------------------------------------- | -------------------------------------- |
| `Ctrl+Shift+P` / `Cmd+Shift+P`            | Alternar command palette               |
| `Ctrl+n` / `Cmd+n`                        | Nueva pestaña de query                 |
| `Ctrl+w` / `Cmd+w`                        | Cerrar pestaña actual                  |
| `Ctrl+Tab` / `Ctrl+Shift+Tab`             | Pestaña siguiente / anterior           |
| `Ctrl+1` .. `Ctrl+9` / `Cmd+1` .. `Cmd+9` | Cambiar a la pestaña N                 |
| `Ctrl+o` / `Cmd+o`                        | Abrir archivo de script                |
| `Ctrl+Enter` / `Cmd+Enter`                | Ejecutar query                         |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter`    | Ejecutar query en nueva pestaña        |
| `Escape`                                  | Cancelar / cerrar modal                |
| `Tab` / `Shift+Tab`                       | Ciclar el foco adelante / atrás        |
| `Ctrl+Shift+1`                            | Enfocar sidebar                        |
| `Ctrl+Shift+2`                            | Enfocar editor                         |
| `Ctrl+Shift+3`                            | Enfocar resultados                     |
| `Ctrl+Shift+4`                            | Enfocar tareas en segundo plano        |
| `Ctrl+Shift+A` / `Cmd+Shift+A`            | Abrir el visor de auditoría            |
| `Ctrl+b` / `Cmd+b`                        | Alternar sidebar                       |
| `Ctrl+m`                                  | Abrir el menú contextual de la pestaña |

### Sidebar

| Teclas                                        | Acción                                                 |
| --------------------------------------------- | ------------------------------------------------------ |
| `q` / `e`                                     | Cambiar de pestaña del sidebar (Connections / Scripts) |
| `/`                                           | Enfocar búsqueda                                       |
| `j` / `k` (o `Down` / `Up`)                   | Seleccionar siguiente / anterior                       |
| `h` / `l`                                     | Colapsar / expandir nodo                               |
| `Space`                                       | Expandir / colapsar                                    |
| `g` / `Shift+g` (o `Home` / `End`)            | Primer / último elemento                               |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Página abajo / arriba                                  |
| `Enter`                                       | Abrir / ejecutar elemento                              |
| `r`                                           | Refrescar schema                                       |
| `c`                                           | Abrir el Connection Manager                            |
| `d`                                           | Desconectar                                            |
| `m`                                           | Abrir el menú del elemento                             |
| `Shift+j` / `Shift+k`                         | Extender la selección abajo / arriba                   |
| `Space` (con Shift)                           | Alternar selección                                     |
| `Ctrl+j` / `Ctrl+k`                           | Mover el elemento seleccionado abajo / arriba          |
| `Shift+r`                                     | Renombrar                                              |
| `x`                                           | Eliminar                                               |
| `Shift+n`                                     | Crear carpeta                                          |
| `Ctrl+l`                                      | Enfocar el panel de la derecha                         |

### Editor

| Teclas                         | Acción                                   |
| ------------------------------ | ---------------------------------------- |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Enfocar panel izquierda / abajo / arriba |
| `Alt+h`                        | Alternar desplegable de historial        |
| `Ctrl+p` / `Cmd+p`             | Abrir queries guardadas                  |
| `Ctrl+s` / `Cmd+s`             | Guardar query                            |
| `Ctrl+Shift+s` / `Cmd+Shift+s` | Guardar archivo como                     |
| `Enter`                        | Enfocar / ejecutar                       |

(Las letras sin modificador se dejan intencionadamente para el input de texto,
así la escritura funciona con normalidad.)

### Resultados

| Teclas                                        | Acción                                     |
| --------------------------------------------- | ------------------------------------------ |
| `Ctrl+h` / `Ctrl+k` / `Ctrl+l`                | Enfocar panel izquierda / arriba / derecha |
| `Ctrl+j`                                      | Focus toolbar                              |
| `j` / `k` (o `Down` / `Up`)                   | Fila siguiente / anterior                  |
| `h` / `l` (o `Left` / `Right`)                | Columna izquierda / derecha                |
| `g` / `Shift+g` (o `Home` / `End`)            | Primera / última fila                      |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Página abajo / arriba                      |
| `]` / `[`                                     | Página siguiente / anterior de resultados  |
| `Ctrl+e` / `Cmd+e`                            | Exportar resultados                        |
| `f`                                           | Focus toolbar                              |
| `/`                                           | Enfocar búsqueda/filtro                    |
| `x`                                           | Eliminar fila                              |
| `r`                                           | Renombrar / editar                         |
| `o`                                           | Añadir fila                                |
| `y`                                           | Copiar fila                                |
| `Ctrl+c` / `Cmd+c`                            | Copiar celda(s)                            |
| `z`                                           | Alternar colapso del panel                 |
| `m` (o `Shift+F10`)                           | Abrir menú contextual                      |

### Tareas en segundo plano

| Teclas                                        | Acción                                   |
| --------------------------------------------- | ---------------------------------------- |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k`                | Enfocar panel izquierda / abajo / arriba |
| `j` / `k` (o `Down` / `Up`)                   | Seleccionar siguiente / anterior         |
| `g` / `Shift+g` (o `Home` / `End`)            | Primero / último                         |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Página abajo / arriba                    |
| `z`                                           | Alternar colapso del panel               |

### Command palette

| Teclas                      | Acción                           |
| --------------------------- | -------------------------------- |
| `j` / `k` (o `Down` / `Up`) | Seleccionar siguiente / anterior |
| `Enter`                     | Ejecutar                         |
| `Escape`                    | Cancelar                         |

### Menú contextual

| Teclas                      | Acción                          |
| --------------------------- | ------------------------------- |
| `j` / `k` (o `Down` / `Up`) | Mover abajo / arriba            |
| `Enter` / `l` (o `Right`)   | Seleccionar / entrar en submenú |
| `Escape` / `h` (o `Left`)   | Volver / cerrar                 |

### Modal de historial

| Teclas                                | Acción                           |
| ------------------------------------- | -------------------------------- |
| `Ctrl+j` / `Ctrl+k` (o `Down` / `Up`) | Seleccionar siguiente / anterior |
| `Enter`                               | Abrir entrada                    |
| `Ctrl+f`                              | Alternar favorito                |
| `Ctrl+r`                              | Renombrar                        |
| `Ctrl+d`                              | Eliminar                         |
| `/`                                   | Enfocar búsqueda                 |
| `Ctrl+s` / `Cmd+s`                    | Guardar query                    |

---

## 8. Resumen de Settings

Settings tiene estas secciones (las secciones de MCP solo aparecen en builds con
soporte de AI/MCP, que es el valor por defecto):

- **General** — preferencias globales de la aplicación: tema, inicio/sesión,
  valores por defecto de refresco, y el comportamiento de confirmación de
  queries peligrosas.
- **Audit** — qué captura el log de auditoría (nivel mínimo de captura de log) y
  la retención.
- **MCP Clients / Roles / Policies** — gobernanza de clientes de AI (clientes
  confiables, roles, políticas). Ver `docs/MCP_AI_INTEGRATION.md`.
- **Keybindings** — un visor de **solo lectura** del keymap activo, con un
  filtro de texto y avisos de conflicto. Reasignar teclas desde la UI no está
  disponible.
- **Proxies** — perfiles de proxy SOCKS5 / HTTP CONNECT.
- **SSH Tunnels** — perfiles de túnel SSH seleccionables por conexión.
- **Auth Profiles** — perfiles de autenticación gestionados por un provider (AWS
  SSO / credenciales compartidas).
- **Services** — servicios RPC registrados externamente (drivers y auth
  providers). Ver `docs/RPC_SERVICES_CONFIG.md`.
- **Hooks** — definiciones globales de connection hooks (modos Command, Script y
  Lua). Los bindings de fase por perfil viven en la pestaña Hooks del Connection
  Manager.
- **Drivers** — overrides y ajustes por driver.
- **About** — información de versión y build.

Para una referencia completa por ajuste y la guía de connection hooks, ver
`docs/SETTINGS.md`.

### Documentación relacionada

- Configuración avanzada de conexión (SSH, proxy, AWS SSO, fuentes de valores):
  `docs/CONNECTIONS.md`
- Referencia completa de Settings y connection hooks: `docs/SETTINGS.md`
- Almacenamiento de datos y privacidad (dónde viven los datos y secretos,
  backup, reset): `docs/DATA_AND_PRIVACY.md`
- Dashboards, gráficos guardados y el visor de auditoría (uso):
  `docs/DASHBOARDS_AND_AUDIT.md`
- Connection hooks y el runtime de Lua embebido: `docs/LUA.md`
- Integración de clientes de AI (MCP): `docs/MCP_AI_INTEGRATION.md`
- Log de auditoría y schema de eventos: `docs/AUDIT.md`
- Drivers/servicios RPC externos: `docs/RPC_SERVICES_CONFIG.md`,
  `docs/DRIVER_RPC_PROTOCOL.md`
