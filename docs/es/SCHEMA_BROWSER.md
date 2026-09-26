# Explorar el schema

El sidebar tiene tres vistas, que se eligen desde la barra de actividad a su
izquierda:

- **Connections** — el árbol del schema (bases de datos, schemas,
  tablas/collections, columnas, índices y — donde el driver lo soporte — una
  carpeta Routines).
- **Scripts** — gestión de archivos y carpetas para archivos de query guardados,
  script hooks y otros archivos de usuario.
- **Dashboards** — todos los dashboards guardados, agrupados por la conexión a
  la que pertenecen, esté abierta o no. Los dashboards sin conexión aparecen en
  **Sin conexión**. Haz doble clic en un dashboard, o selecciónalo y pulsa
  `Enter`, para abrirlo; haz clic derecho para abrirlo, renombrarlo,
  duplicarlo o eliminarlo. El botón `+` crea un dashboard.

Recorre las vistas con `q` o `e`. Elegir desde la barra la vista que ya está en
pantalla contrae el sidebar.

## Navegar el árbol

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

## Carga diferida (lazy loading)

El schema se carga de forma diferida. Al conectar, DBFlux obtiene metadatos
superficiales (nombres). Los metadatos detallados — columnas, índices y
similares — se obtienen bajo demanda al expandir un nodo. Esto mantiene rápida
la conexión inicial en bases de datos grandes.

## Vista temporal del panel lateral contraído

Al pasar el puntero sobre el panel lateral contraído, este aparece tras 250 ms;
entrar mediante el teclado (FocusSidebar, ciclo de foco, navegación direccional
o paleta de comandos) lo muestra de inmediato. La vista temporal se cierra
cuando tanto el puntero como el foco salen, salvo que haya un menú, selector de
hijos, destino de arrastre detectado o ajuste de tamaño activo. Fuera de la
vista temporal, ToggleSidebar (Ctrl+B) cambia la elección explícita entre
contraído y expandido. Durante la vista temporal, Ctrl+B o la flecha visible
solo la cierran. Los selectores de origen y destino del asistente de migración
comparten la jerarquía de carga diferida, pero mantienen selecciones
independientes.

## Rutinas / procedimientos almacenados

Para los drivers que declaran soporte de rutinas (PostgreSQL es la primera
implementación), el árbol del schema incluye una carpeta **Routines** con
funciones, procedimientos, agregados y rutinas de ventana. Abrir una rutina abre
un documento de código de solo lectura que muestra su definición. El documento
no es editable, pero puedes seleccionar y copiar su texto; los controles de
ejecución y mutación están ocultos.

## Diagrama de esquema

Las conexiones relacionales cuyo driver declara soporte de claves foráneas (por
ejemplo PostgreSQL, MySQL/MariaDB, SQLite y SQL Server) pueden dibujar las
tablas y sus claves foráneas como un diagrama. Ábrelo desde el menú contextual
del sidebar:

- **Ver diagrama de esquema** sobre una base de datos cargada dibuja todas sus
  tablas, hasta 100. Si la base de datos tiene más, se muestra el aviso "Se
  muestran las primeras 100 tablas — el esquema tiene más."
- **Ver relaciones** sobre una tabla dibuja esa tabla, las tablas a las que
  referencia y las tablas que la referencian.

El diagrama se abre en su propia pestaña, y abrir el mismo diagrama otra vez
cambia a esa pestaña. La carga se ejecuta como una tarea en segundo plano
("Diagrama de esquema: _base de datos_") que puedes cancelar desde el panel
Tasks. Cada tabla lista sus columnas con las insignias `PK`, `FK` y `NN` (not
null), y unas líneas conectan cada clave foránea con la tabla que referencia.

| Control de la toolbar | Qué hace |
|---|---|
| `+` / `-` | Acerca / aleja, entre 25% y 400%. El zoom actual aparece a su lado. |
| **Restablecer** | Vuelve al 100% de zoom y a la posición inicial. |
| **Organizar** | Descarta las posiciones de las tablas que moviste y recalcula el diseño. |
| **Ajustar** | Aplica zoom y desplaza la vista para que todas las tablas sean visibles. |
| Desplegable de diseño | **Izquierda-Derecha** (predeterminado) coloca a la izquierda las tablas que tienen claves foráneas y a la derecha las tablas que referencian. **Copo de nieve** pone una tabla en el centro y sus vecinas directas en un círculo alrededor: la tabla elegida en **Ver relaciones**, la tabla con más relaciones en un diagrama de base de datos. **Compacto** agrupa las tablas en una cuadrícula ajustada, ordenada por nombre. Cambiar el diseño también restablece el zoom, la posición y las tablas movidas. |
| **Exportar** | **Copiar como DBML** o **Copiar como SQL** copia al portapapeles las tablas que muestra el diagrama. El SQL son sentencias `CREATE TABLE` más `ALTER TABLE ... ADD CONSTRAINT` para las claves foráneas. |
| _N_ tablas · _M_ relaciones | Cuántas tablas y claves foráneas muestra el diagrama. |
| **Tipos** / **Índices** | Muestra los tipos de columna (activado por defecto) / una lista de índices bajo cada tabla (desactivado por defecto). |

Arrastra un espacio vacío para desplazar la vista, arrastra una tabla para
moverla (se ajusta a la cuadrícula) y usa la rueda del ratón para hacer zoom
alrededor del puntero. Haz clic en una tabla para seleccionarla. El clic derecho
abre un menú contextual con **Acercar**, **Alejar**, **Diseño** y **Copiar
como**. El clic derecho sobre una tabla también la selecciona, y mientras haya
una tabla seleccionada el menú agrega **Inspeccionar esquema**. **Inspeccionar
esquema**, o un doble clic sobre una tabla, abre la tabla en el panel inspector
de la derecha, con las secciones TABLA, COLUMNAS, ÍNDICES y CLAVES FORÁNEAS. Las
dos últimas aparecen solo cuando la tabla tiene índices o declara claves
foráneas. Los atajos de teclado están en la
[Referencia de teclado](KEYBOARD.md#diagrama-de-esquema).
