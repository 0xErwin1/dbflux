# Colecciones de documentos

Una colección de una base de datos de documentos se abre como una tabla de sus
documentos: primero `_id` y después los campos en el orden en que los devuelve
la página. **Árbol** y **JSON** muestran la misma página; cambia con el control
Árbol / Tabla / JSON o pulsa `t`.

- Un objeto anidado muestra su número de campos y un arreglo su longitud. `e`
  sobre una columna de objeto la expande en su lugar en un grupo de columnas;
  `Enter` sobre un objeto o arreglo entra en él y lista su contenido como filas,
  con una ruta de navegación; `Backspace` sale.
- El pie cuenta documentos. Cuando el driver solo puede estimar cuántos
  documentos coinciden, el conteo se marca como estimado.
- Los drivers que lo soportan (MongoDB) añaden una barra de consulta con cuatro
  campos — `filter`, `project`, `sort` y `limit` — que aceptan JSON relajado con
  autocompletado de rutas de campo a partir de una muestra de la colección.
  `Ctrl+Enter` o **Buscar** ejecuta la consulta; el botón de historial vuelve a
  ejecutar una anterior.
- La vista **Esquema**, junto a Documentos, muestrea la colección y lista cada
  ruta de campo con su distribución de tipos, su presencia y un resumen de sus
  valores. El tamaño de la muestra se puede cambiar, los campos con más de un
  tipo se marcan y al hacer clic en un valor se añade al filtro. La vista
  Esquema muestra solo los controles de la muestra y la tabla de campos, sin la
  barra de consulta, el pie de documentos ni el conteo de documentos en la
  cabecera.
- Los drivers que ejecutan pipelines de agregación (MongoDB) añaden una vista
  **Agregación** después de Esquema. Escribe el pipeline como un arreglo JSON de
  etapas, con claves relajadas
  (`[{ $match: { status: 'paid' } }, { $group: { _id: '$region' } }]`), y
  ejecútalo con **Ejecutar** o `Ctrl+Enter`. Un pipeline que no se puede
  analizar, o una etapa que no es un documento con un único operador `$`, se
  informa debajo del editor y no se ejecuta nada. Los documentos de resultado
  se muestran en sus propias vistas Árbol, Tabla y JSON, separadas de la página
  de Documentos, y son de solo lectura: no se pueden editar, borrar ni
  confirmar. Una ejecución muestra como máximo 1.000 documentos, y el pie indica
  cuándo se recortó el resultado. Un pipeline con una etapa `$out` o `$merge`
  escribe en una colección, así que pide la misma confirmación que cualquier
  otra consulta peligrosa antes de ejecutarse. El botón de historial recupera
  un pipeline anterior de la misma pestaña.
- El atajo o la acción de fila que abre el inspector de filas en una tabla abre
  el panel **Documento** en una colección: el tamaño del documento y luego sus
  campos como un árbol de filas `clave : valor`, con el valor coloreado según su
  tipo y un tipo corto (`oid`, `obj`, `str`, `arr`, `date`, `dec`, `bool`, ...)
  a la derecha. Los objetos y arreglos se expanden y contraen con un clic; los
  campos de primer nivel empiezan expandidos. El botón de expandir abre el
  documento en el editor JSON. El panel sigue a la fila seleccionada, y un
  campo con una edición pendiente sin confirmar muestra su nuevo valor,
  resaltado como la celda editada.
- Con MongoDB, las ediciones en la tabla quedan pendientes: una celda editada
  muestra su valor anterior y el nuevo, el menú de la celda ofrece **Revertir
  cambio** y **Quitar campo**, y **Confirmar** (`Ctrl+S`) escribe cada documento
  como `$set` / `$unset` solo sobre las rutas modificadas. La vista JSON
  reemplaza documentos completos; las ediciones en el árbol se escriben al
  momento. Antes de escribir, DBFlux vuelve a leer el documento: si cambió en el
  servidor después de cargar la página, una tarjeta indica si tu cambio puede
  aplicarse encima, muestra la actualización exacta y ofrece **Recargar
  documento** o **Aplicar mi cambio**.
