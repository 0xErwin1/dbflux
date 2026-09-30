# Colecciones de documentos

Una colección de una base de datos de documentos se abre como un árbol de sus
documentos, con un nodo plegable por documento. **Tabla** y **JSON** muestran la
misma página; cambia con el control Árbol / Tabla / JSON o pulsa `t`. La vista
que elijas se mantiene en la pestaña entre páginas y actualizaciones. La tabla
muestra primero `_id` y después los campos en el orden en que los devuelve la
página.

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
  se muestran en sus propias vistas Árbol, Tabla y JSON, abriéndose en el
  Árbol, separadas de la página de Documentos, y son de solo lectura: no se pueden editar, borrar ni
  confirmar. Una ejecución muestra como máximo 1.000 documentos, y el pie indica
  cuándo se recortó el resultado. Un pipeline con una etapa `$out` o `$merge`
  escribe en una colección, así que pide la misma confirmación que cualquier
  otra consulta peligrosa antes de ejecutarse. El botón de historial recupera
  un pipeline anterior de la misma pestaña.
- Los drivers que lo ofrecen (MongoDB) añaden un botón **Constructor** en la
  cabecera de la colección. Abre un constructor visual de consultas en el rail
  derecho que se mantiene sincronizado con los campos de la barra de consulta,
  puede agrupar documentos con `$count`, `$sum` y `$avg`, y guarda consultas
  por colección. Mientras el constructor está en modo Aggregate, la barra de
  consulta y la vista Agregación muestran un resumen de su pipeline en lugar de
  los campos y del editor de pipeline, y **Ejecutar pipeline** lo ejecuta en la
  vista Agregación. En otros drivers de documentos el botón está deshabilitado.
  Ver [Colecciones de documentos](QUERY_BUILDER.md#colecciones-de-documentos)
  en la guía del constructor de queries.
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
- Los drivers que ofrecen una consola nativa (MongoDB) la acoplan bajo la
  colección: `` Ctrl+` `` la muestra u oculta. Ejecuta un comando de shell a la
  vez, como `db.orders.find({"status": "paid"})`, contra la base de datos de la
  colección e imprime los documentos como un objeto JSON por línea. Los comandos
  pasan por la misma validación y confirmación de queries peligrosas que el
  editor, quedan registrados en el registro de auditoría y se suman al
  historial de consultas, que `Up`/`Down` recorren. Un comando que escribe
  refresca los documentos en pantalla. La consola muestra como máximo 200
  líneas de un resultado; usa el editor para resultados más grandes.
