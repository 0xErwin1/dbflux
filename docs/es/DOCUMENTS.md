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
  tipo se marcan y al hacer clic en un valor se añade al filtro.
- Con MongoDB, las ediciones en la tabla quedan pendientes: una celda editada
  muestra su valor anterior y el nuevo, el menú de la celda ofrece **Revertir
  cambio** y **Quitar campo**, y **Confirmar** (`Ctrl+S`) escribe cada documento
  como `$set` / `$unset` solo sobre las rutas modificadas. La vista JSON
  reemplaza documentos completos; las ediciones en el árbol se escriben al
  momento. Antes de escribir, DBFlux vuelve a leer el documento: si cambió en el
  servidor después de cargar la página, una tarjeta indica si tu cambio puede
  aplicarse encima, muestra la actualización exacta y ofrece **Recargar
  documento** o **Aplicar mi cambio**.
