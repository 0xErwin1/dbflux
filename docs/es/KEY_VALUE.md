# Vista clave-valor

Una base de datos clave-valor se abre como explorador de claves: la lista de
claves a la izquierda, el valor de la clave seleccionada a la derecha y una
consola de comandos debajo.

- **Lista de claves.** Las claves se cargan por páginas; **Cargar más**
  (`Ctrl+J`) continúa el escaneo y el pie muestra "Cargadas N de total" frente
  al conteo de claves de la base de datos. **Árbol** agrupa las claves en
  carpetas según el delimitador de espacios de nombres de la conexión (`:` salvo
  que los ajustes de la conexión definan otro) y ordena cada nivel; **Lista**
  muestra todas las claves ordenadas por nombre. Los conteos de carpeta solo
  cubren las claves cargadas y se leen como "≥ N claves" hasta escanear todo el
  keyspace. Las columnas TTL y Tamaño se completan para las filas visibles, en un
  lote por posición de desplazamiento; un TTL menor a un minuto se muestra en
  rojo.
- **Patrón y tipo.** El campo de patrón acepta un glob (`user:*`); el texto
  simple coincide en cualquier parte de la clave. El filtro de tipo (Todos,
  String, Hash, List, Set, ZSet, Stream, JSON) se ejecuta en el servidor, así que
  cubre todo el keyspace. Una búsqueda filtrada sigue escaneando hasta tener una
  página de coincidencias; **Detener** la termina y **Buscar en todo el
  keyspace** lee hasta el final.
- **Caducidad.** Haz clic en el TTL de la cabecera del valor, o pulsa `t`, para
  fijar la caducidad: **Nunca**, **En** una duración (`1h`, `24h`, `7d`, `30d` o
  cualquier valor tipo `1h30m`) o **El** una fecha y hora local. La hora absoluta
  de caducidad se muestra antes de aplicar. Editar un valor conserva la
  caducidad de la clave.
- **Ver como.** Los valores string se pueden mostrar como Auto, JSON (con
  formato y números de línea), Texto, MsgPack o Hex, tras un paso opcional de
  descompresión (gzip, zstd, snappy, lz4). La detección solo se ejecuta sobre el
  valor que abres. Un valor que supera el límite de vista previa muestra primero
  su tamaño, con **Ver los primeros 64 KB** y **Cargar de todos modos**.
- **Colecciones.** Hashes, lists y sets muestran una tabla de miembros con una
  insignia de formato por valor. Los sorted sets se paginan por posición, de
  mayor o menor puntuación, con una barra relativa a la puntuación máxima. Los
  streams se paginan entre un ID de inicio y uno de fin, de más reciente o más
  antigua, con una columna por campo y los grupos de consumidores al lado:
  lectores, entradas pendientes, último ID entregado y **Reclamar** para pasar
  las pendientes a otro lector.
- **Eliminación masiva.** **Acciones masivas → Eliminar claves que coinciden con
  el patrón** escanea toda la base de datos con el patrón y el tipo actuales,
  lista las primeras coincidencias con el total y pide escribir el patrón.
  **Exportar claves antes** guarda las claves y sus valores en un archivo JSON
  Lines. Las claves se eliminan en lotes con `UNLINK` y la eliminación queda
  registrada en el registro de auditoría.
- **Consola.** `` Ctrl+` `` abre una consola que ejecuta comandos contra la base de
  datos abierta. Los comandos peligrosos pasan por la misma confirmación que el
  editor (ver
  [Confirmación de queries peligrosas](EDITOR.md#confirmación-de-queries-peligrosas)),
  cada comando queda registrado en el registro de auditoría como una query del
  editor, y los comandos exitosos se suman al historial de consultas.
  `Up`/`Down` recorren ese historial de la conexión, junto con los comandos que
  la consola rechazó o que fallaron en esta sesión. Las colecciones de
  documentos ofrecen la misma consola (ver
  [Colecciones de documentos](DOCUMENTS.md)).

En la barra lateral, cada base de datos de una conexión clave-valor muestra su
conteo de claves, y las bases de datos sin claves se agrupan en una sola fila
"N bases de datos vacías".
