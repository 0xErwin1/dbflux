# Referencia de teclado

DBFlux usa un keymap por capas, sensible al contexto. La capa activa depende de
qué panel tiene el foco. Los atajos escritos con el modificador **primary** usan
`Cmd` en macOS y `Ctrl` en el resto de plataformas; los atajos escritos con
`Ctrl` literal se mantienen como `Ctrl` en todas las plataformas (para evitar
conflictos con los atajos del sistema en macOS).

Cada atajo de abajo se puede cambiar en **Settings → Keybindings**: sus teclas
(una combinación o una secuencia como `g g`) y el contexto en el que se aplica.
Las teclas escritas con un espacio, como `y y`, se pulsan una tras otra; tras la
primera, DBFlux espera hasta un segundo la siguiente. Mientras hay un diálogo
abierto, los paneles detrás no reaccionan a sus teclas.

El foco se muestra solo después de usar el teclado: el anillo de acento aparece
en el control con foco tras pulsar una tecla, se mantiene mientras se mueve el
puntero y se oculta con el siguiente clic. Un botón, casilla o fila de lista con
foco toma `Enter` y `Space` para sí.

## Global (disponible sin importar el foco)

| Teclas                                           | Acción                                                            |
| ------------------------------------------------ | ----------------------------------------------------------------- |
| `Ctrl+Shift+P` / `Cmd+Shift+P`                   | Alternar command palette                                          |
| `Ctrl+Shift+N` / `Cmd+Shift+N`                   | Abrir el Connection Manager                                       |
| `Ctrl+n` / `Cmd+n`                               | Nueva pestaña de query                                            |
| `Ctrl+w` / `Cmd+w`                               | Cerrar pestaña actual                                             |
| `Ctrl+Tab` / `Ctrl+Shift+Tab`                    | Pestaña siguiente / anterior                                      |
| `Ctrl+Shift+PageUp` / `Ctrl+Shift+PageDown`      | Mover la pestaña activa a la izquierda / derecha                  |
| `Ctrl+1` .. `Ctrl+9` / `Cmd+1` .. `Cmd+9`        | Cambiar a la pestaña N                                            |
| `Ctrl+o` / `Cmd+o`                               | Abrir archivo de script                                           |
| `Ctrl+Enter` / `Cmd+Enter`                       | Ejecutar query                                                    |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter`           | Ejecutar query en nueva pestaña                                   |
| `Escape`                                         | Cancelar / cerrar modal                                           |
| `Tab` / `Shift+Tab`                              | Ciclar el foco adelante / atrás                                   |
| `Ctrl+Shift+1`                                   | Enfocar sidebar                                                   |
| `Ctrl+Shift+2`                                   | Enfocar editor                                                    |
| `Ctrl+Shift+3`                                   | Enfocar resultados                                                |
| `Ctrl+Shift+4`                                   | Enfocar tareas en segundo plano                                   |
| `Ctrl+Shift+A` / `Cmd+Shift+A`                   | Abrir el visor de auditoría                                       |
| `Ctrl+b` / `Cmd+b`                               | Alternar sidebar                                                  |
| `Ctrl+m`                                         | Abrir el menú contextual de la pestaña                            |
| `Ctrl+,` / `Cmd+,`                               | Abrir la configuración                                            |
| `Ctrl+Shift+E` / `Cmd+Shift+E`                   | Ocultar o mostrar los resultados de un documento de consulta, dejando solo el editor |
| `Ctrl+Shift+R` / `Cmd+Shift+R`                   | Maximizar los resultados de un documento de consulta sobre el editor, o restaurar la división |
| `Ctrl+Shift+T` / `Cmd+Shift+T`                   | Mostrar u ocultar el panel de tareas en segundo plano             |
| `Ctrl+Shift+5` / `Ctrl+Shift+6` / `Ctrl+Shift+7` | Mostrar la vista Connections / Scripts / Dashboards de la sidebar |
| `Ctrl+Shift+B` / `Cmd+Shift+B`                   | Abrir o cerrar el centro de notificaciones                        |
| `Ctrl+Shift+X` / `Cmd+Shift+X`                   | Abrir el error más reciente en el visor de auditoría              |
| `Ctrl+Shift+Y` / `Cmd+Shift+Y`                   | Abrir en un menú los botones del toast más reciente               |
| `Ctrl+Shift+L` / `Cmd+Shift+L`                   | Abrir el inicio de sesión de Auth Profile                         |
| `Ctrl+Shift+O` / `Cmd+Shift+O`                   | Abrir el asistente de AWS SSO                                     |
| `Ctrl+Shift+C` / `Cmd+Shift+C`                   | Abrir un gráfico guardado                                         |
| `Ctrl+Shift+D` / `Cmd+Shift+D`                   | Nuevo dashboard                                                   |
| `Ctrl+Shift+M` / `Cmd+Shift+M`                   | Abrir aprobaciones de MCP                                         |
| `Ctrl+Shift+G` / `Cmd+Shift+G`                   | Actualizar gobernanza de MCP                                      |

`Ctrl+Shift+X` abre el visor de auditoría en el error más reciente de esta sesión, como hace **Ver en auditoría** en el toast del error, y pone a cero el contador de la insignia de errores de la barra de estado; antes de cualquier error muestra los errores de usuario. `Ctrl+Shift+5` .. `Ctrl+Shift+7` se comportan como la barra de actividad: elegir la vista que ya se muestra colapsa la sidebar. `Ctrl+Shift+Y` lista los botones del toast más reciente en pantalla, como **Copiar**, **Ver en auditoría** o **Reconectar ahora**, luego **Mostrar detalles** u **Ocultar detalles** cuando tiene detalles, y **Descartar**; se maneja con las teclas del menú contextual y, sin ningún toast en pantalla, no abre nada. Los atajos de MCP existen en las compilaciones con soporte de MCP. **Exportar conexiones** e **Importar dashboard desde JSON** se ejecutan desde la command palette.

Todo lo que se puede clicar en la estructura de la ventana tiene una tecla, y nada de eso entra en el ciclo de `Tab`: las entradas de la barra de actividad son `Ctrl+Shift+5` .. `Ctrl+Shift+7`, `Ctrl+Shift+A` (Auditoría), `Ctrl+Shift+M` (Aprobaciones) y `Ctrl+,` (Configuración); la búsqueda de comandos de la barra de título es `Ctrl+Shift+P` y su campana `Ctrl+Shift+B`; la entrada de tareas de la barra de estado es `Ctrl+Shift+T`, la de aprobaciones `Ctrl+Shift+M` y la insignia de errores `Ctrl+Shift+X`; una pestaña se cierra con `Ctrl+w`, se abre con `Ctrl+n`, se reordena con `Ctrl+Shift+PageUp` / `Ctrl+Shift+PageDown` y su menú de clic derecho se abre con `Ctrl+m`. Los botones de minimizar, maximizar y cerrar de la ventana quedan en manos de los atajos del escritorio.

## Sidebar

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

`Escape` en el campo de búsqueda devuelve el foco al árbol y conserva el filtro escrito.

## Editor

| Teclas                         | Acción                                   |
| ------------------------------ | ---------------------------------------- |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Enfocar panel izquierda / abajo / arriba |
| `Ctrl+f` / `Cmd+f`             | Buscar en el editor                      |
| `Ctrl+Shift+h` / `Cmd+Shift+f` | Buscar y reemplazar en el editor         |
| `Alt+h`                        | Alternar desplegable de historial        |
| `Ctrl+p` / `Cmd+p`             | Abrir queries guardadas                  |
| `Ctrl+s` / `Cmd+s`             | Guardar query                            |
| `Ctrl+Shift+s` / `Cmd+Shift+s` | Guardar archivo como                     |
| `Ctrl+/` / `Cmd+/`             | Alternar comentario de línea             |
| `Shift+F10`                    | Abrir el menú de acciones del panel      |
| `Enter`                        | Enfocar / ejecutar                       |

(Las letras sin modificador se dejan intencionadamente para el input de texto,
así la escritura funciona con normalidad.)

`Ctrl+h` / `Ctrl+j` / `Ctrl+k` mueven el foco entre paneles con o sin el modo Vim
activo; nunca mueven el cursor. Con un menú de sugerencias abierto, `Ctrl+j` /
`Ctrl+k` recorren el menú. En el panel de búsqueda, `Ctrl+j` cierra el panel y
vuelve al editor, igual que `Escape`, y `Ctrl+h` / `Ctrl+k` lo cierran antes de
mover el foco. Las mismas teclas de buscar y reemplazar muestran u ocultan el
campo de reemplazo con el panel abierto. En el modo Normal de Vim el editor es de
solo lectura, así que el panel de búsqueda se abre sin el campo de reemplazo.

Mientras se escribe texto, `Tab` indenta y `Shift+Tab` quita la indentación, así
que para salir del editor se usan `Ctrl+h` / `Ctrl+j` / `Ctrl+k`. En el panel de
búsqueda, `Tab` / `Shift+Tab` alternan entre el campo de consulta y el de
reemplazo mientras el campo de reemplazo está visible; si no, mueven el foco
entre paneles, igual que fuera del editor.

La toolbar del editor también es un menú. `Shift+F10` abre el menú de **acciones
del panel** desde el texto del editor, en cualquier modo de Vim y sin Vim.
`Ctrl+k` mueve el foco a la barra de contexto de ejecución, donde `m` (o
`Shift+F10`) también lo abre. Un editor de scripts (Lua, Python, Bash) no tiene
controles de conexión, así que su barra de contexto solo contiene un botón
**Acciones**, que `Ctrl+k` enfoca y que se pulsa con `Enter`, `m` o un clic. El
menú lista Ejecutar (Cancelar mientras corre una query), Ejecutar en
una pestaña nueva, Guardar, Formatear, Historial de consultas, Explicar, Gráfico,
Actualizar y el intervalo de actualización automática, cada una con su atajo
cuando lo tiene. Muévete con `j` / `k` y elige con `Enter`, como en cualquier
[menú contextual](#menú-contextual); `Escape` lo cierra. La entrada de
actualización automática abre la lista de intervalos con el foco del teclado (ver
[Desplegables](#desplegables)). Mientras la consulta tiene resultados, el menú
sigue con la cabecera de resultados: Pestaña de resultados siguiente y anterior,
Cerrar pestaña de resultados, Maximizar (Restaurar) resultados y Ocultar
(Mostrar) resultados. El menú también está en la command palette como
**Abrir acciones del panel**.

## Modo Vim (opcional)

Los editores de varias líneas pueden usar edición modal con un conjunto
reducido de comandos de Vim. Viene desactivado. Actívalo en **Settings → General
→ Editor → Modo Vim en los editores** y guarda: los editores abiertos
cambian al instante. Se aplica a todos los editores de varias líneas:

- el editor de código (SQL y los demás lenguajes de query, Lua, Python, Bash);
- la pestaña del editor de objetos de S3 y el editor de vista previa del
  explorador de objetos;
- los diálogos del editor de celdas y de vista previa de documentos;
- el editor JSON del diálogo de importar dashboard y el editor de query del
  diálogo de agregar panel;
- el panel de valor, la vista JSON de una colección de documentos y su editor
  de pipeline de agregación.

Los visores de solo lectura aceptan movimientos, selección Visual y copias, sin
ediciones: un objeto de S3 decodificado en la pestaña del editor de objetos o en
la vista previa del explorador de objetos, la vista Raw JSON del árbol de
documentos, la vista previa SQL del constructor de queries, los diálogos de
vista previa de SQL y de query, y los detalles de un evento de auditoría
externo. Los campos de una línea, los cuadros de búsqueda, los formularios y la
paleta de comandos siguen escribiendo como siempre.

Fuera del editor de código, `Escape` funciona en dos pasos. En el modo Insertar
el primer `Escape` vuelve al modo Normal, y en el modo Normal `Escape` hace lo
mismo que sin Vim: cierra el diálogo, sale del editor o devuelve el teclado a la
lista o al árbol que lo rodea. `Enter` en el modo Normal baja una línea y nunca
confirma un diálogo. Con el modo Vim activo, los diálogos de vista previa de SQL
y de query se abren con el teclado en la query, así que `j` y `k` mueven el
cursor en lugar de desplazar el diálogo.

Un editor empieza en modo Normal al abrirse y al activar el modo Vim. Una franja
debajo del editor muestra el modo: `NORMAL`, `INSERTAR`, `REEMPLAZAR`, `VISUAL`, `VISUAL LÍNEA` o `VISUAL BLOQUE`. Cada tab conserva su
propio modo al cambiar de tab o al mover el focus y volver. La franja también
muestra la secuencia de teclas incompleta, como `2`, `2d3` o `4g`. Se borra al
completarse o interrumpirse el comando, al salir el foco del editor y con
`Escape` o `Tab`. No muestra el historial de comandos ni aparece en la barra
de estado del espacio de trabajo.

| Modo | Teclas | Acción |
|------|--------|--------|
| Normal | `h` / `l` | Mover un carácter a la izquierda / derecha dentro de la línea |
| Normal | `j` / `k` | Mover una línea abajo / arriba, conservando la columna a través de líneas más cortas |
| Normal | `Enter` | Mover una línea abajo |
| Normal | `/` | Abrir el panel de búsqueda del editor con el campo de consulta enfocado |
| Normal | `n` / `N` | Ir a la coincidencia siguiente / anterior de la consulta del panel de búsqueda (admite contador previo) |
| Normal | `m{a-z}` | Fijar o sobrescribir una marca local minúscula en el cursor |
| Normal | `'{a-z}` / `` `{a-z} `` | Ir al primer carácter no blanco de la línea marcada / a la posición exacta marcada (limitada a un cursor Normal) |
| Normal / Visual / Visual Línea / Visual Bloque | `gg` / `G` / `Ngg` / `NG` | Ir a la primera / última / línea lógica absoluta N (desde 1, limitada al archivo); en Visual se extiende la selección |
| Normal | `i` | Insertar antes del cursor |
| Normal | `a` / `A` / `I` | Insertar después del cursor / al final de la línea / en el primer carácter no blanco de la línea |
| Normal | `e` / `w` / `b` | Ir al final de una palabra / al inicio de la siguiente / al inicio de la anterior |
| Normal | `E` / `W` / `B` | Los mismos movimientos, con palabras separadas por espacios en blanco |
| Normal | `x` | Borrar el carácter bajo el cursor |
| Normal | `r{car}` / `Nr{car}` | Reemplazar el carácter bajo el cursor, o los N siguientes de la línea, por `{car}`; el cursor queda en el primer carácter reemplazado |
| Normal | `R` | Entrar en modo Reemplazar |
| Normal | `dd` / `yy` / `cc` | Borrar / copiar / cambiar líneas lógicas completas (`yy` usa el portapapeles del sistema) |
| Normal | `c` + `h` / `l` / `j` / `k`, `w` / `W` / `e` / `E` / `b` / `B`, `gg` / `G` | Cambiar caracteres con movimientos horizontales o de palabra, o líneas completas con movimientos verticales o absolutos |
| Normal | `d` / `y` + `h` / `l` / `j` / `k` | Borrar / copiar caracteres con movimientos horizontales o líneas con movimientos verticales (`y` usa el portapapeles del sistema) |
| Normal | `d` / `y` + `w` / `W` / `e` / `E` / `b` / `B` | Borrar / copiar el rango de caracteres del movimiento (`y` usa el portapapeles del sistema) |
| Normal | `d` / `y` + `gg` / `G` | Borrar / copiar líneas lógicas completas hasta un destino absoluto (`y` usa el portapapeles del sistema) |
| Normal | `u` | Deshacer |
| Normal | `v` / `V` / `Ctrl+v` | Seleccionar caracteres / líneas completas / un rectángulo de filas mostradas en modo Visual |
| Visual / Visual Línea | `h` / `j` / `k` / `l`, `e` / `E` / `w` / `W` / `b` / `B`, `0`, `Enter` | Extender la selección con los mismos movimientos y contadores del modo Normal |
| Visual / Visual Línea | `v` / `V` | Salir del modo Visual activo / alternar entre selección de caracteres y líneas |
| Visual / Visual Línea / Visual Bloque | `c` | Cambiar los caracteres seleccionados inclusive, las líneas lógicas o las columnas del bloque y entrar en modo Insertar |
| Visual / Visual Línea / Visual Bloque | `d` / `x` / `y` | Borrar la selección (`d` / `x`) o copiarla al portapapeles del sistema (`y`) |
| Visual / Visual Línea / Visual Bloque | `Escape` | Borrar la selección y volver al modo Normal |
| Insertar | `Escape` | Cerrar un menú de autocompletado abierto; si no hay ninguno, volver al modo Normal |
| Reemplazar | Caracteres escritos | Sobrescribir el carácter bajo el cursor; al final de una línea se agregan |
| Reemplazar | `Backspace` | Restaurar el carácter que sobrescribió esta sesión de Reemplazar; si no hay ninguno, moverse a la izquierda |
| Reemplazar | `Escape` | Volver al modo Normal |

Puedes anteponer un contador a un movimiento, a `x` / `u` o a `dd` / `yy` (por
ejemplo, `3w`, `2x`, `2u`, `3dd`, `2yy`). También se acepta entre las letras
repetidas (`d2d`); ambos contadores se multiplican (`2d3d` afecta seis
líneas). Los contadores de operador y movimiento también se multiplican:
`2d3w` abarca seis movimientos `w` y `2d3j`, seis líneas. `h` / `l` abarcan
caracteres; `j` / `k`, líneas lógicas completas. `x` con contador borra
hasta el final de la línea sin unir líneas; `u` con contador deshace esa cantidad de pasos. `0` sin contador mueve
al inicio de la línea; después de un dígito distinto de cero forma parte del
contador (por ejemplo, `20w`). Un contador interrumpido no se aplica al
siguiente comando. En modo Visual, los movimientos con contador extienden la
selección del editor. `gg` y `G` sitúan el cursor en el primer carácter no blanco
de la línea lógica de destino; `G` es una sola tecla mayúscula. Una `g` pendiente
se descarta al interrumpir la secuencia o perder el foco. En modo Normal, `d` / `y` / `c` con `gg` / `G` actúa por líneas desde la fila actual hasta el destino, limitado al archivo: `gg` sin contador apunta a la fila 1 y `G` sin contador a la última. Un contador antes del operador o del movimiento indica una fila absoluta desde 1; juntos se multiplican (`2d3G` apunta a la fila 6). Por eso `1dG` apunta a la fila 1, a diferencia de `dG`. El borrado se deshace en un solo paso; en editores de solo lectura no hace nada, mientras que copiar sigue usando el portapapeles del sistema.

`Ctrl+Enter` usa la selección sin espacios al inicio ni al final si contiene
texto no blanco; si no, usa todo el editor. En Visual Bloque, une con saltos de
línea los fragmentos no vacíos en orden, como al seleccionar con Alt y
arrastrar el mouse. Si el bloque solo contiene espacios en blanco, usa todo el
editor. Las columnas del bloque cuentan escalares Unicode, no celdas visuales:
las tabulaciones, los caracteres anchos y las secuencias combinadas pueden no
alinearse con las columnas en pantalla.

En modo Normal, `/` abre el panel de búsqueda del editor, el mismo que
`Ctrl+f`, con el campo de consulta enfocado y la última consulta seleccionada.
Escribe una cadena literal; no distingue mayúsculas y minúsculas salvo que actives
el botón correspondiente del panel. `Enter` mueve el cursor a la siguiente
coincidencia tras él y `Shift+Enter` a la anterior, volviendo al principio o al
final del texto, y el panel sigue abierto. `Escape` cierra el panel y vuelve al
editor en modo Normal, con el cursor en la última coincidencia alcanzada y la
consulta conservada. Después, `n` / `N` van a la coincidencia siguiente / anterior
de esa consulta desde el cursor, y un contador previo repite el salto ese número
de veces; el contador de coincidencias del panel los acompaña. Funciona también
en editores de solo lectura, y cada pestaña conserva su consulta. Mientras el
panel tiene el foco, las teclas se escriben en él en lugar de leerse como
comandos de Vim. Es una búsqueda de texto literal, no de expresiones regulares.

**Marcas locales.** Las marcas pertenecen al documento de código actual, no a
otras pestañas ni a otras sesiones. También se pueden fijar en editores de solo
lectura. Las ediciones nativas del texto, incluida la entrada en modo Insertar y
las confirmaciones del IME, desplazan las marcas con el texto durante deshacer y
rehacer. Insertar en una marca la mueve después del texto insertado; borrar o
sustituir el texto marcado la lleva al inicio del rango modificado. Por eso,
deshacer no tiene por qué recuperar la posición exacta dentro del texto borrado.
Reemplazar todo el contenido del editor, desactivar el modo Vim o cerrar el
documento borra sus marcas. No se ha validado el IME de escritorio ni la
interfaz renderizada.

Todo lo demás en modo Normal:

| Entrada | Comportamiento en modo Normal |
|---------|-------------------------------|
| Otras letras no admitidas y puntuación | Nada |
| `Space` | Inicia una secuencia de líder (ver **Tecla líder** más abajo); sola, nada |
| `Tab` / `Shift+Tab` | Mover el foco al panel siguiente / anterior, igual que fuera del editor (también en los modos Visual); no indenta |
| `Ctrl+v` | Entrar en Visual Bloque (no pegar) |
| Pegar (`Cmd+v` o el menú contextual) | Nada |
| Composición y confirmación del método de entrada (IME) | Se descartan |
| `Backspace` / `Delete` | Nada |
| `Escape` | Su significado habitual: cancelar una query en curso o salir del editor |
| Atajos con `Ctrl`, `Alt` o `Cmd`; flechas; el mouse | Funcionan como siempre, incluidos deshacer y rehacer |

En modo Normal el cursor está sobre un carácter, nunca después del final de una
línea. Al salir del modo Insertar retrocede un carácter, como en Vim. En una
línea vacía `x` no hace nada, así que nunca une líneas.

En modo Insertar el editor se comporta igual que con el modo Vim desactivado,
incluido pegar con `Ctrl+v`, salvo por `Escape`. Con un menú de autocompletado o de acciones de código
abierto, `Escape` cierra el menú y se queda en modo Insertar; si no, vuelve al
modo Normal. En ambos casos el focus se queda en el editor. Con varios cursores
o una sugerencia en línea visible, el primer `Escape` los descarta y el
siguiente vuelve al modo Normal.

En modo Visual, `d` / `x` borra selecciones de caracteres, líneas o bloques; los bloques borran los rangos separados de cada fila en un solo paso de deshacer. `y` copia la selección al portapapeles del sistema. Si la selección está vacía, estos comandos vuelven al modo Normal sin editar ni cambiar el portapapeles. En editores de solo lectura, `d` / `x` conserva la selección sin editar ni cambiar el portapapeles; `y` sigue funcionando. `dd` y `cc` solo existen en modo Normal. Visual Bloque `c` borra las columnas del bloque en cada fila que alcanza su columna izquierda, omite las filas más cortas y entra en modo Insertar en la primera de esas filas. Al salir de Insertar con `Escape`, el texto escrito allí se inserta en la misma columna de las demás filas. No se copia nada si el texto contiene un salto de línea, si no se escribió nada o si el foco sale antes del editor. Las columnas del bloque cuentan escalares Unicode, como en la selección de bloque. El borrado, el texto escrito y las copias forman un solo paso de deshacer.

**Cambiar y deshacer.** En modo Normal, `c` admite `h` / `l` por caracteres, `j` / `k` por líneas, `w` / `W` / `e` / `E` / `b` / `B` por palabras y `gg` / `G` por líneas, además de `cc`. `cw` cambia hasta el siguiente límite de `w`. Los contadores anterior e interior se multiplican (`2c3w` abarca seis movimientos `w`); los destinos absolutos son filas desde 1 limitadas al archivo (`2c3G` apunta a la fila 6), mientras que `cG` sin contador apunta a la última. Los cambios por líneas conservan el separador anterior a la fila siguiente; `cc` con contador incluye los terminadores LF o CRLF existentes de las líneas afectadas. El cambio borra mediante edición nativa y entra en modo Insertar para escribir el reemplazo. En sesiones normales, borrado y reemplazo forman un solo paso de deshacer que restaura el primer cursor. En editores de solo lectura no cambia el texto ni entra en modo Insertar.

Visual `c` por caracteres o líneas cambia la selección inclusiva mediante edición nativa y entra en modo Insertar para reemplazarla. En una sesión ordinaria, un solo paso de deshacer restaura el texto original y el ancla colapsada; los bytes de la selección usada para ejecutar una query no cambian. En editores de solo lectura, `c` conserva la selección sin entrar en modo Insertar. Con una selección vacía de caracteres o líneas, `c` entra en modo Insertar sin borrar texto. El cambio por líneas contempla una última línea lógica vacía tras LF o CRLF.

**Reemplazar.** `r{car}` reemplaza el carácter bajo el cursor y deja el cursor sobre él. Con contador, `3rx` reemplaza los tres caracteres siguientes de la línea por `x`; si quedan menos antes del final de la línea, no cambia nada. Nunca reemplaza un salto de línea y en una línea vacía no hace nada. `r` seguido de `Enter` reemplaza los caracteres por un solo salto de línea que conserva la indentación de la línea; `r` seguido de `Tab` escribe tabulaciones. `Escape`, `Backspace`, `Delete`, las flechas o salir del editor cancelan `r` sin editar; un atajo con `Ctrl`, `Alt` o `Cmd` lo cancela y luego se ejecuta como siempre. `r` acepta un carácter compuesto con un método de entrada (IME). El reemplazo es un solo paso de deshacer.

`R` entra en modo Reemplazar. Cada carácter escrito sobrescribe el carácter bajo el cursor; al final de una línea se agrega en lugar de reemplazar el salto de línea. `Backspace` restaura en orden inverso los caracteres sobrescritos en esta sesión de Reemplazar y, si no queda ninguno, solo mueve el cursor a la izquierda. `Enter` inserta un salto de línea y `Tab` indenta, como en modo Insertar. `Escape` vuelve al modo Normal y retrocede el cursor un carácter. Toda la sesión de Reemplazar es un solo paso de deshacer. Se ignora un contador antes de `R`.

Cada ejecución de `x`, `dd` o `d` con movimiento es un paso de deshacer, también con contador. Todo lo escrito en una sesión ordinaria de modo Insertar es un paso; cada nueva sesión empieza otro. Un grupo de deshacer tiene un límite de 1000 cambios: una sesión larga puede requerir varios pasos. `u` deshace los mismos pasos que `Ctrl+z` / `Cmd+z`.

**Limitación del IME.** Si una señal tardía de fin de composición anterior llega después de iniciar la siguiente, puede confirmar prematuramente la composición nativa activa y dividir el grupo de deshacer de Vim. Al pasar a solo lectura o modo Normal, el texto de preedición pendiente que se muestra se confirma tal cual, sin aceptar una propuesta posterior. En modo Reemplazar, el texto que llega sin pulsar una tecla, como una confirmación del IME, se inserta en lugar de sobrescribir, y `Backspace` no restaura caracteres a su alrededor. No se garantiza la seguridad completa del IME ni se ha validado la interfaz en vivo.

**Tecla líder.** En modo Normal y en los modos Visual, la tecla líder inicia
una secuencia de dos teclas que ejecuta un comando sin salir de la fila
central del teclado. La tecla líder es `Space` por defecto; elige `,` o `\` en
**Settings → General → Editor → Tecla líder**, y las secuencias la siguen. Después de ella, DBFlux espera
hasta un segundo la tecla siguiente: una tecla que ninguna secuencia usa actúa
como lo haría sola, y si no llega ninguna, la tecla líder no hace nada. En los
modos Insertar y Reemplazar, y mientras el panel de búsqueda tiene el foco, la
tecla líder escribe como siempre.

| Teclas | Acción |
|--------|--------|
| `Leader a` | Abre el menú de acciones del panel, como `Shift+F10` |
| `Leader r` | Ejecuta la consulta |
| `Leader e` | Explica la consulta (editor de código) |
| `Leader s` | Guarda |
| `Leader f` | Abre el panel de búsqueda del editor, como `/` |
| `Leader h` / `Leader l` | Pestaña anterior / siguiente del panel, como `Alt+h` / `Alt+l` |
| `Leader p` | Abre la paleta de comandos |

Un comando que el documento del editor no ofrece no hace nada, y la tecla que
sigue a la líder nunca llega a Vim. Las secuencias funcionan en todos los
editores con modo Vim salvo los que están dentro de un diálogo, que conservan
sus propias teclas. Aparecen en **Vim Normal** en **Settings → Keybindings**,
donde se cambian como cualquier otro atajo. Al grabar teclas que empiezan por
la tecla líder se guarda la propia líder, mostrada como `Leader`, así que el
atajo sigue a la tecla líder cuando esta cambia.

**Editores de solo lectura** (definiciones de rutinas): aceptan los
movimientos, `yy` y `y` con movimiento; `x`, `r`, `R`, `dd`, `cc`, `c` / `d` con movimiento, `c` en Visual y `u`
no hacen nada. Borrar tampoco modifica el portapapeles.

**Limitaciones.**

- Solo existen los comandos de la primera tabla. `dd` y `yy` abarcan líneas lógicas
  completas, con sus terminadores si existen. En el fin del archivo, el contador
  se detiene en la última línea; borrar la última línea quita también el separador
  anterior, pero copiarla no agrega un salto de línea inexistente. En una línea
  final vacía creada por LF o CRLF, `y` por líneas copia ese separador existente;
  un archivo vacío no tiene ninguno. `d` / `y`
  admiten `w` / `W` / `e` / `E` / `b` / `B`: `w` / `W` y `b` / `B` excluyen
  el carácter de destino; `e` / `E` lo incluyen. Los movimientos horizontales
  `h` / `l` con operador abarcan caracteres; los verticales `j` / `k`, líneas.
  No se admiten otras marcas, objetos de texto, registros,
  macros, repetición con `.`, comandos `:` ni una tecla de rehacer. No es Vim
  completo.
- Los movimientos avanzan un code point de Unicode por vez, como las flechas, así
  que una letra escrita con un acento combinante separado requiere dos pulsaciones.
- El modo Normal solo bloquea lo que escribes y pegas. Las ediciones que hace
  DBFlux, como cargar un archivo o una query del historial, se siguen aplicando.

## Resultados

| Teclas                                        | Acción                                     |
| --------------------------------------------- | ------------------------------------------ |
| `Ctrl+h` / `Ctrl+k`                           | Enfocar panel izquierda / arriba           |
| `Ctrl+l`                                      | Entrar al panel lateral abierto a la derecha (panel de valor, inspector de fila, panel de documento o constructor de consultas); ver [Paneles laterales](#paneles-laterales) |
| `Ctrl+j`                                      | Enfocar la toolbar                         |
| `j` / `k` (o `Down` / `Up`)                   | Fila siguiente / anterior                  |
| `h` / `l` (o `Left` / `Right`)                | Columna izquierda / derecha                |
| `g` / `Shift+g` (o `Home` / `End`)            | Primera / última fila                      |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Página abajo / arriba                      |
| `]` / `[`                                     | Página siguiente / anterior de resultados  |
| `Alt+l` / `Alt+h`                             | Pestaña de resultados siguiente / anterior de una consulta, o vista siguiente / anterior (Documentos, Esquema, Agregación) de una colección de documentos, dando la vuelta en los extremos |
| `Alt+w`                                       | Cerrar la pestaña de resultados visible; si era la última, el editor recibe el foco |
| `F5`                                          | Recargar el documento enfocado (filas de la tabla, lista de buckets, listado de objetos, claves) |
| `Ctrl+e` / `Cmd+e`                            | Abrir el menú de exportación: las teclas del menú contextual recorren los formatos de guardar y copiar, `Enter` ejecuta uno, `Escape` lo cierra |
| `f`                                           | Enfocar la toolbar                         |
| `Shift+f`                                     | Limpiar el filtro WHERE y recargar las filas; en una colección de documentos, vaciar el campo de filtro y buscar |
| `/`                                           | Enfocar búsqueda/filtro                    |
| `x`                                           | Eliminar fila                              |
| `r`                                           | Renombrar / editar                         |
| `o`                                           | Añadir fila                                |
| `y`                                           | Copiar fila                                |
| `i`                                           | Alternar la vista de registro (una fila)   |
| `Shift+t`                                     | Mostrar la vista siguiente del resultado (Datos o Cuadrícula, JSON, Gráfico y las demás que ofrezca), dando la vuelta; el teclado sigue en los resultados |
| `v`                                           | Alternar el panel de valor de la celda     |
| `Ctrl+Space`                                  | Alternar el inspector de la fila           |
| `Ctrl+c` / `Cmd+c`                            | Copiar celda(s)                            |
| `z`                                           | Maximizar los resultados de un documento de consulta sobre el editor, o restaurar la división |
| `m` (o `Shift+F10`)                           | Abrir menú contextual. Su última entrada, Barra de herramientas, lista los botones de la toolbar y la cabecera de resultados visibles en ese momento (exportar, limpiar filtro, restablecer la consulta del constructor, cambiar de vista, mostrar otra vista del resultado, los botones del panel de valor y del inspector de fila, abrir el constructor de consultas, intervalo de actualización automática, guardar o revertir todos los cambios, estadísticas del gráfico, guardar gráfico, mostrar en la tabla el punto del gráfico bajo el puntero o resaltado, tipo de gráfico siguiente, los selectores de eje, rango de tiempo siguiente / anterior, los controles del rango personalizado y Aplicar, maximizar, ocultar) con sus atajos. En la vista de gráfico las teclas de navegación mueven el punto resaltado del gráfico; ver [Gráficos](#gráficos) |

Una pestaña de inspector de instancia también toma estas teclas: `m` abre el
menú contextual de la fila elegida con las acciones de fila del driver (por
ejemplo Kill session), `Enter` o `Escape` responden la confirmación y `F5` pide
una instantánea nueva.

En una colección de documentos, el submenú Barra de herramientas también lista
Buscar, Historial de consultas, Volver a los documentos mientras se está dentro de
un valor anidado, las otras vistas, y Recargar documento / Aplicar mi cambio mientras
se muestra un conflicto de commit; Guardar todos los cambios y Revertir todos los
cambios ejecutan el Commit y el Revert de la barra de edición. Historial de
consultas abre el menú del historial con el teclado dentro: las teclas del menú
contextual se mueven, `Enter` vuelve a ejecutar la consulta resaltada y `Escape`
lo cierra.

Cuando los resultados no tienen menú contextual propio, `m` abre en su lugar el
menú de acciones del panel, el mismo que muestra **Open pane actions** en la
command palette.

Una pestaña de análisis de dump toma las teclas de tabla en cada una de sus dos
tablas: `Alt+l` y `Alt+h` pasan de Claves más grandes a Por prefijo y viceversa,
`Escape` cancela un análisis que todavía lee el archivo, y `m` lista la otra
tabla, un orden por columna de la tabla donde está el teclado (como lo hace un
clic en la cabecera) y Cancelar mientras se lee el archivo.

## Diferencia de esquemas

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Fila siguiente / anterior: el modo de comparación, cada base de datos, conexión o instantánea de referencia, Calcular diferencia, cada cambio aplicable y los botones Vista previa del DDL / Aplicar |
| `g` / `Shift+g` (o `Home` / `End`) | Primera / última fila |
| `h` / `l` (o `Left` / `Right`) | Botón anterior / siguiente de la fila (En vivo o Instantánea, Vista previa del DDL o Aplicar) |
| `Enter` | Pulsar el botón bajo el cursor: elegir el modo o la referencia, calcular, marcar un cambio, previsualizar o aplicar |
| `Space` | Marcar o desmarcar el cambio bajo el cursor |
| `F5` | Volver a calcular la diferencia |
| `m` (o `Shift+F10`) | Acciones del panel: Calcular diferencia, Vista previa del DDL, Aplicar y los dos modos de comparación |

Aplicar sigue pidiendo la misma confirmación que el botón.

## Diagrama de esquema

| Teclas                                      | Acción                                                      |
| ------------------------------------------- | ----------------------------------------------------------- |
| `+` (o `=`) / `-`                           | Acercar / alejar                                            |
| `h` / `j` / `k` / `l` (o flechas)           | Desplazar la vista                                          |
| `Shift` + `h` / `j` / `k` / `l` (o flechas) | Seleccionar la siguiente tabla en esa dirección y centrarla |
| `Alt` + `h` / `j` / `k` / `l` (o flechas)   | Mover la tabla seleccionada                                 |
| `r` / `s` / `c`                             | Diseño De izquierda a derecha / Copo de nieve / Compacto    |
| `m`                                         | Abrir menú contextual: acercar / alejar, restablecer la vista al 100%, ajustar a la vista, disposición, reorganizar las tablas, copiar como DBML o SQL, mostrar tipos de columna, mostrar índices y, con una tabla seleccionada, inspeccionar y enfocar |
| `Escape`                                    | Quitar la selección                                         |

## Gráficos

Estas teclas valen en una pestaña de gráfico y en la vista de gráfico de un
resultado (Gráfico, no Tabla + Gráfico, donde las teclas siguen en la tabla).
Un punto resaltado hace de puntero: la cruz y la lectura lo muestran. En el
gráfico de una pestaña de tabla o de colección el inspector de puntos también lo
sigue; los gráficos de resultados de queries y las pestañas de gráfico no tienen
inspector de puntos. Mover el puntero sobre el gráfico lo reemplaza.

| Teclas | Acción |
|--------|--------|
| `h` / `l` (o `Left` / `Right`) | Resaltar el punto anterior / siguiente de la serie enfocada; la primera tecla empieza en el primer punto |
| `g` / `Shift+g` (o `Home` / `End`) | Resaltar el primer / último punto |
| `j` / `k` (o `Down` / `Up`) | Llevar el punto resaltado a la serie visible siguiente / anterior, en la X más cercana |
| `Space` | Ocultar o mostrar la serie enfocada, como su entrada de la leyenda |
| `Escape` | Quitar el punto resaltado, o cerrar un selector de eje abierto |
| `Alt+l` / `Alt+h` | Tipo de gráfico siguiente / anterior (pestaña de gráfico) |
| `]` / `[` | Rango de tiempo siguiente / anterior, Personalizado incluido (pestaña de gráfico) |
| `F5` | Volver a ejecutar el gráfico (pestaña de gráfico) |
| `Ctrl+s` / `Cmd+s` | Guardar el gráfico; en el diálogo del nombre `Enter` guarda y `Escape` cancela (pestaña de gráfico) |
| `m` (o `Shift+F10`) | Acciones del panel (pestaña de gráfico): actualizar, intervalo de actualización automática, rango de tiempo siguiente / anterior, los controles del rango personalizado y Aplicar mientras está elegido Personalizado, tipo de gráfico siguiente / anterior, los selectores X, Y, Agrupar y Agregación, Estadísticas, el selector de métricas y sus controles en un gráfico de métrica, Guardar gráfico |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` / `Ctrl+l` | Enfocar el panel en esa dirección |

Un selector de eje abierto desde el menú toma estas teclas hasta cerrarse:
`j` / `k` recorren sus filas, `h` / `l` pasan al selector vecino (X, Y,
Agrupar, Agregación), `Enter` elige la fila y lo cierra, `Space` alterna una
columna Y y lo deja abierto, `Escape` lo cierra. En la vista de gráfico de un
resultado, la entrada Barra de herramientas del menú contextual (`m`) lista el
tipo de gráfico, los selectores de eje, el rango de tiempo y los controles del
rango personalizado. El rango de fechas de un rango personalizado recibe el
teclado desde su entrada del menú y se abre con `Enter`; elegir los días en su
calendario todavía necesita el puntero.

## Dashboards

Estas teclas valen en una pestaña de dashboard, en modo View y Edit. Un anillo
marca el panel elegido mientras el teclado está en el dashboard.

| Teclas | Acción |
|--------|--------|
| `h` / `l` (o `Left` / `Right`) | Elegir el panel anterior / siguiente en orden de lectura |
| `j` / `k` (o `Down` / `Up`) | Elegir el panel más cercano en la fila siguiente / anterior |
| `g` / `Shift+g` (o `Home` / `End`) | Elegir el primer / último panel |
| `Enter` / `i` | Abrir el panel elegido: un chart toma las [teclas de gráficos](#gráficos) y un inspector las de la tabla, hasta `Escape`; en un divider, plegar o desplegar su sección |
| `Space` | Plegar o desplegar la sección del divider elegido |
| `c` | Configurar el panel de chart elegido |
| `r` / `F2` | Renombrar el panel elegido |
| `x` / `Delete` | Eliminar el panel elegido |
| `a` | Añadir un panel |
| `Shift` + `h` / `j` / `k` / `l` (o flechas) | Mover el panel elegido una celda (modo Edit) |
| `Alt+Shift` + `h` / `l` | Estrechar / ensanchar el panel elegido (modo Edit) |
| `Alt+Shift` + `k` / `j` | Acortar / alargar el panel elegido (modo Edit) |
| `Alt+l` / `Alt+h` | Cambiar entre View y Edit |
| `]` / `[` | Rango de tiempo compartido siguiente / anterior, Personalizado incluido |
| `F5` | Actualizar todos los panels, o solo el abierto |
| `m` (o `Shift+F10`) | Acciones del panel: las del panel elegido, añadir panel, actualizar, intervalo de actualización automática, el rango de tiempo y los controles del rango personalizado, View / Edit o Guardar como editable. Con un panel de chart abierto, su tipo de chart y Stats |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` / `Ctrl+l` | Enfocar el panel en esa dirección |

Un movimiento o cambio de tamaño que sale de la cuadrícula o cae sobre otro
panel se rechaza, igual que al arrastrar. En el popover Configure, `h` / `l`
abren por turno los selectores X, Y, Agrupar y Agregación, `j` / `k` recorren
el abierto, `Space` o `Enter` eligen, `Alt+l` / `Alt+h` cambian el tipo de chart,
`Enter` sin selector abierto aplica y `Escape` cierra el selector y luego el
popover.

En el diálogo Add Panel, `Alt+l` / `Alt+h` cambian sus pestañas (también desde
sus campos de texto, salvo en macOS). Las flechas recorren la lista de charts
desde la búsqueda y `Enter` añade los charts marcados, o el resaltado si no hay
ninguno marcado. `Tab` entra en una lista, donde funcionan `j` / `k`, `g` /
`Shift+g` y `Space` (marcar un chart, elegir un namespace o una métrica),
`h` / `l` pasan entre las listas de namespaces y métricas de la pestaña Metric
y `/` vuelve a la búsqueda. `Escape` cierra el diálogo.

## Tareas en segundo plano

El panel de tareas está debajo de los documentos y empieza colapsado. Colapsado no ocupa espacio: se abre con la entrada de tareas en segundo plano de la barra de estado, o con `Ctrl+Shift+4`, que además le pasa el foco. `Tab` y `Shift+Tab` lo saltan mientras está colapsado.

| Teclas                                        | Acción                                   |
| --------------------------------------------- | ---------------------------------------- |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k`                | Enfocar panel izquierda / abajo / arriba |
| `j` / `k` (o `Down` / `Up`)                   | Seleccionar la tarea siguiente / anterior |
| `g` / `Shift+g` (o `Home` / `End`)            | Seleccionar la primera / última tarea    |
| `Space` / `Enter`                             | Mostrar u ocultar la salida de la tarea seleccionada |
| `c`                                           | Cancelar la tarea seleccionada           |
| `x`                                           | Descartar la tarea seleccionada cuando terminó |
| `Shift+x`                                     | Limpiar las tareas terminadas            |
| `m` / `Shift+F10`                             | Abrir el menú de acciones del panel      |
| `z`                                           | Alternar colapso del panel               |

La tarea seleccionada se resalta mientras el panel tiene el foco; hacer clic en una fila también la selecciona. El menú de acciones lista **Mostrar salida**, **Cancelar tarea** y **Descartar** de la tarea seleccionada, y después **Limpiar terminadas** y **Ocultar el panel de tareas**, cada una con su atajo; se maneja con las teclas del menú contextual. **Borrar tareas terminadas** también está en la paleta de comandos.

## Centro de notificaciones

La campana del extremo derecho de la barra de título abre el centro de
notificaciones, un popover que flota sobre el espacio de trabajo. Lista las
aprobaciones de MCP que esperan una decisión, los errores de acciones que
ejecutaste, una actualización de DBFlux disponible y los trabajos de
exportación, importación, migración y análisis de volcados que terminaron. El
badge de la campana cuenta los elementos sin leer y toma el color del más
urgente: rojo para un error, el color de acento para una aprobación y neutro
para actualizaciones y trabajos terminados. Sin nada sin leer, la campana no
tiene badge.

Abrir el popover no marca nada como leído. Hacer clic en una fila abre su
destino y la marca como leída: una aprobación abre la pestaña de aprobaciones
de MCP en esa solicitud, un error abre Audit filtrado por su correlation id, la
actualización abre sus notas de versión y un trabajo terminado abre el panel de
tareas en segundo plano. **Marcar todo como leído** lee todo y **Borrar leídas**
quita los elementos leídos. La lista dura lo que dura la sesión. Las
actualizaciones aparecen aquí en lugar de en la barra de estado.

| Teclas                         | Acción                                                         |
| ------------------------------ | -------------------------------------------------------------- |
| `Ctrl+Shift+B` / `Cmd+Shift+B` | Abrir o cerrar el popover desde cualquier parte del workspace  |
| `j` / `k` (o `Down` / `Up`)    | Seleccionar la fila siguiente / anterior                       |
| `g` / `Shift+g` (o `Home` / `End`) | Seleccionar la primera / última fila                       |
| `Enter`                        | Abrir el destino de la fila seleccionada, como un clic en ella |
| `r`                            | Marcar como leída la fila seleccionada                         |
| `x`                            | Descartar la fila seleccionada, como **Más tarde** en la actualización |
| `i`                            | Instalar la actualización listada (builds instalados desde la descarga directa) |
| `Alt+l` / `Alt+h`              | Mostrar el filtro siguiente / anterior                         |
| `Shift+r`                      | Marcar todo como leído                                         |
| `Shift+x`                      | Borrar leídas                                                  |
| `Escape`                       | Cerrar el popover (un clic fuera de él hace lo mismo)          |

Mientras el popover está abierto se queda con el teclado: los paneles de atrás no reciben estas teclas. La fila seleccionada se dibuja sobre un tinte. Descartar quita un error o un trabajo terminado, y oculta una aprobación o la actualización hasta que termina la sesión. Las teclas aparecen en el contexto Notificaciones de Configuración > Atajos de teclado.

## Command palette

| Teclas | Acción |
|--------|--------|
| `Down` / `Up` (o `Ctrl+j` / `Ctrl+k`) | Seleccionar siguiente / anterior |
| `Enter` | Ejecutar |
| `Escape` | Cancelar |

Las letras quedan para el campo de búsqueda, así que escribir filtra la lista.

## Tabla de datos

Estas teclas se aplican mientras una grilla de resultados o una tabla tiene el
foco y no se está editando ninguna celda.

| Teclas | Acción |
|--------|--------|
| `j` / `k` / `h` / `l` (o las flechas) | Mover el cursor |
| `Shift` + flechas | Extender la selección |
| `Home` / `End` | Primera / última celda de la fila |
| `Ctrl+Home` / `Ctrl+End` | Primera / última fila |
| `Shift+Home` / `Shift+End`, `Ctrl+Shift+Home` / `Ctrl+Shift+End` | Extender la selección hasta el borde de la fila o de la tabla |
| `Ctrl+a` / `Cmd+a` | Seleccionar todo |
| `Escape` | Quitar la selección |
| `Ctrl+c` / `Cmd+c`, `y y` | Copiar la selección |
| `Shift+y Shift+y` | Copiar la fila |
| `Enter` / `F2` | Editar la celda |
| `Ctrl+Enter` / `Cmd+Enter`, `Ctrl+s` / `Cmd+s` | Guardar los cambios pendientes |
| `d d` / `Delete` | Borrar la fila |
| `a a` / `Shift+a Shift+a` | Agregar / duplicar una fila |
| `Ctrl+n` | Poner la celda en NULL |
| `u` / `Ctrl+z` / `Cmd+z` | Deshacer |
| `Ctrl+r` / `Ctrl+Shift+z` / `Cmd+Shift+z` | Rehacer |
| `e` | Expandir o contraer una columna anidada (grillas de documentos) |
| `Backspace` | Salir de un valor anidado (grillas de documentos) |

## Paneles laterales

Estas teclas se aplican después de que `Ctrl+l` mueve el foco desde una grilla de
resultados al panel de valor, el inspector de fila, el panel de documento o el
constructor de consultas que está a su lado.

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Desplazar una línea abajo / arriba |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Desplazar una página abajo / arriba |
| `g` / `Shift+g` (o `Home` / `End`) | Ir al principio / al final |
| `Enter` | Editar el valor (panel de valor) |
| `Escape` | Dejar de editar el valor, o volver a la grilla |
| `Ctrl+h` | Volver a la grilla |
| `m` (o `Shift+F10`) | Abrir el menú contextual de la grilla. Su entrada Barra de herramientas lista los botones del panel abierto: en el panel de valor los demás formatos, el ajuste de línea, Formatear, Compactar y, cuando el valor cambió, Revertir y Guardar; en el inspector de fila Fijar o Soltar |

Los constructores de consultas tienen teclas propias, descritas en
[Constructores de consultas](#constructores-de-consultas).

## Constructores de consultas

Después de que `Ctrl+l` mueve el foco desde la grilla de una tabla a su
constructor de consultas, o desde los documentos de una colección al
constructor de documentos, un cursor marca una fila del constructor: la lista
de columnas, una condición o grupo del filtro, un join, una fila de agrupación
u orden, una asignación o una opción de ejecución, y en el constructor de
documentos el nombre de la consulta, una consulta guardada, un campo
proyectado o una fila de la etapa de grupo. Las teclas aparecen en los
contextos Query Builder y Document Builder de Settings > Keybindings.

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Fila siguiente / anterior |
| `g` / `Shift+g` (o `Home` / `End`) | Primera / última fila |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Ocho filas abajo / arriba |
| `h` / `l` (o `Left` / `Right`) | Campo anterior / siguiente de la fila |
| `Enter` / `i` | Usar el campo: escribir en un campo de texto, abrir un desplegable, pulsar un botón |
| `Space` | Cambiar el interruptor de la fila (AND / OR, ASC / DESC, la casilla de una columna, el tipo de valor de una asignación) |
| `a` | Agregar una entrada a la lista de la fila (una condición, un join, una clave de orden, una asignación) |
| `Shift+a` | Agregar un grupo dentro del grupo de filtros de la fila |
| `x` / `d` | Quitar la fila |
| `Shift+j` / `Shift+k` | Bajar / subir una clave de orden de documentos |
| `Alt+l` / `Alt+h` | Modo siguiente / anterior (SELECT, UPDATE, DELETE; Find, Aggregate) |
| `Ctrl+Enter` | Ejecutar |
| `Ctrl+s` | Guardar |
| `m` / `Shift+F10` | Menú con las acciones de la fila y las del constructor (Run o Find, Open in Editor, Save, Reset o las consultas guardadas, los modos, Close) |
| `Escape` | Salir de un campo a las filas, o volver a la grilla |
| `Ctrl+h` | Volver a la grilla |

En un campo de texto las letras se escriben y Escape vuelve a las filas. Un
desplegable abierto con Enter responde a las teclas de los desplegables y
devuelve el teclado al constructor al cerrarse. Una ejecución desde el teclado
pasa por la misma confirmación y política de mutaciones que el botón Run, así
que un UPDATE o DELETE sin WHERE sigue pidiendo confirmación. En el
constructor de documentos, Enter sobre un campo abre el selector de campos con
su búsqueda enfocada: escribe la ruta y pulsa Enter. Enter sobre un operador
abre la lista de operadores, donde `j`, `k` y Enter eligen. Un Find desde el
teclado deja el teclado en el constructor. macOS usa Cmd en
lugar de Ctrl para `Ctrl+Enter` y `Ctrl+s`, y allí `Alt+l` / `Alt+h` funcionan
solo fuera de los campos de texto.

## Árbol de documentos

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Nodo siguiente / anterior |
| `h` / `l` (o `Left` / `Right`) | Contraer / expandir, o ir al padre / primer hijo |
| `g` / `Shift+g` (o `Home` / `End`) | Primer / último nodo |
| `Ctrl+u` / `Ctrl+d` (o `PageUp` / `PageDown`) | Página arriba / abajo |
| `Space` | Expandir / contraer |
| `Enter` / `F2` | Editar el valor |
| `e` | Vista previa del documento |
| `d d` / `Delete` | Borrar el documento |
| `t` | Cambiar la vista de datos |
| `r` | Alternar la vista JSON sin formato |
| `/` / `Ctrl+f` | Buscar; `n` / `Shift+n` coincidencia siguiente / anterior, `Escape` cierra |

El campo de búsqueda y el editor de valores en línea conservan las letras que
escribís. En el campo de búsqueda, `Enter` devuelve el teclado al árbol y mantiene
las coincidencias, así `n` y `Shift+n` las recorren, y `Escape` cierra la búsqueda.

## Explorador clave-valor

| Teclas | Acción |
|--------|--------|
| `` Ctrl+` `` | Mostrar u ocultar la consola de comandos, también desde su campo |
| `Ctrl+j` | Cargar más claves |
| `t` | Editar la expiración de la clave seleccionada |
| `Alt+l` / `Alt+h` | Filtro de tipo de clave siguiente / anterior (Todos primero); en el editor de expiración, su modo siguiente / anterior (Nunca, En, El) |

`Ctrl+j` y `t` se aplican mientras la lista de claves tiene el foco, no dentro de
un campo de texto. `Alt+l` y `Alt+h` también funcionan desde los campos de patrón
y de expiración, salvo en macOS, donde `Option` con una letra escribe un carácter.

`m` abre el menú de la clave o el miembro seleccionado. Después de las acciones
de la clave o el miembro lista los botones del panel de valor (recargar el valor,
previsualizar los primeros bytes o cargar igual un valor grande, y las entradas
pendientes y el formulario de claim de un stream) y los de la barra (mostrar las
claves como árbol o lista, el intervalo de actualización automática, el borrado
masivo, y Detener o Buscar en todo el keyspace mientras un escaneo filtrado lee
página por página). En los diálogos Nueva clave y Añadir miembro, `Tab` y
`Shift+Tab` recorren los campos como `j` y `k` y no salen del diálogo. Cuando la
consola pide confirmar un comando peligroso, `Enter` en su campo vacío lo ejecuta
y `Escape` lo cancela.

## Almacenamiento de objetos

En el explorador de objetos, `m` abre el menú de la fila seleccionada. Después
de las entradas de la fila (un objeto también ofrece Abrir en el visor del
sistema) lista los botones del listado: Subir, Nueva carpeta, Copiar la ruta
actual, Cargar más mientras el nivel tenga otra página, Mostrar como lista o como
árbol y, mientras una vista previa los muestra, Ver versiones, Cargar de todas
formas y Descartar para cambios sin guardar. Sin una fila seleccionada, `m` abre
esas mismas entradas como acciones del panel.

En la lista de buckets, `m` lista Explorar, Calcular tamaño, Nuevo bucket y
Actualizar.

En una pestaña de editor de objetos, `Escape` saca el teclado del texto, `Enter`
lo devuelve, y `m` lista entonces Guardar, Descartar, Buscar, la interpretación
Automático / Raw, Recargar y, para un objeto por encima del límite de tamaño,
Cargar de todas formas.

Al salir de un objeto con ediciones sin guardar, el diálogo que pregunta qué
hacer se abre con **Guardar** enfocado, así que `Enter` guarda. `Tab` y
`Shift+Tab` pasan entre **Guardar**, **Cancelar** y **Descartar** sin salir del
diálogo, `Enter` o `Space` presionan el botón enfocado y `Escape` cancela.

## Visor de auditoría

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Evento siguiente / anterior |
| `g` / `Shift+g` (o `Home` / `End`) | Primer / último evento |
| `Ctrl+d` / `Ctrl+u` (o `PageDown` / `PageUp`) | Bajar / subir un cuarto de página |
| `]` / `[` | Página siguiente / anterior |
| `Enter` / `Space` | Expandir o contraer el evento seleccionado |
| `f` / `/` | Entrar a la barra de filtros; `h` / `l` se mueven, `Enter` activa, `Escape` sale |
| `r` | Actualizar |
| `Ctrl+e` / `Cmd+e` | Abrir el menú de exportación: las teclas del menú contextual pasan entre CSV y JSON, `Enter` exporta y `Escape` lo cierra |
| `Alt+l` / `Alt+h` | Cambiar entre la tabla de eventos y el gráfico |
| `m` (o `Shift+F10`) | Menú contextual del evento seleccionado: copiar como CSV, copiar el resumen, copiar como JSON, filtrar por su id de correlación y abrir una aprobación pendiente |

## Aprobaciones MCP

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Llamada pendiente siguiente / anterior |
| `g` / `Shift+g` (o `Home` / `End`) | Primera / última llamada pendiente |
| `a` | Aprobar la llamada seleccionada |
| `r` | Rechazar la llamada seleccionada con el motivo escrito |
| `Enter` / `i` | Escribir el motivo del rechazo; `Escape` vuelve a la lista |
| `F5` | Recargar las llamadas pendientes |
| `m` (o `Shift+F10`) | Acciones del panel: aprobar, rechazar, escribir el motivo, recargar |

Las teclas aparecen en el contexto Aprobaciones MCP de Settings > Keybindings.

## Asistente de migración

| Teclas | Acción |
|--------|--------|
| `Alt+l` / `Alt+h` | Continuar / volver, como los botones del pie (también desde un campo de texto, salvo en macOS) |
| `Ctrl+Enter` / `Cmd+Enter` | Continuar; en el paso Confirmar, iniciar la migración |
| `j` / `k` (o `Down` / `Up`) | Mover el cursor del paso |
| `h` / `l` (o `Left` / `Right`) | Origen y destino: contraer o expandir el nodo, o pasar de un árbol al otro. Mapeo de tablas: moverse entre el nombre de destino, el modo y **Columnas…** de una fila |
| `Enter` / `i` | Marcar una tabla de origen, elegir la base de destino, escribir en un campo, abrir un desplegable o el detalle de columnas |
| `Space` | Alternar el elemento bajo el cursor; en Confirmar, marcar la confirmación del plan destructivo |
| `Shift+k` / `Shift+j` | Subir / bajar una tabla en el orden de carga |
| `Escape` | Salir de un campo de texto o cerrar el detalle de columnas |
| `m` (o `Shift+F10`) | Acciones del panel: Continuar, Volver, fijar el modo de todas las tablas, aceptar el orden de carga, la confirmación, Iniciar migración, Cancelar migración mientras corre y Cerrar al terminar |

Las teclas aparecen en el contexto Migrate Wizard de Settings > Keybindings.

## Campos de texto

| Teclas | Acción |
|--------|--------|
| `Ctrl+j` / `Ctrl+k` | Línea siguiente / anterior, o sugerencia siguiente / anterior (fuera del editor de código, donde mueven el foco; ver [Editor](#editor)) |
| `Ctrl+Space` | Mostrar sugerencias |
| `Ctrl+Enter` / `Cmd+Enter` | Ejecutar la consulta |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` | Ejecutar la consulta en una pestaña nueva |
| `Ctrl+Shift+z` | Rehacer (Linux y Windows; macOS usa `Cmd+Shift+z`) |

Mientras escribes en un campo de texto fuera de un diálogo, como la búsqueda de
la sidebar o la barra de contexto de ejecución, los atajos globales que llevan
`Ctrl` o `Cmd` siguen funcionando: `Ctrl+Tab`, `Ctrl+1` .. `Ctrl+9`, `Ctrl+w`,
`Ctrl+Shift+P` y los demás de [Global](#global-disponible-sin-importar-el-foco).
Las teclas sin esos modificadores, incluidas `Tab`, `Escape`, `Enter` y las
flechas, se quedan en el campo. Un atajo que el propio campo define, como
`Ctrl+a` o `Ctrl+c`, tiene prioridad sobre el global. Dentro de un diálogo, un
menú, un desplegable o un selector, los atajos globales esperan a que se cierre.

## Diálogos

| Teclas | Acción |
|--------|--------|
| `Escape` | Cerrar, o en diálogos con formulario salir primero del campo en edición |
| `Enter` | Confirmar, cuando el botón principal está habilitado |
| `Up` / `Down`, `PageUp` / `PageDown`, `Home` / `End` | Desplazar el contenido de un diálogo largo |
| `Escape` / `Ctrl+s` / `Cmd+s` | Cerrar / guardar el editor de celda y la vista previa de documento |
| `Tab` / `Shift+Tab` | Pasar al control siguiente / anterior dentro del diálogo |

`Tab` y `Shift+Tab` dan la vuelta dentro de un diálogo abierto: desde el último
control vuelven al primero, y nunca llevan el foco a los paneles de detrás.

En los diálogos **Vista previa de SQL** y **Vista previa de consulta**, `Enter`
o `Ctrl+c` / `Cmd+c` copian la query y cierran la vista previa, `j` / `k` y `Up` / `Down` la
desplazan una línea, y `PageUp` / `PageDown` una página. Estas teclas aparecen en
el contexto **Vista previa de SQL** de **Settings → Keybindings**.

## Formularios y la ventana de Settings

Estas teclas recorren los formularios de los diálogos y la ventana de Settings
cuando no se está editando ningún campo de texto.

| Teclas | Acción |
|--------|--------|
| `j` / `k` (o `Down` / `Up`) | Campo siguiente / anterior |
| `Left` / `Right` | Moverse dentro de una fila; cambiar la opción de un campo segmentado |
| `h` / `l` | Volver a la lista / entrar al formulario |
| `g` / `Shift+g` | Primer / último campo |
| `Tab` / `Shift+Tab` | Campo siguiente / anterior |
| `Space` | Alternar |
| `Enter` | Activar o editar el campo |
| `Escape` | Salir del campo o del formulario |
| `/` | Enfocar la búsqueda |
| `Ctrl+w` / `Ctrl+q` | Cerrar la ventana de Settings |
| `Ctrl+s` | Guardar la sección |
| `Ctrl+h` / `Ctrl+l` | Moverse entre la navegación y la sección |
| `n` / `d` / `i` | En una lista de perfiles (proxies, túneles SSH, perfiles de autenticación, hooks, servicios, MCP): nuevo, eliminar, importar |

En **Settings → Keybindings**, `Enter` o `Space` graban teclas nuevas para el
binding seleccionado, `r` lo restablece, `p` edita su contexto, `Delete` o
`Backspace` quitan su atajo, `Shift+r` restablece todos los bindings, `c` abre
el filtro de contexto y `/` o `f` enfocan el filtro de texto. `Enter` sobre un
campo desplegable, como el proveedor de un perfil de autenticación, abre su
lista con el foco del teclado (ver [Desplegables](#desplegables)). Mientras un
login espera al navegador, los botones Abrir navegador, Copiar URL y Cancelar
forman la fila debajo del botón de login. En la página Acerca de, `j` / `k` se
mueven entre sus dos enlaces y `Enter` abre uno. Estas teclas, como las de
navegación, aparecen en los contextos Settings Window y Form Navigation de
**Settings → Keybindings** y se pueden reasignar ahí.

En el Connection Manager, `Ctrl+s` / `Cmd+s` guarda la conexión desde cualquier
parte del formulario, y `Left` / `Right` cambian la opción de **Introducir como**
y del método de autenticación SSH. `Down` / `Up` se mueven como `j` / `k`;
mientras se edita un campo lo dejan por el campo siguiente o anterior, y
`Ctrl+l` / `Ctrl+h` lo dejan por la pestaña siguiente o anterior. `PageDown` /
`PageUp` mueven el resaltado de un desplegable abierto una página. Después de
la contraseña, el recorrido del formulario sigue por los demás campos del
driver, como el selector de perfil de autenticación de una conexión AWS, que
`Enter` abre, luego el modo SSL, cuya opción cambian `Left` / `Right` o `Enter`,
y los selectores de certificados, donde `Enter` abre el explorador. Tras una
prueba de conexión fallida, el botón **Copiar** del aviso queda entre **Probar
conexión** y **Guardar**. En la pestaña Settings, el desplegable de hook de cada
fase de conexión es una parada encima de su campo de hooks adicionales, y
`Enter` le pasa el teclado. En la pestaña MCP, `j` / `k` recorren el interruptor
de MCP, el filtro de clientes, cada cliente listado (`Enter` lo selecciona), el
interruptor de acceso del cliente seleccionado y sus selectores de rol y de
política. En el visor de auditoría, `Left` / `Right`
sobre los intervalos de tiempo cambian el intervalo.

## Desplegables

Estas teclas se aplican a un desplegable o a una selección múltiple con el foco
del teclado, por ejemplo después de que una entrada de menú lo abra. Las teclas
que el desplegable no usa pasan al panel que lo contiene.

| Teclas | Acción |
|--------|--------|
| `Enter` / `Space` | Abrir la lista |
| `j` / `k` (o `Down` / `Up`) | Mover abajo / arriba en la lista abierta |
| `Enter` | Elegir el elemento resaltado, o cerrar una selección múltiple |
| `Space` | Elegir el elemento resaltado; marcarlo o desmarcarlo en una selección múltiple |
| `Escape` | Cerrar la lista sin elegir |

Al elegir un elemento o pulsar `Escape`, el foco vuelve al control que lo tenía
antes del desplegable. Los desplegables que forman parte de un anillo de teclado,
como la barra de contexto de ejecución y los filtros de auditoría, siguen
respondiendo a las teclas de ese anillo. La única selección múltiple de la barra
de contexto de ejecución, la lista de destinos de una fuente como un grupo de
logs o un flujo de eventos, es la excepción: `Enter` sobre ella abre la lista con
el foco del teclado, así que la manejan las teclas de arriba, y al cerrarla el
foco vuelve a la barra.

## Menú contextual

| Teclas                      | Acción                          |
| --------------------------- | ------------------------------- |
| `j` / `k` (o `Down` / `Up`) | Mover abajo / arriba            |
| `Enter` / `l` (o `Right`)   | Seleccionar / entrar en submenú |
| `Escape` / `h` (o `Left`)   | Volver / cerrar                 |

## Modal de historial

| Teclas                                | Acción                           |
| ------------------------------------- | -------------------------------- |
| `Ctrl+j` / `Ctrl+k` (o `Down` / `Up`) | Seleccionar siguiente / anterior |
| `Enter`                               | Abrir entrada                    |
| `Ctrl+f`                              | Alternar favorito                |
| `Ctrl+r`                              | Renombrar                        |
| `Ctrl+d`                              | Eliminar                         |
| `/`                                   | Enfocar búsqueda; en los campos de búsqueda, renombrar y guardar escribe una `/` |
| `Ctrl+s` / `Cmd+s`                    | Guardar query                    |
| `Alt+l` / `Alt+h`                     | Mostrar la lista siguiente / anterior (Recientes, Guardadas) |

`Alt+l` y `Alt+h` también funcionan en los campos de búsqueda, renombrar y guardar del historial, salvo en macOS, donde `Option` con una letra escribe un carácter: allí funcionan solo desde la lista.
