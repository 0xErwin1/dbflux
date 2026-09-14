# Política de marca de DBFlux

El código fuente de DBFlux es software libre bajo `MIT OR Apache-2.0`. El
nombre **DBFlux**, el logo de DBFlux y el dominio `dbflux.dev` no forman parte
de esa concesión. Este documento indica qué puedes hacer con ellos sin pedir
permiso.

## Ruta rápida

1. Puedes usar el nombre para indicar de dónde viene tu trabajo: "basado en
   DBFlux", "un fork de DBFlux", "compatible con DBFlux".
2. Puedes redistribuir los artefactos de release oficiales **sin modificar**
   bajo el nombre DBFlux, por ejemplo como paquete de Homebrew, AUR o nixpkgs.
3. Si publicas builds **modificadas**, usa otro nombre y otro logo. Indica que
   tu proyecto deriva de DBFlux y que no es el oficial.

Si tu caso no es ninguno de estos tres, abre un issue en este repositorio y
pregunta.

## Marcas cubiertas

| Marca | Qué significa |
|-------|---------------|
| DBFlux | El nombre del producto, en cualquier capitalización, solo o en compuestos como "DBFlux Pro" o "DBFlux Nightly". |
| El logo | Todos los archivos bajo `resources/branding/` y `packaging/icons/`, y cualquier derivado de ellos. |
| dbflux.dev | El dominio y todas las páginas bajo él. |

La licencia del código no cambia esto. El texto de Apache 2.0 lo dice en su
sección 6. MIT no menciona las marcas, así que elegir MIT en lugar de Apache no
otorga un derecho sobre el nombre que Apache retiene.

## Qué puedes hacer

| Uso | Permitido sin pedir |
|-----|---------------------|
| Describir origen, compatibilidad o comparación ("construido sobre DBFlux", "funciona con DBFlux") | Sí |
| Conservar el nombre en los archivos fuente, el historial de commits y la documentación de un fork | Sí |
| Empaquetar o replicar releases oficiales **sin modificar** bajo el nombre DBFlux | Sí |
| Escribir artículos, charlas o tutoriales sobre DBFlux, incluyendo el logo en una captura | Sí |
| Usar el nombre y el logo en una build que incluye parches que no están en una release oficial | No |
| Publicar releases, instaladores o un canal de actualización con la marca DBFlux desde otro repositorio | No |
| Registrar un dominio, nombre de paquete, ficha en una tienda de aplicaciones o cuenta cuyo elemento principal sea "DBFlux" | No |
| Usar el logo, o una versión editada de él, como icono de un producto derivado | No |

"Sin modificar" significa que el artefacto fue producido por el flujo de
release oficial a partir de un tag de este repositorio y que su checksum
coincide con el publicado junto a la release. Una recompilación del mismo tag
cuenta como sin modificar cuando el árbol de fuentes es idéntico byte a byte a
ese tag.

## Cómo nombrar un fork

Un producto derivado necesita un nombre que un usuario no pueda confundir con
DBFlux. Lo siguiente funciona:

- Un nombre distinto con una nota de origen: "Fluxbase, un fork de DBFlux".
- Un calificador que lo marque claramente como no oficial: "DBFlux (edición de
  la comunidad, no oficial)" es aceptable en un README, pero el binario, el
  título de la ventana, el identificador del bundle y el instalador no deben
  presentarse como "DBFlux" a secas.

Lo siguiente no funciona:

- Publicar una release llamada "DBFlux vX.Y.Z" desde un fork.
- Apuntar una comprobación de actualizaciones integrada en la aplicación a un
  fork mientras la aplicación sigue llamándose DBFlux.
- Reutilizar el icono de DBFlux, o uno recoloreado, como icono de la
  aplicación.

El cambio de nombre no tiene que tocar el árbol de fuentes. Cambiar el nombre
mostrado, el identificador del bundle, el conjunto de iconos, el campo
`repository` y el título de la release es suficiente.

## Por qué existe esto

Un usuario que instala "DBFlux" espera las builds publicadas por este
proyecto: cambios revisados, el comportamiento documentado de datos y
privacidad, y artefactos firmados donde la plataforma lo permite. Una build
modificada bajo el mismo nombre rompe esa expectativa, y los reportes de error
que genera acaban aquí.

## Contacto

Las preguntas y solicitudes de permiso van a un issue en este repositorio con
el prefijo `[trademark]` en el título. No hay tarifa ni formulario. La mayoría
de las solicitudes que explican el caso de uso se responden con un sí.
