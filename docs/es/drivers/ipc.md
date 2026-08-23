# Drivers RPC externos

Puente que permite que un driver ejecutándose en su propio proceso sirva a DBFlux a través de un
socket local, de modo que se pueda agregar una base de datos que DBFlux no soporta de forma nativa
sin necesidad de hacer un fork de la aplicación.

## De un vistazo

- **Categoría** — declarada por el driver remoto
- **Lenguaje de query** — declarado por el driver remoto
- **Id de registro** — `rpc:<socket_id>`
- **Protocolo** — ver la referencia del protocolo RPC de drivers

## Cómo funciona

El puente no tiene conocimiento de ninguna base de datos en particular. Al conectar, realiza un
handshake `Hello` con el proceso remoto, y todo lo que DBFlux necesita para presentar
el driver — su tipo, su metadata, su formulario de conexión — vuelve en esa
respuesta. A partir de ahí el puente reenvía las operaciones a través del socket y traduce
las respuestas a los mismos contratos del núcleo que implementa un driver de Rust integrado, de modo que
el resto de la aplicación no puede notar la diferencia.

Los valores de conexión se persisten como `DbConfig::External { kind, values }`, indexados por
el formulario que declaró el driver remoto.

### Capacidades negociadas en el handshake

El driver remoto anuncia lo que soporta, y el puente se adapta:

- `SchemaIntrospection` — la barra lateral construye un árbol de schema a partir del driver
- `MultiDatabase` — la conexión expone más de una database
- `ChunkedResults` — los result sets grandes se transmiten en chunks en lugar de en una sola respuesta
- `Cancellation` — una query en ejecución se puede cancelar desde la UI
- `AuditEmit` — el driver puede emitir sus propios eventos de auditoría (protocolo v1.2 y posteriores)

Todo lo que no se anuncia simplemente está ausente de la interfaz, de la misma forma en que
una capability flag no establecida en un driver integrado elimina funcionalidades de la UI.

### Ciclo de vida del host

Un servicio RPC configurado puede ser gestionado por DBFlux: el proceso host se lanza bajo
demanda, se espera hasta que reporte estar saludable, y se rastrea para su apagado. Un servicio
sin configuración de lanzamiento se asume ya en ejecución.

### Reenvío de auditoría

Los drivers que anuncian `AuditEmit` pueden enviar frames `EmitAuditEvent` intercalados
con sus respuestas. El puente intercepta esos frames y los despacha al
sink de sanitización del host, de modo que los eventos de un driver externo terminan en el mismo log
de auditoría que todo lo demás, con la misma redacción aplicada. Los frames de un driver que
no anunció la capacidad se descartan en lugar de ser confiados.

## Limitaciones

- Requiere un proceso host de driver compatible y un socket alcanzable. El puente
  no puede iniciar un host que no tenga configuración de lanzamiento, así que un servicio no disponible
  permanece no disponible.
- El conjunto de funcionalidades efectivo está acotado por la metadata anunciada del driver remoto
  y por su implementación. El puente nunca agrega una capacidad que el driver no tenga.
- La emisión de auditoría necesita protocolo v1.2 o posterior y `AuditEmit` en el handshake.
  Los drivers más antiguos no emiten nada, y sus operaciones aparecen en el log solo a través de
  los eventos que DBFlux registra en su nombre.
