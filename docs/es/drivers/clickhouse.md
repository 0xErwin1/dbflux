# ClickHouse

Base de datos analítica orientada a columnas sobre HTTP.

## De un vistazo

- **Categoría** — Relacional
- **Query language** — SQL
- **Puerto por defecto** — 8123
- **Esquema de URI** — `http`

## Conexión

El driver habla la interfaz HTTP de ClickHouse en lugar del protocolo nativo,
así que el endpoint es una URL y no un par host/port.

| Campo              | Por defecto             | Notas                                                                         |
| ------------------ | ----------------------- | ----------------------------------------------------------------------------- |
| URL HTTP           | `http://localhost:8123` | los endpoints `https://` se sirven a través de rustls                         |
| Database           | `default`               | delimita el descubrimiento de schema y las queries sin calificar              |
| Timeout de request | `30` segundos           | debe ser mayor que cero                                                       |
| Usuario            | `default`               |                                                                               |
| Contraseña         | —                       | se guarda en el keyring del sistema operativo y se envía como HTTP Basic auth |

## Funcionalidades

- Transporte HTTP(S) bloqueante construido sobre rustls, autenticando con HTTP
  Basic.
- SQL arbitrario de un solo statement, con respuestas forzadas a `JSONCompact`
  para que los nombres y tipos de columna lleguen junto con las filas.
- El ancho de fila se verifica contra el número de columnas declaradas en cada
  respuesta, así que una respuesta malformada falla con un error claro en lugar
  de desplazar valores entre columnas.
- Descubrimiento de schema desde `system.databases`, `system.tables` y
  `system.columns`: databases, tables, views, columns, engine, sorting y
  partition keys, tamaño en disco y compresión.
- La carga de schema es perezosa por database, así que un servidor con muchas
  databases no paga el costo de todas ellas al conectar.
- Paginación, ordenamiento y filtrado se aplican por el driver como
  `LIMIT`/`OFFSET` alrededor del statement, que es lo que hace funcionar el
  browsing de resultados sin un cursor.
- Generación de SELECT visual de solo lectura, usando las reglas de quoting de
  identificadores y literales de ClickHouse.
- Creación de gráficos a partir de resultados de queries, y exportación a CSV y
  JSON.
- Cada request HTTP reporta `dbflux/<versión>` como header `User-Agent`, visible
  en los logs de request del lado del servidor.
- La detección de queries peligrosas usa el `SqlLanguageService` compartido (sin
  override específico de ClickHouse): `ALTER TABLE ... DELETE WHERE ...` y
  `ALTER TABLE ... UPDATE ... WHERE ...` ya se detectan como `Alter` (cualquier
  statement que empiece con `ALTER` se marca sin importar el subcomando), y
  `TRUNCATE`/`DROP` se detectan como de costumbre. `KILL QUERY`/`KILL MUTATION`
  y `OPTIMIZE TABLE ... FINAL` no se marcan — ninguno borra filas ni cambia la
  estructura de la tabla — igual que otros drivers relacionales tratan
  statements administrativos no destructivos comparables.
- Sondeo de privilegio de escritura: tras conectar, revisa el setting de
  servidor `readonly` y `system.grants` para el usuario actual y sus roles de
  sesión activos, para detectar una sesión de solo lectura o un usuario sin
  grants `INSERT`/`ALTER UPDATE`/`ALTER DELETE`, ajustando la política de
  mutación resuelta a solo lectura cuando el servidor rechazaría las
  escrituras de todos modos (sin efectos secundarios; los grants alcanzables
  solo a través de un rol dentro de otro rol no se resuelven y dejan la
  política sin cambios).

### Manejo de tipos

Los valores se decodifican de forma recursiva, así que un `Map(String,
Array(Nullable(Decimal256)))` llega completamente estructurado en lugar de como
texto crudo:

- Wrappers — `Nullable`, `LowCardinality`
- Contenedores — `Array`, `Tuple`, `Map`, `Nested`
- Números — enteros hasta `UInt256`, `Decimal256`, `BFloat16`, `Bool`
- Tiempo — `Date32`, `DateTime64`
- Otros — `Enum16`, `Nothing`

## Limitaciones

- Sin túneles SSH.
- Sin transacciones, prepared statements ni cancelación de query. Un statement
  en ejecución está acotado únicamente por el timeout de request, y el driver no
  reporta soporte de lock-timeout.
- Sin soporte estructurado de `INSERT`, `UPDATE`, `DELETE`, DDL ni transferencia
  de datos. El SQL de escritura solo se ejecuta cuando se escribe explícitamente
  en el editor, así que la grilla es de solo lectura y el driver no es un
  destino de transferencia.
- Un statement SQL por request; los scripts multi-statement no se procesan por
  lotes.
- Los cuerpos de respuesta HTTP están limitados a 128 MiB.
- Las zonas horarias con nombre de ClickHouse no se interpretan del lado del
  cliente. Los timestamps ISO con un offset se manejan correctamente; los
  timestamps sin uno se tratan como UTC.
