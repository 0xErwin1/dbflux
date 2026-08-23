# Amazon Redshift

Data warehouse gestionado por AWS, compatible a nivel de wire protocol con PostgreSQL. Solo lectura.

## De un vistazo

- **Categoría** — Relacional
- **Lenguaje de query** — SQL
- **Puerto por defecto** — 5439
- **Esquema de URI** — `redshift`

Driver de Amazon Redshift para DBFlux (v1 de solo lectura), construido directamente sobre el cliente de wire protocol [`postgres`](https://crates.io/crates/postgres) en lugar de sobre `dbflux_driver_postgres`.

## Funcionalidades

- Driver relacional (`DatabaseCategory::Relational`, `QueryLanguage::Sql`) que habla el protocolo wire de PostgreSQL contra un cluster de Redshift o un endpoint de Redshift Serverless.
- Formulario de conexión con host, port (por defecto `5439`), database, user, password, SSL/`sslmode` (`disable`/`allow`/`prefer`/`require`/`verify-ca`/`verify-full`), un modo de URI de conexión (`redshift://...`, normalizado internamente a `postgresql://...`), y túnel SSH.
- Confianza TLS personalizada y TLS mutuo: para los modos con TLS habilitado se agrega una CA raíz privada fijada (PEM) al almacén de confianza además de las raíces del sistema, y un certificado de cliente + clave privada (PEM/PKCS#8) habilitan el TLS mutuo. El material del certificado se carga por conexión desde las rutas configuradas en el formulario; la validación nunca se debilita (`verify-ca`/`verify-full` siguen rechazando un certificado no confiable). Un archivo de certificado/clave faltante, ilegible o malformado se muestra como un error de conexión claro en lugar de recurrir silenciosamente al almacén de confianza del sistema, y el contenido de la clave privada nunca se registra en logs.
- Introspección de schema sobre `information_schema` para databases, schemas, tables, views y columns, con clasificación de `ColumnKind` (timestamp/integer/float/text) reflejando los OIDs estándar de PostgreSQL.
- Los tipos extendidos exclusivos de Redshift (`SUPER`, `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`) se clasifican como `ColumnKind::Text` y se renderizan como texto; cualquier otro OID no reconocido recurre a una decodificación defensiva a texto UTF-8 en lugar de causar un panic.
- Los detalles de table exponen metadata de almacenamiento específica de Redshift a través del seam genérico `TableInfo.storage_hints` (leído de `SVV_TABLE_INFO` y `PG_TABLE_DEF`): distribution key (`KEY`/`EVEN`/`ALL`/`AUTO`, con la columna clave cuando aplica) y sort key (compuesta o interleaved, con sus columnas ordenadas). Las constraints declaradas de primary key, foreign key y unique se siguen exponiendo a través de las formas de metadata estándar del núcleo, cada una etiquetada como advisory/no forzada — Redshift acepta pero nunca fuerza estas constraints, y no se fabrica ninguna lista de índices a partir de ellas.
- La ejecución de queries (`SELECT`/browse) devuelve filas con columnas tipadas a través de la ruta estándar `Connection::execute`; la cancelación de query está soportada a través del token de cancelación del cliente de wire protocol.
- `RedshiftErrorFormatter` mapea fallos comunes de conexión y de query (timeouts, conexiones rechazadas, fallos de autenticación, clusters inalcanzables, errores de query con `SQLSTATE`) a mensajes claros formateados por el driver en lugar de output de debug crudo.

## Limitaciones

- Solo lectura: `DriverMetadata.capabilities` omite `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, `BULK_INSERT`, `TRUNCATE_TABLE`, y todas las flags de DDL/DDL-transaccional. No hay edición inline en la grilla ni mutation/visual-query builder para este driver. `Connection::execute` además rechaza cualquier statement que no sea de lectura a nivel del wire con un error explícito, así que un intento de escritura nunca se convierte en un no-op silencioso.
- Solo un statement por vez: `Connection::execute` ejecuta un statement de solo lectura a la vez. La entrada multi-statement (p. ej. `SELECT 1; SELECT 2`) se rechaza con un error explícito antes de llegar al wire; se permite un único `;` final opcional, y `;` dentro de literales de cadena, identificadores entrecomillados o comentarios no se trata como separador.
- Sin capacidad `INDEXES`: Redshift no tiene estructuras de índice reales, así que `TableDetails.indexes` siempre es `None` en lugar de sintetizarse a partir de la primary key (no forzada).
- Sin triggers: Redshift no soporta triggers, así que ninguno se descubre ni se expone.
- Sin autenticación basada en IAM/SSO. Solo se soporta username/password (opcionalmente vía túnel SSH); el `GetClusterCredentials` basado en IAM de Redshift y los flujos de SSO por navegador no están implementados.
- Los certificados de cliente para TLS mutuo deben suministrarse como un certificado PEM más una clave privada PEM PKCS#8 (ambos se configuran como rutas de archivo separadas); no se acepta un bundle PKCS#12 combinado. El parseo PEM y el manejo de errores de la ruta de carga de CA raíz/certificado de cliente están cubiertos por pruebas unitarias, pero el handshake TLS de extremo a extremo contra un cluster respaldado por una CA privada o que requiere un certificado de cliente solo se valida mediante una prueba de integración en vivo marcada `#[ignore]` (`redshift_live_verify_full_with_private_ca_and_client_cert`), ya que no existe ningún motor de Redshift local ni basado en Docker.
- Sin soporte de `COPY`/`UNLOAD` ni integración de transferencia de datos/exportación masiva específica de Redshift.
- Sin visualización de plan de query (el output de `EXPLAIN` no se parsea ni se renderiza).
- Sin métricas de instancia ni inspector de instancia (`INSTANCE_METRICS`/`INSTANCE_INSPECTOR` no están declaradas).
- Los valores de OID de tipo extendido usados para `SUPER`/`VARBYTE`/`GEOMETRY`/`GEOGRAPHY`/`HLLSKETCH`, y las formas exactas de query de `SVV_TABLE_INFO`/`PG_TABLE_DEF` usadas para los storage hints, solo se validan mediante pruebas de integración en vivo marcadas `#[ignore]` (`crates/dbflux_driver_redshift/tests/live_integration.rs`), ya que no existe ningún motor de Redshift local ni basado en Docker. Ejecútalas explícitamente contra un cluster real con `cargo nextest run -p dbflux_driver_redshift --run-ignored all`.
- Los valores `NUMERIC`/`DECIMAL` SÍ se decodifican: el driver parsea el formato wire binario `NUMERIC` de PostgreSQL directamente a una cadena `Value::Decimal` exacta (reconstrucción de parte entera/fraccionaria a la escala declarada de la columna, más `NaN`/±`Infinity`). Los payloads malformados recurren a un fallback seguro en lugar de corromper datos. El decodificador binario está cubierto por pruebas unitarias sobre payloads wire sintéticos; la fidelidad de extremo a extremo solo se valida contra un cluster real vía las pruebas de integración marcadas `#[ignore]`.
