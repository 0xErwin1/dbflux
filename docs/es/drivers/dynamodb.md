# DynamoDB

Base de datos NoSQL clave-valor y de documentos gestionada por AWS.

## De un vistazo

- **Categoría** — Documento
- **Lenguaje de query** — Expresiones DynamoDB
- **Esquema de URI** — `dynamodb`

Driver de AWS DynamoDB para DBFlux, construido sobre el SDK
[`aws-sdk-dynamodb`](https://crates.io/crates/aws-sdk-dynamodb).

## Funcionalidades

- Driver NoSQL gestionado clasificado como `DatabaseCategory::Document` con un
  envelope de comandos `QueryLanguage::Custom("DynamoDB")`; el editor usa una
  sintaxis específica de DynamoDB, no SQL.
- Configuración de conexión AWS vía región, perfil con nombre y override de
  endpoint opcional (para DynamoDB Local o endpoints VPC). `deployment_class` es
  `CloudManaged`.
- Descubrimiento de tables con `ListTables` y `DescribeTable`, mapeando la
  metadata de clave de partition key (PK), sort key (SK) y Global/Local
  Secondary Index (GSI/LSI) a las abstracciones de schema de DBFlux.
- Ejecución nativa de envelope de comandos para `scan`, `query`, `put`, `update`
  y `delete`. El generador de queries emite envelopes de vista previa con forma
  de scan y anota que la ejecución puede optimizarse a `Query` cuando el filtro
  coincide con el key schema de la table.
- Opciones de lectura para el direccionamiento de índices, control de lectura
  consistente y una política de fallback de traducción de filtros (filtro del
  lado del servidor vs. fallback del lado del cliente; el filtrado del lado del
  cliente se rechaza cuando la política de fallback está configurada para
  rechazar).
- Operadores WHERE en filtros semánticos: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`,
  `In`, `NotIn`, y el lógico `And`/`Or` (ver Limitaciones para `Not`).
- Mutaciones: insert (`put`), update y delete (`INSERT`/`UPDATE`/`DELETE`). Las
  escrituras por lotes soportan hasta 25 items (`max_insert_values: 25`,
  `supports_batch: true`) con reintento acotado para items de batch-write no
  procesados.
- Soporte de upsert de un solo item vía un update condicional con fallback a
  put; el mapa de clave se resuelve desde el filtro o el payload de update
  (partition key requerida, sort key requerida cuando la table define una).
- Ruta de update multi-item (`update` con `many=true`) usando una expresión de
  update compartida.
- Documentos anidados y arrays mapeados a la vista de árbol de documentos
  (`NESTED_DOCUMENTS`, `ARRAYS`).
- DDL: drop table (`supports_drop_table: true`).
- Paginación vía page tokens (`PaginationStyle::PageToken`).
- Identidad del cliente: cada request lleva `dbflux-<versión>` como app name del
  SDK de AWS, visible en el campo `userAgent` de CloudTrail.

## Limitaciones

- El campo `profile` (perfil con nombre de AWS) es un campo de formulario
  `AuthProfileRef`. El seam genérico de portabilidad
  (`DbDriver::export_field_hint`) mapea todos los campos `AuthProfileRef` a
  `RequiredOnImport`, así que el valor del campo se omite de cualquier bundle
  exportado y los destinatarios deben suministrar o crear un auth profile
  coincidente al momento de importar. No se requiere ningún override específico
  del driver.
- La cancelación de query no está soportada; el driver devuelve `NotSupported`
  para las solicitudes de cancelación.
- La API de envelope de comandos no expone PartiQL ni operaciones de transacción
  de DynamoDB; las transacciones están deshabilitadas (`supports_transactions:
  false`).
- El upsert de un solo item está soportado (`supports_upsert: true`); `update`
  con `many=true` y `upsert=true` juntos se rechaza (`update_many_with_upsert`).
- El update masivo y el delete masivo no están soportados
  (`supports_bulk_update: false`, `supports_bulk_delete: false`), y `RETURNING`
  no está soportado.
- Los filtros semánticos no soportan expresiones `NOT` ni operadores fuera del
  conjunto soportado; los operadores no soportados devuelven `NotSupported`.
- Sin formulario SSL (TLS es manejado por el transporte del SDK de AWS), sin
  schemas, y sin DDL más allá de drop-table (sin creación/alteración de table,
  sin creación de índices).
- Las requests de agregación no están soportadas por el planificador semántico.
- El browsing de collections en la capa de request del núcleo permanece basado
  en offset, mientras que la API subyacente está basada en page-token.
