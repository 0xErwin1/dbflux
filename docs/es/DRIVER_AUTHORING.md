# Guía de autoría de drivers

Usa esta guía para elegir e implementar una integración de driver de base de
datos de DBFlux. Cubre el camino del contribuidor sin repetir las referencias
más amplias de arquitectura o protocolo RPC.

## Elegir un camino de integración

| Elige | Driver Rust integrado | Driver RPC externo |
| --- | --- | --- |
| Mejor opción cuando | El driver debe distribuirse dentro del workspace y proceso de DBFlux | El driver debe ejecutarse fuera de proceso o desarrollarse y desplegarse de forma independiente |
| Implementación | Un crate `crates/dbflux_driver_<name>/` que implementa los contratos Rust del core | Un servicio que implementa el protocolo RPC de drivers |
| Registro | Wiring de features en tiempo de compilación y `AppState::build_builtin_drivers()` | Settings -> RPC Services con `kind=driver` y un `socket_id` |
| Clave estable | `builtin:<name>` | `rpc:<socket_id>` |
| Configuración | `DriverFormDef` propio del driver convertido a una variante `DbConfig` integrada | Datos de formulario provistos por el handshake y almacenados como `DbConfig::External` |

## Driver integrado: camino feliz

1. Copia la estructura del crate `crates/dbflux_driver_*/` existente más
   cercano.
2. Implementa `DbDriver` y `Connection`, incluyendo metadata, conversión de
   form/config, comportamiento de conexión, errores y columnas de resultado
   tipadas.
3. Declara solo las capacidades respaldadas por implementaciones funcionales;
   añade seams opcionales únicamente cuando el driver los soporte.
4. Conecta el crate y el feature a través del workspace, la app y el binario,
   y luego regístralo en `build_builtin_drivers()`.
5. Añade tests focalizados, un README del crate y actualiza la matriz de
   soporte de drivers.

El [checklist detallado del driver integrado](#checklist-de-driver-integrado)
desarrolla cada paso.

## Driver RPC externo: camino feliz

1. Implementa el [Protocolo RPC de drivers](DRIVER_RPC_PROTOCOL.md) canónico,
   usando el [ejemplo de driver personalizado](../examples/custom_driver/README.md)
   como punto de partida.
2. Construye y ejecuta el servicio, ya sea de forma independiente o con un
   comando gestionado.
3. Añádelo en Settings -> RPC Services con `kind=driver`, un `socket_id`
   estable y el comando gestionado opcional.
4. Reinicia DBFlux y verifica que la metadata y el formulario provistos por el
   handshake aparezcan en el connection manager.

Consulta la [referencia de configuración de RPC Services](RPC_SERVICES_CONFIG.md)
para el comportamiento de configuración vigente. No copies flags de
lanzamiento de esta guía; el protocolo, la referencia de configuración y el
ejemplo son las fuentes autoritativas.

## Contratos principales y regla de desacoplamiento

El contrato principal es [`DbDriver` más `Connection`](../crates/dbflux_core/src/core/traits.rs):

- `DbDriver` provee la metadata del driver, su definición de formulario de
  conexión, la construcción y extracción de config, la construcción de la
  conexión y una `DriverKey` estable.
- `Connection` provee el comportamiento en tiempo de ejecución de query,
  schema, mutation y el comportamiento específico de capacidad opcional.
  Existen defaults para muchas operaciones no soportadas; los métodos
  requeridos y las capacidades anunciadas deben seguir siendo consistentes
  entre sí.
- Los valores `driver_key()` integrados usan `builtin:<name>`. Los drivers
  externos usan `rpc:<socket_id>`.

La metadata y la adaptación se definen mediante
[`DriverMetadata`, `DatabaseCategory`, `QueryLanguage` y `DriverCapabilities`](../crates/dbflux_core/src/driver/capabilities.rs),
incluyendo metadata genérica de presentación de editor. El comportamiento de
fuente y presentación en tiempo de ejecución se expone mediante seams
genéricos en `Connection` en [`traits.rs`](../crates/dbflux_core/src/core/traits.rs).

**Regla estricta:** el código de la UI y del workflow de la app no debe
ramificarse por driver ID concreto. Adapta a partir de metadata, category,
query language, flags de capacidad, definiciones de formulario y seams
genéricos de fuente/presentación. Si una nueva distinción de UI es
necesaria, añade un contrato genérico del core que otro driver también
pudiera implementar.

Para los datos de resultado, completa
[`ColumnMeta.kind` con `ColumnKind`](../crates/dbflux_core/src/query/types.rs).
Los charts y otros consumidores usan este kind semántico; no lo infieren de
un driver ID ni de `type_name`.

## Checklist de driver integrado

### 1. Crate y contratos

- [ ] Añade `crates/dbflux_driver_<name>/Cargo.toml`, `src/lib.rs`, los
      módulos de implementación y los tests. Sigue el driver más cercano en
      lugar de asumir que todos los drivers tienen módulos idénticos.
- [ ] Implementa `DbDriver` y una `Connection` thread-safe desde
      [`crates/dbflux_core/src/core/traits.rs`](../crates/dbflux_core/src/core/traits.rs).
- [ ] Devuelve una `DriverKey` estable con la forma `builtin:<name>`.
- [ ] Mantén los tipos de cliente de base de datos y el comportamiento
      específico del driver dentro del crate del driver; expón el
      comportamiento a través de los contratos del core.

### 2. Metadata y capacidades

- [ ] Define un `DriverMetadata` factual: identidad, campos de display,
      `DatabaseCategory`, `QueryLanguage`, `DriverCapabilities`, defaults de
      conexión y las estructuras de capacidad genéricas aplicables.
- [ ] Usa la metadata y los seams genéricos de presentación/fuente para la
      adaptación de la UI. No añadas condicionales de driver ID a la UI ni a
      los workflows de la app.
- [ ] Anuncia una capacidad solo cuando la operación o el seam opcional
      correspondiente funcione. Confirma tanto las afirmaciones negativas
      como el comportamiento soportado.

### 3. Formularios y configuración

- [ ] Define y posee el `DriverFormDef` del crate; la UI de conexión lo
      renderiza de forma genérica.
- [ ] Implementa la validación de `build_config()` y los round trips de
      edición de `extract_values()`.
- [ ] Mantén los secretos en los paths de secretos establecidos en lugar de
      embeberlos en los valores de formulario persistidos.
- [ ] Implementa el parseo/construcción de URI o los overrides de campos de
      export solo cuando aplique.

### 4. Conexiones, errores y resultados

- [ ] Construye y prueba la conexión a través de los métodos de `DbDriver`,
      incluyendo el manejo de secretos requerido y los tests de conexión.
- [ ] Implementa formateo estructurado de errores de query y conexión
      mediante [`QueryErrorFormatter` y `ConnectionErrorFormatter`](../crates/dbflux_core/src/core/error_formatter.rs).
      Preserva el contexto útil de la base de datos sin exponer secretos.
- [ ] Devuelve datos de schema y query a través de tipos del core, incluyendo
      `ColumnMeta.kind` para cada columna de resultado usando el
      `ColumnKind` correcto.
- [ ] Prueba el mapeo de tipos directamente. No dependas de que los
      consumidores deriven la semántica a partir de cadenas `type_name` en
      crudo.

### 5. Seams opcionales

Implementa estos únicamente cuando la base de datos los soporte, y mantén los
flags de capacidad sincronizados con la implementación:

- [ ] Un `LanguageService` no-default para validación específica del lenguaje
      y clasificación de mutations.
- [ ] Comportamiento de dialecto SQL, generador de código, generador de
      queries o semantic planner según aplique.
- [ ] Comportamiento de contexto de fuente, catálogo de métricas, importador
      de dashboards o fuente de dashboards según aplique.
- [ ] Un catálogo de instancia para métricas o inspectors según aplique.
- [ ] Otros seams de schema, CRUD, cancelación, transfer o key-value
      representados por los traits del core y los flags de capacidad.

### 6. Wiring de features y registro

- [ ] Añade membresía de workspace y una dependencia de workspace en el
      [`Cargo.toml`](../Cargo.toml) raíz.
- [ ] Añade una dependencia opcional y un relay de feature en
      [`crates/dbflux_app/Cargo.toml`](../crates/dbflux_app/Cargo.toml).
- [ ] Reenvía el feature del binario en
      [`crates/dbflux/Cargo.toml`](../crates/dbflux/Cargo.toml).
- [ ] Añade imports y registro con feature gate en
      [`AppState::build_builtin_drivers()`](../crates/dbflux_app/src/app_state/bootstrap.rs).
- [ ] Verifica tanto el build con el feature habilitado como uno
      representativo con el feature deshabilitado para que el registro se
      mantenga correctamente controlado por el gate.

### 7. Tests y documentación

- [ ] Prueba metadata, declaraciones de capacidad, round trips de
      form/config, errores, comportamiento de conexión, mapeo de schema,
      resultados de query y cada seam opcional anunciado.
- [ ] Añade tests de integración donde el comportamiento cruce el límite
      driver/core; mantén los tests de servicio en vivo ignorados o
      controlados por gate según las convenciones existentes del crate.
- [ ] Añade `crates/dbflux_driver_<name>/README.md` con secciones claras de
      **Features** y **Limitations**.
- [ ] Actualiza [`docs/DRIVERS.md`](DRIVERS.md) y mantén sus afirmaciones de
      capacidad alineadas con el README del crate y la implementación.

## Checklist de driver RPC externo

- [ ] Implementa handshake, form, session, query y las operaciones opcionales
      soportadas frente al [Protocolo RPC de drivers](DRIVER_RPC_PROTOCOL.md).
- [ ] Provee metadata, capabilities y la definición de formulario a través
      del handshake del protocolo; mantén cada afirmación de capacidad
      alineada con las operaciones RPC implementadas.
- [ ] Configura el servicio bajo Settings -> RPC Services como `kind=driver`
      con un `socket_id` estable; añade un comando gestionado solo cuando
      DBFlux deba controlar el ciclo de vida del proceso.
- [ ] Espera la clave de runtime `rpc:<socket_id>` y el almacenamiento de
      configuración genérico a través de `DbConfig::External`.
- [ ] Sigue [RPC Services Config](RPC_SERVICES_CONFIG.md) para la
      configuración persistida y la semántica del ciclo de vida.
- [ ] Construye y haz smoke-test desde el
      [ejemplo de driver personalizado](../examples/custom_driver/README.md),
      y luego prueba reinicio, fallo de handshake, operaciones no soportadas
      y round trips del formulario de conexión.

Los drivers RPC externos no usan el wiring de features de Cargo integrado ni
el path de registro de `build_builtin_drivers()`.

## Checklist de revisión

Antes de abrir un PR, confirma:

- [ ] El camino integrado o RPC seleccionado se usa de forma consistente; los
      dos paths de registro no se mezclan.
- [ ] Ninguna ramificación de la UI ni del workflow de la app depende de un
      driver ID concreto.
- [ ] La metadata, los flags de capacidad, los seams opcionales, los tests,
      el README del crate y `docs/DRIVERS.md` afirman lo mismo.
- [ ] Los round trips de edición de form/config funcionan y los secretos no
      se persisten ni se registran de forma inesperada.
- [ ] Los resultados de query completan `ColumnMeta.kind` correctamente.
- [ ] Los fallos de conexión y query producen errores estructurados, útiles
      y seguros respecto a los secretos.
- [ ] Los builds con el feature integrado deshabilitado y habilitado pasan, o
      el servicio RPC completa su handshake y el smoke test de reinicio.
- [ ] Los checks del repositorio en [CONTRIBUTING.md](../CONTRIBUTING.md)
      pasan.
