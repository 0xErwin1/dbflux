# Política de privacidad

DBFlux es una aplicación de escritorio local-first. No recopila, transmite ni
almacena información sobre ti ni sobre tu uso. El sitio web del proyecto no
usa cookies ni carga scripts de terceros. Este documento explica qué significa
eso en la práctica y nombra a los proveedores de infraestructura que ven el
tráfico en su camino hacia ti.

## Ruta rápida

1. La aplicación envía datos únicamente a los servidores de base de datos que
   configuras, y solo lo necesario para ejecutar las consultas que pides.
2. No hay telemetría, ni reporte de fallos, ni analítica de uso, ni cuenta de
   usuario. Nada llama a casa.
3. El sitio web y la documentación son páginas estáticas. No usan cookies, no
   cargan scripts de terceros y no guardan registro de visitantes
   individuales. Una medición anónima del uso de las páginas, gestionada por el
   servidor propio del proyecto, se describe en
   [Medición de uso del sitio web](#medición-de-uso-del-sitio-web); todavía no
   está activa.

## La aplicación

| Pregunta | Respuesta |
|----------|-----------|
| ¿DBFlux envía datos de uso a algún sitio? | No. No hay telemetría ni reporte de fallos. |
| ¿Comprueba si hay actualizaciones? | No. Las actualizaciones se encuentran en la página de releases de GitHub. |
| ¿Qué conexiones de red abre? | Solo las que configuras: servidores de base de datos, túneles SSH, proxies y APIs cloud de los drivers que uses. |
| ¿Dónde viven mis datos? | En tu máquina, en un único archivo SQLite más el llavero del sistema operativo para los secretos. |
| ¿Guarda un registro de lo que hago? | Solo el log de auditoría, en tu máquina. Registra conexiones, consultas y ejecuciones de hooks para que puedas revisarlas, y nunca sale del equipo en el que se escribió. |
| ¿Se comparte algo con el proyecto? | No. Los reportes de error y los logs llegan al proyecto solo cuando tú los adjuntas a un issue. |

Las conexiones a una base de datos, un host SSH, un proxy o un proveedor cloud
van directamente desde tu máquina al servidor que indicaste. El proyecto no
opera ninguno de esos servidores y no tiene visibilidad de ese tráfico.

[Tus datos en este equipo](#tus-datos-en-este-equipo) documenta los archivos que DBFlux
escribe, qué registra el log de auditoría, cómo se guardan los secretos y cómo
hacer copia de seguridad o reiniciar la aplicación por completo.

## Tus datos en este equipo

Dónde almacena DBFlux tus datos, cómo protege tus credenciales, qué guarda el
audit log y cómo hacer un backup o un reseteo completo.

### De un vistazo

| Tus datos                                                                  | Dónde viven                                                          |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Perfiles de conexión, settings, historial, saved charts/queries, audit log | Un único archivo SQLite: `dbflux.db` en el directorio de datos       |
| Pestañas abiertas / sesión                                                 | El mismo `dbflux.db`, más archivos de script y archivos scratch/shadow en disco |
| Contraseñas, passphrases, secretos de API                                  | Tu **keyring del sistema operativo** — nunca en `dbflux.db`          |
| Token de auth de IPC/MCP                                                   | Un archivo `0600` en el directorio de configuración                  |

DBFlux mantiene casi todo en una única base de datos SQLite. Los secretos son la
excepción deliberada: van al keyring del sistema operativo, y la base de datos
solo almacena una *referencia* a ellos.

### Ubicaciones de datos

DBFlux usa los directorios estándar de tu plataforma.

| Plataforma  | Directorio de datos                     | Directorio de configuración             |
| ----------- | --------------------------------------- | --------------------------------------- |
| **Linux**   | `~/.local/share/dbflux/`                | `~/.config/dbflux/`                     |
| **macOS**   | `~/Library/Application Support/dbflux/` | `~/Library/Application Support/dbflux/` |
| **Windows** | `%APPDATA%\dbflux\`                     | `%APPDATA%\dbflux\`                     |

El directorio de datos contiene:

- **`dbflux.db`** — la base de datos unificada (todo lo de más abajo en [Qué hay
  en la base de datos](#qué-hay-en-la-base-de-datos)).
- **`sessions/`** — archivos scratch/shadow para las pestañas de editor
  abiertas: contenido sin título, más una copia de recuperación de las ediciones
  sin guardar.
- **`ipc_auth_token`** — el token de auth de IPC/MCP (ver [más
  abajo](#token-de-auth-de-ipcmcp)).
- **`ssh_known_hosts`** — claves de host SSH aceptadas (TOFU).

DBFlux ya no usa el directorio de configuración. Versiones antiguas almacenaban
ahí el token de auth de IPC y los known-hosts de SSH; pueden quedar archivos
residuales tras actualizar y se pueden eliminar.

#### Stable vs. Nightly

Un build Nightly usa un archivo de base de datos separado, `dbflux-nightly.db`,
así que una migración de pre-lanzamiento nunca puede tocar tus datos stable. Los
builds Stable y release candidate usan ambos `dbflux.db`.

Puedes hacer que un build Nightly comparta la base de datos stable mediante
**Settings → General → Storage → Use the stable database** (aplica en el
siguiente arranque). Internamente esto solo crea un archivo marcador vacío
`use-stable-db` en el directorio de datos.

### Qué hay en la base de datos

`dbflux.db` es un único archivo SQLite. Sus tablas se agrupan por prefijo:

| Prefijo | Contiene                                                                                                                                                                                                                                   |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cfg_*` | Configuración: perfiles de conexión, perfiles de auth/proxy/túnel SSH, servicios RPC, hooks de conexión, gobernanza MCP, y los settings de General/Audit. (Los *valores* de los secretos **no** están aquí — solo referencias al keyring.) |
| `st_*`  | Estado del workbench: sesiones/pestañas abiertas, **historial de queries** (texto completo de la query), saved queries, elementos recientes, caché de schema, estado de la UI.                                                             |
| `aud_*` | El audit log y los filtros de auditoría guardados.                                                                                                                                                                                         |
| `viz_*` | Saved charts y dashboards.                                                                                                                                                                                                                 |
| `qry_*` | Saved queries del Visual Query Builder.                                                                                                                                                                                                    |
| `sys_*` | Interno: versión de migración del schema, metadatos de la app.                                                                                                                                                                             |

> **Nota sobre el historial de queries.** El historial de queries del workbench
> (`st_*`) almacena el **texto completo** de las queries que ejecutas, en claro.
> Esto es distinto del audit log, que por defecto convierte el texto de la query
> en fingerprint (ver abajo). Si no quieres que se retenga el texto de las
> queries, reduce **Max history entries** en Settings → General, o borra el
> historial desde la vista de historial del editor.

### Secretos y el keyring del sistema operativo

Las contraseñas, passphrases SSH, credenciales de proxy y secretos de provider
se almacenan en el keyring de tu sistema operativo, **no** en `dbflux.db`.

| Plataforma  | Backend de keyring                                              |
| ----------- | --------------------------------------------------------------- |
| **Linux**   | Secret Service (GNOME Keyring / KWallet, a través de libsecret) |
| **macOS**   | Keychain                                                        |
| **Windows** | Windows Credential Manager                                      |

Todas las entradas se almacenan bajo el nombre de servicio **`dbflux`**. La base
de datos solo guarda un string de referencia por secreto:

| Secreto                          | Referencia                                         |
| -------------------------------- | -------------------------------------------------- |
| Contraseña de conexión           | `dbflux:conn:<profile-id>`                         |
| Contraseña/passphrase SSH inline | `dbflux:ssh:<profile-id>`                          |
| Túnel SSH guardado               | `dbflux:ssh_tunnel:<tunnel-id>`                    |
| Credencial de proxy              | `dbflux:proxy:<proxy-id>`                          |
| Campo de auth profile            | `dbflux:auth:<profile-id>:<field>` (uno por campo) |

#### Cuándo se guardan los secretos (y cuándo no)

- La contraseña de una conexión solo se almacena cuando marcas **Save
  password**; los secretos de SSH y proxy solo cuando marcas su casilla
  **Save**.
- Si no hay ningún keyring disponible, DBFlux oculta las casillas **Save** y no
  persiste secretos — tendrás que reintroducirlos cada sesión.
- Un keyring *bloqueado* sigue contando como disponible: las escrituras pueden
  fallar hasta que lo desbloquees, pero DBFlux mantiene el soporte de secretos
  habilitado.

### Restauración de sesión y pestañas

Qué pestañas tienes abiertas — su tipo, rutas de archivo, orden, pestaña activa
y estado de pin — se registra en `dbflux.db` (`st_sessions` /
`st_session_tabs`). Las copias scratch/shadow que se usan para restaurarlas
viven bajo `sessions/` en el directorio de datos. Un script respaldado por un
archivo se guarda en ese archivo mismo: en el intervalo de auto-guardado, al
cerrar su pestaña y al salir. El contenido sin título sin guardar se conserva
en la carpeta `sessions/`. Esas escrituras automáticas nunca sobrescriben un
archivo de script que cambió fuera de dbflux: la escritura se rechaza y las
ediciones pendientes quedan en el editor (y en la copia de la carpeta
`sessions/`). `Ctrl+s` y **Save file as…** son deliberados: escriben el archivo
tal como lo pediste. Al arrancar,
DBFlux restaura esta sesión cuando **Settings → General → Restore session on
startup** está activado (el valor por defecto).

### Auditoría y privacidad

DBFlux registra operaciones significativas (queries, conexiones, hooks, scripts,
cambios de configuración, decisiones de MCP/gobernanza) en el audit log dentro
de `dbflux.db`. Está diseñado para preservar la privacidad por defecto:

| Comportamiento              | Por defecto | Efecto                                                                                                                                          |
| --------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Capture query text**      | Desactivado | El texto de la query se reemplaza por un **fingerprint** SHA-256 más su longitud — el texto completo nunca se almacena en la fila de auditoría. |
| **Redact sensitive values** | Activado    | Los patrones sensibles (claves de AWS, JWTs, connection strings con credenciales, etc.) se reemplazan por `[REDACTED]`.                         |
| **Detail size cap**         | 64 KiB      | Los payloads de evento sobredimensionados se truncan a un pequeño envelope parcial.                                                             |

Las **claves JSON** sensibles (`password`, `token`, `secret`, `api_key`,
`access_key`, `session_token`, `connection_string`, `url`, …) siempre se
redactan — incluso si desactivas la redacción basada en patrones.

> Recuerda la [salvedad del historial de queries](#qué-hay-en-la-base-de-datos): el
> audit log convierte el texto de la query en fingerprint, pero el *historial
> del workbench* lo almacena completo. Son dos almacenes distintos.

Para el schema completo de eventos, las categorías y el visor, ver
[Audit](AUDIT.md) y [Audit → Visor de audit](AUDIT.md#visor-de-audit).

### Token de auth de IPC/MCP

DBFlux expone una superficie IPC local (usada por el servidor MCP y los
servicios RPC externos). Autentica a los llamantes con un token almacenado en:

```
<data dir>/dbflux/ipc_auth_token
```

(en Linux, `~/.local/share/dbflux/ipc_auth_token`). Es un valor aleatorio que se
regenera en cada arranque, se escribe con permisos `0600` de solo el
propietario, y también se exporta a las variables de entorno `DBFLUX_IPC_TOKEN`,
`DBFLUX_DRIVER_IPC_TOKEN` y `DBFLUX_AUTH_PROVIDER_IPC_TOKEN` para los procesos
hijos.

Este token es **solo de identidad de proceso** — cualquier proceso local que
pueda leerlo puede conectarse. No expongas la superficie IPC/MCP más allá de
localhost sin una capa de autenticación adicional. Ver [AI + MCP
Integration](MCP_AI_INTEGRATION.md) para el modelo de confianza.

### Backup y reseteo

DBFlux no tiene un comando dedicado de backup/restore, pero como todo vive en un
único archivo, ambas operaciones son sencillas.

#### Hacer un backup

Copia el único archivo de base de datos mientras DBFlux está cerrado:

```
~/.local/share/dbflux/dbflux.db        # Linux (ajusta según la plataforma)
```

Ese archivo contiene tus perfiles, historial, saved charts/queries y audit log.
Tus **secretos no están en él** — permanecen en el keyring del sistema operativo
— así que una base de datos copiada en otra máquina hará referencia a entradas
de keyring que no existen ahí hasta que reintroduzcas los secretos.

#### Reseteo completo

Para borrar los datos de DBFlux:

1. Elimina el **directorio de datos** (`~/.local/share/dbflux/` en Linux) —
   elimina la base de datos, los archivos de sesión, el token de auth de IPC y
   los known-hosts de SSH.
2. **Solo versiones antiguas:** elimina el directorio de configuración legado
   (`~/.config/dbflux/` en Linux) si todavía existe — las versiones actuales ya
   no lo usan.
3. **Borra las entradas del keyring manualmente.** Los secretos bajo el servicio
   `dbflux` permanecen en tu keyring del sistema operativo después de eliminar
   los directorios; elimínalos con la herramienta de keyring de tu plataforma si
   quieres un borrado completo.

> Eliminar el directorio de datos es irreversible. Haz un backup de `dbflux.db`
> primero si podrías querer recuperar tus perfiles o tu historial.

## El sitio web y la documentación

`dbflux.dev` y `docs.dbflux.dev` son sitios estáticos construidos desde este
repositorio. Estos sitios:

- no usan cookies, ni propias ni de terceros, y solo guardan una cosa en tu
  navegador: el tema que elijas (automático, claro u oscuro), que queda en el
  almacenamiento local y nunca se envía a ningún sitio;
- no cargan scripts de terceros de analítica, publicidad ni seguimiento;
- sirven sus fuentes, recursos y scripts desde el mismo host, así que una
  visita no contacta con ningún otro dominio;
- no mantienen ningún registro por visitante en el servidor que el proyecto
  pueda leer.

La búsqueda de la documentación se ejecuta en tu navegador contra un archivo
de índice descargado del mismo host. La consulta nunca sale de la página.

## Medición de uso del sitio web

> **Estado: no activa.** Esta sección describe la medición antes de que se
> publique. La versión que la active lo anunciará en sus notas de versión, y
> esta línea cambiará en el mismo commit.

Para saber qué páginas se leen y cuánto tardan en cargar, cada visita a una
página envía un pequeño informe anónimo. El script, el colector y el schema
que define cada campo aceptado están en este repositorio.

### Qué contiene un informe

| Campo | Ejemplo | De dónde sale |
|-------|---------|---------------|
| Host | `site` o `docs` | La página |
| Página | La ruta, como `/docs/usage/`. Nunca la query string ni el fragmento. | La página, comprobada contra la lista de páginas del build |
| Versión de la documentación e idioma de la página | `v0.7`, `es` | La página |
| Idioma del navegador | `es`, solo el idioma principal | El navegador |
| Tipo de pantalla | `mobile`, `tablet` o `desktop` | El navegador |
| Origen de la visita | `none`, `internal`, `search`, `github`, `social` u `other`. Nunca la dirección. | El navegador |
| Tiempos de carga | Tiempo hasta el primer byte, DOM listo, carga completa y largest contentful paint, en milisegundos | El navegador |
| Tiempo en la página | Solo el tiempo visible, en milisegundos | El navegador |
| Profundidad de lectura | Posición de scroll máxima en pasos del 25 %, y tiempo visible de cada encabezado de sección | El navegador |
| País | Código de dos letras, como `AR` | Cloudflare, a partir de la petición, antes de reenviarla |
| Navegador y sistema operativo | Familia y versión mayor, como Firefox 131 en Linux | Derivados del User-Agent antes de reenviar; la cadena completa se descarta |

Cada campo de texto acepta solo una lista fija de valores o un patrón
estricto. El colector rechaza cualquier otra cosa, así que un informe no puede
llevar texto libre.

### Qué no se recopila nunca

- La dirección IP. El Worker de Cloudflare que recibe un informe construye una
  petición nueva que contiene solo los campos anteriores, así que el servidor
  del proyecto nunca recibe la dirección del visitante.
- La cadena User-Agent completa, la dirección de referencia completa, las
  query strings ni los fragmentos.
- Cookies, almacenamiento del navegador, huellas digitales del navegador ni
  identificadores de ningún tipo: la medición no lee ni escribe ninguno de
  ellos. Dos informes de la misma persona no se pueden relacionar, y el
  proyecto no cuenta visitantes únicos.
- Movimientos del ratón, clics, pulsaciones de teclas, datos de formularios ni
  contenido de la página.

### Cómo desactivarla

El script no envía nada cuando el navegador indica Global Privacy Control o Do
Not Track, y el Worker descarta cualquier informe que llegue con alguna de
esas señales. Bloquear el script con un bloqueador de contenido también
funciona; las páginas se comportan igual sin él.

### Dónde se guardan los datos

Los informes se guardan en un servidor que el proyecto alquila a OVHcloud en la
Unión Europea, no en un servicio de analítica. Los informes sin procesar se
conservan 90 días; los totales diarios derivados de ellos se conservan de forma
indefinida. Solo el mantenedor los lee, a través de una red privada que el
proyecto gestiona por su cuenta. Las copias de seguridad se cifran antes de
salir del servidor y se guardan en Cloudflare R2, que conserva la copia cifrada
pero no la clave.

### Eliminación de datos

Un informe no contiene nada que identifique a una persona, así que no hay
ningún registro que buscar para un visitante concreto. Si crees que un informe
llevó algo que no debía, infórmalo de forma privada a través de
[GitHub Security Advisories](https://github.com/0xErwin1/dbflux/security/advisories/new)
en lugar de en un issue público. Los datos afectados se eliminan del
almacenamiento y de las copias de seguridad que los contengan.

## Proveedores de infraestructura

Estos servicios se sitúan entre el proyecto y tú, o guardan datos en nombre
del proyecto. Ninguno se usa para identificar visitantes individuales.

| Proveedor | Función | Qué ve |
|-----------|---------|--------|
| Cloudflare | Aloja el sitio web, la documentación y el endpoint MCP de documentación en `mcp.dbflux.dev`. Ejecuta el Worker que reenvía los informes de uso y guarda en R2 las copias de seguridad cifradas de esos informes. | Cada petición HTTP a esos hosts, incluyendo la dirección IP y el user agent, como cualquier host, y el contenido de un informe de uso mientras lo reenvía. Cloudflare expone al proyecto recuentos de tráfico agregados. No expone registros por visitante, y el proyecto no ha activado ninguna función que lo haga. En R2 solo guarda copias de seguridad cifradas. |
| OVHcloud | Aloja el servidor que guarda los informes de uso del sitio web, en un centro de datos de la Unión Europea. | Los informes guardados, como operador de la máquina en la que están. Nunca recibe la dirección IP ni el User-Agent de un visitante, porque los informes llegan al servidor sin ellos. No tiene ningún papel en la aplicación. |
| Google Search Console | Informa de cómo aparece el sitio en los resultados de búsqueda de Google. | Solo lo que el rastreador y los resultados de Google ya conocen. Ninguna página carga scripts de Google. |

El tratamiento que Cloudflare hace de los datos de las peticiones se describe
en la [política de privacidad de Cloudflare](https://www.cloudflare.com/privacypolicy/).

## El endpoint MCP de documentación

`mcp.dbflux.dev` permite a un cliente de IA buscar en la documentación. Una
sesión dura lo que dura una conexión y solo contiene los mensajes
intercambiados en ella. No hay cuentas, no se almacenan consultas de forma
persistente y no queda ningún registro que vincule una sesión con un visitante
una vez termina.

## GitHub

Los issues, pull requests, discusiones y descargas de releases están en GitHub.
Todo lo que publiques allí es público y se rige por la
[declaración de privacidad de GitHub](https://docs.github.com/site-policy/privacy-policies/github-general-privacy-statement).

## Cambios en esta política

Este archivo se versiona junto con el código fuente. El historial de commits de
`PRIVACY.md` es el registro de cambios. Cualquier cambio que haga que la
aplicación o el sitio web recopilen algo que hoy no recopilan se anunciará en
las notas de la versión que lo introduzca.

## Contacto

Las preguntas van a un issue en este repositorio con el prefijo `[privacy]` en
el título. Para informar de datos que no deberían ser públicos, usa
[GitHub Security Advisories](https://github.com/0xErwin1/dbflux/security/advisories/new)
en su lugar.

## Relacionado

- [Settings & Hooks](SETTINGS.md) — los controles de General/Audit/Storage
  referenciados aquí.
- [Connecting → Advanced Setup](CONNECTIONS.md) — dónde se introducen los
  secretos.
- [Audit](AUDIT.md) — el schema completo de eventos de auditoría y los detalles
  de redacción.
