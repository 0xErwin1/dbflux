# Settings y Hooks de Conexión

Una referencia para cada sección de Settings y para los connection hooks — los
comandos, scripts o snippets de Lua que DBFlux ejecuta alrededor del ciclo de
vida de una conexión.

Abre Settings desde la command palette (**Open Settings**) o desde la barra
lateral. La ventana está organizada en secciones a lo largo del lado izquierdo.

| Sección                                             | Cubre                                                                        |
| --------------------------------------------------- | ---------------------------------------------------------------------------- |
| [General](#general)                                 | Comportamiento a nivel de app: theme, inicio, refresh, seguridad de queries. |
| [Audit](#audit)                                     | Qué captura el audit log y cuánto tiempo se conserva.                        |
| [Keybindings](#keybindings)                         | Explora el keymap (solo lectura).                                            |
| [Auth Profiles](#auth-profiles-proxies-ssh-tunnels) | Perfiles AWS SSO / shared-credentials.                                       |
| [Proxies](#auth-profiles-proxies-ssh-tunnels)       | Perfiles de proxy SOCKS5 / HTTP.                                             |
| [SSH Tunnels](#auth-profiles-proxies-ssh-tunnels)   | Perfiles reutilizables de túnel SSH.                                         |
| [Services](#services-rpc)                           | Drivers RPC externos y auth providers.                                       |
| [Hooks](#connection-hooks)                          | Definiciones reutilizables de connection hooks.                              |
| [Drivers](#drivers)                                 | Overrides y ajustes por driver.                                              |

Las secciones relacionadas con MCP (Clients, Roles, Policies) aparecen solo
cuando el binario se construye con el feature `mcp`; ver [AI + MCP
Integration](MCP_AI_INTEGRATION.md).

---

## General

### Apariencia

| Setting      | Opciones                 | Default |
| ------------ | ------------------------ | ------- |
| **Theme**    | Dark, Mirage, Light      | Dark    |
| **Style**    | Default, Compact         | Default |
| **Language** | System, English, Spanish | System  |

System sigue el locale de tu sistema operativo y recurre a English cuando ese
locale no está soportado. Un cambio de idioma tiene efecto después de reiniciar
DBFlux, por lo que el control muestra una nota permanente al respecto. Este
release solo traduce la sección General; el resto de la UI se está convirtiendo
crate por crate y permanece en English por ahora.

### Inicio y sesión

| Setting                        | Default | Qué hace                                                         |
| ------------------------------ | ------- | ---------------------------------------------------------------- |
| **Restore session on startup** | On      | Reabre las tabs que tenías abiertas la última vez.               |
| **Reopen last connections**    | Off     | Reconecta a las conexiones que estaban activas.                  |
| **Default focus**              | Sidebar | Dónde cae el focus al iniciar (Sidebar o la última tab).         |
| **Max history entries**        | 1000    | Tope del historial de queries (mínimo 10).                       |
| **Auto-save interval (ms)**    | 2000    | Cada cuánto se auto-guardan los buffers del editor (mínimo 500). |

### Actualización y segundo plano

| Setting                                 | Default | Qué hace                                                  |
| --------------------------------------- | ------- | --------------------------------------------------------- |
| **Default refresh policy**              | Manual  | Manual o Interval auto-refresh para las data views.       |
| **Default refresh interval (seconds)**  | 5       | Intervalo usado cuando la policy es Interval (mínimo 1).  |
| **Max concurrent background tasks**     | 8       | Tope de trabajo simultáneo en segundo plano (mínimo 1).   |
| **Pause auto-refresh on error**         | On      | Detiene el auto-refresh de una view después de que falla. |
| **Auto-refresh only if tab is visible** | Off     | Se salta el refresh de las tabs que no estás mirando.     |

### Seguridad de ejecución (confirmación de queries peligrosas)

Estos tres settings gobiernan cómo DBFlux trata las queries riesgosas en
**todos** los drivers y query languages. No hay un toggle por base de datos —
las mismas reglas aplican a `DELETE`/`DROP`/`TRUNCATE` de SQL,
`deleteMany`/`drop` de MongoDB, `FLUSHALL`/`FLUSHDB` de Redis, etc.

| Setting                                          | Default | Qué hace                                                                                                    |
| ------------------------------------------------ | ------- | ----------------------------------------------------------------------------------------------------------- |
| **Confirm dangerous queries**                    | On      | Muestra una confirmación antes de ejecutar una query peligrosa. Desactívalo para permitirlas sin preguntar. |
| **Require WHERE for DELETE/UPDATE**              | On      | Trata un `DELETE`/`UPDATE` sin `WHERE` como peligroso.                                                      |
| **Always require preview (ignore suppressions)** | Off     | Fuerza el modal de confirmación/preview incluso para queries que anteriormente elegiste dejar de confirmar. |

### Almacenamiento (solo builds Nightly)

| Setting                     | Default | Qué hace                                                                                                               |
| --------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Use the stable database** | Off     | Hace que un build Nightly comparta el `dbflux.db` stable en lugar de `dbflux-nightly.db`. Aplica en el próximo inicio. |

Ver [Data & Privacy](DATA_AND_PRIVACY.md#data-locations) para cómo se separan
las bases de datos Nightly y stable.

---

## Audit

La sección Audit controla el audit log unificado. El control principal orientado
al usuario es **Log Capture → Minimum Level** (trace / debug / info / warn /
error), que determina cuánto del logging interno de DBFlux se pliega en el audit
trail. Guardar tiene efecto sin reiniciar.

La retention (cuánto tiempo se conservan los eventos) impulsa un purge periódico
en segundo plano cuando está configurada. Para la experiencia diaria de audit —
abrir el viewer, filtrar, exportar — ver [Dashboards &
Audit](DASHBOARDS_AND_AUDIT.md#audit-viewer). Para el schema completo de eventos
y el comportamiento de redaction ver [Audit](AUDIT.md) y [Data &
Privacy](DATA_AND_PRIVACY.md#audit-and-privacy).

---

## Keybindings

Esta sección es un **viewer de solo lectura**. Lista el keymap activo agrupado
por contexto, con un filtro de texto y advertencias inline cuando un chord está
vinculado a más de un comando. Actualmente **no** te permite rebind ni guardar
shortcuts personalizados desde la UI. Úsala para descubrir y verificar bindings;
el keymap por defecto completo está documentado en [Usage → Keyboard
Reference](USAGE.md#7-keyboard-reference).

---

## Auth Profiles, Proxies, Túneles SSH

Estas tres secciones gestionan los perfiles reutilizables que luego seleccionas
por conexión en la pestaña Access. Están documentadas en detalle — campos, flujo
AWS SSO, reglas de no-proxy, métodos de auth SSH — en [Connecting to a Database
→ Advanced Setup](CONNECTIONS.md):

- [Auth Profiles](CONNECTIONS.md#auth-profiles-aws-sso-and-shared-credentials)
- [Proxies](CONNECTIONS.md#proxies)
- [SSH Tunnels](CONNECTIONS.md#ssh-tunnels)

Las credenciales que ingresas aquí se guardan en el keyring de tu sistema
operativo, no en la base de datos. Ver [Data & Privacy →
Secrets](DATA_AND_PRIVACY.md#secrets-and-the-os-keyring).

---

## Servicios (RPC)

Los drivers externos y los auth providers corren como procesos separados con los
que DBFlux se comunica a través de un socket local. Cada service que agregas
aquí tiene:

| Campo                     | Notas                                                                                                     |
| ------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Socket ID**             | Identificador único, usado como nombre del archivo del socket. Solo letras ASCII, dígitos, `.`, `_`, `-`. |
| **Command**               | El ejecutable a lanzar (opcional para algunas configuraciones).                                           |
| **Startup Timeout (ms)**  | Cuánto esperar a que el proceso arranque. Default 5000.                                                   |
| **Service Type**          | **Driver** o **Auth Provider**.                                                                           |
| **Enable this service**   | Si el service arranca. Default on.                                                                        |
| **Arguments**             | Argumentos ordenados del proceso.                                                                         |
| **Environment Variables** | Pares `KEY=value` pasados al proceso.                                                                     |

Los cambios aquí **tienen efecto en el próximo inicio**. Referencia completa:
[RPC Services Config](RPC_SERVICES_CONFIG.md) y el [Driver RPC
Protocol](DRIVER_RPC_PROTOCOL.md).

---

## Drivers

Elige un driver para ver y sobrescribir su comportamiento. Dos grupos son
editables:

**Global overrides** — versiones por driver de los settings de General. Cada uno
es un tri-state (Inherit / On / Off, o un valor explícito); dejarlo en *Inherit*
usa el default de General mostrado junto al control:

- Refresh policy e interval
- Confirm dangerous queries
- Require WHERE
- Require preview

**Driver settings** — opciones definidas por el propio driver (renderizadas de
forma genérica a partir del schema del driver, así que los campos disponibles
dependen del driver).

La sección también muestra, en solo lectura, la **capability matrix**, la
category, y el query language del driver.

---

## Connection Hooks

Los hooks son comandos, scripts o snippets de Lua reutilizables que corren
alrededor del ciclo de vida de una conexión. Los **defines** globalmente en
**Settings → Hooks**, y luego los **vinculas** a fases en conexiones
individuales en la pestaña **Hooks** del Connection Manager.

### Camino rápido

1. **Settings → Hooks → agrega un hook.** Dale un **Hook ID**, elige un
   **Type**, y completa el command/script.
2. Abre una conexión en **Connection Manager → Hooks tab**.
3. Selecciona tu hook en uno de los cuatro dropdowns de fase (Pre-connect,
   Post-connect, Pre-disconnect, Post-disconnect).
4. Conecta. La salida del hook se transmite al panel de **Tasks**.

### Tipos de Hook

| Type        | Qué ejecuta              | Qué proporcionas                                                                                                                         |
| ----------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Command** | Un ejecutable            | Un comando y argumentos separados por espacios.                                                                                          |
| **Script**  | Un archivo Bash o Python | Un lenguaje, una ruta de archivo, y un override opcional del interpreter (en blanco = `bash` / `python3`, ajustado según la plataforma). |
| **Lua**     | Un script Lua in-process | Una ruta de archivo y un conjunto de capabilities (ver abajo). Lua corre dentro de DBFlux — sin interpreter externo.                     |

Los scripts se editan en el editor de DBFlux y se guardan por defecto bajo una
carpeta `hooks/`.

#### Capacidades de Lua

Un hook de Lua solo obtiene las habilidades que habilitas:

| Capability                 | Default | Otorga                                                            |
| -------------------------- | ------- | ----------------------------------------------------------------- |
| **Logging**                | On      | Escribir en la salida del hook.                                   |
| **Environment read**       | On      | Leer variables de entorno.                                        |
| **Connection metadata**    | On      | Leer la metadata del perfil que se está conectando.               |
| **Controlled process run** | Off     | Llamar a `dbflux.process.run(...)` para lanzar procesos externos. |

> Habilitar **Controlled process run** permite que el hook ejecute comandos
> externos arbitrarios. DBFlux muestra una advertencia de seguridad cuando está
> activado, tanto en la definición del hook como en el binding por conexión.
> Habilítalo solo para hooks en los que confíes.

El runtime de Lua embebido (APIs disponibles, sandboxing) está documentado en
[Lua Scripting](LUA.md).

### Opciones de Hook

| Option                         | Notas                                                                                                       |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **Enabled**                    | Los hooks deshabilitados se omiten.                                                                         |
| **Working Directory**          | El cwd del process/script (no usado por Lua).                                                               |
| **Environment**                | Pares extra `KEY=value`.                                                                                    |
| **Inherit parent environment** | On por defecto; pasa el env de DBFlux al hook.                                                              |
| **Env Denylist**               | Nombres de variables a quitar del env heredado.                                                             |
| **Timeout (ms)**               | En blanco = sin timeout. Al hacer timeout se mata el process group.                                         |
| **Execution mode**             | **Blocking** (default) espera al hook; **Detached** corre en segundo plano y no bloquea connect/disconnect. |
| **Ready signal** (Detached)    | Texto que DBFlux espera en la salida del hook antes de continuar.                                           |
| **On Failure**                 | La failure policy — ver abajo.                                                                              |

DBFlux siempre inyecta variables de entorno de contexto en los hooks de proceso:
`DBFLUX_PROFILE_ID`, `DBFLUX_PROFILE_NAME`, `DBFLUX_DB_KIND`, y, cuando se
conocen, `DBFLUX_HOST`, `DBFLUX_PORT`, `DBFLUX_DATABASE`.

> **Los secrets nunca se filtran accidentalmente a los hooks.** Además de tu Env
> Denylist, DBFlux siempre quita las variables heredadas cuyo nombre contiene
> `SECRET`, `TOKEN`, `PASSWORD`, o `_KEY`, y cualquier variable `AWS_*`.

### Políticas de fallo

Qué pasa cuando un hook falla (exit distinto de cero, timeout, o error):

| Policy                   | Efecto                                                        |
| ------------------------ | ------------------------------------------------------------- |
| **Disconnect** (default) | Aborta la fase — el flujo de connect o disconnect se detiene. |
| **Warn**                 | Continúa, pero muestra una advertencia.                       |
| **Ignore**               | Continúa; el fallo solo se registra en el log.                |

### Fases

| Phase               | Corre                             |
| ------------------- | --------------------------------- |
| **Pre-connect**     | Antes de que la conexión se abra. |
| **Post-connect**    | Después de un connect exitoso.    |
| **Pre-disconnect**  | Antes de desconectar.             |
| **Post-disconnect** | Después de desconectar.           |

La pestaña Hooks de una conexión tiene un dropdown por fase (más un input
"Extra" para vincular hook IDs adicionales). Los dropdowns listan los hooks
reutilizables que definiste en Settings → Hooks. Cada hook corre como su propia
background task con stdout/stderr en vivo en el panel Tasks; la salida tiene un
tope de 4 MiB por hook.

---

## Relacionado

- [Usage Guide](USAGE.md) — flujo principal y referencia de teclado.
- [Connecting → Advanced Setup](CONNECTIONS.md) — SSH, proxy, auth, fuentes de
  valores.
- [Data & Privacy](DATA_AND_PRIVACY.md) — dónde se almacenan los settings y
  secrets.
- [Lua Scripting](LUA.md) — el runtime de Lua embebido para hooks.
