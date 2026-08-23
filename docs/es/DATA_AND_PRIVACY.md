# Datos y privacidad

Dónde almacena DBFlux tus datos, cómo protege tus credenciales, qué guarda el
audit log y cómo hacer un backup o un reseteo completo.

## De un vistazo

| Tus datos | Dónde viven |
|-----------|----------------|
| Perfiles de conexión, settings, historial, saved charts/queries, audit log | Un único archivo SQLite: `dbflux.db` en el directorio de datos |
| Pestañas abiertas / sesión | El mismo `dbflux.db`, más archivos scratch en el directorio de datos |
| Contraseñas, passphrases, secretos de API | Tu **keyring del sistema operativo** — nunca en `dbflux.db` |
| Token de auth de IPC/MCP | Un archivo `0600` en el directorio de configuración |

DBFlux mantiene casi todo en una única base de datos SQLite. Los secretos son
la excepción deliberada: van al keyring del sistema operativo, y la base de
datos solo almacena una *referencia* a ellos.

---

## Ubicaciones de datos

DBFlux usa los directorios estándar de tu plataforma.

| Plataforma | Directorio de datos | Directorio de configuración |
|----------|----------------|------------------|
| **Linux** | `~/.local/share/dbflux/` | `~/.config/dbflux/` |
| **macOS** | `~/Library/Application Support/dbflux/` | `~/Library/Application Support/dbflux/` |
| **Windows** | `%APPDATA%\dbflux\` | `%APPDATA%\dbflux\` |

El directorio de datos contiene:

- **`dbflux.db`** — la base de datos unificada (todo lo de más abajo en
  [Qué hay en la base de datos](#qué-hay-en-la-base-de-datos)).
- **`st_sessions/`** — archivos scratch/shadow para las pestañas de editor
  abiertas.
- **`ipc_auth_token`** — el token de auth de IPC/MCP (ver
  [más abajo](#token-de-auth-de-ipcmcp)).
- **`ssh_known_hosts`** — claves de host SSH aceptadas (TOFU).

DBFlux ya no usa el directorio de configuración. Versiones antiguas
almacenaban ahí el token de auth de IPC y los known-hosts de SSH; pueden
quedar archivos residuales tras actualizar y se pueden eliminar.

### Stable vs. Nightly

Un build Nightly usa un archivo de base de datos separado,
`dbflux-nightly.db`, así que una migración de pre-lanzamiento nunca puede
tocar tus datos stable. Los builds Stable y release candidate usan ambos
`dbflux.db`.

Puedes hacer que un build Nightly comparta la base de datos stable mediante
**Settings → General → Storage → Use the stable database** (aplica en el
siguiente arranque). Internamente esto solo crea un archivo marcador vacío
`use-stable-db` en el directorio de datos.

---

## Qué hay en la base de datos

`dbflux.db` es un único archivo SQLite. Sus tablas se agrupan por prefijo:

| Prefijo | Contiene |
|--------|----------|
| `cfg_*` | Configuración: perfiles de conexión, perfiles de auth/proxy/túnel SSH, servicios RPC, hooks de conexión, gobernanza MCP, y los settings de General/Audit. (Los *valores* de los secretos **no** están aquí — solo referencias al keyring.) |
| `st_*` | Estado del workbench: sesiones/pestañas abiertas, **historial de queries** (texto completo de la query), saved queries, elementos recientes, caché de schema, estado de la UI. |
| `aud_*` | El audit log y los filtros de auditoría guardados. |
| `viz_*` | Saved charts y dashboards. |
| `qry_*` | Saved queries del Visual Query Builder. |
| `sys_*` | Interno: versión de migración del schema, metadatos de la app. |

> **Nota sobre el historial de queries.** El historial de queries del
> workbench (`st_*`) almacena el **texto completo** de las queries que
> ejecutas, en claro. Esto es distinto del audit log, que por defecto
> convierte el texto de la query en fingerprint (ver abajo). Si no quieres
> que se retenga el texto de las queries, reduce **Max history entries** en
> Settings → General, o borra el historial desde la vista de historial del
> editor.

---

## Secretos y el keyring del sistema operativo

Las contraseñas, passphrases SSH, credenciales de proxy y secretos de
provider se almacenan en el keyring de tu sistema operativo, **no** en
`dbflux.db`.

| Plataforma | Backend de keyring |
|----------|-----------------|
| **Linux** | Secret Service (GNOME Keyring / KWallet, a través de libsecret) |
| **macOS** | Keychain |
| **Windows** | Windows Credential Manager |

Todas las entradas se almacenan bajo el nombre de servicio **`dbflux`**. La
base de datos solo guarda un string de referencia por secreto:

| Secreto | Referencia |
|--------|-----------|
| Contraseña de conexión | `dbflux:conn:<profile-id>` |
| Contraseña/passphrase SSH inline | `dbflux:ssh:<profile-id>` |
| Túnel SSH guardado | `dbflux:ssh_tunnel:<tunnel-id>` |
| Credencial de proxy | `dbflux:proxy:<proxy-id>` |
| Campo de auth profile | `dbflux:auth:<profile-id>:<field>` (uno por campo) |

### Cuándo se guardan los secretos (y cuándo no)

- La contraseña de una conexión solo se almacena cuando marcas **Save
  password**; los secretos de SSH y proxy solo cuando marcas su casilla
  **Save**.
- Si no hay ningún keyring disponible, DBFlux oculta las casillas **Save** y
  no persiste secretos — tendrás que reintroducirlos cada sesión.
- Un keyring *bloqueado* sigue contando como disponible: las escrituras
  pueden fallar hasta que lo desbloquees, pero DBFlux mantiene el soporte de
  secretos habilitado.

---

## Restauración de sesión y pestañas

Qué pestañas tienes abiertas — su tipo, rutas de archivo, orden, pestaña
activa y estado de pin — se registra en `dbflux.db` (`st_sessions` /
`st_session_tabs`). El contenido real de los archivos scratch/shadow vive
junto a ella bajo `st_sessions/` en el directorio de datos. Al arrancar,
DBFlux restaura esta sesión cuando **Settings → General → Restore session on
startup** está activado (el valor por defecto).

---

## Auditoría y privacidad

DBFlux registra operaciones significativas (queries, conexiones, hooks,
scripts, cambios de configuración, decisiones de MCP/gobernanza) en el audit
log dentro de `dbflux.db`. Está diseñado para preservar la privacidad por
defecto:

| Comportamiento | Por defecto | Efecto |
|----------|---------|--------|
| **Capture query text** | Desactivado | El texto de la query se reemplaza por un **fingerprint** SHA-256 más su longitud — el texto completo nunca se almacena en la fila de auditoría. |
| **Redact sensitive values** | Activado | Los patrones sensibles (claves de AWS, JWTs, connection strings con credenciales, etc.) se reemplazan por `[REDACTED]`. |
| **Detail size cap** | 64 KiB | Los payloads de evento sobredimensionados se truncan a un pequeño envelope parcial. |

Las **claves JSON** sensibles (`password`, `token`, `secret`, `api_key`,
`access_key`, `session_token`, `connection_string`, `url`, …) siempre se
redactan — incluso si desactivas la redacción basada en patrones.

> Recuerda la [salvedad del historial de queries](#qué-hay-en-la-base-de-datos):
> el audit log convierte el texto de la query en fingerprint, pero el
> *historial del workbench* lo almacena completo. Son dos almacenes
> distintos.

Para el schema completo de eventos, las categorías y el visor, ver
[Audit](AUDIT.md) y
[Dashboards & Audit → Audit viewer](DASHBOARDS_AND_AUDIT.md#audit-viewer).

---

## Token de auth de IPC/MCP

DBFlux expone una superficie IPC local (usada por el servidor MCP y los
servicios RPC externos). Autentica a los llamantes con un token almacenado
en:

```
<data dir>/dbflux/ipc_auth_token
```

(en Linux, `~/.local/share/dbflux/ipc_auth_token`). Es un valor aleatorio que
se regenera en cada arranque, se escribe con permisos `0600` de solo el
propietario, y también se exporta a las variables de entorno
`DBFLUX_IPC_TOKEN`, `DBFLUX_DRIVER_IPC_TOKEN` y
`DBFLUX_AUTH_PROVIDER_IPC_TOKEN` para los procesos hijos.

Este token es **solo de identidad de proceso** — cualquier proceso local que
pueda leerlo puede conectarse. No expongas la superficie IPC/MCP más allá de
localhost sin una capa de autenticación adicional. Ver
[AI + MCP Integration](MCP_AI_INTEGRATION.md) para el modelo de confianza.

---

## Backup y reseteo

DBFlux no tiene un comando dedicado de backup/restore, pero como todo vive en
un único archivo, ambas operaciones son sencillas.

### Hacer un backup

Copia el único archivo de base de datos mientras DBFlux está cerrado:

```
~/.local/share/dbflux/dbflux.db        # Linux (ajusta según la plataforma)
```

Ese archivo contiene tus perfiles, historial, saved charts/queries y audit
log. Tus **secretos no están en él** — permanecen en el keyring del sistema
operativo — así que una base de datos copiada en otra máquina hará
referencia a entradas de keyring que no existen ahí hasta que reintroduzcas
los secretos.

### Reseteo completo

Para borrar los datos de DBFlux:

1. Elimina el **directorio de datos** (`~/.local/share/dbflux/` en Linux) —
   elimina la base de datos, los archivos de sesión, el token de auth de IPC
   y los known-hosts de SSH.
2. **Solo versiones antiguas:** elimina el directorio de configuración legado
   (`~/.config/dbflux/` en Linux) si todavía existe — las versiones actuales
   ya no lo usan.
3. **Borra las entradas del keyring manualmente.** Los secretos bajo el
   servicio `dbflux` permanecen en tu keyring del sistema operativo después
   de eliminar los directorios; elimínalos con la herramienta de keyring de
   tu plataforma si quieres un borrado completo.

> Eliminar el directorio de datos es irreversible. Haz un backup de
> `dbflux.db` primero si podrías querer recuperar tus perfiles o tu
> historial.

---

## Relacionado

- [Settings & Hooks](SETTINGS.md) — los controles de General/Audit/Storage referenciados aquí.
- [Connecting → Advanced Setup](CONNECTIONS.md) — dónde se introducen los secretos.
- [Audit](AUDIT.md) — el schema completo de eventos de auditoría y los detalles de redacción.
