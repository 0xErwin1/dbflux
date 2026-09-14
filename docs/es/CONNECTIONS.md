# Conectar a una base de datos — Configuración avanzada

Esta guía cubre todo lo que hay en el Connection Manager más allá del formulario
básico de "host, port, user, password": llegar a una base de datos a través de
un túnel SSH, un proxy o AWS SSM; autenticarse con Auth Profiles gestionados por
un provider (AWS SSO); y obtener valores de campos individuales desde un secret
manager o parameter store en lugar de escribirlos directamente.

Para el flujo del día a día (crear una conexión, explorar el schema, ejecutar
queries) consulta la [Guía de uso](USAGE.md). Este documento continúa desde la
pestaña **Access** del Connection Manager y los selectores de fuente de valores.

---

## La pestaña Access: cómo llega DBFlux a la base de datos

Cada conexión usa exactamente **un** método de acceso, elegido en el desplegable
**Access Method**. Cambiar de método borra la configuración de los demás — una
conexión es Direct, SSH, Proxy o SSM, nunca una combinación.

| Método                  | Qué hace                                                                                                                                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Direct**              | Conecta directamente al host/port de la pestaña Main. Puede seguir resolviendo fuentes de valores por campo (ver [Fuentes de valores](#value-sources-secret-manager-parameter-store-auth-session)). |
| **SSH Tunnel**          | Abre un port-forward local a través de un host SSH, y conecta a través de él.                                                                                                                       |
| **Proxy**               | Enruta la conexión a través de un proxy SOCKS5 o HTTP/HTTPS.                                                                                                                                        |
| **SSM Port Forwarding** | Usa AWS Systems Manager para hacer port-forward a una instancia y conecta a través del túnel. Requiere el build feature `aws`.                                                                      |

### Qué ocurre cuando pulsas Connect

DBFlux ejecuta un pipeline pre-connect fijo antes de que el driver abra un
socket:

1. **Authenticating** — valida o refresca la sesión del Auth Profile
   seleccionado (aquí puede ocurrir un login por navegador de AWS SSO).
2. **Resolving values** — resuelve cada fuente de valores por campo (secret
   manager, parameter store, variable de entorno, campo de auth-session) y las
   aplica a la configuración.
3. **Opening access** — establece el túnel SSH, el proxy o la sesión SSM (o
   nada, en el caso de Direct).
4. **Driver connect + schema fetch** — el driver conecta y DBFlux carga el
   schema superficial.

Los **hooks** de conexión (si tienes alguno vinculado) se ejecutan en las fases
PreConnect, PostConnect, PreDisconnect y PostDisconnect alrededor de este
pipeline. Ver [Settings & Hooks](SETTINGS.md#connection-hooks).

---

## Túneles SSH

Puedes usar un túnel SSH de dos formas:

- **Referenciar un túnel guardado** — elige un perfil de túnel que gestiones de
  forma centralizada en **Settings → SSH Tunnels**. Recomendado cuando
  reutilizas el mismo bastion en varias conexiones.
- **Inline** — rellena los campos SSH directamente en la pestaña Access. Más
  adelante puedes pulsar **Save as tunnel** para convertirlo en un perfil
  reutilizable.

### Campos SSH

| Campo                            | Notas                                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Host** / **Port**              | El servidor SSH. El puerto suele ser `22`.                                                                         |
| **Username**                     | Usuario SSH.                                                                                                       |
| **Auth method**                  | **Private Key** o **Password**.                                                                                    |
| **Key path** (Private Key)       | Ruta a la clave privada. **Déjalo vacío para usar tu SSH agent o las claves por defecto** (`~/.ssh/id_rsa`, etc.). |
| **Key passphrase** (Private Key) | Opcional; se guarda en el keyring del sistema operativo al marcar **Save**.                                        |
| **Password** (Password auth)     | Se guarda en el keyring del sistema operativo al marcar **Save**.                                                  |

No existe una opción separada de "SSH agent" — la autenticación basada en agente
es lo que obtienes al elegir **Private Key** y dejar la ruta de la clave vacía.

**Test SSH** verifica el túnel sin guardar la conexión.

### Dónde viven los secretos SSH

Las passphrases y contraseñas se guardan en el **keyring del sistema
operativo**, nunca en la base de datos. La casilla **Save** solo aparece cuando
hay un keyring disponible; si no lo hay, los secretos no se persisten y tendrás
que reintroducirlos cada sesión. Ver [Datos y privacidad →
Secretos](DATA_AND_PRIVACY.md#secrets-and-the-os-keyring).

---

## Proxies

Los proxies se gestionan en **Settings → Proxies**; la pestaña Access solo
*selecciona* un proxy guardado y muestra sus detalles. Si no tienes ninguno, la
pestaña te enlaza a Settings.

| Campo               | Notas                                                                                                                                                                                                              |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Type**            | `SOCKS5`, `HTTP` o `HTTPS`. Puerto por defecto: `1080` para SOCKS5, `8080` para HTTP/HTTPS.                                                                                                                        |
| **Host** / **Port** | El endpoint del proxy.                                                                                                                                                                                             |
| **Auth**            | `None`, o `Basic` con un usuario (la contraseña se guarda en el keyring).                                                                                                                                          |
| **No Proxy**        | Hosts/patrones separados por comas para omitir el proxy. Soporta `*` (todos), hosts exactos y coincidencias de sufijo (con o sin punto inicial), sin distinguir mayúsculas/minúsculas. **No soporta rangos CIDR.** |
| **Enabled**         | Cuando un perfil de proxy está deshabilitado, la conexión recurre a una conexión **directa** (con un aviso) en lugar de fallar.                                                                                    |

> **Aviso:** un proxy deshabilitado, o un host remoto que coincide con **No
> Proxy**, resulta en una conexión directa silenciosa. Si esperabas que el
> tráfico pasara por el proxy y no fue así, revisa primero estos dos casos.

---

## Auth Profiles (AWS SSO y credenciales compartidas)

Los Auth Profiles contienen autenticación gestionada por un provider que DBFlux
resuelve en el momento de conectar. Se crean en **Settings → Auth Profiles** y
se seleccionan por conexión. En este build los providers integrados son **solo
AWS**:

| Provider                   | Para qué se usa                                                                                          |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| **AWS SSO**                | Login de IAM Identity Center (SSO) que resuelve una cuenta + rol.                                        |
| **AWS SSO Session**        | Una sesión SSO reutilizable (Start URL + región + scopes) de la que pueden heredar los perfiles AWS SSO. |
| **AWS Shared Credentials** | Un perfil con nombre escrito en `~/.aws/credentials` (access key / secret / session token opcional).     |

Los providers de auth RPC registrados externamente pueden añadir más entradas
aquí; ver [RPC Services](RPC_SERVICES_CONFIG.md).

> Los perfiles AWS que DBFlux refleja en vivo desde tu `~/.aws/config` aparecen
> como **de solo lectura** — puedes seleccionarlos pero no editarlos aquí; edita
> los archivos de AWS directamente.

### Crear un perfil AWS SSO

El formulario está guiado por el provider. Para AWS SSO rellenas:

| Campo             | Notas                                                                                                                                                 |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Profile name**  | p. ej. `dev`.                                                                                                                                         |
| **SSO session**   | Referencia opcional a un perfil **AWS SSO Session**. Al fijarla, el Start URL y la región se heredan y sus campos inline se deshabilitan visualmente. |
| **SSO Start URL** | La URL de tu portal de Identity Center (omítela si usas una sesión).                                                                                  |
| **Region**        | p. ej. `us-east-1`.                                                                                                                                   |
| **Account**       | Un desplegable que se rellena **después de iniciar sesión** — lista las cuentas a las que tu sesión SSO tiene acceso.                                 |
| **Role**          | Un desplegable que se rellena una vez elegida una cuenta.                                                                                             |

Los desplegables **Account** y **Role** son dinámicos: requieren una sesión SSO
activa y se refrescan cuando cambian sus dependencias. Si están vacíos, inicia
sesión primero (ver abajo).

El **SSO Wizard** ofrece el mismo flujo como un creador guiado, paso a paso:
introduce nombre, Start URL y región; te inicia sesión y luego lista las cuentas
y roles para que elijas.

### El flujo de login SSO

Cuando una conexión (o los desplegables Account/Role) necesita una sesión SSO,
DBFlux abre un modal de login:

- **Abre tu navegador automáticamente** en la URL de verificación.
- Si el navegador no se puede abrir, el modal muestra la URL con una acción
  **Copy URL** para que la abras manualmente.
- DBFlux continúa automáticamente en cuanto terminas de autenticarte en el
  navegador.
- **El login SSO expira a los 5 minutos.**

### Seleccionar un Auth Profile por conexión

- **Modo Direct** — el Auth Profile es *opcional*. Se usa solo para resolver
  fuentes de valores Secret/Parameter/Auth (siguiente sección).
- **Modo SSM** — el Auth Profile es **obligatorio**.
- Si algún campo usa una fuente de valores Secret/Parameter cuyo provider es un
  auth provider, se requiere un Auth Profile correspondiente o la conexión se
  rechaza antes de conectar.

Cada fila de perfil tiene botones **Manage**, **Login** y **Refresh**. **Login**
solo está habilitado cuando el perfil seleccionado realmente necesita iniciar
sesión.

---

## SSM Port Forwarding (acceso gestionado)

El acceso "gestionado" permite que un provider abra el camino hacia el host por
ti. La implementación incluida es **AWS SSM Port Forwarding** (requiere el
feature `aws`).

| Campo            | Notas                                                               |
| ---------------- | ------------------------------------------------------------------- |
| **Instance ID**  | Instancia EC2 de destino. Soporta un selector de fuente de valores. |
| **Region**       | Por defecto `us-east-1` si se deja en blanco.                       |
| **Remote Port**  | El puerto de la instancia al que se hace el forward.                |
| **Auth Profile** | **Obligatorio** — el perfil AWS usado para iniciar la sesión SSM.   |

El puerto **local** del túnel lo asigna automáticamente DBFlux y el sistema
operativo — solo el puerto remoto es configurable.

---

## Fuentes de valores: Secret Manager, Parameter Store, Auth Session

Cualquier campo individual de una conexión (host, password, etc.) puede obtener
su valor desde una fuente externa en lugar de un literal. Haz clic en el
selector de fuente junto a un campo y elige:

| Fuente                   | Qué hace                                                           |
| ------------------------ | ------------------------------------------------------------------ |
| **Literal**              | El valor que escribes (por defecto).                               |
| **Environment Variable** | Se lee de una variable de entorno por nombre.                      |
| **Secret Manager**       | Se obtiene de un secret provider (AWS Secrets Manager).            |
| **Parameter Store**      | Se obtiene de un parameter provider (AWS SSM Parameter Store).     |
| **Auth Session Field**   | Toma un campo de la sesión/credenciales resuelta del Auth Profile. |

Notas:

- Las fuentes Secret/Parameter pueden apuntar a una **clave JSON** dentro de un
  secreto JSON, de forma que un único documento JSON almacenado puede alimentar
  varios campos.
- Los valores resueltos se cachean durante **5 minutos** para evitar volver a
  obtenerlos en cada reconexión.
- Las fuentes Secret/Parameter respaldadas por un auth provider **requieren un
  Auth Profile** en la conexión (se valida antes de conectar).

---

## Modo formulario vs. URI directa

La mayoría de los drivers relacionales te permiten indicar los detalles de
conexión como campos individuales o como una única cadena de conexión. Un
interruptor **Use URI** en la pestaña Main alterna entre ambos.

- El modo URI está disponible para **PostgreSQL, MySQL/MariaDB, SQL Server,
  MongoDB y Redis**. SQLite, DynamoDB, CloudWatch e InfluxDB usan sus propios
  formularios basados en campos.
- Con el modo URI **activado**, el campo único de URI es la fuente de verdad y
  los campos individuales se ignoran; con él **desactivado**, se usan los
  campos.
- Una contraseña embebida en una URI se extrae y se guarda por separado (en el
  keyring al guardarla), no se conserva en el texto de la URI.
- Al conectar a través de un túnel SSH/proxy/SSM, DBFlux siempre usa la
  configuración basada en campos (reescrita a `127.0.0.1:<local port>`), incluso
  si escribiste una URI.

---

## Referencia rápida: problemas frecuentes

- **Un método de acceso por conexión.** Cambiar de método borra los demás.
- **Los secretos viven en el keyring del sistema operativo**, nunca en la base
  de datos de DBFlux. Si el keyring no está disponible, las casillas "Save"
  desaparecen y los secretos no se conservan.
- **Un proxy deshabilitado o una coincidencia en No-Proxy conecta directamente
  de forma silenciosa.**
- **`No Proxy` no soporta CIDR** — lista hosts/sufijos, no rangos de IP.
- **SSM y las fuentes de valores respaldadas por auth requieren ambas un Auth
  Profile.**
- **Solo los providers de auth de AWS están integrados** (SSO, SSO Session,
  Shared Credentials). Otros providers vienen de servicios RPC externos.
- **El login SSO abre el navegador automáticamente y expira a los 5 minutos.**

## Relacionado

- [Guía de uso](USAGE.md) — el flujo básico de conectar/consultar/resultados.
- [Settings & Hooks](SETTINGS.md) — gestión de perfiles SSH/proxy/auth y hooks.
- [Datos y privacidad](DATA_AND_PRIVACY.md) — dónde se almacenan las
  credenciales y los datos.
- [RPC Services](RPC_SERVICES_CONFIG.md) — drivers externos y auth providers.
