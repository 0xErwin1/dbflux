# Política de seguridad

## Reportar una vulnerabilidad

Reporta en privado a través de GitHub Security Advisories:

**https://github.com/0xErwin1/dbflux/security/advisories/new**

Por favor no abras un issue público para una vulnerabilidad sospechada. Un
reporte es más útil con la versión de DBFlux, la plataforma, el driver
involucrado cuando aplica, y el conjunto mínimo de pasos que la reproduce.

Los datos de contacto legibles por máquina se publican en
[`/.well-known/security.txt`](https://dbflux.dev/.well-known/security.txt).

## Versiones soportadas

Los fixes llegan a la release branch actual. DBFlux desarrolla sobre `main` y
corta una rama `release/vX.Y` por minor, que recibe fixes cherry-picked hasta
llegar a su fin de vida; los minors anteriores no. Ver [el proceso de
release](docs/RELEASE.md) para cómo funcionan las ramas y los canales.

Si estás en un minor anterior, la respuesta a un reporte de seguridad será
actualizar al actual.

## Limitaciones conocidas, por diseño

Estos son comportamientos documentados y no vulnerabilidades. Un reporte sobre
ellos es bienvenido como discusión de diseño, pero no se tratan como un riesgo
no divulgado.

- **La autenticación de MCP es solo identidad de proceso.** Presentar
  `--client-id` es la única señal de autenticación, así que cualquier proceso
  local que conozca el client id puede conectarse. No es una garantía
  criptográfica, y el servidor MCP no debería exponerse más allá de localhost
  sin una capa de autenticación adicional. Ver [integración de IA +
  MCP](docs/MCP_AI_INTEGRATION.md).
- **Los hooks de conexión y los scripts Lua ejecutan el código que
  configuraste.** Se ejecutan con los privilegios del proceso de DBFlux por
  diseño; eso es lo que hace un hook. Ver [ajustes y
  hooks](docs/SETTINGS.md) y [scripting con Lua](docs/LUA.md).
- **El log de auditoría es local.** Registra lo que ocurrió en esa máquina y
  es legible por cualquier cosa que pueda leer tu directorio de datos. Ver
  [datos y privacidad](docs/DATA_AND_PRIVACY.md).

## Dónde viven los secretos

Las credenciales se guardan en el keyring del sistema operativo, nunca en un
archivo de perfil de conexión, y el log de auditoría almacena un fingerprint
del texto de la query en lugar del texto en sí. [Datos y
privacidad](docs/DATA_AND_PRIVACY.md) describe qué se escribe dónde, y cómo
inspeccionarlo o eliminarlo.
