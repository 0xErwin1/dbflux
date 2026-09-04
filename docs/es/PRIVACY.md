# Política de privacidad

DBFlux es una aplicación de escritorio local-first. No recopila, transmite ni
almacena información sobre ti ni sobre tu uso. El sitio web del proyecto no
usa cookies ni carga scripts de terceros. Este documento explica qué significa
eso en la práctica y nombra a los dos proveedores de infraestructura que ven
el tráfico en su camino hacia ti.

## Ruta rápida

1. La aplicación envía datos únicamente a los servidores de base de datos que
   configuras, y solo lo necesario para ejecutar las consultas que pides.
2. No hay telemetría, ni reporte de fallos, ni analítica de uso, ni cuenta de
   usuario. Nada llama a casa.
3. El sitio web y la documentación son páginas estáticas. No usan cookies, no
   ejecutan scripts de analítica y no guardan registro de visitantes
   individuales.

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

[Datos y privacidad](DATA_AND_PRIVACY.md) documenta los archivos que DBFlux
escribe, qué registra el log de auditoría, cómo se guardan los secretos y cómo
hacer copia de seguridad o reiniciar la aplicación por completo.

## El sitio web y la documentación

`dbflux.dev` y `docs.dbflux.dev` son sitios estáticos construidos desde este
repositorio. Estos sitios:

- no usan cookies, ni propias ni de terceros;
- no cargan scripts de analítica, publicidad ni seguimiento;
- sirven sus fuentes y recursos desde el mismo host, así que una visita no
  contacta con ningún otro dominio;
- no mantienen ningún registro por visitante en el servidor que el proyecto
  pueda leer.

La búsqueda de la documentación se ejecuta en tu navegador contra un archivo
de índice descargado del mismo host. La consulta nunca sale de la página.

## Proveedores de infraestructura

Dos servicios se sitúan entre el proyecto y tú. Ninguno se usa para
identificar visitantes individuales.

| Proveedor | Función | Qué ve |
|-----------|---------|--------|
| Cloudflare | Aloja el sitio web, la documentación y el endpoint MCP de documentación en `mcp.dbflux.dev`. | Cada petición HTTP a esos hosts, incluyendo la dirección IP y el user agent, como cualquier host. Cloudflare expone al proyecto recuentos de tráfico agregados. No expone registros por visitante, y el proyecto no ha activado ninguna función que lo haga. |
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
el título.
