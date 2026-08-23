import type { Dictionary } from './index';

export const es: Dictionary = {
  nav: {
    features: 'Funciones',
    drivers: 'Drivers',
    docs: 'Documentación',
    about: 'Acerca de',
    github: 'GitHub',
    download: 'Descargar',
    menu: 'Menú',
    switch_locale: 'English',
  },
  footer: {
    product: 'Producto',
    features: 'Funciones',
    drivers: 'Drivers',
    releases: 'Versiones',
    docs: 'Documentación',
    usage: 'Guía de uso',
    connecting: 'Conexión',
    mcp: 'IA + MCP',
    project: 'Proyecto',
    about: 'Acerca de',
    contributing: 'Cómo contribuir',
    source: 'Código fuente',
    tagline: 'Una alternativa totalmente de código abierto a DBeaver, desarrollada en abierto.',
    license: 'MIT o Apache-2.0, a tu elección.',
  },
  search: {
    placeholder: 'Busca en la documentación',
    move: 'mover',
    open: 'abrir',
    close: 'cerrar',
    no_results: 'Ninguna página coincide con “{query}”.',
    unavailable: 'La búsqueda no está disponible en este momento.',
    result_count_one: '{n} resultado',
    result_count_other: '{n} resultados',
  },
  versions: {
    label: 'Versión',
    index_tag_title: 'Esta página no existe en esa versión',
    index_tag: 'índice',
    default_tag: 'predeterminada',
  },
  docs_tree: {
    search_cta: 'Busca en la documentación',
    rail_toggle: 'Menú de documentación',
    on_this_page: 'En esta página',
    crumb_docs: 'Documentación',
    crumb_overview: 'Resumen',
    edit_page: 'Editar esta página',
    report_issue: 'Reportar un problema',
    not_translated: 'Esta página aún no está traducida. Se muestra la versión en inglés.',
    view_in_english: 'Ver en inglés',
  },
  docs_index: {
    title: 'Documentación',
    intro:
      'Cada página aquí se genera a partir del markdown del directorio <code>docs/</code> del repositorio, así que un cambio de comportamiento y el párrafo que lo describe se publican en el mismo commit.',
    unfiled_title: 'Sin clasificar',
    unfiled_body:
      'Estas páginas existen en esta versión pero no tienen un lugar en el orden de lectura declarado en <code>src/data/nav.ts</code>.',
  },
  landing: {
    title: 'Todas las bases de datos que usas, en una sola ventana controlada por teclado.',
    lede: 'Una plataforma de datos extensible y centrada en el teclado. Doce drivers integrados, un núcleo neutral respecto al driver, y lo que necesites además a través del protocolo RPC de drivers.',
    download_linux: 'Descargar para Linux',
    download_macos: 'Descargar para macOS',
    download_windows: 'Descargar para Windows',
    view_source: 'Ver código fuente',
    platforms_meta: 'Linux · macOS · Windows — MIT o Apache-2.0',
    hero_caption: 'Servidor principal — SELECT * FROM public.transactions (1.5s)',
    hero_alt:
      'DBFlux con un árbol de conexión abierto, mostrando bases de datos, esquemas, rutinas y métricas de instancia de un servidor PostgreSQL.',
    drivers_eyebrow: 'Drivers integrados',
    drivers_link: 'Matriz de capacidades →',
    drivers_note:
      'Los almacenes relacionales, de documentos, clave-valor, de series temporales y de objetos comparten una misma cuadrícula de resultados, un motor de gráficos y un registro de auditoría. Los drivers externos se registran mediante el protocolo RPC sin necesidad de un fork.',
    features_eyebrow: 'Qué obtienes',
    feature: {
      editor: {
        title: 'Editor consciente del dialecto',
        body: 'El autocompletado, la validación y la detección de sentencias peligrosas provienen del driver, no de una suposición compartida. Un DELETE sin WHERE se detecta antes de ejecutarse.',
      },
      grid: {
        title: 'Cuadrícula de resultados editable',
        body: 'Edita celdas directamente cuando el resultado se corresponde con una sola tabla, navega por millones de filas mediante keyset, y copia cualquier selección como una query nativa.',
      },
      charts: {
        title: 'Gráficos y dashboards',
        body: 'Convierte cualquier resultado en un gráfico, guárdalo y fíjalo en un dashboard junto a las métricas de instancia de la misma conexión.',
      },
      hooks: {
        title: 'Hooks de conexión',
        body: 'Ejecuta un comando, un script o Lua en proceso alrededor de la conexión y desconexión, con salida en vivo en el panel de tareas y una política de fallo que eliges.',
      },
      reach: {
        title: 'Llega a todo',
        body: 'Los túneles SSH, los proxies HTTP y AWS SSO son de primera clase. Los secretos viven en el keyring del sistema operativo, nunca en el archivo de perfil.',
      },
      audit: {
        title: 'Auditable por defecto',
        body: 'Las queries, hooks, scripts y llamadas MCP escriben en el mismo registro de eventos, con redacción y retención que tú controlas.',
      },
    },
    keyboard_eyebrow: 'Basado en teclado',
    keyboard_title: 'El ratón es opcional, no obligatorio.',
    keyboard_body:
      'Cada superficie tiene un atajo y una entrada en la paleta de comandos: abrir una conexión, ejecutar una sentencia, saltar a una tabla, convertir un resultado en gráfico. El estado vacío te muestra los cuatro que necesitas el primer día.',
    keyboard_link: 'Referencia completa de teclado →',
    shortcut: {
      new_query: 'nueva query',
      command_palette: 'paleta de comandos',
      open_script: 'abrir script desde disco',
      new_connection: 'nueva conexión',
    },
    governance_eyebrow: 'Gobernanza',
    governance_title: 'Dale a un cliente de IA una conexión, no tu base de datos.',
    governance_body:
      'El servidor MCP clasifica cada operación — metadata, lectura, escritura, destructiva, administrativa — y un motor de políticas decide por rol y por conexión. Las llamadas de escritura y destructivas pueden requerir aprobación humana.',
    audit_eyebrow: 'Auditoría',
    audit_title: 'Cada query, hook y llamada de herramienta, registrada.',
    audit_body:
      'Los eventos se registran en un log local de SQLite con categoría, severidad, actor y resultado. El texto de la query se guarda como huella, no en claro; los valores sensibles se redactan, y el log completo se exporta a JSON o CSV.',
    docs_eyebrow: 'Documentación',
    docs_link: 'Todas las guías →',
    doc_card: {
      usage: {
        title: 'Guía de uso',
        body: 'Primer arranque, creación de una conexión, ejecución de queries, exploración de resultados, gráficos y exportación.',
      },
      connecting: {
        title: 'Conexión',
        body: 'Túneles SSH, proxies, AWS SSO y fuentes de valores para todo lo que no sea un host y puerto simples.',
      },
      mcp: {
        title: 'IA + MCP',
        body: 'Conecta un cliente de IA a DBFlux y define los roles, políticas y aprobaciones que lo mantienen dentro de los límites.',
      },
    },
  },
  install: {
    all_downloads: 'Todas las descargas →',
    copy: 'copiar',
    copied: 'copiado',
    copy_fallback: 'usa ctrl+c',
    hint: {
      tarball:
        '¿Prefieres no usar sudo? Añade -s -- --prefix ~/.local para instalar en tu directorio home.',
      aur: 'Funciona cualquier ayudante de AUR. yay -S dbflux es equivalente.',
      deb: 'Cambia amd64 por arm64 en máquinas ARM. El .rpm se instala igual con dnf.',
      appimage: 'Totalmente portable. No se escribe nada fuera de tu directorio home.',
      nix: 'El paquete por defecto es un binario precompilado. Usa #dbflux-source para compilar desde el código fuente.',
      dmg: 'El build no está firmado con un certificado de desarrollador de Apple. Para omitir el diálogo: xattr -cr /Applications/DBFlux.app. Requiere macOS 11 Big Sur o posterior.',
      installer:
        'El ejecutable no está firmado con un certificado de firma de código de Windows. Requiere Windows 10 o posterior en x86_64; ARM64 aún no es compatible.',
      portable: 'No se instala nada y no se escribe nada fuera de la carpeta donde extraes.',
    },
    steps: {
      dmg: [
        'Descarga dbflux-macos-arm64.dmg para Apple Silicon, o dbflux-macos-amd64.dmg para Intel.',
        'Abre el DMG y arrastra DBFlux a Aplicaciones.',
        'En el aviso de "desarrollador no identificado", ve a Ajustes del Sistema → Privacidad y Seguridad y pulsa Abrir de todos modos.',
      ],
      installer: [
        'Descarga dbflux-windows-amd64-setup.exe.',
        'Ejecútalo y sigue el asistente.',
        'Si SmartScreen avisa, elige Más información → Ejecutar de todos modos.',
      ],
      portable: [
        'Descarga dbflux-windows-amd64.zip.',
        'Extráelo donde quieras.',
        'Ejecuta dbflux.exe.',
      ],
    },
  },
  about: {
    page_title: 'Acerca de DBFlux',
    page_description:
      'Por qué existe DBFlux, los principios detrás de él, y cómo está construido el código.',
    h1: 'Por qué existe DBFlux',
    intro_p1:
      'Todo cliente de bases de datos termina por pedirte que elijas un bando: el nativo rápido que habla un único motor, o el universal que habla todos ellos y te hace esperar. DBFlux toma la tercera opción — un núcleo neutral respecto al driver, drivers que se conectan a él, y una interfaz que nunca aprende el nombre de ninguno de ellos.',
    intro_p2:
      'Esa restricción se aplica en el código, no en una guía de estilo. La interfaz se adapta mediante flags de capacidades y metadatos, de modo que un almacén de documentos obtiene una vista de documentos y una fuente de series temporales obtiene un selector de rango sin una sola condición sobre el nombre del driver. Añadir una base de datos es escribir un driver, no parchear la aplicación.',
    intro_p3:
      'El objetivo a largo plazo se declara sin rodeos en el README: una alternativa totalmente de código abierto a DBeaver. Rust y GPUI son lo que lo mantienen lo bastante rápido como para valer la pena cambiarse.',
    principles_eyebrow: 'Principios',
    principle: {
      p01: {
        title: 'El teclado antes que el puntero',
        body: 'Si una acción existe, tiene un atajo y una entrada en la paleta de comandos. El ratón es un respaldo, y ningún flujo de trabajo depende de él.',
      },
      p02: {
        title: 'La interfaz nunca conoce el nombre de un driver',
        body: 'La categoría, el lenguaje de consulta y los flags de capacidades deciden qué se renderiza. Un driver que necesita un comportamiento nuevo añade un punto de extensión al núcleo en lugar de un caso especial a la interfaz.',
      },
      p03: {
        title: 'Denso antes que decorativo',
        body: 'Esquinas cuadradas, bordes finos, un solo color de acento, monoespaciado en todas partes. El espacio en pantalla pertenece a tus datos.',
      },
      p04: {
        title: 'Nada se ejecuta sin quedar registrado',
        body: 'Las queries, hooks, scripts y llamadas de herramientas de IA escriben todas en el mismo registro de auditoría, redactado por defecto y solo tuyo — nunca sale de la máquina.',
      },
    },
    layers_eyebrow: 'Cómo está construido',
    layer: {
      ui: {
        detail: 'Seis crates, cero dependencias de driver y cero feature flags por driver.',
      },
      app: {
        detail: 'Registra drivers, resuelve servicios RPC, gestiona el estado de conexión.',
      },
      core: {
        detail:
          'DbDriver, Connection, capabilities, metadata, language services, query generators.',
      },
      drivers: {
        detail:
          'Doce integrados como crates de Rust; cualquier otro a través del protocolo RPC de drivers.',
      },
    },
    muted_links:
      'El mapa completo de crates y los flujos entre crates viven en la guía de arquitectura. Si quieres escribir un driver, empieza por la guía de autoría de drivers.',
    maintainer_title: 'Mantenedor',
    maintainer_body:
      'Ignacio Perez, desarrollador backend y de sistemas que trabaja en Rust y C. DBFlux es suyo, y la gran mayoría de sus commits también.',
    contribute_title: 'Contribuir',
    contribute_body:
      'Se aceptan issues, drivers y documentación. La guía de contribución cubre las verificaciones que debe pasar un pull request antes de la revisión.',
    contribute_link: 'Lee la guía de contribución →',
  },
  notfound: {
    title: 'Esa página no existe.',
    lede: 'Puede que haya sido renombrada, o que pertenezca a una versión de DBFlux distinta a la que estabas leyendo.',
    docs_button: 'Documentación',
    home_button: 'Inicio',
    versions_label: 'Documentación por versión:',
  },
  banner: {
    skip_link: 'Saltar al contenido',
  },
};
