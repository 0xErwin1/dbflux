# Proceso de Release

DBFlux usa **desarrollo trunk-based con release branches de corta duración**. Un
branch de larga duración (`main`) es el objetivo de integración; un branch
`release/vX.Y` se crea por cada minor durante la estabilización y se descarta
después de su EOL.

Este documento es la referencia orientada a humanos. El skill automatizado
`dbflux-release` (`skills/dbflux-release/SKILL.md`) sigue estas mismas reglas.

## Canales

| Canal (branch de origen)    | Patrón de tag       | GitHub release                       |
| --------------------------- | ------------------- | ------------------------------------ |
| **nightly** (`main` HEAD)   | `nightly` (rolling) | prerelease, construido por cron diario |
| **rc** (`release/vX.Y`)     | `vX.Y.Z-rc.N`       | prerelease, construido con push del tag |
| **stable** (`release/vX.Y`) | `vX.Y.Z`            | published, construido con push del tag |

El canal `-dev.N` está **retirado**. Nightly lo reemplaza. Los tags `-dev.N`
antiguos permanecen en GitHub pero no se crean nuevos.

Los íconos de aplicación por canal se rastrean en el [issue
#183](https://github.com/0xErwin1/dbflux/issues/183). No los implementes aquí.

## Modelo de Changelog

Dos artefactos, cada uno con una única fuente de autoría:

- **El `CHANGELOG.md` de este repositorio es el changelog de referencia, escrito
  a mano.** Lleva la prosa curada y orientada al usuario de cada release.
- **El cuerpo del release de GitHub se genera** en CI con
  [git-cliff](https://git-cliff.org) a partir de commits convencionales, para
  todos los canales. `cliff.toml` en la raíz del repositorio lo configura, y
  nunca se edita a mano.

Un mismo commit alimenta ambos: su type convencional decide qué sección usan
las release notes generadas, y el bullet escrito bajo `## [Unreleased]` decide
lo que dice el changelog del repositorio. No son copias uno del otro. El cuerpo
del release es lacónico (subject, número de PR, sección); el changelog del
repositorio es prosa.

### Changelog del repositorio (`CHANGELOG.md`)

- Cada cambio visible para el usuario (`feat`, `fix`, `perf`) agrega su bullet
  bajo `## [Unreleased]`, en el mismo commit del cambio, bajo `### Added`,
  `### Fixed` o `### Changed`. El checklist de la plantilla de PR lo pide.
- `[Unreleased]` acumula el trabajo que se publica como el próximo **minor**.
  Los tags rc y nightly son transparentes: no lo cierran.
- En la promoción a stable el heading se renombra **una sola vez**, a
  `## [X.Y.0] - <date>`, en el release branch, en el mismo commit que el bump
  de versión (ver Promoción a Stable abajo). `main` abre un `[Unreleased]`
  fresco cuando la sección publicada se lleva de vuelta a él.
- **Los patches son la excepción:** una sección `## [X.Y.Z]` para un patch
  release se genera en el release branch con `git-cliff --prepend`. Las
  secciones de patches no se curan.
- Nunca ejecutes `git-cliff -o CHANGELOG.md`. Una regeneración completa colapsa
  todas las secciones históricas en un único rango desde el último tag stable.
  Anteponer, o editar a mano, es la única escritura segura.

> **Transición a v0.7.0:** las secciones de patches las genera git-cliff desde
> v0.7.x. Las secciones `## [0.6.0]` y `## [0.6.0-dev.N]` son baselines escritas
> a mano, comiteadas en `CHANGELOG.md`. Nunca deben regenerarse — hacerlo las
> duplicaría o colapsaría.

### Cuerpo del release (GitHub)

- Compuesto por `.github/workflows/release.yml` a partir de la salida de
  git-cliff. Un commit `feat`, `fix` o `perf` aparece; un commit `chore`, `ci`,
  `docs`, `test`, `refactor` o `style` se descarta. Los cambios relevantes de
  seguridad usan `fix(security):` o un footer `Security:`. Los breaking changes
  (`feat!:`, `fix!:`, o un footer `BREAKING CHANGE:`) siempre aparecen.
- El cuerpo stable cubre cada commit visible para el usuario desde el último
  tag **stable**. Los tags rc y nightly son transparentes (`skip_tags` en
  `cliff.toml`).
- Editar el cuerpo publicado en la UI de GitHub está permitido, para una
  introducción editorial o una corrección. No toca `CHANGELOG.md`.
- Un tag stable cuya versión no tiene una sección `## [X.Y.Z]` en
  `CHANGELOG.md` hace fallar el job de release en lugar de publicar.

## Branches

| Branch         | Duración   | Acepta                                         | Tags producidos                       |
| -------------- | ---------- | ---------------------------------------------- | ------------------------------------- |
| `main`         | permanente | cada commit nuevo (features, fixes, refactors) | (ninguno — nightly rolling)           |
| `release/vX.Y` | hasta EOL  | solo fixes cherry-picked (sin features nuevas) | `vX.Y.Z-rc.N`, `vX.Y.Z`, `vX.Y.(Z+1)` |

### Reglas inviolables

- Un commit **nunca** se autoriza directamente en un release branch. Siempre
  aterriza primero en `main`, y luego se hace `git cherry-pick -x <sha>` hacia
  el release branch. Los únicos commits que se escriben ahí son los propios
  bumps de version-artifact del release, que llevan el renombre del heading de
  `CHANGELOG.md`, y las secciones de patches generadas.
- Un release branch **nunca** se hace merge de vuelta a `main`.
- **Sin features nuevas** en un release branch una vez creado. Solo bugfixes y
  los propios bumps de version-artifact del release.
- `main` siempre está abierto para desarrollo. Cada cambio visible para el
  usuario agrega su bullet `[Unreleased]` en el mismo commit que hace el cambio.

## Tags

Los tags deben ser anotados:

```bash
git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
git push origin vX.Y.Z[-suffix.N]
```

El workflow de release (`.github/workflows/release.yml`) clasifica los tags
automáticamente:

| Patrón de tag (branch de origen permitido) | Tipo de GitHub release |
| ------------------------------------------ | ---------------------- |
| `vX.Y.Z-rc.N` desde `release/vX.Y`          | prerelease             |
| `vX.Y.Z` desde `release/vX.Y`               | stable (published)     |
| cualquier otro (red de seguridad)           | draft                  |

## Reglas de Versionado

La versión del workspace (`Cargo.toml` `[workspace.package].version`) es la
fuente de verdad. Todos los demás manifiestos deben mantenerse sincronizados.

**En `main`:**

La versión del manifiesto es `X.(Y+1).0-dev.0`, donde `X.Y` es el minor que se
está estabilizando actualmente en `release/vX.Y`. Este marcador se fija cuando
se crea `release/vX.Y` y permanece en `main` durante toda la ventana de
estabilización y más allá, hasta el siguiente corte. Es solo un marcador de
desarrollo — nunca se publican releases `-dev.N`. El workflow de nightly deriva
`X.(Y+1).0-nightly+<sha>` a partir de él, quitando el sufijo de pre-release y
agregando `-nightly+<short-sha>`.

**En `release/vX.Y`:**

- Próximo RC: si el último tag es `vX.Y.Z-rc.N` → `-rc.(N+1)`. Si no hay ninguno
  → `-rc.0`.
- Promover a stable: quitar el sufijo RC → `vX.Y.0`.
- Patch: incrementar `Z` → `vX.Y.(Z+1)`. Nunca subir el minor en un release
  branch.

## Ejemplo de Ciclo: `0.7.0`

1. Las features aterrizan en `main`, cada una agregando su bullet
   `## [Unreleased]` a `CHANGELOG.md` en el mismo commit.
2. Cuando está listo para estabilizar, se crea `release/v0.7` desde `main` HEAD.
   - En `release/v0.7`: subir cada artefacto versionado a `0.7.0-rc.0`. Commit y
     push.
   - En `main`: subir cada artefacto versionado a `0.8.0-dev.0`. Commit y push.
     `main` ahora apunta al siguiente minor.
   - Tag `v0.7.0-rc.0` en el release branch. git-cliff renderiza el rango
     unreleased como el cuerpo del RC automáticamente.
3. Se encuentra un bug durante el RC:
   - Commitear el fix en `main`.
   - `git cherry-pick -x <sha>` hacia `release/v0.7`.
   - Subir a `v0.7.0-rc.1` y taggear.
4. Cuando está limpio, subir el release branch de `v0.7.0-rc.N` a `v0.7.0` y
   renombrar el heading superior de `CHANGELOG.md` a `## [0.7.0] - <date>` en
   ese mismo commit. Tag `v0.7.0`. git-cliff renderiza el rango completo desde
   `v0.6.0` como el cuerpo del release stable.
5. `main` ya está en `0.8.0-dev.0` — no se necesita más bump después de stable.
   Un commit cierra la sección `[Unreleased]` publicada y abre una nueva encima.
6. Los patches (`v0.7.1`, `v0.7.2`, …) vienen del mismo release branch vía
   cherry-picks desde `main`, y cada uno antepone a `CHANGELOG.md` una sección
   `## [0.7.N]` generada.

## Identidad y Firma de Artefactos

Tres cosas distintas llevan la señal de "quién construyó esto", y no se
solapan:

| Capa | Qué cubre | Secret | Si falta |
|------|-----------|--------|----------|
| Firma GPG separada (`.asc`) y `.sha256` | El archivo descargado | `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` | El build falla. Toda release va firmada. |
| Attestation de procedencia del build | La ejecución del workflow y el commit que produjo el artefacto | ninguno (OIDC sin clave) | Siempre activa. |
| Firma de código macOS en el `.app` | La identidad de la aplicación que miran el llavero y Gatekeeper | `MACOS_CERTIFICATE_P12`, `MACOS_CERTIFICATE_PASSWORD` | El bundle se firma ad-hoc con un warning. |

Ni el ejecutable de Windows ni el instalador llevan firma Authenticode, así
que SmartScreen avisa en la primera ejecución. Eso necesita un certificado de
una CA y se sigue por separado.

### Firma de código en macOS

macOS ata un permiso del llavero a la firma de la aplicación que lo pidió.
Una firma ad-hoc cambia con cada build, así que sin una identidad estable cada
actualización obliga al usuario a introducir de nuevo su contraseña de login
antes de que DBFlux pueda leer las contraseñas de base de datos guardadas.
Firmar cada release con un mismo certificado hace que ese aviso aparezca una
sola vez.

Un certificado autofirmado basta para el llavero. **No** satisface a
Gatekeeper: la app sigue siendo "no identificada" en el primer arranque hasta
que el proyecto tenga un certificado Developer ID de pago y notarización, que
los mismos dos secrets llevarían.

Crea el certificado una vez, en cualquier máquina con OpenSSL:

```bash
scripts/macos-signing-cert.sh ~/secure/dbflux-signing
```

El script imprime los dos secrets del repositorio a configurar. Guarda el
`.p12` y su contraseña en un lugar duradero: emitir un certificado nuevo más
adelante hace que todos los usuarios vuelvan a autorizar el llavero una vez.
El certificado es válido diez años.

El bundle se firma en `build.yml` antes de crear el DMG. El job importa el
certificado en un llavero temporal, lo marca como de confianza para firma de
código en el runner, firma, verifica con `codesign --verify --deep --strict`
y borra el llavero.

### Identidad del ejecutable de Windows

`crates/dbflux/build.rs` embebe el icono del channel y un bloque
`VERSIONINFO` en `dbflux.exe` en tiempo de build, así que el Explorador, la
barra de tareas y el diálogo Abrir con muestran el icono de DBFlux y
Propiedades muestra el nombre del producto y la versión. Los iconos están
commiteados bajo `packaging/icons/` (`dbflux.ico`, `dbflux-nightly.ico`), y
los mismos archivos alimentan el zip portable y los accesos directos del
instalador. Regenéralos desde `resources/branding/<channel>/` cuando cambie
el arte:

```bash
magick -background none resources/branding/stable/mark.svg -resize 256x256 256.png
# ... 128, 64 desde mark.svg; 48, 32, 16 desde mark-small.svg
magick 16.png 32.png 48.png 64.png 128.png 256.png packaging/icons/dbflux.ico
```

Los archivos `.icns` de macOS que están al lado (`dbflux.icns`,
`dbflux-nightly.icns`) se construyen del mismo modo con `png2icns` de
`libicns`, añadiendo los tamaños 512 y 1024, y llegan al bundle como
`AppIcon.icns`.

## Procedimiento de Corte: `main` → `release/vX.Y`

1. Verifica que estás en `main`, árbol limpio, actualizado con `origin/main`.
2. Verifica que `.github/workflows/release.yml` en `main` contiene el job
   `Classify release`. Si falta, arréglalo en `main` primero — de lo contrario
   los tags stable se publicarán como drafts.
3. Crea el branch (usa un worktree dedicado si usas el layout de bare-repo para
   que `main` siga checked out):

   ```bash
   git worktree add ../release-vX.Y -b release/vX.Y main
   # o en un repo de checkout único:
   git checkout -b release/vX.Y
   ```

4. En `release/vX.Y`:
   - Sube cada artefacto versionado a `X.Y.0-rc.0` (ver [Archivos a
     Actualizar](#files-to-bump)).
   - No toques `CHANGELOG.md`. El branch lleva el bloque `[Unreleased]` que
     heredó de `main`, y los fixes cherry-picked dentro de él traen sus propios
     bullets. El heading se renombra en la promoción a stable, no aquí: un RC no
     es un release de referencia.
   - Commit: `chore(release): cut release/vX.Y at vX.Y.0-rc.0`.
   - Push: `git push -u origin release/vX.Y`.

5. De vuelta en `main`:
   - Sube cada artefacto versionado a `X.(Y+1).0-dev.0` (main ahora apunta al
     siguiente minor).
   - Commit: `chore(version): move main to X.(Y+1).0-dev.0 marker`.
   - Push.

6. Tag `vX.Y.0-rc.0` en el release branch.

El cuerpo del release RC se genera automáticamente a partir de commits
convencionales, así que un RC no necesita ningún paso de CHANGELOG.

## Promoción a Stable: `release/vX.Y` → `vX.Y.0`

Ejecuta esto en `release/vX.Y` cuando el RC está limpio:

1. Sube cada artefacto versionado de `X.Y.0-rc.N` a `X.Y.0`.
2. Cierra la sección de changelog: renombra el heading `## [Unreleased]`
   superior de `CHANGELOG.md` a `## [X.Y.0] - <date>`, con la fecha de hoy. Los
   bullets acumulados en `main` y traídos por los cherry-picks ya son el
   contenido de este release; nada se genera en el archivo aquí.

   ```text
   ## [Unreleased]     ->     ## [X.Y.0] - 2026-07-31
   ```

   > **Advertencia:** no antepongas una sección generada, y NO uses
   > `git-cliff -o CHANGELOG.md`. El changelog del repositorio es curado; el
   > cuerpo del release se genera por separado.

3. Commit: `chore(release): promote release/vX.Y to vX.Y.0`.
4. Tag `vX.Y.0` en el release branch y push del branch + tag.
5. CI genera el cuerpo del release stable a partir de todos los commits
   visibles para el usuario desde el último tag stable.

El workflow de release se niega a publicar un tag stable cuya versión no tiene
una sección `## [X.Y.Z]` en `CHANGELOG.md`, así que el paso 2 no puede saltarse
en silencio.

## Próximo Ciclo de Desarrollo

`main` se sube a `X.(Y+1).0-dev.0` **cuando se crea `release/vX.Y`** (ver
Procedimiento de Corte, paso 5). No se requiere más bump a `main` después del
tag stable. Los builds de nightly continúan desde `main` HEAD automáticamente,
produciendo `X.(Y+1).0-nightly+<sha>` durante toda la ventana de estabilización.

Una vez hecho push del tag stable, `main` recibe un commit que cierra la sección
publicada de la misma manera (renombrar `## [Unreleased]` a
`## [X.Y.0] - <date>`) y abre un `[Unreleased]` fresco encima, para que el
changelog del repositorio conserve el historial publicado. `7a13aceb` es un
ejemplo de ese commit. El trabajo que aterrizó en `main` después del corte y no
se publicó pertenece al nuevo `[Unreleased]`, no a la sección publicada; esa
separación es el único ajuste a mano del modelo.

## Archivos a Actualizar

Por cada release, actualiza todos los siguientes a exactamente la misma versión:

- `Cargo.toml` — `[workspace.package].version`. Los crates del workspace heredan
  vía `version.workspace = true`.
- `flake.nix`
- `resources/windows/installer.iss`
- Revisión manual (no hereda): `examples/custom_driver/Cargo.toml`.

Después de que se publican los artefactos del GitHub Release para el tag,
actualiza también:

- `nix/release-info.nix` — `version` + ambos `url`s y `hash`es del tarball
  prebuilt (ver [Nix](#nix-this-repos-flake) abajo). Este es un puntero de canal
  por branch. Requiere los artefactos publicados, así que aterriza como un
  commit de seguimiento una vez que el workflow de release termina.

El `PKGBUILD` de AUR vive en un **repositorio AUR externo**, no en este repo.
Solo se sube para tags stable.

## Cómo Funciona Nightly

`.github/workflows/nightly.yml` corre diariamente a las 03:17 UTC:

1. Lee la versión del workspace desde `Cargo.toml`, quita cualquier sufijo de
   pre-release existente, y agrega `-nightly+<short-sha>` (p. ej.
   `0.8.0-nightly+abc1234` cuando `main` lleva `0.8.0-dev.0`). No se requiere
   commit de `Cargo.toml`. Como `main` rastrea el **siguiente** minor desde el
   momento en que se crea `release/vX.Y`, la versión nightly siempre está
   claramente por delante de la línea en estabilización.
2. Llama a `build.yml` con `channel: nightly`.
3. Calcula el hash SRI SHA256 de cada tarball de Linux y regenera
   `nix/nightly-info.nix` con los hashes reales y las URLs de release rolling.
4. Commitea el `nix/nightly-info.nix` actualizado encima del `main` HEAD actual.
   Este commit **no se hace push a `main`** — se convierte en el único destino
   del tag `nightly`.
5. Fuerza el movimiento del tag `nightly` al commit de pin y hace push del tag.
   Hacer push del tag es suficiente para que el commit sea alcanzable en el
   remoto; no se requiere push de branch.
6. Publica o actualiza el GitHub prerelease rolling `nightly` con los nuevos
   artefactos y un cuerpo generado por git-cliff cubriendo los commits desde el
   último tag stable. El tag del release apunta al commit de pin, así que
   `nix/nightly-info.nix` en la ref `nightly` siempre coincide con los
   artefactos publicados.

El tag nightly se fuerza (force-push) y el release se reemplaza en cada corrida.
Solo el repositorio canónico (`0xErwin1/dbflux`) ejecuta el schedule.

**Se salta cuando `main` no ha avanzado.** Una corrida programada primero
compara el `main` HEAD actual contra el commit desde el que se construyó el
último nightly (`git rev-parse nightly^`, el primer padre del commit de pin). Si
coinciden, la corrida se salta por completo: sin rebuild, sin mover el tag, sin
churn de release. Esto evita republicar un build idéntico bajo un hash fresco no
reproducible que rompería innecesariamente los pins de Nix. Una corrida manual
`workflow_dispatch` siempre construye, incluso sin commits nuevos.

### Paquete Nix de Nightly

El workflow fija `nix/nightly-info.nix` en la ref `nightly` en cada corrida. Los
usuarios downstream obtienen el binario nightly prebuilt sin compilar desde el
código fuente:

```bash
# Ejecutar nightly directamente
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly

# Instalar en un perfil
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

Un nightly desde el código fuente (sin fijación de hash requerida) también
funciona:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-source
```

**No consumas `#dbflux-nightly` desde `main`.** En `main`,
`nix/nightly-info.nix` contiene hashes de placeholder que no van a fetch. Usa
siempre la ref `nightly` como se muestra arriba.

## Disciplina de Cherry-Pick

Un release branch nunca debería contener commits ausentes en `main`, excepto
commits exclusivos del release (`chore(release): ...`, `chore(version): ...`).

```bash
# En main: aterriza el fix.
git checkout main
# ...commit, push...

# En el release branch: cherry-pick con -x para registrar el SHA de origen.
git checkout release/vX.Y
git cherry-pick -x <sha>
```

Auditoría: cada commit que no sea de release en `release/vX.Y` desde el
branch-off debería mencionar `(cherry picked from commit ...)` en su mensaje.

```bash
git log --grep='cherry picked from' release/vX.Y
```

## Canales Downstream

| Tipo de tag (GitHub Release) | AUR         | Nix flake (este repo)                                      | nixpkgs (futuro) |
| ---------------------------- | ----------- | ---------------------------------------------------------- | ---------------- |
| nightly (prerelease)         | se omite    | auto-fijado — `#dbflux-nightly` en la ref nightly          | se omite         |
| `-rc.N` (prerelease)         | se omite    | actualiza el `release-info` de la release branch y de main | se omite         |
| Stable `vX.Y.Z` (published)  | bump + push | actualiza el `release-info` de la release branch y de main | bump + PR        |

### AUR

`pkgver` de AUR no permite `-` (reservado para `pkgrel`). Para releases stable
la traducción es un no-op (`pkgver=X.Y.Z`). Para hipotéticos prereleases de AUR:

- `vX.Y.Z-rc.N` → `pkgver=X.Y.Z.rc.N`

### Nix (el flake de este repo)

El flake expone varios paquetes en Linux (x86_64 y aarch64):

| Paquete            | Qué provee                                                                   |
| ------------------ | ---------------------------------------------------------------------------- |
| `dbflux` (default) | Binario stable/rc prebuilt cuando está disponible, source en caso contrario  |
| `dbflux-bin`       | Prebuilt explícito desde `nix/release-info.nix`                              |
| `dbflux-source`    | Build desde source vía crane (todas las plataformas)                         |
| `dbflux-nightly`   | Nightly rolling prebuilt desde `nix/nightly-info.nix` (usa la ref `nightly`) |

**Stable / RC (`nix/release-info.nix`):** puntero de canal por branch. `main`
rastrea el tag publicado más reciente de cualquier tipo; cada `release/vX.Y`
rastrea el más reciente de su propia línea. Después de que se publican los
artefactos de un tag, refresca `release-info.nix` en cada branch cuyo canal ese
tag hace avanzar.

```bash
ver=X.Y.Z
for arch in amd64 arm64; do
  hex=$(curl -fsSL "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256" | awk '{print $1}')
  nix-hash --to-sri --type sha256 "$hex"
done
```

Actualiza `version`, ambos `url`s, y ambos `hash`es en `nix/release-info.nix`.
Verifica localmente:

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

**Nightly (`nix/nightly-info.nix`):** auto-actualizado por el workflow de
nightly en la ref `nightly`. No actualices este archivo manualmente. Consúmelo
vía:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
```

### nixpkgs (futuro)

Aún no está en upstream. Cuando lo esté, solo los tags stable recibirán un PR a
`NixOS/nixpkgs`. Convención de título de PR: `dbflux: A -> B`.

## Anti-Patrones (evita esto)

- Taggear `vX.Y.Z` o `vX.Y.Z-rc.N` mientras HEAD está en `main`.
- Taggear un RC mientras HEAD está en `main`.
- Hacer merge de `release/vX.Y` de vuelta a `main`.
- Crear features nuevas (commits que no sean fix) en un branch `release/*`.
- Subir el minor o major dentro de un branch `release/*`.
- Hacer push de un tag sin un árbol de trabajo limpio.
- Hacer push del bump de AUR con `pkgver` conteniendo un guion.
- Cortar `release/vX.Y` desde un `main` HEAD que no contiene el job `Classify
  release` en `release.yml`.
- Crear tags `-dev.N` nuevos (el canal está retirado; usa nightly en su lugar).

## Validación Local Antes de Etiquetar

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Relacionado

- `.github/workflows/release.yml` — lógica de clasificación y publicación de
  artefactos
- `.github/workflows/nightly.yml` — build nightly diario
- `.github/workflows/build.yml` — jobs de build reutilizables (llamados por
  release y nightly)
- `.github/release-template.md` — sección de instalación agregada a cada cuerpo
  de release
- `cliff.toml` — configuración de git-cliff para la generación de changelog
- `skills/dbflux-release/SKILL.md` — skill orientado a agentes que automatiza
  este proceso
