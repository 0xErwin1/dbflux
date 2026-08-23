# Instalar DBFlux

## Linux

### Tarball (recomendado)

```bash
# Instalar en /usr/local (requiere sudo)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | sudo bash

# Instalar en ~/.local (sin sudo)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --prefix ~/.local
```

### AppImage (portable)

```bash
# Descargar desde releases (reemplaza amd64 por arm64 para ARM)
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.AppImage
chmod +x dbflux-linux-amd64.AppImage
./dbflux-linux-amd64.AppImage
```

### Arch Linux

Disponible en el AUR:

```bash
# Usando un helper de AUR
paru -S dbflux
# o
yay -S dbflux
```

### Debian / Ubuntu

Descarga el paquete `.deb` desde [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Reemplaza amd64 por arm64 para ARM
wget https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.deb
sudo dpkg -i dbflux-linux-amd64.deb
```

### Fedora / RHEL / CentOS

Descarga el paquete `.rpm` desde [Releases](https://github.com/0xErwin1/dbflux/releases):

```bash
# Reemplaza amd64 por arm64 para ARM
sudo dnf install https://github.com/0xErwin1/dbflux/releases/latest/download/dbflux-linux-amd64.rpm
```

### Nix

Usando flakes (el paquete por defecto es un **binario prebuilt** para Linux x86_64 / aarch64, sin compilación):

```bash
# Ejecutar directamente (prebuilt)
nix run github:0xErwin1/dbflux

# Instalar en el perfil (prebuilt)
nix profile install github:0xErwin1/dbflux

# Shell de desarrollo
nix develop github:0xErwin1/dbflux
```

Compilar desde el código fuente en lugar de usar el binario prebuilt:

```bash
nix run    github:0xErwin1/dbflux#dbflux-source
nix build  github:0xErwin1/dbflux#dbflux-source
```

Los builds nightly siguen `main` y se instalan en paralelo con la versión estable (app id, icono y base de datos `dbflux-nightly.db` distintos). Consúmelos desde la referencia `nightly`:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

Consulta [docs/RELEASE.md](RELEASE.md) para el modelo de canales.

NixOS / nix-darwin vía overlay:

```nix
{
  inputs.dbflux.url = "github:0xErwin1/dbflux";

  outputs = { nixpkgs, dbflux, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        ({ pkgs, ... }: {
          nixpkgs.overlays = [ dbflux.overlays.default ];
          environment.systemPackages = [
            pkgs.dbflux         # binario prebuilt, sin compilación local
            # pkgs.dbflux-source  # alternativa: compilar desde el código fuente
          ];
        })
      ];
    };
  };
}
```

## macOS

DBFlux para macOS no está firmado con un certificado de desarrollador de Apple. Al abrirlo por primera vez, verás una advertencia sobre un "desarrollador no identificado".

### Instalación

1. Descarga el DMG para tu arquitectura desde [Releases](https://github.com/0xErwin1/dbflux/releases):
   - **Macs Intel**: `dbflux-macos-amd64.dmg`
   - **Apple Silicon (M1/M2/M3/M4)**: `dbflux-macos-arm64.dmg`
2. Abre el DMG y arrastra DBFlux a Applications
3. Cuando veas la advertencia de "desarrollador no identificado":
   - Ve a **Ajustes del Sistema → Privacidad y Seguridad**
   - Haz clic en **Abrir de todos modos** junto a la advertencia de seguridad
   - Confirma que quieres abrir la aplicación

### Saltar Gatekeeper desde la terminal

```bash
# Eliminar el atributo de cuarentena (permite abrir sin confirmación por GUI)
xattr -cr /Applications/DBFlux.app

# Ahora puedes abrirlo normalmente
open /Applications/DBFlux.app
```

### Requisitos

- macOS 11.0 (Big Sur) o posterior

## Windows

### Instalador

1. Descarga `dbflux-windows-amd64-setup.exe` desde [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Ejecuta el instalador y sigue el asistente

### Portable

1. Descarga `dbflux-windows-amd64.zip` desde [Releases](https://github.com/0xErwin1/dbflux/releases)
2. Extrae en cualquier carpeta
3. Ejecuta `dbflux.exe`

> **Nota**: El ejecutable no está firmado con un certificado de firma de código de Windows. Windows SmartScreen puede mostrar una advertencia. Haz clic en "Más información" → "Ejecutar de todas formas" para continuar.

### Requisitos

- Windows 10 o posterior
- x86_64 (ARM64 aún no soportado)

## Compilar desde el código fuente

```bash
# Vía script de instalación (Linux)
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/install.sh | bash -s -- --build

# O manualmente
git clone https://github.com/0xErwin1/dbflux.git
cd dbflux

# Recomendado: compilar con el conjunto completo de features por defecto
cargo build --release --features sqlite,postgres,mysql,mssql,mongodb,redis,dynamodb,cloudwatch,influxdb,clickhouse,lua,aws,mcp

# Build mínimo (solo drivers relacionales, sin AI/MCP, sin Lua)
cargo build --release --no-default-features --features sqlite,postgres,mysql

./target/release/dbflux
```

## Desinstalar (Linux)

```bash
# Si se instaló con install.sh
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | sudo bash

# Desde ~/.local
curl -fsSL https://raw.githubusercontent.com/0xErwin1/dbflux/main/scripts/uninstall.sh | bash -s -- --prefix ~/.local

# Eliminar también la configuración y los datos del usuario
./scripts/uninstall.sh --remove-config
```

## Próximos pasos

- [Guía de uso](USAGE.md) — primer inicio, creación de una conexión y tu primera consulta
- [Conectar — Configuración avanzada](CONNECTIONS.md) — túneles SSH, proxies, AWS SSO y fuentes de valores
