---
title: Linux Installer Design
date: 2026-01-28
status: Draft
author: DBFlux Team
---

## Overview

This design defines a Linux installer for DBFlux that simplifies distribution to end users. The installer handles binary placement, desktop integration, and cleanup.

## Goals

- Provide a straightforward installation process for Linux users.
- Support common desktop environments with proper application integration.
- Enable clean uninstallation without leftover artifacts.
- Automate artifact generation and distribution via GitHub Actions.

## Installer Script

### Script Responsibilities

- Detect and validate the target Linux distribution.
- Verify system dependencies and installation requirements.
- Copy the DBFlux binary to an appropriate system location.
- Set appropriate file permissions on installed files.
- Handle installation errors gracefully with clear user messages.

### Installation Flow

The installer will check for root privileges using standard POSIX methods. It will validate the binary file exists and is executable before proceeding.

Directory placement follows the Filesystem Hierarchy Standard (FHS). The binary installs to `/usr/local/bin/` by default, with `/usr/bin/` as an alternative.

### Uninstaller Capabilities

- Remove the installed binary from the system path.
- Delete desktop entry files from standard locations.
- Clean up icon files from icon directories.
- Remove DBFlux configuration data directories.
- Report successful or failed removal to the user.

## Desktop Integration

### Desktop Entry File

The installer creates a `.desktop` file in `/usr/share/applications/` for system-wide access. This follows the XDG Desktop Entry Specification for Linux desktop environments.

The desktop entry defines application metadata including name, icon, and launch command. It specifies the window type as a normal GUI application.

### Icon Placement

Icons are installed to support multiple display densities and themes. This ensures proper rendering across different screen resolutions and desktop environments.

Icon directories follow the Freedesktop icon theme specification. The installer supports common pixel sizes for scalable vector and raster icons.

### Configuration Directories

User configuration lives in the XDG configuration home directory. This separates user settings from system files and supports multi-user systems.

Application data persists in the XDG data home directory. This location stores user-specific application data and caches.

### MIME Type Association

DBFlux may associate with specific file types like SQL files. This enables double-clicking query files to open them directly in the application.

## GitHub Actions Artifacts

### Build Workflow

The workflow compiles DBFlux for multiple Linux targets. It uses a build matrix to cover common architectures and distributions.

Build artifacts include the release binary and required libraries. All builds produce reproducible checksums for verification.

### Packaging Process

The packaging stage bundles the binary with the installer script. It creates a tar archive with proper directory structure.

Each artifact includes version information and build metadata. This helps users identify compatibility and release details.

### Asset Management

Uploaded assets include the installer package and standalone binary. Both are available from the GitHub releases page.

Checksum files verify download integrity. Users can validate files before installation to prevent corruption.

### Release Automation

Version tags trigger the release workflow automatically. This ensures consistent packaging for every release.

The workflow generates changelog snippets and release notes. These help users understand changes between versions.

## Security Considerations

- The installer requires root privileges for system-wide installation.
- All files are installed with restricted permissions (755 for binaries, 644 for data).
- The installer validates digital signatures if provided.
- Temporary files are cleaned up after installation completes.

## Future Enhancements

- Support for `.deb` and `.rpm` package formats.
- Automatic update mechanism for installed versions.
- Integration with system package managers where possible.
- GUI-based installer for users uncomfortable with terminals.
