# DBFlux v0.1.0 Release Notes

We're excited to announce the first release of DBFlux, a fast, keyboard-first database client built with Rust and GPUI.

## What is DBFlux?

DBFlux is an open-source database client focused on performance, clean UX, and keyboard-first workflows. Our long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

## Highlights

### Keyboard-First Design

DBFlux is built for developers who prefer to keep their hands on the keyboard. Navigate the entire application using Vim-style bindings:

- `j/k` to move up/down in lists and tables
- `h/l` to collapse/expand tree nodes or navigate columns
- `Tab/Shift+Tab` to cycle between panels
- `Ctrl+Shift+P` to open the command palette
- `Ctrl+Enter` to execute queries

### Database Support

This release includes drivers for:

- **PostgreSQL** - Full support including SSL/TLS and SSH tunneling
- **SQLite** - Direct file access for local databases

Both drivers support query cancellation, allowing you to stop long-running queries without restarting the application.

### Three-Panel Workspace

The interface is organized into three resizable panels:

1. **Sidebar** - Browse your connections and explore database schemas (tables, views, columns, indexes)
2. **Editor** - Write and execute SQL with syntax highlighting and multi-tab support
3. **Results** - View query results in a virtualized table with pagination

### SSH Tunnel Support

Connect securely to remote databases through SSH tunnels with support for:

- Private key authentication (with optional passphrase)
- Password authentication
- SSH agent integration
- Reusable tunnel profiles

### Query History and Saved Queries

Never lose a query again:

- Automatic history tracking with execution metadata
- Save frequently used queries with custom names
- Mark queries as favorites for quick access
- Search across your entire query history

## Quick Start

1. Launch DBFlux
2. Press `c` in the sidebar or use the command palette to open the Connection Manager
3. Select your database type and fill in the connection details
4. Click "Test Connection" to verify, then "Save"
5. Double-click your connection in the sidebar to connect
6. Start writing queries in the editor and press `Ctrl+Enter` to execute

## Default Keybindings

| Action | Shortcut |
|--------|----------|
| Command Palette | `Ctrl+Shift+P` |
| Run Query | `Ctrl+Enter` |
| New Tab | `Ctrl+N` |
| Close Tab | `Ctrl+W` |
| Next/Prev Tab | `Ctrl+Tab` / `Ctrl+Shift+Tab` |
| Cycle Panels | `Tab` / `Shift+Tab` |
| Cancel Query | `Escape` |
| Export Results | `Ctrl+E` (in Results) |
| Open History | `Ctrl+P` (in Editor) |

## What's Next

For future releases, we're planning:

- Additional export formats (JSON, SQL, Excel)
- Query autocompletion
- More database drivers (MySQL, MariaDB, MongoDB)
- Theme customization
- Query explain/analyze visualization

## Feedback

DBFlux is in active development. If you encounter issues or have suggestions, please open an issue on GitHub.

---

Thank you for trying DBFlux!
