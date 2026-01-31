# AGENTS.md — DBFlux

Guidelines for AI agents working in this Rust/GPUI codebase.

## Project Overview

DBFlux is a keyboard-first database client built with Rust and GPUI (Zed's UI framework).

**Workspace structure:**
```
crates/
├── dbflux/                    # App + UI (GPUI)
├── dbflux_core/               # Traits, types, errors (stable API)
├── dbflux_driver_postgres/    # PostgreSQL driver
├── dbflux_driver_sqlite/      # SQLite driver
├── dbflux_driver_mysql/       # MySQL/MariaDB driver
├── dbflux_ssh/                # SSH tunnel support
└── dbflux_export/             # CSV export
```

## Build & Run Commands

```bash
cargo check --workspace              # Fast type checking
cargo build -p dbflux --features sqlite,postgres,mysql  # Debug build
cargo build -p dbflux --features sqlite,postgres,mysql --release  # Release build
cargo run -p dbflux --features sqlite,postgres,mysql    # Run app
cargo fmt --all                      # Format
cargo clippy --workspace -- -D warnings  # Lint
cargo test --workspace               # All tests
cargo test --workspace test_name     # Single test
cargo test -p dbflux_core            # Tests in specific crate

# Nix
nix develop                          # Enter dev shell
nix build                            # Build package
nix run                              # Run directly
```

## Rust Guidelines

### General Principles

- Prioritize correctness and clarity over speed
- Do not write comments that summarize code; only explain non-obvious "why"
- Prefer implementing in existing files unless it's a new logical component
- Avoid creating many small files
- Avoid creative additions unless explicitly requested
- Use full words for variable names (no abbreviations like "q" for "queue")

### Error Handling

- Avoid `unwrap()` and functions that panic; use `?` to propagate errors
- Be careful with indexing operations that may panic on out-of-bounds
- Never silently discard errors with `let _ =` on fallible operations:
  - Propagate with `?` when the caller should handle them
  - Use `.log_err()` when ignoring but wanting visibility
  - Use `match` or `if let Err(...)` for custom logic
- Ensure async errors propagate to UI so users get meaningful feedback

### File Organization

- Never create `mod.rs` files; use `src/some_module.rs` instead
- When creating crates, specify library root in `Cargo.toml` with `[lib] path = "..."`

### Async Patterns

Use variable shadowing to scope clones in async contexts:
```rust
executor.spawn({
    let task_ran = task_ran.clone();
    async move {
        *task_ran.borrow_mut() = true;
    }
});
```

### Performance Patterns

**Pre-compute expensive operations**: Move string formatting and allocation into constructors rather than during rendering:
```rust
// Good: Format once during construction
CellValue::Text { display: format!("{}", value), ... }

// Bad: Format on every render
fn render(&self) { format!("{}", self.value) }
```

**Lazy loading for large datasets**: Drivers should return shallow metadata initially and fetch details on-demand:
```rust
fn get_tables(&self) -> Vec<TableInfo> // Names only
fn table_details(&self, name: &str) -> TableDetails // Columns, indexes
```

## GPUI Guidelines

### Context Types

- `App` — root context for global state and entity access
- `Context<T>` — provided when updating `Entity<T>`, derefs to `App`
- `AsyncApp` / `AsyncWindowContext` — from `cx.spawn`, can cross await points
- `Window` — window state, passed before `cx` when present

### Entity Operations

With `thing: Entity<T>`:
- `thing.read(cx)` → `&T`
- `thing.update(cx, |thing, cx| ...)` → mutate with `Context<T>`
- `thing.update_in(cx, |thing, window, cx| ...)` → also provides `Window`

Use the inner `cx` inside closures, not the outer one, to avoid multiple borrows.

### Concurrency

All entity/UI work happens on the foreground thread.

```rust
// Background work + foreground update
let task = cx.background_executor().spawn(async move {
    // expensive work
});

cx.spawn(async move |_this, cx| {
    let result = task.await;
    cx.update(|cx| {
        entity.update(cx, |state, cx| {
            state.pending_result = Some(result);
            cx.notify();
        });
    }).ok();
}).detach();
```

Task handling:
- Await in another async context
- `task.detach()` or `task.detach_and_log_err(cx)` for fire-and-forget
- Store in a field if work should cancel when struct drops

### Rendering

Types implement `Render` for element trees with flexbox layout:
```rust
impl Render for MyComponent {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().border_1().child("Hello")
    }
}
```

- Use `.when(condition, |this| ...)` for conditional attributes/children
- Use `.when_some(option, |this, value| ...)` for Option-based conditionals
- Call `cx.notify()` when state changes affect rendering

### Entity Updates in Render

Use `pending_*` fields with `.take()` to safely update other entities:
```rust
fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
    if let Some(data) = self.pending_data.take() {
        self.other_entity.update(cx, |other, cx| {
            other.apply(data, window, cx);
        });
    }
    // render UI...
}
```

### Input & Actions

Event handlers: `.on_click(cx.listener(|this, event, window, cx| ...))`

Actions defined with `actions!(namespace, [SomeAction])` macro or `#[derive(Action)]`.

### Keyboard & Mouse Patterns

**Focus tracking**: Use `.track_focus(&focus_handle)` on container elements to receive key events:
```rust
div()
    .track_focus(&self.focus_handle)
    .on_key_down(cx.listener(|this, event, window, cx| { ... }))
    .child(content)
```

**Mouse/keyboard sync**: When a component supports both mouse and keyboard navigation, sync state on mouse events:
```rust
.on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
    this.focus_mode = FocusMode::SomeMode;
    this.edit_state = EditState::Editing;
    cx.notify();
}))
```

**Input blur race condition**: When switching between inputs via click, the old input's `Blur` event fires after the new input's `mousedown`. Use a flag to prevent focus theft:
```rust
// In mousedown handler
this.switching_input = true;

// In blur handler / exit_edit_mode
if self.switching_input {
    self.switching_input = false;
    return;
}
```

**Focus state machines**: For complex focus scenarios (e.g., toolbar with editable inputs), use explicit state enums:
```rust
enum FocusMode { Table, Toolbar }
enum EditState { Navigating, Editing }
```

### Subscriptions

```rust
cx.subscribe(other_entity, |this, other_entity, event, cx| ...)
```
Returns `Subscription`; store in `_subscriptions: Vec<Subscription>` field.

### Deprecated Types (NEVER use)

- `Model<T>`, `View<T>` → use `Entity<T>`
- `AppContext` → use `App`
- `ModelContext<T>` → use `Context<T>`
- `WindowContext`, `ViewContext<T>` → use `Window` + `Context<T>`

## Architecture Rules

### Crate Boundaries

- `dbflux_core`: Pure types/traits, no DB-specific code
- `dbflux_driver_*`: Implement `DbDriver` and `Connection` traits
- `dbflux`: UI only, drivers via feature flags

### Adding a New Driver

1. Create `crates/dbflux_driver_<name>/`
2. Implement `DbDriver` and `Connection` from `dbflux_core`
3. Add feature flag in `crates/dbflux/Cargo.toml`
4. Register in `AppState::new()` under `#[cfg(feature = "name")]`

### Document System Pattern

Documents follow a consistent pattern for tab-based UI:

1. **Handle**: `DocumentHandle` wraps the entity and provides metadata
2. **State**: Document struct implements `Render` with internal focus management
3. **Tabs**: SqlQueryDocument supports multiple result tabs with `TabManager`
4. **Focus**: Documents receive `FocusTarget::Document` and manage internal focus

## Common Pitfalls

1. Forgetting `cx.notify()` after state changes
2. Blocking UI thread — use `background_executor().spawn()` for DB ops
3. Entity updates in render loops — guard with `.take()`
4. Missing feature gates on driver code
5. Creating closures per cell in tables — use row-level handlers with hit-testing instead
6. Canvas re-rendering every frame — cache scroll state and only sync on meaningful changes

## Key Files

| File | Purpose |
|------|---------|
| `crates/dbflux/src/app.rs` | AppState, driver registry |
| `crates/dbflux/src/ui/workspace.rs` | Main layout, command dispatch |
| `crates/dbflux/src/ui/dock/sidebar_dock.rs` | Collapsible, resizable sidebar |
| `crates/dbflux/src/ui/sidebar.rs` | Schema tree with lazy loading |
| `crates/dbflux/src/ui/document/mod.rs` | Document system exports |
| `crates/dbflux/src/ui/document/sql_query.rs` | SQL query document with multiple result tabs |
| `crates/dbflux/src/ui/document/tab_manager.rs` | MRU tab ordering |
| `crates/dbflux/src/ui/toast.rs` | Toast notification system |
| `crates/dbflux/src/ui/components/data_table/table.rs` | Virtualized data table with phantom scroller |
| `crates/dbflux/src/keymap/defaults.rs` | Key bindings per context |
| `crates/dbflux/src/keymap/command.rs` | Command enum and dispatch |
| `crates/dbflux/src/keymap/focus.rs` | FocusTarget (Document/Sidebar/BackgroundTasks) |
| `crates/dbflux_core/src/traits.rs` | `DbDriver`, `Connection` traits |
| `crates/dbflux_core/src/schema.rs` | Schema types with lazy loading support |
