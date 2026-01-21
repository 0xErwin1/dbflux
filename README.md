# DBFlux

A fast, keyboard-first database client built with Rust and GPUI.

## Overview

DBFlux is an open-source database client written in Rust, built with GPUI. It focuses on performance, a clean UX, and keyboard-first workflows.
The long-term goal is to provide a fully open-source alternative to DBeaver, supporting both relational and non-relational databases.

## MVP (v0.1)

- Connection profiles (PostgreSQL, SQLite)
- Read-only schema explorer (tables/views, columns, indexes)
- SQL editor with:
  - query execution
  - result pagination
  - query history + favorites
  - cancellation (PostgreSQL)
- Export data to CSV
- SSH tunneling for remote connections (via OpenSSH port-forwarding)
- Keyboard-first navigation + command palette
