# Vendored `gpui-pre-linux`

This directory is the complete published `gpui-pre-linux` 0.3.9 crate (Zed's `gpui_linux` at
`zed@93d7456`) with one local patch to the Wayland client.

- Upstream: [`gpui-pre-linux` 0.3.9 on crates.io](https://crates.io/crates/gpui-pre-linux/0.3.9), from the crates.io registry archive `gpui-pre-linux-0.3.9.crate`.
- License: Apache-2.0; see `LICENSE-APACHE` and the published `Cargo.toml`.
- Archive SHA-256: `e28213c3fde6f13e91ba0b74b90cbaadec9373e36a031eda98561d31059870f3` (the checksum the crates.io index records for 0.3.9).

The first commit that added this directory carries the published source unchanged, so
`git diff` against it shows the whole local delta.

## Local patch list

- `src/linux/wayland/client.rs`: a `wl_keyboard` `Keymap` event keeps the held modifiers.
  Upstream replaces the xkb state with `xkb::State::new`, which starts with no modifiers, and
  only a later `Modifiers` event sets them again. A compositor that switches between keyboards
  sends the new keymap while keys are held, and Hyprland then delivers the next key before any
  `Modifiers` event. With fcitx5 running, this happens every time focus leaves a text input
  with Ctrl+Shift held: the input method's virtual keyboard stops relaying keys and the
  physical keyboard's keymap comes back. The key was then resolved at the base level, so
  `Ctrl+Shift+6` arrived as `ctrl-6` instead of `ctrl-^` and the shortcut needed a second
  press. `keymap_state_keeping_modifiers` builds the new state and copies the depressed,
  latched and locked modifier masks and the locked layout from the previous one. The masks
  are copied bit for bit, which assumes both keymaps order their modifiers the same way, as
  keymaps compiled from xkeyboard-config do. Covered by
  `a_new_keymap_keeps_the_held_modifiers` (`cargo test -p gpui-pre-linux --lib`), which needs
  the xkeyboard-config data files.

## Refreshing

Replace this directory with the new published crate, re-apply the patch above, and bump
the `gpui-pre-platform` version in the root `Cargo.toml`. When a published `gpui-pre-linux`
keeps the modifiers on a keymap change itself, delete this directory and its
`[patch.crates-io]` entry.
