# Vendored `gpui-pre-linux`

This directory is the complete published `gpui-pre-linux` 0.3.9 crate (Zed's `gpui_linux` at
`zed@93d7456`) with two local patches to the Wayland client.

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
  the xkeyboard-config data files. Reported upstream as
  [zed-industries/zed#65650](https://github.com/zed-industries/zed/issues/65650).

- `src/linux/wayland/window.rs`, `src/linux/wayland/client.rs`: a window keeps drawing while
  the compositor withholds frame callbacks. Upstream waits in `AwaitingCallback` after every
  present, ignores frame requests in that state, and leaves it only on a `wl_surface.frame`
  callback. Hyprland sends none to a surface on a hidden workspace, so a window there stopped
  drawing for good: GPUI never ran layout or paint again, and gpui-mcp saw `frame_pending()`
  stay true and inputs never settle. The frame-loop decisions now live in
  `FrameLoop::schedule` and `FrameLoop::complete`:
  - While the toplevel is `suspended` (reported as `WindowVisibility::Hidden`), a requested
    frame runs on the 60Hz retry timer, and `draw` skips the GPU present: GPUI has already
    laid out and painted the frame and updated the accessibility tree, and only the pixels
    wait. Presenting is skipped rather than attempted because the renderer asks for
    `Mailbox` but falls back to `Fifo`, whose swapchain acquire can wait for an image the
    compositor never releases, up to wgpu's 1 s acquire timeout per frame. A failed present
    is not retried while hidden, and showing the window again requests a redraw.
  - While visible, a frame requested during `AwaitingCallback` arms a 250 ms stall timer
    (`FRAME_CALLBACK_STALL_TIMEOUT`) and draws when it fires if no callback came first, for
    compositors that withhold callbacks without suspending the toplevel.
  - Once that stall timer fires with no callback since the last present, the surface is
    treated as not shown (`PresentMode::Stalled`), as under a lock screen such as hyprlock,
    which withholds callbacks without sending `suspended`. Requested frames then run on the
    retry timer with layout, paint and the accessibility tree, but skip the present, for the
    same swapchain reason as a hidden window. The next frame callback, configure or
    visibility change ends the stall and requests a redraw. A window that receives its
    callbacks never stalls and presents every frame as upstream does.
  - Entering the stall, when no callback is outstanding, the window requests a
    `wl_surface.frame` callback and commits without attaching a buffer, the same
    commit upstream uses to wait for a callback after a failed present. The surface keeps
    its current content and the swapchain is not touched, so the commit cannot block. The
    compositor's next repaint of the surface, as on unlock, fires that callback and ends the
    stall with a redraw even when no configure follows. Only one such callback is requested.
  - A window with nothing requested arms no timer, so an idle hidden window stays idle.
  - Each tick bumps a timer generation, and a timer armed before the latest tick is ignored,
    so a callback that arrives first, or a late callback after a timer draw, adds no draw.
  Covered by `frame_loop_tests` (`cargo test -p gpui-pre-linux --lib`).

## Refreshing

Replace this directory with the new published crate, re-apply the patches above, and bump
the `gpui-pre-platform` version in the root `Cargo.toml`. When a published `gpui-pre-linux`
keeps the modifiers on a keymap change itself, drop that patch; when it keeps drawing while
frame callbacks are withheld, drop that one. With neither left, delete this directory and its
`[patch.crates-io]` entry.
