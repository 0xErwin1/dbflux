# Vendored `gpui-pre-linux`

This directory is the complete published `gpui-pre-linux` 0.3.9 crate (Zed's `gpui_linux` at
`zed@93d7456`) with local patches to the Wayland and X11 clients, listed below.

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

- `src/linux/x11/xdnd_source.rs`, `src/linux/x11/xdnd_drag.rs`, `src/linux/x11/client.rs`,
  `src/linux/x11/window.rs`, `src/linux/file_uri_list.rs`: an X11 window can drag files out to
  other applications. Upstream implements only the XDND target side, so
  `can_start_external_drag` stayed false and a drag past the window edge never left the
  window. The window now owns `XdndSelection`, grabs the master pointer (and the keyboard, so
  Escape cancels), follows the pointer down the window tree to the first window with
  `XdndAware` (through `XdndProxy`, speaking the lower of its version and 5), and sends
  `XdndEnter`/`XdndPosition`/`XdndLeave`/`XdndDrop`. It serves `text/uri-list` (and `TARGETS`)
  for `XdndSelection`, encoded as on Wayland (`file_uri_list` moved to a module both
  backends share). The protocol rules are `SourceSession`, a state machine that does no I/O:
  one `XdndPosition` in flight until its `XdndStatus`, the status rectangle, a drop only when
  the last status accepted (otherwise `XdndLeave`), `XdndFinished` or a 10 s timeout ending
  the session, and exactly one `FileDropEvent::Ended` per session. Pointer and key events of
  the dragging window are consumed while the session runs. Covered by `xdnd_source::tests`
  and `file_uri_list::tests`, plus `xdnd_drag::tests`, which find targets on a real X server
  named by `GPUI_X11_TEST_DISPLAY` (for example a scratch Xvfb) and skip without it.

- `src/linux/x11/client.rs`, `src/linux/x11/window.rs`: the X11 target side refuses a drop that
  nothing would take. Upstream answered every `XdndPosition` with `XdndStatus` accept and the
  proposed action, so a source showed an accepting cursor over elements that ignore the
  files. The status now follows `DispatchEventResult::external_drop_accepted` (the `gpui-pre`
  patch `external-drop-acceptance.patch`): accept with `XdndActionCopy`, or no accept and
  action `None`. The answer depends on the dragged paths, so the first status of a drag waits
  for them to arrive (`status_pending`) and is sent once `FileDropEvent::Entered` has been
  dispatched; later ones are sent after each `FileDropEvent::Pending`. A drag that offers no
  type this backend reads, or whose paths cannot be read, is refused. A window that does not
  report an answer (blocked, or no input callback) still accepts, as before. Wayland still
  accepts the offer when it arrives. Covered by `linux::external_drop_tests`
  (`cargo test -p gpui-pre-linux --lib`).

- `src/linux/wayland/drag_source.rs`, `src/linux/wayland/client.rs`,
  `src/linux/wayland/clipboard.rs`: dropping files on another application finishes promptly.
  Upstream ended the app's drag only on `dnd_finished` or `cancelled`, so a target that never
  finishes (an older `wl_data_offer` version has no `finish`, and some targets finish late)
  left the window in a drag, with the pointer stuck in drag state, until the source went away.
  `DragSession` now ends the drag for the app (`FileDropEvent::Ended`) on `dnd_drop_performed`,
  keeps the data source alive so the target can still read the data, and releases it on
  `dnd_finished`, `cancelled` or after 10 s; `Ended` is sent exactly once per session. Events
  of a source a newer drag replaced are ignored (it is destroyed on `cancelled`), and starting
  a drag releases the previous source. `send_bytes` now writes to the target's pipe at once
  (making it non-blocking) and closes it, so the target reads the data and the end of file
  without waiting for another pass of the event loop; only what a full pipe cannot take is
  left to the loop, and a failed registration is logged instead of panicking. The clipboard
  shares `send_bytes`. Covered by `drag_source::tests` and `clipboard::tests`.

- `src/linux/wayland/drag_icon.rs`, `src/linux/wayland/client.rs`,
  `src/linux/wayland/window.rs`, `src/linux/x11/xdnd_drag.rs`, `src/linux/x11/xdnd_source.rs`,
  `src/linux/x11/client.rs`: a drag that leaves the window keeps a picture at the pointer.
  Upstream passed no icon to `wl_data_device.start_drag`, so the compositor drew nothing once
  the pointer left the window. A `wl_shm` icon surface is now rendered on the CPU
  (`drag_icon::render`, no fonts or assets): a page or folder glyph (a folder only when every
  dragged entry is one), a second glyph stacked behind it and a count badge (`99+` past 99)
  when several entries are dragged. It is rendered at the source window's buffer scale, sits
  below and right of the pointer, and is destroyed with the data source. A failure to build it
  is logged and the drag goes on without an icon. X11 has no drag icon in XDND: the grabbed
  pointer shows the `copy` cursor while the target under it accepted the drop and
  `not-allowed` otherwise, switched by grabbing again with the new cursor. Covered by
  `drag_icon::tests`, `xdnd_source::tests::a_drop_is_accepted_only_after_the_target_says_so`
  and `client::tests::the_drag_icon_pixels_are_shared_through_a_file`.

- `src/linux/system_fonts.rs`, `src/linux/platform.rs`, `Cargo.toml`: the system fonts no
  longer block the first window. `CosmicTextSystem::new` filled its font database
  synchronously inside `LinuxCommon::new`, which parses every face fontconfig reports: 594
  faces on the reference machine, about half a second with a cold page cache, all of it
  before a window could be created. The text system is now built with
  `CosmicTextSystem::new_without_system_fonts` and `system_fonts` loads the database on a
  thread of its own, started the first time the app asks for the fonts through
  `Platform::take_system_font_receiver` (the `gpui-pre` patch `background-system-fonts.patch`,
  which installs them, rebuilds every line layout and draws again). Every distinct file behind
  the database's faces is memory-mapped and handed over as `Cow::Borrowed`, not read into
  owned memory: several desktops ship CJK families whose files add up to hundreds of
  megabytes, and copying them into anonymous memory would grow the resident set by the whole
  font set. The mappings are leaked, since a `'static` borrow of them outlives every owner and
  they are never unmapped; `fontdb` mapped these same files before this change and keeps its
  mappings alive the same way. A file that cannot be opened or mapped is logged and skipped,
  so one unreadable font does not cost the rest. The face list is deduplicated and ordered by
  path, so a file holding several faces is loaded once and the result is deterministic.
  Glyph fallback for faces only the system provides (CJK, emoji) therefore arrives shortly
  after the first frame, and a family the app itself added, such as DBFlux's bundled Archivo,
  resolves from the first frame. Covered by `system_fonts::tests`
  (`cargo test -p gpui-pre-linux --lib`): one test maps a directory of real TrueType files and
  checks that each file appears once with its own bytes, the other points `FONTCONFIG_FILE` at
  a scratch config and checks that the faces reached through it are mapped too. Both use the
  fonts in `crates/bolt/assets/fonts` as their fixture and fail if that directory is missing.
  The manifest gains `fontdb` (the version and features `cosmic-text` already uses) and
  `memmap2`.

- `src/linux/vulkan_driver.rs`, `src/gpui_linux.rs`, `src/linux/wayland/window.rs`,
  `src/linux/x11/window.rs`: the Vulkan loader is told which driver to use instead of probing
  every installed ICD. The loader reads its driver variables once, when the process starts,
  and they cannot be set safely afterwards, so `select_vulkan_driver` decides before any
  thread exists and re-executes the process with `exec`, the same `argv` and
  `VK_LOADER_DRIVERS_SELECT` set to the patterns of the firmware-booted GPU's vendor (read
  from `/sys/class/drm/card*/device/boot_vga`; Intel, AMD and NVIDIA each map to their ICD
  manifest names). It does nothing when the user already chose a driver through any of the
  five `VK_*` variables, when `GPUI_VULKAN_DRIVER_SELECT=0` opts out, when the marker is
  already present (no exec loop), or when the vendor has no installed manifest. A failed exec
  is logged and the app continues without a selection. Because a selected driver can turn out
  to be unusable — with no adapter at all and no OpenGL fallback — both window backends call
  `retry_after_renderer_failure` when the renderer cannot be created, which re-executes once
  with the selection removed and the marker set to `fallback`, so the next start probes every
  driver as before and never selects again. The rules are `decide`, a pure function over an
  injected environment and filesystem state. Covered by `vulkan_driver::tests` in this crate.
  Children do not inherit the selection: `restore_child_environment` prepares a
  `std::process::Command` so a child starts with the environment the user gave the app. It
  removes `VK_LOADER_DRIVERS_SELECT` and `GPUI_VULKAN_DRIVER_SELECT` when the marker holds the
  patterns this code chose, removes the marker alone when it is `fallback`, and changes nothing
  when the marker is absent or `0`, so a user's own driver variables are kept and an application
  launched from a GPUI app keeps its own GPU (a game started with PRIME offload does not run on
  the boot GPU). Apps must call it on every `Command` they prepare. The rule is the pure
  `child_environment`; covered by `vulkan_driver::tests`, one test per case.

## Refreshing

Replace this directory with the new published crate, re-apply the patches above, and bump
the `gpui-pre-platform` version in the root `Cargo.toml`. When a published `gpui-pre-linux`
keeps the modifiers on a keymap change itself, drop that patch; when it keeps drawing while
frame callbacks are withheld, drop that one; when it can start an X11 file drag, drop the XDND
source. With neither left, delete this directory and its
`[patch.crates-io]` entry.
