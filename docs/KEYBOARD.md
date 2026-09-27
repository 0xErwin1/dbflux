# Keyboard Reference

DBFlux uses a layered, context-aware keymap. The active layer depends on which
panel has focus. Bindings written with the **primary** modifier use `Cmd` on
macOS and `Ctrl` on every other platform; bindings written with literal `Ctrl`
stay `Ctrl` on all platforms (to avoid clashing with macOS system shortcuts).

Every binding below can be changed in **Settings → Keybindings**: its keys (one
chord or a sequence such as `g g`) and the context it applies in. Keys written
with a space, such as `y y`, are pressed one after the other; after the first
key DBFlux waits up to a second for the next. While a dialog is open, the panels
behind it do not react to their keys.

Focus is shown only after you use the keyboard: the tint ring appears on the
focused control after a key press, stays while the pointer moves, and hides on
the next click. A focused button, checkbox or list row takes `Enter` and
`Space` itself.

## Global (available regardless of focus)

| Keys | Action |
|------|--------|
| `Ctrl+Shift+P` / `Cmd+Shift+P` | Toggle command palette |
| `Ctrl+Shift+N` / `Cmd+Shift+N` | Open connection manager |
| `Ctrl+n` / `Cmd+n` | New query tab |
| `Ctrl+w` / `Cmd+w` | Close tab |
| `Ctrl+Tab` / `Ctrl+Shift+Tab` | Next / previous tab |
| `Ctrl+1` .. `Ctrl+9` / `Cmd+1` .. `Cmd+9` | Switch to tab N |
| `Ctrl+o` / `Cmd+o` | Open script file |
| `Ctrl+Enter` / `Cmd+Enter` | Run query |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` | Run query in new tab |
| `Escape` | Cancel / close modal |
| `Tab` / `Shift+Tab` | Cycle focus forward / backward |
| `Ctrl+Shift+1` | Focus sidebar |
| `Ctrl+Shift+2` | Focus editor |
| `Ctrl+Shift+3` | Focus results |
| `Ctrl+Shift+4` | Focus background tasks |
| `Ctrl+Shift+A` / `Cmd+Shift+A` | Open audit viewer |
| `Ctrl+b` / `Cmd+b` | Toggle sidebar |
| `Ctrl+m` | Open tab context menu |

## Sidebar

| Keys | Action |
|------|--------|
| `q` / `e` | Switch sidebar tab (Connections / Scripts) |
| `/` | Focus search |
| `j` / `k` (or `Down` / `Up`) | Select next / previous |
| `h` / `l` | Collapse / expand node |
| `Space` | Expand / collapse |
| `g` / `Shift+g` (or `Home` / `End`) | First / last item |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `Enter` | Open / execute item |
| `r` | Refresh schema |
| `c` | Open connection manager |
| `d` | Disconnect |
| `m` | Open item menu |
| `Shift+j` / `Shift+k` | Extend selection down / up |
| `Space` (with Shift) | Toggle selection |
| `Ctrl+j` / `Ctrl+k` | Move selected item down / up |
| `Shift+r` | Rename |
| `x` | Delete |
| `Shift+n` | Create folder |
| `Ctrl+l` | Focus panel to the right |

## Editor

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `Alt+h` | Toggle history dropdown |
| `Ctrl+p` / `Cmd+p` | Open saved queries |
| `Ctrl+s` / `Cmd+s` | Save query |
| `Ctrl+Shift+s` / `Cmd+Shift+s` | Save file as |
| `Ctrl+/` / `Cmd+/` | Toggle line comment |
| `Enter` | Focus / execute |

(Unmodified letters are intentionally left to the text input so typing works.)

## Vim mode (opt-in)

Code editors can use modal editing with a small set of Vim commands. It is off
by default. Turn it on in **Settings → General → Editor → Vim mode in code
editors** and save: open editors switch over at once. It applies to every code
editor (SQL and the other query languages, Lua, Python, Bash) and to nothing
else, so search boxes, forms, and the command palette keep typing as usual.

An editor starts in Normal mode when it opens and when you turn Vim mode on. A
strip under the editor shows the mode: `NORMAL`, `INSERT`, `REPLACE`, `VISUAL`, `VISUAL LINE`, or `VISUAL BLOCK`. Each tab keeps its
own mode when you switch tabs or move focus away and back. The strip also shows
an incomplete key sequence, such as `2`, `2d3`, or `4g`. It clears when the
command completes or is interrupted, when focus leaves the editor, and on
`Escape` or `Tab`. It does not show command history or appear in the workspace
status bar.

| Mode | Keys | Action |
|------|------|--------|
| Normal | `h` / `l` | Move one character left / right within the line |
| Normal | `j` / `k` | Move one line down / up, keeping the column across shorter lines |
| Normal | `Enter` | Move one line down |
| Normal | `/` | Open the editor's find panel with its query field focused |
| Normal | `n` / `N` | Move to the next / previous match of the find panel's query (accepts a prefix count) |
| Normal | `m{a-z}` | Set or overwrite a lowercase local mark at the cursor |
| Normal | `'{a-z}` / `` `{a-z} `` | Jump to the marked line's first non-blank character / the exact marked position (clamped to a Normal-mode cursor) |
| Normal / Visual / Visual Line / Visual Block | `gg` / `G` / `Ngg` / `NG` | Go to the first / last / 1-based absolute logical line (clamped to the buffer); Visual extends the selection |
| Normal | `i` | Insert before the cursor |
| Normal | `a` / `A` / `I` | Insert after the cursor / at the end of the line / at the first non-blank character of the line |
| Normal | `e` / `w` / `b` | Move to the end of a word / start of the next word / start of the previous word |
| Normal | `E` / `W` / `B` | Make the corresponding word motion using whitespace-delimited words |
| Normal | `x` | Delete the character under the cursor |
| Normal | `r{char}` / `Nr{char}` | Replace the character under the cursor, or the next N characters on the line, with `{char}`; the cursor stays on the first replaced character |
| Normal | `R` | Enter Replace mode |
| Normal | `dd` / `yy` / `cc` | Delete / yank / change whole logical lines (`yy` copies to the system clipboard) |
| Normal | `c` + `h` / `l` / `j` / `k`, `w` / `W` / `e` / `E` / `b` / `B`, `gg` / `G` | Change a characterwise horizontal or word-motion range, or whole logical lines for vertical and absolute-line motions |
| Normal | `d` / `y` + `h` / `l` / `j` / `k` | Delete / yank a characterwise horizontal or linewise vertical motion (`y` copies to the system clipboard) |
| Normal | `d` / `y` + `w` / `W` / `e` / `E` / `b` / `B` | Delete / yank a characterwise word-motion range (`y` copies to the system clipboard) |
| Normal | `d` / `y` + `gg` / `G` | Delete / yank whole logical lines through an absolute target (`y` copies to the system clipboard) |
| Normal | `u` | Undo |
| Normal | `v` / `V` / `Ctrl+v` | Select characters / whole lines / a display-row rectangle in Visual mode |
| Visual / Visual Line | `h` / `j` / `k` / `l`, `e` / `E` / `w` / `W` / `b` / `B`, `0`, `Enter` | Extend the selection with the same motions and counts as Normal mode |
| Visual / Visual Line | `v` / `V` | Exit the active Visual mode / switch between characterwise and linewise selection |
| Visual / Visual Line / Visual Block | `c` | Change the inclusive selected characters, logical lines, or block columns, then enter Insert mode |
| Visual / Visual Line / Visual Block | `d` / `x` / `y` | Delete the selection (`d` / `x`) or yank it to the system clipboard (`y`) |
| Visual / Visual Line / Visual Block | `Escape` | Clear the selection and return to Normal mode |
| Insert | `Escape` | Close an open completion menu, otherwise return to Normal mode |
| Replace | Typed characters | Overwrite the character under the cursor; at the end of a line they are appended |
| Replace | `Backspace` | Restore the character this Replace session overwrote, otherwise move left |
| Replace | `Escape` | Return to Normal mode |

Prefix a motion, `x` / `u`, or `dd` / `yy` with a count (for example, `3w`, `2x`,
`2u`, `3dd`, `2yy`). A count between the repeated letters also applies (for example,
`d2d`); prefix and inner counts multiply (`2d3d` affects six lines). Operator and motion counts also multiply: `2d3w` deletes through six `w`
motions, and `2d3j` deletes through six lines. `h` / `l` select characters;
`j` / `k` select whole logical lines. A counted `x` deletes up to the end
of the line without joining lines;
a counted `u` undoes that many steps. `0` without a count moves to the start of
the line; after a nonzero digit it remains part of the count (for example,
`20w`). An interrupted count does not carry over to the next command. In
Visual mode, counted motions extend the editor selection. `gg` and `G` place the
cursor at the first non-blank character of the destination logical line; `G`
is a single uppercase key. A pending `g` clears if interrupted or focus leaves
the editor. In Normal mode, `d` / `y` / `c` with `gg` / `G` acts linewise from the current row through the target, clamped to the buffer: bare `gg` targets row 1 and bare `G` targets the last row. A prefix or inner count specifies an absolute 1-based target; together they multiply (`2d3G` targets row 6). Thus `1dG` targets row 1, unlike bare `dG`. Deletion is one undo step; in read-only editors it does nothing, while yank still copies to the system clipboard.

`Ctrl+Enter` uses the trimmed selection if it contains non-whitespace text;
otherwise it uses the full buffer. For a Visual Block selection, it joins
ordered nonempty row fragments with newlines, as with mouse Alt-drag. A
whitespace-only block selection uses the full buffer. Block columns count
Unicode scalars, not visual cells: tabs, wide characters, and combining
sequences may not align with on-screen columns.

In Normal mode, `/` opens the editor's find panel, the same panel as `Ctrl+f`,
with its query field focused and the last query selected. Type a literal query;
matches are case-insensitive unless the panel's case button is on. `Enter` moves
the cursor to the next match after it and `Shift+Enter` to the previous one,
wrapping around the buffer, and the panel stays open. `Escape` closes the panel
and returns to the editor in Normal mode, with the cursor on the last match it
reached and the query kept. `n` / `N` then move to the next / previous match of
that query from the cursor, and a prefix count repeats the move that many times;
the panel's match counter follows them. Search works in read-only editors, and
each tab keeps its own query. While the panel has focus, keys are typed into it
rather than read as Vim commands. This is literal text search, not regex.

**Local marks.** Marks belong to the current code document, not other tabs or
sessions. Setting a mark also works in a read-only editor. Native text edits,
including Insert-mode input and IME commits, move marks with their text through
undo and redo. Insertion at a mark moves it after the inserted text; deleting
or replacing marked text moves it to the start of the changed range, so undo
need not recover its exact former position inside deleted text. Replacing the
entire editor value, disabling Vim mode, or closing the document clears its
marks. Desktop IME behavior and the rendered UI have not been validated.

Everything else in Normal mode:

| Input | Behavior in Normal mode |
|-------|-------------------------|
| Other unsupported letters, punctuation, `Space` | Nothing |
| `Tab` / `Shift+Tab` | Nothing: no indent, and focus stays in the editor |
| `Ctrl+v` | Enter Visual Block mode (not paste) |
| Paste (`Cmd+v` or the context menu) | Nothing |
| Input method (IME) composition and commit | Dropped |
| `Backspace` / `Delete` | Nothing |
| `Escape` | Its usual meaning: cancel a running query, or leave the editor |
| Shortcuts with `Ctrl`, `Alt`, or `Cmd`; arrow keys; the mouse | Work as usual, including undo and redo |

In Normal mode the cursor sits on a character, never past the end of a line.
Leaving Insert mode moves it back one character, as Vim does. On an empty line
`x` does nothing, so it never joins lines.

In Insert mode the editor behaves as it does with Vim mode off, including `Ctrl+v` paste, except for
`Escape`. With a completion or code-action menu open, `Escape` closes the menu
and stays in Insert mode; otherwise it returns to Normal mode. Focus stays in
the editor either way. With several cursors or an inline suggestion showing,
the first `Escape` clears them and the next one returns to Normal mode.

Visual `d` / `x` deletes character, line, or block selections; blocks delete their disjoint row ranges in one undo step. Visual `y` copies the selected text to the system clipboard. If the selection is empty, these commands return to Normal mode without editing or changing the clipboard. In read-only editors, Visual `d` / `x` leaves the selection in place without editing or changing the clipboard; Visual `y` still works. `dd` and `cc` are Normal-only. Visual Block `c` deletes the block columns on every row that reaches the block's left column, skipping shorter rows, and enters Insert on the first of those rows. When Insert ends with `Escape`, the text typed there is inserted at the same column of the other rows. Nothing is copied if the typed text contains a line break, if nothing was typed, or if focus leaves the editor first. Block columns count Unicode scalars, as for block selection. The deletion, the typed text, and the copies are one undo step.

**Change and undo.** Normal `c` accepts `h` / `l` characterwise, `j` / `k` linewise, `w` / `W` / `e` / `E` / `b` / `B` wordwise, and `gg` / `G` linewise, alongside `cc`. `cw` changes through the next `w` boundary. Prefix and inner counts multiply (`2c3w` spans six `w` motions); absolute-line targets use clamped 1-based rows (`2c3G` targets row 6), while bare `cG` targets the last row. Linewise changes preserve the separator before the following row; counted `cc` includes selected lines' existing LF or CRLF terminators. Changes delete through native editing and enter Insert mode for replacement text. Deletion and replacement form one undo step in ordinary sessions, restoring the first caret; read-only editors leave text unchanged and do not enter Insert mode.

Visual character and line `c` change the inclusive selection through native editing and enter Insert for replacement. One ordinary undo restores the original text and collapsed anchor; the selected-query bytes are unchanged. In read-only editors, `c` leaves the selection intact without entering Insert. With an empty character or line selection, `c` enters Insert without deleting text. Linewise changes handle a trailing empty logical row after LF or CRLF.

**Replace.** `r{char}` replaces the character under the cursor and leaves the cursor on it. With a count, `3rx` replaces the next three characters on the line with `x`; if fewer remain before the end of the line, nothing changes. It never replaces a line break, and on an empty line it does nothing. `r` followed by `Enter` replaces the characters with one line break that keeps the line's indentation; `r` followed by `Tab` writes tab characters. `Escape`, `Backspace`, `Delete`, the arrow keys, or leaving the editor cancel `r` without editing; a shortcut with `Ctrl`, `Alt`, or `Cmd` cancels it and then runs as usual. `r` accepts a character composed with an input method (IME). The replacement is one undo step.

`R` enters Replace mode. Each typed character overwrites the character under the cursor; at a line ending it is appended instead of replacing the line break. `Backspace` restores the characters overwritten in this Replace session in reverse order and otherwise only moves left. `Enter` inserts a line break and `Tab` indents, as in Insert mode. `Escape` returns to Normal mode and moves the cursor back one character. The whole Replace session is one undo step. A count before `R` is ignored.

Each `x`, `dd`, or motion-based `d` invocation is one undo step, including counted commands. Everything typed in one ordinary Insert session is one undo step, and each new Insert session starts another. An undo group is capped at 1000 changes, so a long session may require multiple undo steps. `u` undoes the same steps as `Ctrl+z` / `Cmd+z`.

**IME limitation.** A late stale unmark from a prior composition after the next composition starts can prematurely commit the active native composition and split the Vim undo group. On a read-only or Normal-mode transition, pending displayed preedit is finalized as-is rather than accepting a later candidate. In Replace mode, text that arrives without a key press, such as an IME commit, is inserted rather than overwriting, and `Backspace` does not restore characters around it. This is not a claim of full IME safety; live UI behavior has not been validated.

**Read-only editors** (routine definitions) accept motions, `yy`, and
motion-based `y`; `x`, `r`, `R`, `dd`, `cc`, motion-based `c` / `d`, Visual `c`, and `u` do nothing there.
A read-only delete does not change the clipboard.

**Limitations.**

- Only the commands in the first table exist. `dd` and `yy` operate on whole logical
  lines, including line endings when present. At EOF, a count stops at the last line;
  deleting the last line also removes its preceding separator, without inventing
  a trailing newline for yanks. On an empty trailing line created by LF or CRLF,
  linewise `y` copies that existing separator; an empty buffer has none. Word-motion `d` / `y` supports
  `w` / `W` / `e` / `E` / `b` / `B`: `w` / `W` and `b` / `B` exclude the
  destination character, while `e` / `E` include it. Horizontal operator
  motions `h` / `l` are characterwise; vertical `j` / `k` are linewise.
  Other marks, text objects, registers, macros, `.` repeat,
  `:` commands, and a redo key are unsupported. This is not full Vim.
- Motions step one Unicode code point at a time, like the arrow keys, so a
  letter written with a separate combining accent takes two presses.
- Normal mode blocks your typing and pasting only. Edits DBFlux makes itself,
  such as loading a file or a query from history, still apply.

## Results

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+k` / `Ctrl+l` | Focus left / up / right panel |
| `Ctrl+j` | Focus toolbar |
| `j` / `k` (or `Down` / `Up`) | Next / previous row |
| `h` / `l` (or `Left` / `Right`) | Column left / right |
| `g` / `Shift+g` (or `Home` / `End`) | First / last row |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `]` / `[` | Next / previous results page |
| `F5` | Refresh the focused document (table rows, bucket list, object listing, keys) |
| `Ctrl+e` / `Cmd+e` | Export results |
| `f` | Focus toolbar |
| `/` | Focus search/filter |
| `x` | Delete row |
| `r` | Rename / edit |
| `o` | Add row |
| `y` | Copy row |
| `i` | Toggle the record view (one row, field per line) |
| `v` | Toggle the value panel for the selected cell |
| `Ctrl+Space` | Toggle the row inspector for the selected row |
| `Ctrl+c` / `Cmd+c` | Copy cell(s) |
| `z` | Toggle panel collapse |
| `m` (or `Shift+F10`) | Open context menu |

## Schema diagram

| Keys | Action |
|------|--------|
| `+` (or `=`) / `-` | Zoom in / out |
| `h` / `j` / `k` / `l` (or arrow keys) | Pan the view |
| `Shift` + `h` / `j` / `k` / `l` (or arrow keys) | Select the next table in that direction and center on it |
| `Alt` + `h` / `j` / `k` / `l` (or arrow keys) | Move the selected table |
| `r` / `s` / `c` | Left to right / Snowflake / Compact layout |
| `m` | Open context menu |
| `Escape` | Clear the selection |

## Background Tasks

The tasks panel sits under the documents and starts collapsed. Collapsed, it takes no space: open it with the background tasks entry in the status bar, or with `Ctrl+Shift+4`, which also moves focus to it. `Tab` and `Shift+Tab` skip the panel while it is collapsed.

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `j` / `k` (or `Down` / `Up`) | Select next / previous |
| `g` / `Shift+g` (or `Home` / `End`) | First / last |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `z` | Toggle panel collapse |

## Notifications center

The bell at the right end of the title bar opens the notifications center, a
popover that floats over the workspace. It lists MCP approvals waiting for a
decision, errors reported by actions you ran, an available DBFlux update, and
export, import, migration, and dump-analysis jobs that finished. The bell's
badge counts unread items and takes the color of the most urgent one: red for
an error, the accent color for an approval, and neutral for updates and
finished jobs. With nothing unread the bell has no badge.

Opening the popover marks nothing read. Clicking a row opens its target and
marks it read: an approval opens the MCP approvals tab on that request, an error
opens Audit filtered by its correlation id, the update opens its release notes,
and a finished job opens the background tasks panel. **Mark all read** reads
everything, and **Clear read** removes read items. The list lasts for the
session. Updates show here instead of in the status bar.

| Keys | Action |
|------|--------|
| `Escape` | Close the popover (a click outside it does the same) |

## Command palette

| Keys | Action |
|------|--------|
| `Down` / `Up` (or `Ctrl+j` / `Ctrl+k`) | Select next / previous |
| `Enter` | Execute |
| `Escape` | Cancel |

Letters are left to the search field, so typing filters the list.

## Data table

These keys apply while a result grid or table has focus and no cell is being
edited.

| Keys | Action |
|------|--------|
| `j` / `k` / `h` / `l` (or arrow keys) | Move the cursor |
| `Shift` + arrow keys | Extend the selection |
| `Home` / `End` | First / last cell of the row |
| `Ctrl+Home` / `Ctrl+End` | First / last row |
| `Shift+Home` / `Shift+End`, `Ctrl+Shift+Home` / `Ctrl+Shift+End` | Extend the selection to the row or table edge |
| `Ctrl+a` / `Cmd+a` | Select all |
| `Escape` | Clear the selection |
| `Ctrl+c` / `Cmd+c`, `y y` | Copy the selection |
| `Shift+y Shift+y` | Copy the row |
| `Enter` / `F2` | Edit the cell |
| `Ctrl+Enter` / `Cmd+Enter`, `Ctrl+s` / `Cmd+s` | Save the pending changes |
| `d d` / `Delete` | Delete the row |
| `a a` / `Shift+a Shift+a` | Add / duplicate a row |
| `Ctrl+n` | Set the cell to NULL |
| `u` / `Ctrl+z` / `Cmd+z` | Undo |
| `Ctrl+r` / `Ctrl+Shift+z` / `Cmd+Shift+z` | Redo |
| `e` | Expand or collapse a nested column (document grids) |
| `Backspace` | Step out of a nested value (document grids) |

## Document tree

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous node |
| `h` / `l` (or `Left` / `Right`) | Collapse / expand, or go to the parent / first child |
| `g` / `Shift+g` (or `Home` / `End`) | First / last node |
| `Ctrl+u` / `Ctrl+d` (or `PageUp` / `PageDown`) | Page up / down |
| `Space` | Expand / collapse |
| `Enter` / `F2` | Edit the value |
| `e` | Preview the document |
| `d d` / `Delete` | Delete the document |
| `t` | Cycle the data view |
| `r` | Toggle the raw JSON view |
| `/` / `Ctrl+f` | Search; `n` / `Shift+n` next / previous match, `Escape` closes |

## Key-value browser

| Keys | Action |
|------|--------|
| `` Ctrl+` `` | Show or hide the command console, also from the console input |
| `Ctrl+j` | Load more keys |
| `t` | Edit the expiry of the selected key |

`Ctrl+j` and `t` apply while the key list has focus, not inside a text field.

## Text fields

| Keys | Action |
|------|--------|
| `Ctrl+j` / `Ctrl+k` | Next / previous line, or the next / previous completion |
| `Ctrl+Space` | Show completions |
| `Ctrl+Enter` / `Cmd+Enter` | Run the query |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` | Run the query in a new tab |
| `Ctrl+Shift+z` | Redo (Linux and Windows; macOS uses `Cmd+Shift+z`) |

## Dialogs

| Keys | Action |
|------|--------|
| `Escape` | Close, or leave the field being edited first in form dialogs |
| `Enter` | Confirm, when the primary button is enabled |
| `Up` / `Down`, `PageUp` / `PageDown`, `Home` / `End` | Scroll a long dialog body |
| `Escape` / `Ctrl+s` / `Cmd+s` | Close / save the cell editor and the document preview |

## Forms and the settings window

These keys move through the forms of dialogs and through the settings window
when no text field is being edited.

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous field |
| `Left` / `Right` | Move within a row; change the choice of a segmented field |
| `h` / `l` | Back to the list / into the form |
| `g` / `Shift+g` | First / last field |
| `Tab` / `Shift+Tab` | Next / previous field |
| `Space` | Toggle |
| `Enter` | Activate or edit the field |
| `Escape` | Leave the field or the form |
| `/` | Focus the search |
| `Ctrl+w` / `Ctrl+q` | Close the settings window |
| `Ctrl+s` | Save the section |
| `Ctrl+h` / `Ctrl+l` | Move between the navigation and the section |

In the Connection Manager, `Ctrl+s` / `Cmd+s` saves the connection from
anywhere in the form, and `Left` / `Right` change the choice of **Enter as** and
of the SSH authentication method. In the audit viewer, `Left` / `Right` on the
time presets change the preset.

## Context menu

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Move down / up |
| `Enter` / `l` (or `Right`) | Select / enter submenu |
| `Escape` / `h` (or `Left`) | Back / close |

## History modal

| Keys | Action |
|------|--------|
| `Ctrl+j` / `Ctrl+k` (or `Down` / `Up`) | Select next / previous |
| `Enter` | Open entry |
| `Ctrl+f` | Toggle favorite |
| `Ctrl+r` | Rename |
| `Ctrl+d` | Delete |
| `/` | Focus search |
| `Ctrl+s` / `Cmd+s` | Save query |
