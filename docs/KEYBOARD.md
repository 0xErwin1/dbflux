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
| `Ctrl+Shift+PageUp` / `Ctrl+Shift+PageDown` | Move the active tab left / right |
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
| `Ctrl+,` / `Cmd+,` | Open settings |
| `Ctrl+Shift+E` / `Cmd+Shift+E` | Hide or show the results of a query document, leaving the editor alone |
| `Ctrl+Shift+R` / `Cmd+Shift+R` | Maximize the results of a query document over the editor, or restore the split |
| `Ctrl+Shift+T` / `Cmd+Shift+T` | Show or hide the background tasks panel |
| `Ctrl+Shift+5` / `Ctrl+Shift+6` / `Ctrl+Shift+7` | Show the Connections / Scripts / Dashboards view of the sidebar |
| `Ctrl+Shift+B` / `Cmd+Shift+B` | Open or close the notifications center |
| `Ctrl+Shift+X` / `Cmd+Shift+X` | Open the most recent error in the audit viewer |
| `Ctrl+Shift+Y` / `Cmd+Shift+Y` | Open the buttons of the newest toast in a menu |
| `Ctrl+Shift+L` / `Cmd+Shift+L` | Open auth profile login |
| `Ctrl+Shift+O` / `Cmd+Shift+O` | Open the AWS SSO wizard |
| `Ctrl+Shift+C` / `Cmd+Shift+C` | Open a saved chart |
| `Ctrl+Shift+D` / `Cmd+Shift+D` | New dashboard |
| `Ctrl+Shift+M` / `Cmd+Shift+M` | Open MCP approvals |
| `Ctrl+Shift+G` / `Cmd+Shift+G` | Refresh MCP governance |

`Ctrl+Shift+X` opens the audit viewer on the most recent error reported in this session, as the error toast's **View in Audit** does, and clears the count on the status bar's error badge; before any error it shows the user errors. `Ctrl+Shift+5` .. `Ctrl+Shift+7` behave like the activity rail: choosing the view already shown collapses the sidebar. `Ctrl+Shift+Y` lists the buttons of the newest toast on screen, such as **Copy**, **View in Audit** or **Reconnect now**, then **Show details** or **Hide details** when it has details, and **Dismiss**; the context menu keys drive it, and with no toast on screen it opens nothing. The MCP shortcuts exist in builds with MCP support. **Export connections** and **Import dashboard from JSON** run from the command palette.

The shell's clickable items all have a key, and none of them joins the `Tab` cycle: the activity rail entries are `Ctrl+Shift+5` .. `Ctrl+Shift+7`, `Ctrl+Shift+A` (Audit), `Ctrl+Shift+M` (Approvals) and `Ctrl+,` (Settings); the title bar's command search is `Ctrl+Shift+P` and its bell `Ctrl+Shift+B`; the status bar's tasks entry is `Ctrl+Shift+T`, its approvals entry `Ctrl+Shift+M` and its error badge `Ctrl+Shift+X`; a tab is closed with `Ctrl+w`, opened with `Ctrl+n`, reordered with `Ctrl+Shift+PageUp` / `Ctrl+Shift+PageDown`, and its right-click menu opens with `Ctrl+m`. The window's own minimize, maximize and close buttons are left to the desktop's shortcuts.

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

`Escape` in the search field returns focus to the tree and keeps the typed filter.

When the tree gains focus with no row selected, the row selected last is selected again, or the first row when that one is gone, so `h`, `l` and `Enter` act right away.

## Editor

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `Ctrl+f` / `Cmd+f` | Find in the editor |
| `Ctrl+Shift+h` / `Cmd+Shift+f` | Find and replace in the editor |
| `Alt+h` | Toggle history dropdown |
| `Ctrl+p` / `Cmd+p` | Open saved queries |
| `Ctrl+s` / `Cmd+s` | Save query |
| `Ctrl+Shift+s` / `Cmd+Shift+s` | Save file as |
| `Ctrl+/` / `Cmd+/` | Toggle line comment |
| `Shift+F10` | Open the pane actions menu |
| `Enter` | Focus / execute |

(Unmodified letters are intentionally left to the text input so typing works.)

`Ctrl+h` / `Ctrl+j` / `Ctrl+k` move focus between panels whether or not Vim mode
is on; they never move the cursor. While a completion menu is open, `Ctrl+j` /
`Ctrl+k` step through it instead. In the find panel, `Ctrl+j` closes the panel
and returns to the editor, as `Escape` does, and `Ctrl+h` / `Ctrl+k` close it
before moving focus. The same find-and-replace keys toggle the replace field
while the panel is open. In Vim Normal mode the editor is read-only, so the find
panel opens without its replace field.

While text is being typed, `Tab` indents and `Shift+Tab` outdents, so use
`Ctrl+h` / `Ctrl+j` / `Ctrl+k` to leave the editor. In the find panel, `Tab` /
`Shift+Tab` move between the query and replace fields while the replace field is
shown; otherwise they cycle focus between panels, as they do outside the editor.

The editor's toolbar is also a menu. `Shift+F10` opens the **pane actions** menu
from the editor text, in every Vim mode and without Vim. `Ctrl+k` moves focus to
the execution context bar, where `m` (or `Shift+F10`) opens it too. A script
editor (Lua, Python, Bash) has no connection controls, so its context bar holds
only an **Actions** button, which `Ctrl+k` focuses and `Enter`, `m` or a click
presses. The menu lists Run (Cancel while a query runs), Run in new tab, Save, Format, Query
history, Explain, Chart, Refresh and the auto-refresh interval, each with its
shortcut when it has one. Move with `j` / `k` and choose with `Enter`, as in any
[context menu](#context-menu); `Escape` closes it. The auto-refresh entry opens
the interval list with keyboard focus (see [Dropdowns](#dropdowns)). While the
query has results, the menu goes on with the results header: Next and Previous
result tab, Close result tab, Maximize (Restore) results and Hide (Show)
results. The menu is also in the command palette as **Open pane actions**.

## Vim mode (opt-in)

Multi-line editors can use modal editing with a small set of Vim commands. It
is off by default. Turn it on in **Settings → General → Editor → Vim mode in
editors** and save: open editors switch over at once. It applies to every
multi-line editor:

- the code editor (SQL and the other query languages, Lua, Python, Bash);
- the S3 object editor tab and the object browser's preview editor;
- the cell editor and document preview dialogs;
- the JSON editor of the import dashboard dialog and the query editor of the
  add panel dialog;
- the value panel, the JSON view of a document collection and its aggregation
  pipeline editor.

Read-only viewers take motions, Visual selection and yanks, and no edits: a
decoded S3 object in the object editor tab or the object browser's preview, the
document tree's Raw JSON view, the query builder's SQL preview, the SQL and
query preview dialogs, and the details of an external audit event. Single-line
fields, search boxes, forms, and the command palette keep typing as usual.

Outside the code editor, `Escape` works in two steps. In Insert mode the first
`Escape` returns to Normal mode, and in Normal mode `Escape` does what it does
without Vim: it closes the dialog, leaves the editor, or hands the keyboard back
to the list or tree around it. `Enter` in Normal mode moves down a line and
never confirms a dialog. With Vim mode on, the SQL and query preview dialogs
open with the keyboard in the query, so `j` and `k` move the cursor instead of
scrolling the dialog.

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
| Other unsupported letters and punctuation | Nothing |
| `Space` | Starts a leader sequence (see **Leader key** below); on its own, nothing |
| `Tab` / `Shift+Tab` | Cycle focus forward / backward between panels, as outside the editor (also in the Visual modes); no indent |
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

**Leader key.** In Normal mode and the Visual modes, the leader key starts a
two-key sequence that runs a command without leaving the home row. The leader
is `Space` by default; choose `,` or `\` in **Settings → General → Editor →
Leader key**, and the sequences move with it. After it, DBFlux waits up to one second for the next key: a key no
sequence uses runs as it would on its own, and when no key follows, the leader
does nothing. The leader types as usual in Insert and Replace modes and while
the find panel has focus.

| Keys | Action |
|------|--------|
| `Leader a` | Open the pane actions menu, as `Shift+F10` |
| `Leader r` | Run the query |
| `Leader e` | Explain the query (code editor) |
| `Leader s` | Save |
| `Leader f` | Open the editor's find panel, as `/` |
| `Leader h` / `Leader l` | Previous / next tab of the panel, as `Alt+h` / `Alt+l` |
| `Leader p` | Open the command palette |

A command the editor's document does not offer does nothing, and the key after
the leader never reaches Vim. The sequences apply to every editor with Vim
mode. Inside a dialog (the cell editor, the document preview, Import
dashboard) they belong to the dialog: `Leader s` saves or confirms it, as its
primary button does, `Leader f` opens the editor's find panel, and a command
the dialog has no use for, such as running the query or opening the command
palette, does nothing and never reaches the document behind the dialog. They
are listed under **Vim Normal** in **Settings → Keybindings**, where they can be
changed like any other binding. Recording keys that start with the leader key
stores the leader itself, shown as `Leader`, so the binding moves with the
leader.

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
| `Ctrl+h` / `Ctrl+k` | Focus left / up panel |
| `Ctrl+l` | Move into the side panel that is open on the right (value panel, row inspector, document panel or query builder); see [Side panels](#side-panels) |
| `Ctrl+j` | Focus toolbar |
| `j` / `k` (or `Down` / `Up`) | Next / previous row |
| `h` / `l` (or `Left` / `Right`) | Column left / right |
| `g` / `Shift+g` (or `Home` / `End`) | First / last row |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Page down / up |
| `]` / `[` | Next / previous results page |
| `Alt+l` / `Alt+h` | Next / previous result tab of a query, or next / previous view (Documents, Schema, Aggregate) of a document collection, wrapping at either end |
| `Alt+w` | Close the result tab shown; the editor gets focus when it was the last |
| `F5` | Refresh the focused document (table rows, bucket list, object listing, keys) |
| `Ctrl+e` / `Cmd+e` | Open the export menu: the context menu keys move through its save and copy formats, `Enter` runs one, `Escape` closes it |
| `f` | Focus toolbar |
| `Shift+f` | Clear the WHERE filter and reload the rows; in a document collection, empty the filter slot and find |
| `/` | Focus search/filter |
| `x` | Delete row |
| `r` | Rename / edit |
| `o` | Add row |
| `y` | Copy row |
| `i` | Toggle the record view (one row, field per line) |
| `Shift+t` | Show the next view of the result (Data or Grid, JSON, Chart and the other views it offers), wrapping; the keyboard stays in the results |
| `v` | Toggle the value panel for the selected cell |
| `Ctrl+Space` | Toggle the row inspector for the selected row |
| `Ctrl+c` / `Cmd+c` | Copy cell(s) |
| `z` | Maximize the results of a query document over the editor, or restore the split |
| `m` (or `Shift+F10`) | Open context menu. Its last entry, Toolbar, lists the result toolbar and header buttons shown at that moment (export, clear filter, reset the builder query, switch view, show another view of the result, the value panel's and row inspector's buttons, open the query builder, auto-refresh interval, save or revert all changes, chart stats, save chart, show the chart point under the pointer or highlighted in the table, next chart type, the axis pickers, next / previous time range, the custom range controls and Apply, maximize, hide) with their shortcuts. In the chart view the navigation keys move the chart's highlighted point instead; see [Charts](#charts) |

An instance inspector tab takes these keys too: `m` opens the context menu of
the selected row with the driver's row actions (for example Kill session),
`Enter` or `Escape` answer the confirmation, and `F5` fetches a fresh snapshot.

In a document collection the Toolbar submenu also lists Find, Query history,
Back to the documents while stepped into a nested value, the other views, and
Reload document / Apply my change while a commit conflict is shown; Save all changes and
Revert all changes run the edit bar's Commit and Revert. Query history opens the
history menu with the keyboard in it: the context menu keys move, `Enter` runs
the highlighted query again and `Escape` closes it.

When the results offer no context menu of their own, `m` opens the pane actions
menu instead, the same one the command palette's **Open pane actions** shows.

A dump analysis tab takes the table keys in each of its two tables: `Alt+l` and
`Alt+h` move between Largest keys and By prefix, `Escape` cancels an analysis
that is still reading the file, and `m` lists the other table, a sort per column
of the table the keyboard is in (as a header click sorts it) and Cancel while
the file is read.

## Schema diff

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous row: the comparison mode, each reference database, connection or snapshot, Compute, each applicable change, and the Preview DDL / Apply buttons |
| `g` / `Shift+g` (or `Home` / `End`) | First / last row |
| `h` / `l` (or `Left` / `Right`) | Previous / next button of the row (Live or Snapshot, Preview DDL or Apply) |
| `Enter` | Press the button under the cursor: choose the mode or the reference, compute, check a change, preview or apply |
| `Space` | Check or uncheck the change under the cursor |
| `F5` | Compute the diff again |
| `m` (or `Shift+F10`) | Pane actions: Compute, Preview DDL, Apply and the two comparison modes |

Apply still asks for the same confirmation as the button.

## Schema diagram

| Keys | Action |
|------|--------|
| `+` (or `=`) / `-` | Zoom in / out |
| `h` / `j` / `k` / `l` (or arrow keys) | Pan the view |
| `Shift` + `h` / `j` / `k` / `l` (or arrow keys) | Select the next table in that direction and center on it |
| `Alt` + `h` / `j` / `k` / `l` (or arrow keys) | Move the selected table |
| `r` / `s` / `c` | Left to right / Snowflake / Compact layout |
| `m` | Open context menu: zoom in / out, reset the view to 100%, fit to view, layout, arrange the tables, copy as DBML or SQL, show column types, show indexes, and for a selected table inspect and focus |
| `Escape` | Clear the selection |

## Charts

These keys apply in a chart tab and in the chart view of a result (Chart, not
Table + Chart, where the keys stay with the table). A highlighted point stands
in for the pointer: the crosshair and the readout show it. In the chart of a
table or collection tab the point inspector follows it too; charts of query
results and chart tabs have no point inspector. Moving the pointer over the
chart replaces it.

| Keys | Action |
|------|--------|
| `h` / `l` (or `Left` / `Right`) | Highlight the previous / next point of the focused series; the first key starts at the first point |
| `g` / `Shift+g` (or `Home` / `End`) | Highlight the first / last point |
| `j` / `k` (or `Down` / `Up`) | Move the highlighted point to the next / previous visible series, at the nearest X |
| `Space` | Hide or show the focused series, like its legend entry |
| `Escape` | Clear the highlighted point, or close an open axis picker |
| `Alt+l` / `Alt+h` | Next / previous chart type (chart tab) |
| `]` / `[` | Next / previous time range, Custom included (chart tab) |
| `F5` | Run the chart again (chart tab) |
| `Ctrl+s` / `Cmd+s` | Save the chart; in the name prompt `Enter` saves and `Escape` cancels (chart tab) |
| `m` (or `Shift+F10`) | Pane actions (chart tab): refresh, auto-refresh interval, next / previous time range, the custom range controls and Apply while Custom is selected, next / previous chart type, the X, Y, Group and Aggregation pickers, Stats, the metric picker and its controls for a metric chart, Save chart |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` / `Ctrl+l` | Focus the panel in that direction |

An axis picker opened from the menu takes these keys until it closes: `j` / `k`
move through its rows, `h` / `l` switch to the neighboring picker (X, Y, Group,
Aggregation), `Enter` picks the row and closes it, `Space` toggles a Y column
and keeps it open, `Escape` closes it. In the chart view of a result, the
Toolbar entry of the context menu (`m`) lists the chart type, the axis pickers,
the time range and the custom range controls. The date range of a custom range
takes the keyboard from its menu entry and opens with `Enter`; picking the
days in its calendar still needs the pointer.

## Dashboards

These keys apply in a dashboard tab, in View and Edit mode. A ring marks the
selected panel while the keyboard is in the dashboard.

| Keys | Action |
|------|--------|
| `h` / `l` (or `Left` / `Right`) | Select the previous / next panel in reading order |
| `j` / `k` (or `Down` / `Up`) | Select the nearest panel on the next / previous row |
| `g` / `Shift+g` (or `Home` / `End`) | Select the first / last panel |
| `Enter` / `i` | Open the selected panel: a chart takes the [chart keys](#charts), an inspector table the table keys, until `Escape`; on a divider, fold or unfold its section |
| `Space` | Fold or unfold the selected divider's section |
| `c` | Configure the selected chart panel |
| `r` / `F2` | Rename the selected panel |
| `x` / `Delete` | Remove the selected panel |
| `a` | Add a panel |
| `Shift` + `h` / `j` / `k` / `l` (or arrow keys) | Move the selected panel one grid cell (Edit mode) |
| `Alt+Shift` + `h` / `l` | Make the selected panel narrower / wider (Edit mode) |
| `Alt+Shift` + `k` / `j` | Make the selected panel shorter / taller (Edit mode) |
| `Alt+l` / `Alt+h` | Switch between View and Edit |
| `]` / `[` | Next / previous shared time range, Custom included |
| `F5` | Refresh every panel, or only the open one |
| `m` (or `Shift+F10`) | Pane actions: the selected panel's actions, add panel, refresh, auto-refresh interval, the time range and the custom range controls, View / Edit or Save as editable. With a chart panel open, its own auto-refresh interval, its chart type and Stats |
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` / `Ctrl+l` | Focus the panel in that direction |

A move or resize that leaves the grid or lands on another panel is refused, as
a drag is. In the Configure popover, `h` / `l` open the X, Y, Group and
Aggregation pickers in turn, `j` / `k` move through the open one, `Space` or
`Enter` pick, `Alt+l` / `Alt+h` switch the chart type, `Enter` without a picker
applies and `Escape` closes the picker and then the popover.

In the Add Panel dialog, `Alt+l` / `Alt+h` switch its tabs (also from its text
fields, except on macOS). The arrows move through the chart list from the
search and `Enter` adds the checked charts, or the highlighted one when none is
checked. `Tab` moves into a list, where `j` / `k`, `g` / `Shift+g` and `Space`
(check a chart, pick a namespace or metric) work, `h` / `l` switch between the
Metric tab's namespace and metric lists and `/` goes back to the search.
`Escape` closes the dialog.

## Background Tasks

The tasks panel sits under the documents and starts collapsed. Collapsed, it takes no space: open it with the background tasks entry in the status bar, or with `Ctrl+Shift+4`, which also moves focus to it. `Tab` and `Shift+Tab` skip the panel while it is collapsed.

| Keys | Action |
|------|--------|
| `Ctrl+h` / `Ctrl+j` / `Ctrl+k` | Focus left / down / up panel |
| `j` / `k` (or `Down` / `Up`) | Select the next / previous task |
| `g` / `Shift+g` (or `Home` / `End`) | Select the first / last task |
| `Space` / `Enter` | Show or hide the selected task's output |
| `c` | Cancel the selected task |
| `x` | Dismiss the selected task once it has finished |
| `Shift+x` | Clear the finished tasks |
| `m` / `Shift+F10` | Open the panel's actions menu |
| `z` | Toggle panel collapse |

The selected task is highlighted while the panel has focus; clicking a row selects it too. The actions menu lists the selected task's **Show output**, **Cancel task** and **Dismiss**, then **Clear finished** and **Hide the tasks panel**, each with its shortcut; the context menu keys drive it. **Clear finished tasks** is also a command palette entry.

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
| `Ctrl+Shift+B` / `Cmd+Shift+B` | Open or close the popover from anywhere in the workspace |
| `j` / `k` (or `Down` / `Up`) | Select the next / previous row |
| `g` / `Shift+g` (or `Home` / `End`) | Select the first / last row |
| `Enter` | Open the selected row's target, as a click on the row does |
| `r` | Mark the selected row read |
| `x` | Dismiss the selected row, as **Later** does for the update |
| `i` | Install the listed update (builds installed from the direct download) |
| `Alt+l` / `Alt+h` | Show the next / previous filter |
| `Shift+r` | Mark all read |
| `Shift+x` | Clear read |
| `Escape` | Close the popover (a click outside it does the same) |

While the popover is open it keeps the keyboard: the panels behind it see none of these keys. The selected row is drawn on a tint. Dismissing removes an error or a finished job, and hides an approval or the update until the session ends. The keys are listed under the Notifications context in Settings > Keybindings.

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

## Side panels

These keys apply after `Ctrl+l` moves focus from a result grid into the value
panel, row inspector, document panel or query builder beside it.

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Scroll a line down / up |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Scroll a page down / up |
| `g` / `Shift+g` (or `Home` / `End`) | Scroll to the top / end |
| `Enter` | Edit the value (value panel) |
| `Escape` | Stop editing the value, or go back to the grid |
| `Ctrl+h` | Go back to the grid |
| `m` (or `Shift+F10`) | Open the grid's context menu. Its Toolbar entry lists the buttons of the open panel: in the value panel the other formats, word wrap, Format, Compact and, once the value changed, Revert and Save; in the row inspector Pin or Unpin |

The query builders have keys of their own, listed under
[Query builders](#query-builders).

## Query builders

After `Ctrl+l` moves focus from a table's grid into its query builder, or from
a collection's documents into the document builder, a cursor marks one row of
the builder: the column list, a condition or group of the filter, a join, a
grouping or sort row, an assignment or an execution option, and in the
document builder the query name, a saved query, a projected field or a group
stage row. The keys are listed under the Query Builder and Document Builder
contexts in Settings > Keybindings.

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous row |
| `g` / `Shift+g` (or `Home` / `End`) | First / last row |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Eight rows down / up |
| `h` / `l` (or `Left` / `Right`) | Previous / next field of the row |
| `Enter` / `i` | Work the field: type in a text field, open a dropdown, press a button |
| `Space` | Flip the row's switch (AND / OR, ASC / DESC, a column checkbox, an assignment's value kind) |
| `a` | Add an entry to the row's list (a condition, a join, a sort key, an assignment) |
| `Shift+a` | Add a group inside the row's filter group |
| `x` / `d` | Remove the row |
| `Shift+j` / `Shift+k` | Move a document sort key down / up |
| `Alt+l` / `Alt+h` | Next / previous mode (SELECT, UPDATE, DELETE; Find, Aggregate) |
| `Ctrl+Enter` | Run |
| `Ctrl+s` | Save |
| `m` / `Shift+F10` | Menu of the row's actions and the builder's own (Run or Find, Open in Editor, Save, Reset or the saved queries, the modes, Close) |
| `Escape` | Leave a field back to the rows, or go back to the grid |
| `Ctrl+h` | Go back to the grid |

In a text field the letters are typed text and Escape returns to the rows. A
dropdown opened with Enter takes the dropdown keys and gives the keyboard back
to the builder when it closes. A run from the keyboard goes through the same
confirmation and mutation policy as the Run button, so an UPDATE or DELETE
without WHERE still asks first. In the document builder, Enter on a field
opens the field picker with its search focused: type the path and press Enter.
Enter on an operator opens the operator list, where `j`, `k` and Enter pick.
A Find from the keyboard keeps the keyboard in the builder. macOS uses Cmd instead of Ctrl for
`Ctrl+Enter` and `Ctrl+s`, and there `Alt+l` / `Alt+h` work only outside the
text fields.

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

The search field and the inline value editor keep the letters you type. In the
search field, `Enter` returns the keyboard to the tree with the matches kept, so
`n` and `Shift+n` step through them, and `Escape` closes the search.

## Key-value browser

| Keys | Action |
|------|--------|
| `` Ctrl+` `` | Show or hide the command console, also from the console input |
| `Ctrl+j` | Load more keys |
| `t` | Edit the expiry of the selected key |
| `Alt+l` / `Alt+h` | Next / previous key type filter (All first); in the expiry editor, its next / previous mode (Never, In, At) |

`Ctrl+j` and `t` apply while the key list has focus, not inside a text field.
`Alt+l` and `Alt+h` also work from the pattern and expiry fields, except on
macOS, where `Option` with a letter types a character.

`m` opens the menu of the selected key or member. After the key or member
actions it lists the value panel's buttons (reload the value, the other View
as choices and the decompression list of a string value, preview the first
bytes or load a large value anyway, and a stream's pending entries and claim
form) and the toolbar's (show the keys as a tree or a list, the auto-refresh
interval, bulk delete, and Stop or Search whole keyspace while a filtered scan
reads page by page). In the New key and Add member dialogs `Tab` and `Shift+Tab`
move through the fields like `j` and `k` and stay in the dialog. When the
console asks to confirm a dangerous command, `Enter` in its empty field runs it
and `Escape` cancels it.

## Object storage

In the object browser, `m` opens the selected row's menu. After the row's
entries (an object also offers Open in system viewer) it lists the listing's
buttons: Upload, New folder, Copy the current path, Load more while the level
has another page, Show as a list or a tree, and, while a preview shows them,
View versions, Load anyway and Discard for unsaved edits. With no row selected,
`m` opens the same listing entries as the pane actions.

In the bucket list, `m` lists Browse, Calculate size, New bucket and Refresh.

In an object editor tab, `Escape` takes the keyboard out of the text, `Enter`
puts it back, and `m` then lists Save, Discard, Find, the Auto / Raw
interpretation, Reload and, for an object over the size limit, Load anyway.

When you leave an object with unsaved edits, the dialog that asks what to do
opens with **Save** focused, so `Enter` saves. `Tab` and `Shift+Tab` move
between **Save**, **Cancel** and **Discard** without leaving the dialog, `Enter`
or `Space` presses the focused button, and `Escape` cancels.

## Audit viewer

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous event |
| `g` / `Shift+g` (or `Home` / `End`) | First / last event |
| `Ctrl+d` / `Ctrl+u` (or `PageDown` / `PageUp`) | Move a quarter page down / up |
| `]` / `[` | Next / previous page |
| `Enter` / `Space` | Expand or collapse the selected event |
| `f` / `/` | Enter the filter toolbar; `h` / `l` move, `Enter` activates, `Escape` leaves |
| `r` | Refresh |
| `Ctrl+e` / `Cmd+e` | Open the export menu: the context menu keys move between CSV and JSON, `Enter` exports, `Escape` closes it |
| `Alt+l` / `Alt+h` | Switch between the event table and the chart |
| `m` (or `Shift+F10`) | Context menu of the selected event: copy as CSV, copy the summary, copy as JSON, filter by its correlation id, and open a pending approval |

## MCP approvals

| Keys | Action |
|------|--------|
| `j` / `k` (or `Down` / `Up`) | Next / previous pending call |
| `g` / `Shift+g` (or `Home` / `End`) | First / last pending call |
| `a` | Approve the selected call |
| `r` | Reject the selected call, sending the typed reason |
| `Enter` / `i` | Type the rejection reason; `Escape` returns to the list |
| `F5` | Reload the pending calls |
| `m` (or `Shift+F10`) | Pane actions: approve, reject, type the reason, refresh |

The keys are listed under the MCP Approvals context in Settings > Keybindings.

## Migrate wizard

| Keys | Action |
|------|--------|
| `Alt+l` / `Alt+h` | Continue / back, like the footer buttons (also from a text field, except on macOS) |
| `Ctrl+Enter` / `Cmd+Enter` | Continue; on the Confirm step, start the migration |
| `j` / `k` (or `Down` / `Up`) | Move the cursor of the step |
| `h` / `l` (or `Left` / `Right`) | Source and Target: collapse or expand the node, or move between the two trees. Tables Mapping: move between a row's target name, mode and **Columns…** |
| `Enter` / `i` | Check a source table, choose the target database, type in a field, open a dropdown or the column drill-in |
| `Space` | Toggle the item under the cursor; on Confirm, check the destructive-plan acknowledgment |
| `Shift+k` / `Shift+j` | Move a table up / down in the load order |
| `Escape` | Leave a text field or close the column drill-in |
| `m` (or `Shift+F10`) | Pane actions: Continue, Back, set every table's mode, accept the load order, the acknowledgment, Start migration, Cancel migration while it runs and Close once it is done |

The keys are listed under the Migrate Wizard context in Settings > Keybindings.

## Text fields

| Keys | Action |
|------|--------|
| `Ctrl+j` / `Ctrl+k` | Next / previous line, or the next / previous completion (outside the code editor, where they move focus; see [Editor](#editor)) |
| `Ctrl+Space` | Show completions |
| `Ctrl+Enter` / `Cmd+Enter` | Run the query |
| `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` | Run the query in a new tab |
| `Ctrl+Shift+z` | Redo (Linux and Windows; macOS uses `Cmd+Shift+z`) |

While you type in a text field outside a dialog, such as the sidebar search or
the execution context bar, the global shortcuts that hold `Ctrl` or `Cmd` keep
working: `Ctrl+Tab`, `Ctrl+1` .. `Ctrl+9`, `Ctrl+w`, `Ctrl+Shift+P` and the
others in [Global](#global-available-regardless-of-focus). Keys without those
modifiers, including `Tab`, `Escape`, `Enter` and the arrows, stay with the
field. A shortcut the field binds itself, such as `Ctrl+a` or `Ctrl+c`, wins
over the global one. Inside a dialog, a menu, a dropdown or a picker the global
shortcuts wait until it closes.

## Dialogs

| Keys | Action |
|------|--------|
| `Escape` | Close, or leave the field being edited first in form dialogs |
| `Enter` | Confirm, when the primary button is enabled |
| `Up` / `Down`, `PageUp` / `PageDown`, `Home` / `End` | Scroll a long dialog body |
| `Escape` / `Ctrl+s` / `Cmd+s` | Close / save the cell editor and the document preview |
| `Tab` / `Shift+Tab` | Move to the next / previous control inside the dialog |

`Tab` and `Shift+Tab` wrap around inside an open dialog: from the last control
they return to the first, and they never move focus to the panels behind it.

In the **SQL Preview** and **Query Preview** dialogs, `Enter` or
`Ctrl+c` / `Cmd+c` copies the query and closes the preview, `j` / `k` and `Up` /
`Down` scroll it a line, and `PageUp` / `PageDown` a page. These keys are listed
under the **SQL Preview** context in **Settings → Keybindings**.

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
| `n` / `d` / `i` | In a profile list (proxies, SSH tunnels, auth profiles, hooks, services, MCP): new, delete, import |

In **Settings → Keybindings**, `Enter` or `Space` records new keys for the
selected binding, `r` resets it, `p` edits its context, `Delete` or `Backspace`
removes its shortcut, `Shift+r` resets every binding, `c` opens the context
filter and `/` or `f` focuses the text filter. `Enter` on a dropdown field, such
as the provider of an auth profile, opens its list with keyboard focus (see
[Dropdowns](#dropdowns)). While a login waits for the browser, the Open browser,
Copy URL and Cancel buttons are the row under the login button. On the About
page, `j` / `k` move between its two links and `Enter` opens one. These keys,
like the navigation keys, are listed under the Settings Window and Form
Navigation contexts in **Settings → Keybindings** and can be rebound there.

In the Connection Manager's driver list, `i` opens Import connections and
`Shift+i` opens Import from another client, as the buttons beside Cancel do;
typed into the driver filter, they stay text. In the Connection Manager,
`Ctrl+s` / `Cmd+s` saves the connection from anywhere in the form, and `Left` / `Right` change the choice of **Enter as** and
of the SSH authentication method. `Down` / `Up` move like `j` / `k`; while a
field is being edited they leave it for the next or previous field, and
`Ctrl+l` / `Ctrl+h` leave it for the next or previous tab. `PageDown` /
`PageUp` move the highlight of an open dropdown a page at a time. After the
password, the form's ring continues through the driver's other fields, such as
the auth profile picker of an AWS connection, which `Enter` opens, then the SSL
mode, whose choice `Left` / `Right` or `Enter` change, and the certificate
pickers, where `Enter` browses. After a failed connection test, the banner's
**Copy** button sits between **Test connection** and **Save**. On the Settings
tab, each connection phase's hook dropdown is a stop above its extra hooks
field, and `Enter` hands it the keyboard. On the MCP tab, `j` / `k` move through
the MCP switch, the client filter, each listed client (`Enter` selects it), the
selected client's access switch and its role and policy pickers. In the audit viewer, `Left` / `Right` on the
time presets change the preset.

## Dropdowns

These keys apply to a dropdown or a multi-select that has keyboard focus, for
example after a menu entry opens one. Keys a dropdown does not use pass to the
panel around it.

| Keys | Action |
|------|--------|
| `Enter` / `Space` | Open the list |
| `j` / `k` (or `Down` / `Up`) | Move down / up in the open list |
| `Enter` | Choose the highlighted item, or close a multi-select |
| `Space` | Choose the highlighted item; toggle it in a multi-select |
| `Escape` | Close the list without choosing |

Choosing an item or pressing `Escape` gives focus back to the control that held
it before the dropdown. Dropdowns that belong to a keyboard ring, such as the
execution context bar and the audit filters, are still driven by that ring's
keys. The one multi-select of the execution context bar, the targets list of a
source such as a log group or event stream, is the exception: `Enter` on it
opens the list with keyboard focus, so the keys above drive it, and closing it
returns focus to the bar.

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
| `/` | Focus search; inside the search, rename and save fields it types a `/` |
| `Ctrl+s` / `Cmd+s` | Save query |
| `Alt+l` / `Alt+h` | Show the next / previous list (Recent, Saved) |

`Alt+l` and `Alt+h` also work in the history's search, rename and save fields, except on macOS, where `Option` with a letter types a character: there they work from the list only.
