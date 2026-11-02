mod annotation;
pub mod clipboard;
pub mod document;
mod events;
pub mod model;
mod record;
pub mod selection;
pub mod source;
mod state;
mod table;
mod theme;

pub use annotation::HeaderAnnotation;
pub use document::{ColumnGroupHeader, DocumentColumnHeader, DocumentPresentation, GroupSpan};
pub use events::{ContextMenuAction, DataTableEvent, Direction, Edge, FilterOperator, SortState};
pub use model::TableModel;
pub use state::{DataTableState, ModelSwap};
pub use table::{CONTEXT, DataTable, actions, context_menu_keystroke};
pub use theme::ROW_NUMBER_WIDTH;
// The row list and cell painter moved to `bolt::components::grid`;
// re-exported here so the old surface keeps compiling.
pub use bolt::components::grid::{CellPaint, GridCellCoord, GridHost, cell_paint, render_rows};
