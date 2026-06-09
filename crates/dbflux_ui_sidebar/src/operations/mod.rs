use crate::*;

mod connection;
mod dnd;
mod script_ops;
mod tree_edit;
mod tree_ops;

#[allow(unused_imports)]
pub(crate) use connection::{connect_prepare_error_toast, format_connect_prepare_error};
pub(crate) use connection::{
    HeldDatabaseConnection, retain_database_cache_entries, try_close_held_database_connection,
};

impl Sidebar {
    fn track_operation_task(&mut self, task_id: TaskId, task: Task<()>) {
        self.tracked_operation_tasks.insert(task_id, task);
    }

    fn clear_tracked_operation_task(&mut self, task_id: TaskId) {
        self.tracked_operation_tasks.remove(&task_id);
    }
}
