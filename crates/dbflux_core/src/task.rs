use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use uuid::Uuid;

pub type TaskId = Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Query,
    Connect,
    Disconnect,
    SwitchDatabase,
    SchemaRefresh,
    Export,
}

impl TaskKind {
    pub fn label(&self) -> &'static str {
        match self {
            TaskKind::Query => "Query",
            TaskKind::Connect => "Connect",
            TaskKind::Disconnect => "Disconnect",
            TaskKind::SwitchDatabase => "Switch Database",
            TaskKind::SchemaRefresh => "Schema Refresh",
            TaskKind::Export => "Export",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, TaskStatus::Running)
    }
}

#[derive(Clone)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Task {
    pub id: TaskId,
    pub kind: TaskKind,
    pub description: String,
    pub status: TaskStatus,
    pub started_at: Instant,
    pub completed_at: Option<Instant>,
    pub progress: Option<f32>,
    cancel_token: CancelToken,
}

impl Task {
    pub fn elapsed(&self) -> std::time::Duration {
        match self.completed_at {
            Some(completed) => completed.duration_since(self.started_at),
            None => self.started_at.elapsed(),
        }
    }

    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }

    pub fn is_cancellable(&self) -> bool {
        self.status == TaskStatus::Running
    }
}

#[derive(Clone)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub kind: TaskKind,
    pub description: String,
    pub status: TaskStatus,
    pub elapsed_secs: f64,
    pub progress: Option<f32>,
    pub is_cancellable: bool,
}

impl From<&Task> for TaskSnapshot {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id,
            kind: task.kind,
            description: task.description.clone(),
            status: task.status.clone(),
            elapsed_secs: task.elapsed().as_secs_f64(),
            progress: task.progress,
            is_cancellable: task.is_cancellable(),
        }
    }
}

#[derive(Default)]
pub struct TaskManager {
    tasks: HashMap<TaskId, Task>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub fn start(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
    ) -> (TaskId, CancelToken) {
        let id = TaskId::new_v4();
        let cancel_token = CancelToken::new();

        let task = Task {
            id,
            kind,
            description: description.into(),
            status: TaskStatus::Running,
            started_at: Instant::now(),
            completed_at: None,
            progress: None,
            cancel_token: cancel_token.clone(),
        };

        self.tasks.insert(id, task);
        (id, cancel_token)
    }

    pub fn complete(&mut self, id: TaskId) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Completed;
            task.completed_at = Some(Instant::now());
        }
    }

    pub fn fail(&mut self, id: TaskId, error: impl Into<String>) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.status = TaskStatus::Failed(error.into());
            task.completed_at = Some(Instant::now());
        }
    }

    pub fn cancel(&mut self, id: TaskId) -> bool {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.cancel_token.cancel();
            task.status = TaskStatus::Cancelled;
            task.completed_at = Some(Instant::now());
            return true;
        }
        false
    }

    pub fn update_progress(&mut self, id: TaskId, progress: f32) {
        if let Some(task) = self.tasks.get_mut(&id)
            && task.status == TaskStatus::Running
        {
            task.progress = Some(progress.clamp(0.0, 1.0));
        }
    }

    pub fn get(&self, id: TaskId) -> Option<TaskSnapshot> {
        self.tasks.get(&id).map(TaskSnapshot::from)
    }

    pub fn running_tasks(&self) -> Vec<TaskSnapshot> {
        self.tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .map(TaskSnapshot::from)
            .collect()
    }

    pub fn recent_tasks(&self, limit: usize) -> Vec<TaskSnapshot> {
        let mut tasks: Vec<_> = self.tasks.values().collect();
        tasks.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        tasks
            .into_iter()
            .take(limit)
            .map(TaskSnapshot::from)
            .collect()
    }

    pub fn active_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .count()
    }

    pub fn has_running_tasks(&self) -> bool {
        self.tasks.values().any(|t| t.status == TaskStatus::Running)
    }

    pub fn cleanup_completed(&mut self, max_age_secs: u64) {
        let now = Instant::now();
        self.tasks.retain(|_, task| {
            if task.status.is_terminal()
                && let Some(completed) = task.completed_at
            {
                return now.duration_since(completed).as_secs() < max_age_secs;
            }
            true
        });
    }

    pub fn remove(&mut self, id: TaskId) {
        self.tasks.remove(&id);
    }

    pub fn current_status_message(&self) -> Option<String> {
        let running: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .collect();

        match running.len() {
            0 => None,
            1 => Some(running[0].description.clone()),
            n => Some(format!("{} tasks running...", n)),
        }
    }

    pub fn last_completed_task(&self) -> Option<TaskSnapshot> {
        self.tasks
            .values()
            .filter(|t| t.status.is_terminal())
            .filter(|t| t.completed_at.is_some())
            .max_by_key(|t| t.completed_at)
            .map(TaskSnapshot::from)
    }
}
