/// What the binary should do after parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// The command ran to completion; exit with this code.
    Exit(i32),
    /// Start the GUI in this process.
    RunGui,
}

impl Default for Dispatch {
    fn default() -> Self {
        Dispatch::Exit(0)
    }
}

/// A parsed command that can be executed.
pub trait Runnable {
    fn run(self) -> Dispatch;
}
