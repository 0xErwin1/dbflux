
/// What the binary should do after parsing.
#[derive(Debug)]
pub enum Dispatch {
    /// The command ran to completion; exit with this code.
    Exit(i32),
    /// Start the GUI in this process.
    RunGui,
}



pub trait Runnable {
    fn run(self) -> Dispatch;
}


