use dbflux_core::{OutputEvent, OutputReceiver, OutputStreamKind};
use std::sync::mpsc::TryRecvError;

#[derive(Clone)]
struct OutputLine {
    stream: OutputStreamKind,
    text: String,
}

pub(super) struct LiveOutputState {
    receiver: OutputReceiver,
    lines: Vec<OutputLine>,
    truncated: bool,
    disconnected: bool,
}

impl LiveOutputState {
    const WAITING_PLACEHOLDER: &str = "(waiting for output...)";
    pub(super) const MAX_LINES: usize = 5000;

    pub(super) fn new(receiver: OutputReceiver) -> Self {
        Self {
            receiver,
            lines: Vec::new(),
            truncated: false,
            disconnected: false,
        }
    }

    pub(super) fn drain(&mut self) -> bool {
        let mut changed = false;

        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    self.push_event(event);
                    changed = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    break;
                }
            }
        }

        changed
    }

    pub(super) fn has_stderr(&self) -> bool {
        self.lines
            .iter()
            .any(|line| matches!(line.stream, OutputStreamKind::Stderr))
    }

    pub(super) fn is_finished(&self) -> bool {
        self.disconnected
    }

    pub(super) fn is_truncated(&self) -> bool {
        self.truncated
    }

    pub(super) fn line_count(&self) -> usize {
        self.lines.len()
    }

    pub(super) fn render_text(&self) -> String {
        if self.lines.is_empty() {
            Self::WAITING_PLACEHOLDER.to_string()
        } else {
            self.lines.iter().map(|line| line.text.as_str()).collect()
        }
    }

    fn push_event(&mut self, event: OutputEvent) {
        if self.truncated {
            return;
        }

        let mut start = 0;

        for (index, ch) in event.text.char_indices() {
            if ch != '\n' {
                continue;
            }

            self.push_line(event.stream, &event.text[start..=index]);
            start = index + 1;

            if self.truncated {
                return;
            }
        }

        if start < event.text.len() {
            self.push_line(event.stream, &event.text[start..]);
        }
    }

    fn push_line(&mut self, stream: OutputStreamKind, text: &str) {
        if self.lines.len() >= Self::MAX_LINES {
            self.truncated = true;
            return;
        }

        self.lines.push(OutputLine {
            stream,
            text: text.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::output_channel;

    #[test]
    fn drains_lines_and_marks_disconnect() {
        let (sender, receiver) = output_channel();
        let mut state = LiveOutputState::new(receiver);

        sender
            .send(OutputEvent::new(OutputStreamKind::Stdout, "first\nsecond"))
            .unwrap();
        sender
            .send(OutputEvent::new(OutputStreamKind::Stderr, "\nerror\n"))
            .unwrap();

        assert!(state.drain());
        assert_eq!(state.line_count(), 4);
        assert!(state.has_stderr());
        assert_eq!(state.render_text(), "first\nsecond\nerror\n");
        assert!(!state.is_finished());

        drop(sender);

        assert!(!state.drain());
        assert!(state.is_finished());
    }

    #[test]
    fn truncates_at_max_lines() {
        let (sender, receiver) = output_channel();
        let mut state = LiveOutputState::new(receiver);

        for index in 0..(LiveOutputState::MAX_LINES + 10) {
            sender
                .send(OutputEvent::new(
                    OutputStreamKind::Stdout,
                    format!("line-{index}\n"),
                ))
                .unwrap();
        }

        assert!(state.drain());
        assert!(state.is_truncated());
        assert_eq!(state.line_count(), LiveOutputState::MAX_LINES);
        assert!(state.render_text().contains("line-0\n"));
        assert!(!state.render_text().contains("line-5009"));
    }

    #[test]
    fn renders_waiting_placeholder_when_empty() {
        let (_sender, receiver) = output_channel();
        let state = LiveOutputState::new(receiver);

        assert_eq!(state.render_text(), LiveOutputState::WAITING_PLACEHOLDER);
    }
}
