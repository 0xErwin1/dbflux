use dbflux_core::{OutputEvent, OutputReceiver, OutputStreamKind};
use std::sync::mpsc::TryRecvError;

pub(super) struct LiveOutputState {
    receiver: OutputReceiver,
    rendered_text: String,
    line_count: usize,
    has_stderr: bool,
    truncated: bool,
    disconnected: bool,
}

impl LiveOutputState {
    const WAITING_PLACEHOLDER: &str = "(waiting for output...)";
    pub(super) const MAX_LINES: usize = 5000;

    pub(super) fn new(receiver: OutputReceiver) -> Self {
        Self {
            receiver,
            rendered_text: String::new(),
            line_count: 0,
            has_stderr: false,
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
                    if !self.disconnected {
                        changed = true;
                    }

                    self.disconnected = true;
                    break;
                }
            }
        }

        changed
    }

    pub(super) fn has_stderr(&self) -> bool {
        self.has_stderr
    }

    pub(super) fn is_finished(&self) -> bool {
        self.disconnected
    }

    pub(super) fn is_truncated(&self) -> bool {
        self.truncated
    }

    pub(super) fn line_count(&self) -> usize {
        self.line_count
    }

    pub(super) fn render_text(&self) -> String {
        if self.rendered_text.is_empty() {
            Self::WAITING_PLACEHOLDER.to_string()
        } else {
            self.rendered_text.clone()
        }
    }

    fn push_event(&mut self, event: OutputEvent) {
        if self.truncated || event.text.is_empty() {
            return;
        }

        if matches!(event.stream, OutputStreamKind::Stderr) {
            self.has_stderr = true;
        }

        self.rendered_text.push_str(&event.text);

        let (truncated_text, was_truncated) =
            truncate_to_max_lines(&self.rendered_text, Self::MAX_LINES);

        self.rendered_text = truncated_text;
        self.truncated = was_truncated;
        self.line_count = visible_line_count(&self.rendered_text);
    }
}

fn visible_line_count(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }

    text.bytes().filter(|byte| *byte == b'\n').count() + usize::from(!text.ends_with('\n'))
}

fn truncate_to_max_lines(text: &str, max_lines: usize) -> (String, bool) {
    if text.is_empty() || max_lines == 0 {
        return (String::new(), !text.is_empty());
    }

    let mut visible_lines = 1;

    for (index, ch) in text.char_indices() {
        if ch != '\n' {
            continue;
        }

        if index + 1 < text.len() {
            visible_lines += 1;

            if visible_lines > max_lines {
                return (text[..=index].to_string(), true);
            }
        }
    }

    (text.to_string(), false)
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
        assert_eq!(state.line_count(), 3);
        assert!(state.has_stderr());
        assert_eq!(state.render_text(), "first\nsecond\nerror\n");
        assert!(!state.is_finished());

        drop(sender);

        assert!(state.drain());
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
    fn preserves_partial_line_output_between_events() {
        let (sender, receiver) = output_channel();
        let mut state = LiveOutputState::new(receiver);

        sender
            .send(OutputEvent::new(OutputStreamKind::Stdout, "partial"))
            .unwrap();
        sender
            .send(OutputEvent::new(OutputStreamKind::Stdout, " line\nnext"))
            .unwrap();

        assert!(state.drain());
        assert_eq!(state.render_text(), "partial line\nnext");
        assert_eq!(state.line_count(), 2);
    }

    #[test]
    fn renders_waiting_placeholder_when_empty() {
        let (_sender, receiver) = output_channel();
        let state = LiveOutputState::new(receiver);

        assert_eq!(state.render_text(), LiveOutputState::WAITING_PLACEHOLDER);
    }
}
