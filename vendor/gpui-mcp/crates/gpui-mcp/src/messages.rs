//! The bounded message log shared by the application and the agent.
//! GPUI-independent so its rules are unit tested.

use std::collections::VecDeque;

use gpui_mcp_protocol::{
    BridgeError, ErrorCode, MAX_MESSAGE_PAGE, MAX_RETAINED_MESSAGES, MAX_UNREAD_MESSAGES, Message,
    MessagePage, MessageSender, NewMessage,
};

#[derive(Debug, Default)]
pub(crate) struct MessageLog {
    messages: VecDeque<Message>,
    latest_id: u64,
    /// Highest app message id the agent has read.
    read_by_agent: u64,
    /// Highest agent message id the app has read.
    read_by_app: u64,
}

impl MessageLog {
    #[cfg(test)]
    pub(crate) fn latest_id(&self) -> u64 {
        self.latest_id
    }

    /// Append a message, refusing it while the other side has too many unread.
    pub(crate) fn post(
        &mut self,
        from: MessageSender,
        message: NewMessage,
        timestamp_ms: u64,
    ) -> Result<Message, BridgeError> {
        message
            .validate()
            .map_err(|reason| BridgeError::new(ErrorCode::InvalidRequest, reason))?;
        if self.unread(from) >= MAX_UNREAD_MESSAGES {
            return Err(BridgeError::new(
                ErrorCode::Busy,
                match from {
                    MessageSender::App => {
                        "the agent has not read the app's last 64 messages; wait for it to read them"
                    }
                    MessageSender::Agent => {
                        "the app has not read the agent's last 64 messages; wait for it to read them"
                    }
                },
            ));
        }
        self.latest_id += 1;
        let message = Message {
            id: self.latest_id,
            from,
            kind: message.kind.unwrap_or_else(|| "chat".to_owned()),
            text: message.text,
            reply_to: message.reply_to,
            data: message.data,
            timestamp_ms,
        };
        if self.messages.len() == MAX_RETAINED_MESSAGES {
            self.messages.pop_front();
        }
        self.messages.push_back(message.clone());
        Ok(message)
    }

    /// Retained messages from `from` that the other side has not read.
    pub(crate) fn unread(&self, from: MessageSender) -> usize {
        let read = match from {
            MessageSender::App => self.read_by_agent,
            MessageSender::Agent => self.read_by_app,
        };
        self.messages
            .iter()
            .filter(|message| message.from == from && message.id > read)
            .count()
    }

    /// Record that `reader` has read the other side's messages up to `id`.
    pub(crate) fn mark_read(&mut self, reader: MessageSender, id: u64) {
        let id = id.min(self.latest_id);
        let read = match reader {
            MessageSender::Agent => &mut self.read_by_agent,
            MessageSender::App => &mut self.read_by_app,
        };
        *read = (*read).max(id);
    }

    /// Whether a message from `from` (or either side) newer than `after` exists.
    pub(crate) fn has_after(&self, after: u64, from: Option<MessageSender>) -> bool {
        self.messages
            .iter()
            .rev()
            .take_while(|message| message.id > after)
            .any(|message| from.is_none_or(|from| message.from == from))
    }

    /// Read up to `limit` messages after `after`, optionally only from one side.
    /// When `reader` is given, that side's read mark advances past the page.
    pub(crate) fn read(
        &mut self,
        after: u64,
        from: Option<MessageSender>,
        limit: usize,
        reader: Option<MessageSender>,
    ) -> MessagePage {
        let limit = limit.clamp(1, MAX_MESSAGE_PAGE);
        let oldest = self.messages.front().map_or(self.latest_id + 1, |m| m.id);
        let mut matching = self
            .messages
            .iter()
            .filter(|message| message.id > after)
            .filter(|message| from.is_none_or(|from| message.from == from));
        let messages: Vec<Message> = matching.by_ref().take(limit).cloned().collect();
        let has_more = matching.next().is_some();
        if let Some(reader) = reader
            && let Some(last) = messages.iter().rev().find(|m| m.from == reader.peer())
        {
            self.mark_read(reader, last.id);
        }
        MessagePage {
            messages,
            latest_id: self.latest_id,
            truncated: after.saturating_add(1) < oldest && after < self.latest_id,
            has_more,
            unread_from_app: count(self.unread(MessageSender::App)),
            unread_from_agent: count(self.unread(MessageSender::Agent)),
        }
    }
}

fn count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use gpui_mcp_protocol::{
        ErrorCode, MAX_RETAINED_MESSAGES, MAX_UNREAD_MESSAGES, MessageSender, NewMessage,
    };

    use super::MessageLog;

    #[test]
    fn ids_are_monotonic_and_pages_resume_by_id() -> Result<(), ErrorCode> {
        let mut log = MessageLog::default();
        let first = log
            .post(MessageSender::App, NewMessage::text("hello"), 1)
            .map_err(|e| e.code)?;
        let reply = log
            .post(
                MessageSender::Agent,
                NewMessage::text("hi").reply_to(first.id),
                2,
            )
            .map_err(|e| e.code)?;
        assert_eq!((first.id, reply.id), (1, 2));
        assert_eq!(first.kind, "chat");
        assert_eq!(reply.reply_to, Some(1));

        let page = log.read(0, None, 1, None);
        assert_eq!(page.messages.len(), 1);
        assert!(page.has_more);
        let page = log.read(page.messages[0].id, None, 10, None);
        assert_eq!(page.messages[0].id, 2);
        assert!(!page.has_more);
        assert_eq!(page.latest_id, 2);

        let from_agent = log.read(0, Some(MessageSender::Agent), 10, None);
        assert_eq!(from_agent.messages.len(), 1);
        assert!(log.has_after(1, Some(MessageSender::Agent)));
        assert!(!log.has_after(1, Some(MessageSender::App)));
        Ok(())
    }

    #[test]
    fn unread_messages_apply_backpressure_until_read() -> Result<(), ErrorCode> {
        let mut log = MessageLog::default();
        for index in 0..MAX_UNREAD_MESSAGES {
            log.post(MessageSender::App, NewMessage::text(format!("m{index}")), 0)
                .map_err(|e| e.code)?;
        }
        assert_eq!(
            log.post(MessageSender::App, NewMessage::text("one more"), 0)
                .map_err(|e| e.code),
            Err(ErrorCode::Busy)
        );
        // The other direction is unaffected.
        log.post(MessageSender::Agent, NewMessage::text("reply"), 0)
            .map_err(|e| e.code)?;

        // Reading without marking leaves the backlog in place.
        let page = log.read(0, Some(MessageSender::App), 10, None);
        assert_eq!(page.unread_from_app, 64);
        // The agent reading ten frees ten slots.
        let page = log.read(0, Some(MessageSender::App), 10, Some(MessageSender::Agent));
        assert_eq!(page.unread_from_app, 54);
        log.post(MessageSender::App, NewMessage::text("fits now"), 0)
            .map_err(|e| e.code)?;
        Ok(())
    }

    #[test]
    fn old_messages_are_dropped_and_reported() -> Result<(), ErrorCode> {
        let mut log = MessageLog::default();
        for index in 0..(MAX_RETAINED_MESSAGES + 10) {
            let from = if index % 2 == 0 {
                MessageSender::App
            } else {
                MessageSender::Agent
            };
            log.post(from, NewMessage::text("x"), 0)
                .map_err(|e| e.code)?;
            log.mark_read(MessageSender::Agent, u64::MAX);
            log.mark_read(MessageSender::App, u64::MAX);
        }
        let page = log.read(0, None, 5, None);
        assert!(page.truncated);
        assert_eq!(page.messages[0].id, 11);
        let page = log.read(10, None, 5, None);
        assert!(!page.truncated);
        let page = log.read(log.latest_id(), None, 5, None);
        assert!(page.messages.is_empty());
        assert!(!page.truncated);
        Ok(())
    }
}
