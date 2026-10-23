use super::model::{
    AgentId, AgentMessage, AgentThread, MessageDisposition, MessageId, MessagePriority,
    MessagePurpose, MessageSender, ThreadId,
};
use std::collections::{HashMap, HashSet};

pub(super) const MAX_MESSAGE_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct MessageThreads {
    next_message_id: u64,
    threads: HashMap<ThreadId, AgentThread>,
    thread_by_message: HashMap<MessageId, ThreadId>,
    pending: HashSet<MessageId>,
}

impl MessageThreads {
    pub(super) fn snapshot(
        &self,
        cache: &super::durable::JournalCache,
    ) -> (
        u64,
        Vec<(MessageId, super::durable::MessageEntry)>,
        Vec<MessageId>,
    ) {
        let mut messages = self
            .threads
            .values()
            .flat_map(|thread| &thread.messages)
            .map(|message| {
                let entry = match cache.message(message.id) {
                    Some(key) => super::durable::MessageEntry::Cached(std::sync::Arc::clone(key)),
                    None => super::durable::MessageEntry::New(message.clone()),
                };
                (message.id, entry)
            })
            .collect::<Vec<_>>();
        messages.sort_by_key(|(id, _)| *id);
        let mut pending = self.pending.iter().copied().collect::<Vec<_>>();
        pending.sort_unstable();
        (self.next_message_id, messages, pending)
    }

    pub(super) fn restore(
        next_message_id: u64,
        messages: Vec<AgentMessage>,
        pending: Vec<MessageId>,
    ) -> std::io::Result<Self> {
        let mut restored = Self {
            next_message_id,
            ..Self::default()
        };
        for message in messages {
            validate_body(&message.body)?;
            validate_reply(message.purpose, message.in_reply_to)?;
            if message.id.value() == 0
                || message.id.value() > next_message_id
                || restored.thread_by_message.contains_key(&message.id)
            {
                return Err(std::io::Error::other("invalid persisted message identity"));
            }
            let expected = match message.in_reply_to {
                Some(previous) => restored.reference_thread(
                    previous,
                    message.from,
                    message.to,
                    message.purpose,
                )?,
                None => ThreadId::for_message(message.id),
            };
            if expected != message.thread_id {
                return Err(std::io::Error::other("invalid persisted message thread"));
            }
            restored.commit(message);
        }
        if pending
            .iter()
            .any(|id| !restored.thread_by_message.contains_key(id))
        {
            return Err(std::io::Error::other(
                "pending message is absent from the persisted thread",
            ));
        }
        restored.pending = pending.into_iter().collect();
        Ok(restored)
    }

    pub(super) fn has_pending_for(&self, agent_id: AgentId) -> bool {
        self.pending.iter().any(|id| {
            self.message(*id)
                .is_some_and(|message| message.to == agent_id)
        })
    }

    pub(super) fn pending_messages(&self) -> Vec<AgentMessage> {
        let mut pending = self
            .pending
            .iter()
            .filter_map(|id| self.message(*id))
            .collect::<Vec<_>>();
        pending.sort_by_key(|message| message.id);
        pending
    }

    pub(super) fn prepare(
        &mut self,
        from: MessageSender,
        to: AgentId,
        priority: MessagePriority,
        purpose: MessagePurpose,
        in_reply_to: Option<MessageId>,
        body: String,
    ) -> std::io::Result<AgentMessage> {
        validate_body(&body)?;
        validate_reply(purpose, in_reply_to)?;
        let id = MessageId::next(&mut self.next_message_id);
        let thread_id = match in_reply_to {
            Some(previous_id) => self.reference_thread(previous_id, from, to, purpose)?,
            None => ThreadId::for_message(id),
        };
        Ok(AgentMessage {
            id,
            thread_id,
            from,
            to,
            priority,
            purpose,
            in_reply_to,
            body,
        })
    }

    pub(super) fn commit(&mut self, message: AgentMessage) -> AgentThread {
        let thread = self
            .threads
            .entry(message.thread_id)
            .or_insert_with(|| AgentThread {
                id: message.thread_id,
                participants: [
                    message.from,
                    MessageSender::Agent {
                        agent_id: message.to,
                    },
                ],
                messages: Vec::new(),
            });
        self.thread_by_message.insert(message.id, message.thread_id);
        self.pending.insert(message.id);
        thread.messages.push(message);
        thread.clone()
    }

    pub(super) fn mark_admitted(&mut self, id: MessageId, disposition: MessageDisposition) {
        if disposition == MessageDisposition::Queued {
            return;
        }
        self.pending.remove(&id);
    }

    pub(super) fn mark_terminal(&mut self, id: MessageId) {
        self.pending.remove(&id);
    }

    pub(super) fn thread_for_message(&self, id: MessageId) -> Option<AgentThread> {
        self.thread_by_message
            .get(&id)
            .and_then(|thread_id| self.threads.get(thread_id))
            .cloned()
    }

    pub(super) fn message(&self, id: MessageId) -> Option<AgentMessage> {
        let thread_id = self.thread_by_message.get(&id)?;
        self.threads
            .get(thread_id)?
            .messages
            .iter()
            .find(|message| message.id == id)
            .cloned()
    }

    pub(super) fn rollback(&mut self, id: MessageId) {
        self.pending.remove(&id);
        self.remove_message(id);
    }

    fn reference_thread(
        &self,
        previous_id: MessageId,
        from: MessageSender,
        to: AgentId,
        purpose: MessagePurpose,
    ) -> std::io::Result<ThreadId> {
        let thread_id = self.thread_by_message.get(&previous_id).ok_or_else(|| {
            std::io::Error::other(format!("unknown in_reply_to message {previous_id}"))
        })?;
        let thread = self
            .threads
            .get(thread_id)
            .expect("message index should reference an existing thread");
        let previous = thread
            .messages
            .iter()
            .find(|message| message.id == previous_id)
            .expect("message index should reference an existing message");
        let same_direction = from == previous.from && to == previous.to;
        let reverse_direction =
            from == (MessageSender::Agent {
                agent_id: previous.to,
            }) && previous.from == (MessageSender::Agent { agent_id: to });
        if purpose == MessagePurpose::Reply {
            if !reverse_direction {
                return Err(std::io::Error::other(format!(
                    "message {previous_id} can only be answered by its recipient; use a non-reply \
                     purpose to continue the same two-party thread in the original direction"
                )));
            }
        } else if !same_direction && !reverse_direction {
            return Err(std::io::Error::other(format!(
                "in_reply_to message {previous_id} belongs to a different two-party thread"
            )));
        }
        Ok(*thread_id)
    }

    fn remove_message(&mut self, id: MessageId) {
        let Some(thread_id) = self.thread_by_message.remove(&id) else {
            return;
        };
        let remove_thread = self.threads.get_mut(&thread_id).is_some_and(|thread| {
            thread.messages.retain(|message| message.id != id);
            thread.messages.is_empty()
        });
        if remove_thread {
            self.threads.remove(&thread_id);
        }
    }
}

fn validate_reply(purpose: MessagePurpose, in_reply_to: Option<MessageId>) -> std::io::Result<()> {
    match (purpose, in_reply_to) {
        (MessagePurpose::Reply, None) => Err(std::io::Error::other(
            "reply messages require an in_reply_to message ID",
        )),
        _ => Ok(()),
    }
}

fn validate_body(body: &str) -> std::io::Result<()> {
    if body.trim().is_empty() {
        return Err(std::io::Error::other("message must not be empty"));
    }
    if body.len() > MAX_MESSAGE_BYTES {
        return Err(std::io::Error::other(format!(
            "message exceeds the {MAX_MESSAGE_BYTES}-byte limit"
        )));
    }
    Ok(())
}
