use std::{
    collections::HashSet,
    io::{self, Write},
};

use nanocode::agent::events::{AgentEvent, AgentEventKind};
use serde_json::Value;

#[derive(Eq, PartialEq)]
enum Block {
    Reasoning(u64),
    Answer(String),
    Notice,
}

pub struct Printer<O> {
    answers: O,
    block: Option<Block>,
    open: bool,
    streamed: HashSet<String>,
}

impl<O: Write> Printer<O> {
    pub fn new(answers: O) -> Self {
        Self {
            answers,
            block: None,
            open: false,
            streamed: HashSet::new(),
        }
    }

    pub fn event(&mut self, event: &AgentEvent) -> io::Result<()> {
        let payload = serde_json::from_str::<Value>(event.payload.get()).unwrap_or_default();
        let text = payload["text"].as_str().unwrap_or_default();
        match event.kind {
            AgentEventKind::ReasoningSummaryDelta => self.write(
                Block::Reasoning(payload["model_call_index"].as_u64().unwrap_or_default()),
                text,
            ),
            AgentEventKind::AssistantDelta => self.answer(&payload, text),
            AgentEventKind::AssistantMessage if !self.streamed.contains(&key(&payload)) => {
                self.answer(&payload, text)
            }
            AgentEventKind::ToolCall => {
                self.notice(&format!("· {}", payload["tool"].as_str().unwrap_or("tool")))
            }
            AgentEventKind::RunError if payload["observational"] == true => self.notice(&format!(
                "warning: {}",
                payload["error"].as_str().unwrap_or_default()
            )),
            _ => Ok(()),
        }
    }

    pub fn finish(mut self, final_message: &str) -> io::Result<()> {
        self.close()?;
        if self.streamed.is_empty() {
            writeln!(self.answers, "{final_message}")?;
        }
        self.answers.flush()
    }

    fn answer(&mut self, payload: &Value, text: &str) -> io::Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        let key = key(payload);
        self.streamed.insert(key.clone());
        self.write(Block::Answer(key), text)
    }

    fn write(&mut self, block: Block, text: &str) -> io::Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        if self.block.as_ref() != Some(&block) {
            self.close()?;
        }
        if matches!(block, Block::Reasoning(_)) {
            let mut stderr = io::stderr().lock();
            stderr.write_all(text.as_bytes())?;
            stderr.flush()?;
        } else {
            self.answers.write_all(text.as_bytes())?;
            self.answers.flush()?;
        }
        self.block = Some(block);
        self.open = !text.ends_with('\n');
        Ok(())
    }

    fn notice(&mut self, line: &str) -> io::Result<()> {
        self.close()?;
        self.block = Some(Block::Notice);
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "{line}")?;
        stderr.flush()
    }

    fn close(&mut self) -> io::Result<()> {
        match self.block {
            Some(Block::Reasoning(_)) => {
                let mut stderr = io::stderr().lock();
                if self.open {
                    stderr.write_all(b"\n")?;
                }
                stderr.write_all(b"\n")?;
                stderr.flush()?;
            }
            Some(Block::Answer(_)) if self.open => {
                self.answers.write_all(b"\n")?;
                self.answers.flush()?;
            }
            _ => {}
        }
        self.block = None;
        self.open = false;
        Ok(())
    }
}

fn key(payload: &Value) -> String {
    payload["item_id"].as_str().map_or_else(
        || format!("call-{}", payload["model_call_index"]),
        str::to_owned,
    )
}
