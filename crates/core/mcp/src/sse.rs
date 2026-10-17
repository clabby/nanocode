use std::fmt;

const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Event {
    pub event: Option<String>,
    pub data: Option<String>,
    pub id: Option<String>,
    pub retry: Option<u64>,
}

impl Event {
    #[must_use]
    pub fn is_message(&self) -> bool {
        matches!(self.event.as_deref(), None | Some("" | "message"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventTooLarge {
    pub max_size: usize,
}

impl fmt::Display for EventTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SSE event exceeded the maximum size of {} bytes",
            self.max_size
        )
    }
}

impl std::error::Error for EventTooLarge {}

pub struct Parser {
    max_event_size: usize,
    line: Vec<u8>,
    retained_size: usize,
    previous_was_cr: bool,
    started: bool,
    bom_matched: usize,
    current: Event,
    has_fields: bool,
}

impl Parser {
    #[must_use]
    pub const fn new(max_event_size: usize) -> Self {
        Self {
            max_event_size,
            line: Vec::new(),
            retained_size: 0,
            previous_was_cr: false,
            started: false,
            bom_matched: 0,
            current: Event {
                event: None,
                data: None,
                id: None,
                retry: None,
            },
            has_fields: false,
        }
    }

    pub fn feed(&mut self, chunk: &[u8], events: &mut Vec<Event>) -> Result<(), EventTooLarge> {
        let mut offset = 0;
        while offset < chunk.len() && !self.started {
            let byte = chunk[offset];
            offset += 1;
            if byte == BOM[self.bom_matched] {
                self.bom_matched += 1;
                self.started = self.bom_matched == BOM.len();
                continue;
            }
            self.started = true;
            for &prefix in &BOM[..self.bom_matched] {
                self.push_byte(prefix, events)?;
            }
            self.push_byte(byte, events)?;
        }
        while offset < chunk.len() {
            if self.previous_was_cr {
                self.previous_was_cr = false;
                if chunk[offset] == b'\n' {
                    offset += 1;
                    continue;
                }
            }
            let remaining = &chunk[offset..];
            let available = self
                .max_event_size
                .saturating_sub(self.retained_size.saturating_add(self.line.len()));
            let scan = &remaining[..remaining.len().min(available.saturating_add(1))];
            let length = memchr::memchr2(b'\r', b'\n', scan).unwrap_or(scan.len());
            if length > available {
                self.line
                    .extend_from_slice(&remaining[..available.saturating_add(1)]);
                return self.check_limit();
            }
            self.line.extend_from_slice(&remaining[..length]);
            offset += length;
            if offset < chunk.len() {
                self.push_byte(chunk[offset], events)?;
                offset += 1;
            }
        }
        Ok(())
    }

    fn push_byte(&mut self, byte: u8, events: &mut Vec<Event>) -> Result<(), EventTooLarge> {
        if self.previous_was_cr {
            self.previous_was_cr = false;
            if byte == b'\n' {
                return Ok(());
            }
        }
        match byte {
            b'\r' => {
                self.finish_line(events)?;
                self.previous_was_cr = true;
            }
            b'\n' => self.finish_line(events)?,
            _ => {
                self.line.push(byte);
                self.check_limit()?;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self, events: &mut Vec<Event>) -> Result<(), EventTooLarge> {
        if !self.started {
            self.started = true;
            for &prefix in &BOM[..self.bom_matched] {
                self.push_byte(prefix, events)?;
            }
        }
        if !self.line.is_empty() {
            self.finish_line(events)?;
        }
        Ok(())
    }

    const fn check_limit(&self) -> Result<(), EventTooLarge> {
        if self.retained_size.saturating_add(self.line.len()) > self.max_event_size {
            Err(EventTooLarge {
                max_size: self.max_event_size,
            })
        } else {
            Ok(())
        }
    }

    fn finish_line(&mut self, events: &mut Vec<Event>) -> Result<(), EventTooLarge> {
        let mut line = std::mem::take(&mut self.line);
        let result = self.process_line(&line, events);
        line.clear();
        self.line = line;
        result
    }

    fn process_line(&mut self, line: &[u8], events: &mut Vec<Event>) -> Result<(), EventTooLarge> {
        if line.is_empty() {
            self.retained_size = 0;
            if std::mem::take(&mut self.has_fields) {
                events.push(std::mem::take(&mut self.current));
            }
            return Ok(());
        }
        if line[0] == b':' {
            return Ok(());
        }
        self.retained_size = self
            .retained_size
            .saturating_add(line.len())
            .saturating_add(1);
        self.check_limit()?;
        let line = String::from_utf8_lossy(line);
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line.as_ref(), ""),
        };
        match field {
            "data" => {
                self.has_fields = true;
                match &mut self.current.data {
                    Some(data) => {
                        data.push('\n');
                        data.push_str(value);
                    }
                    None => self.current.data = Some(value.to_owned()),
                }
            }
            "event" => {
                self.has_fields = true;
                self.current.event = Some(value.to_owned());
            }
            "id" if !value.contains('\0') => {
                self.has_fields = true;
                self.current.id = Some(value.to_owned());
            }
            "retry" => {
                if let Ok(retry) = value.parse::<u64>() {
                    self.has_fields = true;
                    self.current.retry = Some(retry);
                }
            }
            _ => {}
        }
        Ok(())
    }
}
