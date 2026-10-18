use std::{
    io::{self, Stdout, Write, stdout},
    panic,
    sync::{
        Once,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{
        DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        KeyboardEnhancementFlags, MouseButton, MouseEvent, MouseEventKind,
        PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute, queue,
    style::{Attribute, SetAttribute},
    terminal::{
        BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate, EnterAlternateScreen,
        LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    },
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    layout::{Position, Rect},
    style::Color,
};

use crate::selection::Selection;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static MODES: AtomicU8 = AtomicU8::new(0);
static PANIC_HOOK: Once = Once::new();

const MOUSE: u8 = 1;
const KEYBOARD: u8 = 2;

#[derive(Clone, Copy, Debug)]
pub struct TerminalOptions {
    pub mouse_capture: bool,
    pub keyboard_enhancement: bool,
}

impl Default for TerminalOptions {
    fn default() -> Self {
        Self {
            mouse_capture: true,
            keyboard_enhancement: true,
        }
    }
}

pub struct Canvas<'a> {
    area: Rect,
    buffer: &'a mut Buffer,
    cursor: Option<Position>,
    regions: &'a mut Vec<Rect>,
}

impl Canvas<'_> {
    #[must_use]
    pub const fn area(&self) -> Rect {
        self.area
    }

    pub const fn buffer_mut(&mut self) -> &mut Buffer {
        self.buffer
    }

    pub fn set_cursor_position(&mut self, position: impl Into<Position>) {
        self.cursor = Some(position.into());
    }

    pub fn selectable(&mut self, area: Rect) {
        self.regions.push(area.intersection(self.area));
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrameStats {
    pub frames: u64,
    pub skipped: u64,
    pub full_repaints: u64,
    pub bytes: u64,
}

#[derive(Debug)]
pub enum SelectionEvent {
    Ignored,
    Changed,
    Copied(String),
    Failed(io::Error),
}

pub struct TerminalSession {
    output: Stdout,
    previous: Buffer,
    current: Buffer,
    stale: bool,
    cursor: Option<Position>,
    bytes: Vec<u8>,
    truecolor: bool,
    monochrome: bool,
    stats: FrameStats,
    options: TerminalOptions,
    active: bool,
    regions: Vec<Rect>,
    selection: Option<Selection>,
}

impl TerminalSession {
    pub fn enter(options: TerminalOptions) -> io::Result<Self> {
        install_panic_hook();
        enable_raw_mode()?;
        let mut output = stdout();
        if let Err(error) = activate(&mut output, options) {
            restore(&mut output);
            return Err(error);
        }
        Ok(Self {
            output,
            previous: Buffer::empty(Rect::default()),
            current: Buffer::empty(Rect::default()),
            stale: true,
            cursor: None,
            bytes: Vec::new(),
            truecolor: truecolor(),
            monochrome: crossterm::style::Colored::ansi_color_disabled_memoized(),
            stats: FrameStats::default(),
            options,
            active: true,
            regions: Vec::new(),
            selection: None,
        })
    }

    pub fn draw(&mut self, render: impl FnOnce(&mut Canvas<'_>)) -> io::Result<()> {
        let (columns, rows) = crossterm::terminal::size()?;
        let span = tracing::info_span!(
            "tui.terminal.draw",
            width = columns,
            height = rows,
            repaint = tracing::field::Empty,
            frame.skipped = false,
            frame.bytes = tracing::field::Empty,
            frame.changed_cells = tracing::field::Empty
        );
        let _entered = span.enter();
        let area = Rect::new(0, 0, columns, rows);
        if area != self.current.area {
            self.previous = Buffer::empty(area);
            self.current = Buffer::empty(area);
            self.stale = true;
            self.selection = None;
        }
        span.record("repaint", self.stale);
        self.current.reset();
        self.regions.clear();
        self.regions.push(area);
        let mut canvas = Canvas {
            area,
            buffer: &mut self.current,
            cursor: None,
            regions: &mut self.regions,
        };
        render(&mut canvas);
        let cursor = canvas.cursor.filter(|position| area.contains(*position));
        if let Some(selection) = &mut self.selection
            && selection.rebind(&self.regions)
        {
            selection.highlight(&mut self.current);
        } else {
            self.selection = None;
        }
        if self.monochrome {
            monochrome(&mut self.current);
        } else if !self.truecolor {
            downsample(&mut self.current);
        }
        if self.stale {
            self.previous.reset();
        }
        let diff_encode = tracing::info_span!("tui.terminal.diff_encode").entered();
        let mut changed_cells = 0;
        let mut updates = self
            .previous
            .diff_iter(&self.current)
            .inspect(|_| changed_cells += 1)
            .peekable();
        let has_updates = updates.peek().is_some();
        if !self.stale && !has_updates && cursor == self.cursor {
            span.record("frame.changed_cells", 0);
            span.record("frame.skipped", true);
            self.stats.skipped += 1;
            std::mem::swap(&mut self.previous, &mut self.current);
            return Ok(());
        }
        self.bytes.clear();
        queue!(self.bytes, BeginSynchronizedUpdate)?;
        if self.stale {
            queue!(self.bytes, SetAttribute(Attribute::Reset))?;
            for row in area.top()..area.bottom() {
                queue!(self.bytes, MoveTo(0, row), Clear(ClearType::CurrentLine))?;
            }
        }
        if has_updates {
            if self.cursor.is_some() {
                queue!(self.bytes, Hide)?;
            }
            CrosstermBackend::new(&mut self.bytes).draw(updates)?;
        }
        span.record("frame.changed_cells", changed_cells);
        match cursor {
            Some(position) => queue!(self.bytes, MoveTo(position.x, position.y), Show)?,
            None if self.cursor.is_some() => queue!(self.bytes, Hide)?,
            None => {}
        }
        queue!(self.bytes, EndSynchronizedUpdate)?;
        drop(diff_encode);
        span.record("frame.bytes", self.bytes.len());
        tracing::info_span!("tui.terminal.write", bytes = self.bytes.len()).in_scope(|| {
            self.output.write_all(&self.bytes)?;
            self.output.flush()
        })?;
        self.stats.frames += 1;
        self.stats.bytes += self.bytes.len() as u64;
        if self.stale {
            self.stats.full_repaints += 1;
        }
        self.stale = false;
        self.cursor = cursor;
        std::mem::swap(&mut self.previous, &mut self.current);
        Ok(())
    }

    #[must_use]
    pub const fn stats(&self) -> FrameStats {
        self.stats
    }

    pub const fn clear(&mut self) -> io::Result<()> {
        self.stale = true;
        Ok(())
    }

    pub fn suspend(&mut self) -> io::Result<()> {
        if self.active {
            restore(&mut self.output);
            self.active = false;
            self.selection = None;
        }
        Ok(())
    }

    pub fn resume(&mut self) -> io::Result<()> {
        if !self.active {
            enable_raw_mode()?;
            activate(&mut self.output, self.options)?;
            self.stale = true;
            self.cursor = None;
            self.active = true;
        }
        Ok(())
    }

    pub fn set_mouse_capture(&mut self, enabled: bool) -> io::Result<()> {
        if enabled == self.options.mouse_capture {
            return Ok(());
        }
        self.options.mouse_capture = enabled;
        if enabled {
            execute!(self.output, EnableMouseCapture)?;
            MODES.fetch_or(MOUSE, Ordering::AcqRel);
        } else {
            execute!(self.output, DisableMouseCapture)?;
            MODES.fetch_and(!MOUSE, Ordering::AcqRel);
            self.selection = None;
        }
        Ok(())
    }

    pub fn select(&mut self, mouse: MouseEvent) -> SelectionEvent {
        let position = Position::new(mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let visible = self.selection.is_some_and(|selection| selection.active());
                self.selection = Selection::begin(&self.regions, position);
                if visible {
                    SelectionEvent::Changed
                } else {
                    SelectionEvent::Ignored
                }
            }
            MouseEventKind::Drag(MouseButton::Left)
                if self
                    .selection
                    .as_mut()
                    .is_some_and(|selection| selection.extend(position)) =>
            {
                SelectionEvent::Changed
            }
            MouseEventKind::Up(_) => match self.selection.take() {
                Some(selection) if selection.active() => {
                    let text = selection.text(&self.previous);
                    if text.is_empty() {
                        return SelectionEvent::Changed;
                    }
                    match self.copy(&text) {
                        Ok(()) => SelectionEvent::Copied(text),
                        Err(error) => SelectionEvent::Failed(error),
                    }
                }
                _ => SelectionEvent::Ignored,
            },
            MouseEventKind::Moved => match self.selection.take() {
                Some(selection) if selection.active() => SelectionEvent::Changed,
                _ => SelectionEvent::Ignored,
            },
            _ => SelectionEvent::Ignored,
        }
    }

    pub fn copy(&mut self, text: &str) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        if !["SSH_TTY", "SSH_CONNECTION", "SSH_CLIENT"]
            .into_iter()
            .any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()))
        {
            use std::process::{Command, Stdio};

            let mut child = Command::new("pbcopy")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?;
            let written = child
                .stdin
                .take()
                .expect("pbcopy stdin is piped")
                .write_all(text.as_bytes());
            let output = child.wait_with_output()?;
            if !output.status.success() {
                return Err(io::Error::other(format!(
                    "pbcopy exited with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            return written;
        }
        execute!(
            self.output,
            crossterm::clipboard::CopyToClipboard::to_clipboard_from(text)
        )
    }

    #[must_use]
    pub const fn mouse_capture(&self) -> bool {
        self.options.mouse_capture
    }

    #[must_use]
    pub const fn truecolor(&self) -> bool {
        self.truecolor
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        if self.active {
            restore(&mut self.output);
        }
    }
}

fn truecolor() -> bool {
    std::env::var("COLORTERM").is_ok_and(|value| {
        let value = value.to_ascii_lowercase();
        value.contains("truecolor") || value.contains("24bit")
    })
}

fn monochrome(buffer: &mut Buffer) {
    for cell in &mut buffer.content {
        cell.fg = Color::Reset;
        cell.bg = Color::Reset;
    }
}

fn downsample(buffer: &mut Buffer) {
    let mut foreground = None;
    let mut background = None;
    for cell in &mut buffer.content {
        if let Color::Rgb(red, green, blue) = cell.fg {
            cell.fg = indexed_color(&mut foreground, red, green, blue);
        }
        if let Color::Rgb(red, green, blue) = cell.bg {
            cell.bg = indexed_color(&mut background, red, green, blue);
        }
    }
}

fn indexed_color(cache: &mut Option<((u8, u8, u8), Color)>, red: u8, green: u8, blue: u8) -> Color {
    let rgb = (red, green, blue);
    if let Some((previous, indexed)) = *cache
        && previous == rgb
    {
        return indexed;
    }
    let indexed = Color::Indexed(ansi256(red, green, blue));
    *cache = Some((rgb, indexed));
    indexed
}

const fn ansi256(red: u8, green: u8, blue: u8) -> u8 {
    const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    const fn level(value: u8) -> u8 {
        if value < 48 {
            0
        } else if value < 115 {
            1
        } else {
            (value - 35) / 40
        }
    }
    const fn distance(left: (u8, u8, u8), right: (u8, u8, u8)) -> u32 {
        let red = left.0.abs_diff(right.0) as u32;
        let green = left.1.abs_diff(right.1) as u32;
        let blue = left.2.abs_diff(right.2) as u32;
        red * red + green * green + blue * blue
    }
    let (r, g, b) = (level(red), level(green), level(blue));
    let cube = (LEVELS[r as usize], LEVELS[g as usize], LEVELS[b as usize]);
    let average = (red as u32 + green as u32 + blue as u32) / 3;
    let gray_index = if average > 238 {
        23
    } else if average < 8 {
        0
    } else {
        (average - 8) / 10
    };
    let gray_level = (8 + gray_index * 10) as u8;
    let gray = (gray_level, gray_level, gray_level);
    if distance((red, green, blue), gray) < distance((red, green, blue), cube) {
        232 + gray_index as u8
    } else {
        16 + 36 * r + 6 * g + b
    }
}

fn activate(output: &mut impl Write, options: TerminalOptions) -> io::Result<()> {
    ACTIVE.store(true, Ordering::Release);
    queue!(output, EnterAlternateScreen, EnableBracketedPaste, Hide)?;
    let mut modes = 0;
    if options.mouse_capture {
        queue!(output, EnableMouseCapture)?;
        modes |= MOUSE;
    }
    if options.keyboard_enhancement
        && queue!(
            output,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )
        .is_ok()
    {
        modes |= KEYBOARD;
    }
    output.flush()?;
    MODES.store(modes, Ordering::Release);
    Ok(())
}

fn restore(output: &mut impl Write) {
    ACTIVE.store(false, Ordering::Release);
    let modes = MODES.swap(0, Ordering::AcqRel);
    drop(disable_raw_mode());
    if modes & KEYBOARD != 0 {
        drop(queue!(output, PopKeyboardEnhancementFlags));
    }
    if modes & MOUSE != 0 {
        drop(queue!(output, DisableMouseCapture));
    }
    drop(queue!(
        output,
        EndSynchronizedUpdate,
        DisableBracketedPaste,
        LeaveAlternateScreen,
        Show
    ));
    drop(output.flush());
}

fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if ACTIVE.load(Ordering::Acquire) {
                restore(&mut stdout());
            }
            previous(info);
        }));
    });
}
