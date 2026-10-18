use std::{borrow::Cow, rc::Rc, time::Duration};

use ratatui::{
    buffer::{Buffer, CellWidth},
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Paragraph, Widget},
};

use syntect::easy::HighlightLines;
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    beautify::beautify,
    diff,
    format::{format_duration, sanitize_terminal_text, sanitize_terminal_text_inline},
    highlight, markdown,
    theme::Theme,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Yielded,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisplayMode {
    #[default]
    Collapsed,
    Beautified,
    Raw,
}

impl DisplayMode {
    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            Self::Collapsed => Self::Beautified,
            Self::Beautified => Self::Raw,
            Self::Raw => Self::Collapsed,
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Collapsed => "collapsed",
            Self::Beautified => "beautified",
            Self::Raw => "raw",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeLevel {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolBody {
    Text,
    Code,
    Diff,
}

#[derive(Clone, Debug)]
pub struct ToolCell {
    pub call_id: String,
    pub name: String,
    pub title: String,
    pub input: String,
    pub input_body: ToolBody,
    pub output: String,
    pub output_body: ToolBody,
    pub status: ToolStatus,
    pub duration: Option<Duration>,
    pub depth: usize,
    pub raw_output: String,
    pub language: String,
    pub exit_code: Option<i64>,
    pub details: Vec<String>,
    pub children: Vec<Self>,
}

impl ToolCell {
    #[must_use]
    pub fn new(call_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            call_id: call_id.into(),
            name: name.into(),
            title: String::new(),
            input: String::new(),
            input_body: ToolBody::Text,
            output: String::new(),
            output_body: ToolBody::Text,
            status: ToolStatus::Running,
            duration: None,
            depth: 0,
            raw_output: String::new(),
            language: String::new(),
            exit_code: None,
            details: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn find_mut(&mut self, call_id: &str) -> Option<&mut Self> {
        if self.call_id == call_id {
            return Some(self);
        }
        self.children
            .iter_mut()
            .find_map(|child| child.find_mut(call_id))
    }

    #[must_use]
    pub fn contains(&self, call_id: &str) -> bool {
        self.call_id == call_id || self.children.iter().any(|child| child.contains(call_id))
    }

    fn running(&self) -> bool {
        self.status == ToolStatus::Running || self.children.iter().any(Self::running)
    }

    fn finish_running(&mut self, status: ToolStatus) {
        if self.status == ToolStatus::Running {
            self.status = status;
        }
        for child in &mut self.children {
            child.finish_running(status);
        }
    }

    fn unsuccessful(&self) -> bool {
        matches!(self.status, ToolStatus::Failed) || self.exit_code.is_some_and(|code| code != 0)
    }
}

#[derive(Clone, Debug)]
pub enum CellKind {
    User(String),
    Assistant(String),
    Reasoning(String),
    Tool(ToolCell),
    Notice(NoticeLevel, String),
    Btw {
        question: String,
        answer: String,
        done: bool,
    },
    Summary(String),
}

#[derive(Clone, Debug)]
struct Cell {
    kind: CellKind,
    revision: u64,
}

const SPINNER_SLOT: &str = "\u{0}";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CacheKey {
    width: u16,
    revision: u64,
    mode: DisplayMode,
    reasoning: bool,
}

#[derive(Clone)]
struct Layout {
    key: CacheKey,
    lines: Rc<Vec<Line<'static>>>,
    spinners: Rc<[(usize, usize)]>,
    highlights: Rc<markdown::Highlights>,
}

impl Layout {
    fn line(&self, row: usize, spinner: &str) -> Line<'static> {
        let mut line = self.lines[row].clone();
        for (_, span) in self.spinners.iter().filter(|(slot, _)| *slot == row) {
            line.spans[*span].content = Cow::Owned(spinner.to_owned());
        }
        line
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TranscriptPolicy {
    pub collapsed_preview_lines: usize,
    pub collapsed_diff_lines: usize,
}

pub struct Transcript {
    cells: Vec<Cell>,
    cache: Vec<Option<Layout>>,
    revision: u64,
    scroll: usize,
    mode: DisplayMode,
    reasoning: bool,
    policy: TranscriptPolicy,
    last_height: usize,
    last_total: Option<usize>,
    last_width: u16,
    theme: Option<Theme>,
    anchor: Option<(usize, usize)>,
}

impl Transcript {
    #[must_use]
    pub const fn new(policy: TranscriptPolicy) -> Self {
        Self {
            cells: Vec::new(),
            cache: Vec::new(),
            revision: 0,
            scroll: 0,
            mode: DisplayMode::Collapsed,
            reasoning: true,
            policy,
            last_height: 0,
            last_total: None,
            last_width: 0,
            theme: None,
            anchor: None,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.cells.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.cache.clear();
        self.scroll = 0;
        self.last_total = None;
        self.anchor = None;
    }

    pub fn push(&mut self, kind: CellKind) -> usize {
        self.revision += 1;
        self.cells.push(Cell {
            kind,
            revision: self.revision,
        });
        self.cache.push(None);
        self.last_total = None;
        self.cells.len() - 1
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&CellKind> {
        self.cells.get(index).map(|cell| &cell.kind)
    }

    pub fn update(&mut self, index: usize, change: impl FnOnce(&mut CellKind)) -> bool {
        let Some(cell) = self.cells.get_mut(index) else {
            return false;
        };
        change(&mut cell.kind);
        self.revision += 1;
        cell.revision = self.revision;
        self.last_total = None;
        true
    }

    #[must_use]
    pub fn last(&self) -> Option<(usize, &CellKind)> {
        self.cells
            .last()
            .map(|cell| (self.cells.len() - 1, &cell.kind))
    }

    pub fn invalidate(&mut self) {
        self.cache.iter_mut().for_each(|entry| *entry = None);
    }

    #[must_use]
    pub fn has_notice_since(&self, index: usize, level: NoticeLevel, text: &str) -> bool {
        self.cells.iter().skip(index).any(|cell| {
            matches!(&cell.kind, CellKind::Notice(existing, message) if *existing == level && message == text)
        })
    }

    #[must_use]
    pub fn find_tool(&self, call_id: &str) -> Option<usize> {
        self.cells
            .iter()
            .rposition(|cell| matches!(&cell.kind, CellKind::Tool(tool) if tool.contains(call_id)))
    }

    pub fn finish_running_tools(&mut self, status: ToolStatus) {
        for index in 0..self.cells.len() {
            if matches!(&self.cells[index].kind, CellKind::Tool(tool) if tool.running()) {
                self.update(index, |kind| {
                    if let CellKind::Tool(tool) = kind {
                        tool.finish_running(status);
                    }
                });
            }
        }
    }

    #[must_use]
    pub const fn expanded(&self) -> bool {
        !matches!(self.mode, DisplayMode::Collapsed)
    }

    pub const fn toggle_expanded(&mut self) {
        self.mode = if self.expanded() {
            DisplayMode::Collapsed
        } else {
            DisplayMode::Beautified
        };
        self.last_total = None;
    }

    #[must_use]
    pub const fn mode(&self) -> DisplayMode {
        self.mode
    }

    pub const fn set_mode(&mut self, mode: DisplayMode) {
        self.mode = mode;
        self.last_total = None;
    }

    pub const fn cycle_mode(&mut self) -> DisplayMode {
        self.mode = self.mode.next();
        self.last_total = None;
        self.mode
    }

    #[must_use]
    pub const fn shows_reasoning(&self) -> bool {
        self.reasoning
    }

    pub const fn toggle_reasoning(&mut self) {
        self.reasoning = !self.reasoning;
        self.last_total = None;
    }

    #[must_use]
    pub const fn is_following(&self) -> bool {
        self.scroll == 0
    }

    fn limit(&mut self, needed: usize) -> Option<usize> {
        if self.last_total.is_none() {
            let _span = tracing::info_span!(
                "tui.transcript.extent",
                cells = self.cells.len(),
                needed,
                cached = false
            )
            .entered();
            let Some(theme) = self.theme.take() else {
                return Some(0);
            };
            let mut total = 0;
            let mut index = self.cells.len();
            while index > 0 && total < needed {
                index -= 1;
                total += self.layout(index, self.last_width, &theme).lines.len();
            }
            self.theme = Some(theme);
            if index == 0 {
                self.last_total = Some(total);
            }
        }
        self.last_total
            .map(|total| total.saturating_sub(self.last_height))
    }

    #[must_use]
    pub const fn scroll_offset(&self) -> usize {
        self.scroll
    }

    pub fn scroll_up(&mut self, lines: usize) {
        let scroll = self.scroll.saturating_add(lines);
        let needed = scroll.saturating_add(self.last_height);
        self.scroll = self.limit(needed).map_or(scroll, |limit| scroll.min(limit));
    }

    pub fn scroll_down(&mut self, lines: usize) {
        if self.scroll == 0 {
            return;
        }
        let needed = self.scroll.saturating_add(self.last_height);
        let scroll = self
            .limit(needed)
            .map_or(self.scroll, |limit| self.scroll.min(limit));
        self.scroll = scroll.saturating_sub(lines);
    }

    pub const fn scroll_to_bottom(&mut self) {
        self.scroll = 0;
    }

    pub fn scroll_to_top(&mut self) {
        if let (Some(limit), Some(total)) = (self.limit(usize::MAX), self.last_total) {
            self.scroll = limit;
            self.anchor = Some((0, total));
        }
    }

    #[must_use]
    pub const fn page(&self) -> usize {
        if self.last_height > 2 {
            self.last_height - 2
        } else {
            1
        }
    }

    fn layout(&mut self, index: usize, width: u16, theme: &Theme) -> Layout {
        let cell = &self.cells[index];
        let key = CacheKey {
            width,
            revision: cell.revision,
            mode: self.mode,
            reasoning: self.reasoning,
        };
        if let Some(layout) = &self.cache[index]
            && layout.key == key
        {
            return layout.clone();
        }
        let span = tracing::info_span!(
            "tui.transcript.layout",
            cell.index = index,
            cell.revision = cell.revision,
            width,
            mode = self.mode.label(),
            layout.lines = tracing::field::Empty
        );
        let _entered = span.enter();
        let mut highlights = self.cache[index]
            .take()
            .map(|layout| Rc::unwrap_or_clone(layout.highlights))
            .unwrap_or_default();
        let mut lines = Vec::new();
        if index > 0 {
            lines.push(Line::default());
        }
        let context = RenderContext {
            width,
            theme,
            mode: self.mode,
            reasoning: self.reasoning || self.mode != DisplayMode::Collapsed,
            preview: self.policy.collapsed_preview_lines,
            diff_preview: self.policy.collapsed_diff_lines,
        };
        context.render(&cell.kind, &mut lines, &mut highlights);
        let spinners = lines
            .iter()
            .enumerate()
            .flat_map(|(row, line)| {
                line.spans
                    .iter()
                    .enumerate()
                    .filter(|(_, span)| span.content == SPINNER_SLOT)
                    .map(move |(span, _)| (row, span))
            })
            .collect::<Rc<[_]>>();
        let layout = Layout {
            key,
            lines: Rc::new(lines),
            spinners,
            highlights: Rc::new(highlights),
        };
        span.record("layout.lines", layout.lines.len());
        self.cache[index] = Some(layout.clone());
        layout
    }

    fn extend(
        &mut self,
        layouts: &mut Vec<Layout>,
        total: &mut usize,
        needed: usize,
        floor: usize,
        width: u16,
        theme: &Theme,
    ) {
        let mut index = self.cells.len() - layouts.len();
        while index > 0 && (*total < needed || index > floor) {
            index -= 1;
            let layout = self.layout(index, width, theme);
            *total += layout.lines.len();
            layouts.push(layout);
        }
    }

    pub fn render(&mut self, area: Rect, buffer: &mut Buffer, theme: &Theme, spinner: &str) {
        let span = tracing::info_span!(
            "tui.transcript.render",
            width = area.width,
            height = area.height,
            cells = self.cells.len(),
            mode = self.mode.label(),
            scroll.before = self.scroll,
            scroll.after = tracing::field::Empty,
            layout.visited_cells = tracing::field::Empty
        );
        let _entered = span.enter();
        if area.width == 0 || area.height == 0 {
            return;
        }
        let width = area.width;
        let height = usize::from(area.height);
        self.last_width = width;
        if self.theme.as_ref() != Some(theme) {
            self.cache.iter_mut().for_each(|entry| *entry = None);
            self.theme = Some(theme.clone());
        }
        let count = self.cells.len();
        let floor = match self.anchor {
            Some((first, _)) if self.scroll > 0 => first.min(count),
            _ => count,
        };
        let mut layouts = Vec::new();
        let mut total = 0;
        self.extend(
            &mut layouts,
            &mut total,
            self.scroll.saturating_add(height),
            floor,
            width,
            theme,
        );
        if self.scroll > 0
            && let Some((first, lines)) = self.anchor
            && first <= count
            && count - first <= layouts.len()
        {
            let since = layouts[..count - first]
                .iter()
                .map(|layout| layout.lines.len())
                .sum::<usize>();
            self.scroll += since.saturating_sub(lines);
        }
        self.extend(
            &mut layouts,
            &mut total,
            self.scroll.saturating_add(height),
            count,
            width,
            theme,
        );
        let first = count - layouts.len();
        span.record("layout.visited_cells", layouts.len());
        self.anchor = Some((first, total));
        self.last_total = (first == 0).then_some(total);
        self.last_height = height;
        self.scroll = self.scroll.min(total.saturating_sub(height));
        span.record("scroll.after", self.scroll);
        let end = total - self.scroll;
        let start = end.saturating_sub(height);
        let mut rows = area
            .intersection(buffer.area)
            .rows()
            .filter(|row| !row.is_empty());
        let mut offset = 0;
        'layouts: for layout in layouts.iter().rev() {
            let count = layout.lines.len();
            let next = offset + count;
            if next > start && offset < end {
                let from = start.saturating_sub(offset);
                let to = (end - offset).min(count);
                for row in from..to {
                    let Some(row_area) = rows.next() else {
                        break 'layouts;
                    };
                    if layout.spinners.iter().any(|(slot, _)| *slot == row) {
                        render_line(&layout.line(row, spinner), row_area, buffer);
                    } else {
                        render_line(&layout.lines[row], row_area, buffer);
                    }
                }
            }
            offset = next;
            if offset >= end {
                break;
            }
        }
        if self.scroll > 0 {
            let label = format!(" ↓ {} more lines · end to follow ", self.scroll);
            let label_width = u16::try_from(label.chars().count())
                .unwrap_or(u16::MAX)
                .min(area.width);
            let x = area.x + area.width - label_width;
            let visible =
                Rect::new(x, area.y + area.height - 1, label_width, 1).intersection(buffer.area);
            if !visible.is_empty() {
                buffer.set_stringn(
                    visible.x,
                    visible.y,
                    label
                        .chars()
                        .skip(usize::from(visible.x - x))
                        .collect::<String>(),
                    usize::from(visible.width),
                    Style::default()
                        .fg(theme.code_background())
                        .bg(theme.accent()),
                );
            }
        }
    }
}

struct Gutter {
    first: String,
    rest: String,
    style: Style,
}

struct RenderContext<'a> {
    width: u16,
    theme: &'a Theme,
    mode: DisplayMode,
    reasoning: bool,
    preview: usize,
    diff_preview: usize,
}

impl RenderContext<'_> {
    fn render(
        &self,
        kind: &CellKind,
        lines: &mut Vec<Line<'static>>,
        highlights: &mut markdown::Highlights,
    ) {
        let theme = self.theme;
        match kind {
            CellKind::User(text) => {
                let body = markdown::wrap_plain_preserving_whitespace(
                    &sanitize_terminal_text(text),
                    self.width.saturating_sub(2),
                    Style::default()
                        .fg(theme.text())
                        .add_modifier(Modifier::BOLD),
                );
                prefixed(
                    lines,
                    body,
                    Span::styled(
                        "› ",
                        Style::default()
                            .fg(theme.accent())
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("  "),
                );
            }
            CellKind::Assistant(text) => {
                let body =
                    markdown::render_with(text, self.width.saturating_sub(2), theme, highlights)
                        .lines;
                prefixed(
                    lines,
                    body,
                    Span::styled("● ", Style::default().fg(theme.text())),
                    Span::raw("  "),
                );
            }
            CellKind::Reasoning(text) => {
                let style = Style::default()
                    .fg(theme.muted())
                    .add_modifier(Modifier::ITALIC);
                if self.reasoning {
                    let body = markdown::wrap_plain(
                        &sanitize_terminal_text(text.trim()),
                        self.width.saturating_sub(2),
                        style,
                    );
                    prefixed(lines, body, Span::styled("∴ ", style), Span::raw("  "));
                } else {
                    let summary = sanitize_terminal_text_inline(
                        text.trim().lines().next().unwrap_or_default(),
                    )
                    .into_owned();
                    let summary = summary.trim_matches('*').trim().to_owned();
                    let mut collapsed = vec![Span::styled("∴ thinking", style)];
                    if !summary.is_empty() {
                        collapsed.push(Span::styled(format!(" · {summary}"), style));
                    }
                    collapsed.push(Span::styled(
                        "  (ctrl+o)",
                        Style::default().fg(theme.border()),
                    ));
                    let line = Line::from(collapsed);
                    lines.extend(
                        markdown::wrap_spans(&line.spans, self.width, true)
                            .into_iter()
                            .take(1),
                    );
                }
            }
            CellKind::Tool(tool) => self.tool(tool, tool.depth, false, lines),
            CellKind::Notice(level, text) => {
                let (icon, color) = match level {
                    NoticeLevel::Info => ("ℹ ", theme.muted()),
                    NoticeLevel::Warning => ("⚠ ", theme.thinking_high()),
                    NoticeLevel::Error => ("✗ ", theme.thinking_max()),
                };
                let body = markdown::wrap_plain(
                    &sanitize_terminal_text(text),
                    self.width.saturating_sub(2),
                    Style::default().fg(color),
                );
                prefixed(
                    lines,
                    body,
                    Span::styled(icon, Style::default().fg(color)),
                    Span::raw("  "),
                );
            }
            CellKind::Btw {
                question,
                answer,
                done,
            } => {
                let bar = Style::default().fg(theme.thinking_medium());
                let mut header = vec![
                    Span::styled("┃ ", bar),
                    Span::styled("btw", bar.add_modifier(Modifier::BOLD)),
                ];
                if !*done {
                    header.push(Span::raw(" "));
                    header.push(Span::styled(SPINNER_SLOT, bar.add_modifier(Modifier::BOLD)));
                }
                header.push(Span::styled(
                    format!("  {}", sanitize_terminal_text_inline(question)),
                    Style::default().fg(theme.muted()),
                ));
                lines.push(Line::from(header));
                if !answer.is_empty() {
                    let body = markdown::render_with(
                        answer,
                        self.width.saturating_sub(2),
                        theme,
                        highlights,
                    )
                    .lines;
                    prefixed(
                        lines,
                        body,
                        Span::styled("┃ ", bar),
                        Span::styled("┃ ", bar),
                    );
                }
            }
            CellKind::Summary(text) => {
                let label = format!(" {} ", sanitize_terminal_text_inline(text));
                let fill = usize::from(self.width).saturating_sub(label.chars().count() + 2);
                lines.push(Line::from(vec![
                    Span::styled("──", Style::default().fg(theme.border())),
                    Span::styled(label, Style::default().fg(theme.muted())),
                    Span::styled("─".repeat(fill), Style::default().fg(theme.border())),
                ]));
            }
        }
    }

    fn tool(&self, tool: &ToolCell, depth: usize, nested: bool, lines: &mut Vec<Line<'static>>) {
        let theme = self.theme;
        let indent = "  ".repeat(depth);
        let (icon, color) = match tool.status {
            ToolStatus::Running => (SPINNER_SLOT, theme.accent()),
            ToolStatus::Completed if tool.unsuccessful() => ("✗", theme.thinking_max()),
            ToolStatus::Completed => ("✓", theme.thinking_low()),
            ToolStatus::Failed => ("✗", theme.thinking_max()),
            ToolStatus::Cancelled => ("⊘", theme.muted()),
            ToolStatus::Yielded => ("↻", theme.accent()),
        };
        let muted = Style::default().fg(theme.muted());
        let mut header = vec![
            Span::raw(indent.clone()),
            Span::styled(icon, Style::default().fg(color)),
            Span::raw(" "),
            Span::styled(
                sanitize_terminal_text_inline(&tool.name).into_owned(),
                Style::default()
                    .fg(theme.text())
                    .add_modifier(Modifier::BOLD),
            ),
        ];
        if !tool.title.is_empty() {
            header.push(Span::styled(
                format!(" {}", sanitize_terminal_text_inline(&tool.title)),
                muted,
            ));
        }
        if let Some(duration) = tool.duration {
            header.push(Span::styled(
                format!(
                    " · {}",
                    format_duration(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
                ),
                muted,
            ));
        }
        if let Some(code) = tool.exit_code.filter(|code| *code != 0) {
            header.push(Span::styled(
                format!(" · exit {code}"),
                Style::default().fg(theme.thinking_max()),
            ));
        }
        for detail in &tool.details {
            header.push(Span::styled(
                format!(" · {}", sanitize_terminal_text_inline(detail)),
                muted,
            ));
        }
        lines.push(Line::from(header));
        let collapsed = self.mode == DisplayMode::Collapsed;
        let inner = self
            .width
            .saturating_sub(u16::try_from(indent.len() + 4).unwrap_or(u16::MAX));
        let gutter = Style::default().fg(theme.border());
        let show_input = !nested || !collapsed || tool.input_body == ToolBody::Diff;
        if show_input && !tool.input.trim().is_empty() {
            let input = match (tool.input_body, self.mode) {
                (ToolBody::Code, DisplayMode::Collapsed | DisplayMode::Beautified)
                    if matches!(tool.language.as_str(), "js" | "javascript") =>
                {
                    Cow::Owned(beautify(&tool.input, "  "))
                }
                _ => Cow::Borrowed(tool.input.as_str()),
            };
            let body = self.body(&input, tool.input_body, &tool.language, inner);
            let gutter = Gutter {
                first: format!("{indent}  │ "),
                rest: format!("{indent}  │ "),
                style: gutter,
            };
            self.preview(lines, body, self.limit(tool.input_body), &gutter, false);
        }
        for child in &tool.children {
            self.tool(child, depth + 1, true, lines);
        }
        let output = match self.mode {
            DisplayMode::Raw if !tool.raw_output.is_empty() => tool.raw_output.as_str(),
            _ => tool.output.as_str(),
        };
        let show_output = !nested || !collapsed || tool.unsuccessful();
        if show_output && !output.trim().is_empty() {
            let body = if self.mode == DisplayMode::Raw {
                self.body(output, ToolBody::Text, "", inner)
            } else {
                self.body(output, tool.output_body, &tool.language, inner)
            };
            let gutter = Gutter {
                first: format!("{indent}  └ "),
                rest: format!("{indent}    "),
                style: gutter,
            };
            self.preview(lines, body, self.limit(tool.output_body), &gutter, true);
        }
    }

    fn body(&self, text: &str, body: ToolBody, language: &str, width: u16) -> Vec<Line<'static>> {
        let _span = tracing::info_span!(
            "tui.tool.format",
            text.bytes = text.len(),
            ?body,
            width,
            mode = self.mode.label()
        )
        .entered();
        let text = sanitize_terminal_text(text.trim_end());
        match body {
            ToolBody::Diff => diff::render(&text, width, self.theme),
            ToolBody::Code => code_lines(&text, language, width),
            ToolBody::Text => markdown::wrap_plain_preserving_whitespace(
                &text,
                width,
                Style::default().fg(self.theme.muted()),
            ),
        }
    }

    const fn limit(&self, body: ToolBody) -> usize {
        match body {
            ToolBody::Diff => self.diff_preview,
            ToolBody::Text | ToolBody::Code => self.preview,
        }
    }

    fn preview(
        &self,
        lines: &mut Vec<Line<'static>>,
        body: Vec<Line<'static>>,
        preview: usize,
        gutter: &Gutter,
        tail: bool,
    ) {
        let total = body.len();
        let limit = if self.mode == DisplayMode::Collapsed {
            preview.min(total)
        } else {
            total
        };
        let hidden = total - limit;
        let mut shown: Vec<Line<'static>> = if tail {
            body.into_iter().skip(hidden).collect()
        } else {
            body.into_iter().take(limit).collect()
        };
        if hidden > 0 {
            let hint = Line::styled(
                format!("… {hidden} more lines (ctrl+o to expand)"),
                Style::default()
                    .fg(self.theme.muted())
                    .add_modifier(Modifier::ITALIC),
            );
            if tail {
                shown.insert(0, hint);
            } else {
                shown.push(hint);
            }
        }
        prefixed(
            lines,
            shown,
            Span::styled(gutter.first.clone(), gutter.style),
            Span::styled(gutter.rest.clone(), gutter.style),
        );
    }
}

fn code_lines(text: &str, language: &str, width: u16) -> Vec<Line<'static>> {
    let _span = tracing::info_span!(
        "tui.code.highlight",
        text.bytes = text.len(),
        width,
        language
    )
    .entered();
    let assets = highlight::assets();
    let syntax = highlight::syntax_for_token(&assets.syntaxes, language);
    let mut highlighter = HighlightLines::new(syntax, highlight::theme());
    text.lines()
        .flat_map(|line| {
            highlight::wrap(
                highlight::line(&mut highlighter, line, &assets.syntaxes),
                width,
            )
            .into_iter()
            .map(Line::from)
        })
        .collect()
}

fn render_line(line: &Line<'_>, area: Rect, buffer: &mut Buffer) {
    if line
        .alignment
        .is_some_and(|alignment| alignment != Alignment::Left)
    {
        Paragraph::new(line.clone()).render(area, buffer);
        return;
    }
    let start = buffer.index_of(area.x, area.y);
    let cells = &mut buffer.content[start..start + usize::from(area.width)];
    let mut used = 0;
    for span in &line.spans {
        let style = line.style.patch(span.style);
        let content = span.content.as_ref();
        if content.bytes().all(|byte| matches!(byte, b' '..=b'~')) {
            for index in 0..content.len() {
                let Some(cell) = cells.get_mut(used) else {
                    return;
                };
                cell.set_symbol(&content[index..=index]).set_style(style);
                used += 1;
            }
            continue;
        }
        for grapheme in content
            .graphemes(true)
            .filter(|grapheme| !grapheme.contains(char::is_control))
        {
            let width = usize::from(grapheme.cell_width());
            if width > cells.len() {
                continue;
            }
            if used + width > cells.len() {
                return;
            }
            if width > 0 {
                cells[used].set_symbol(grapheme).set_style(style);
            }
            used += width;
        }
    }
}

fn prefixed(
    lines: &mut Vec<Line<'static>>,
    body: Vec<Line<'static>>,
    first: Span<'static>,
    rest: Span<'static>,
) {
    for (index, line) in body.into_iter().enumerate() {
        let prefix = if index == 0 {
            first.clone()
        } else {
            rest.clone()
        };
        let mut spans = Vec::with_capacity(line.spans.len() + 1);
        spans.push(prefix);
        spans.extend(line.spans);
        lines.push(Line::from(spans).style(line.style));
    }
}
