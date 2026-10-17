use crate::{format::sanitize_terminal_text, theme::Theme};
use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::sync::Arc;
use syntect::{easy::HighlightLines, highlighting::HighlightState, parsing::ParseState};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub struct Layout {
    pub lines: Vec<Line<'static>>,
    pub links: Vec<Vec<LinkSpan>>,
}

#[derive(Clone)]
pub struct LinkSpan {
    pub destination: Arc<str>,
    pub start: u16,
    pub end: u16,
}

#[derive(Clone, Default)]
pub(crate) struct Highlights {
    blocks: Vec<HighlightedBlock>,
}

#[derive(Clone)]
struct HighlightedBlock {
    language: Option<String>,
    code: String,
    lines: Vec<Vec<Span<'static>>>,
    resume: Option<Resume>,
}

#[derive(Clone)]
struct Resume {
    offset: usize,
    lines: usize,
    highlight: HighlightState,
    parse: ParseState,
}

pub fn render(markdown: &str, width: u16, theme: &Theme) -> Layout {
    render_with(markdown, width, theme, &mut Highlights::default())
}

pub(crate) fn render_with(
    markdown: &str,
    width: u16,
    theme: &Theme,
    highlights: &mut Highlights,
) -> Layout {
    let span = tracing::info_span!(
        "tui.markdown.render",
        width,
        text.bytes = markdown.len(),
        code.blocks = tracing::field::Empty,
        code.highlighted = tracing::field::Empty
    );
    let _entered = span.enter();
    if width == 0 {
        return Layout {
            lines: Vec::new(),
            links: Vec::new(),
        };
    }
    let options = Options::ENABLE_TABLES
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_SMART_PUNCTUATION
        | Options::ENABLE_GFM;
    let mut renderer = Renderer::new(width, theme, highlights);
    let mut events = Parser::new_ext(markdown, options).peekable();
    while let Some(event) = events.next() {
        match event {
            Event::Start(Tag::CodeBlock(kind)) => {
                renderer.flush();
                renderer.code_block(kind, &mut events);
            }
            Event::Start(Tag::Table(_)) => {
                renderer.flush();
                renderer.table(&mut events);
            }
            event => renderer.event(event),
        }
    }
    span.record("code.blocks", renderer.block);
    span.record("code.highlighted", renderer.highlighted);
    renderer.finish()
}

pub fn wrap_plain(text: &str, width: u16, style: Style) -> Vec<Line<'static>> {
    wrap_plain_with_whitespace(text, width, style, false)
}

pub fn wrap_plain_preserving_whitespace(
    text: &str,
    width: u16,
    style: Style,
) -> Vec<Line<'static>> {
    wrap_plain_with_whitespace(text, width, style, true)
}

fn wrap_plain_with_whitespace(
    text: &str,
    width: u16,
    style: Style,
    preserve_whitespace: bool,
) -> Vec<Line<'static>> {
    if width == 0 {
        return vec![Line::default()];
    }
    let logical = sanitize_terminal_text(text);
    if let Some(lines) = wrap_ascii_spans(
        std::iter::once((logical.as_ref(), style, None)),
        width,
        true,
        preserve_whitespace,
    ) {
        return if lines.is_empty() {
            vec![Line::default()]
        } else {
            lines.into_iter().map(|(line, _)| line).collect()
        };
    }
    let mut lines = Vec::new();
    for line in logical.split('\n') {
        let spans = vec![Span::styled(line, style)];
        lines.extend(wrap_spans_with_whitespace(
            &spans,
            width,
            true,
            preserve_whitespace,
        ));
    }
    if lines.is_empty() {
        lines.push(Line::default());
    }
    lines
}

pub fn sanitize(text: &str) -> String {
    sanitize_terminal_text(text).into_owned()
}

struct Renderer<'a> {
    width: u16,
    theme: &'a Theme,
    lines: Vec<Line<'static>>,
    current: Vec<TaggedSpan>,
    styles: Vec<Style>,
    lists: Vec<ListState>,
    quote_depth: usize,
    links: Vec<LinkState>,
    rendered_links: Vec<(usize, LinkSpan)>,
    image: Option<LinkState>,
    highlights: &'a mut Highlights,
    block: usize,
    highlighted: usize,
}

struct ListState {
    next: Option<u64>,
}

struct LinkState {
    destination: Arc<str>,
    label: String,
}

struct TaggedSpan {
    span: Span<'static>,
    link: Option<Arc<str>>,
}

impl<'a> Renderer<'a> {
    fn new(width: u16, theme: &'a Theme, highlights: &'a mut Highlights) -> Self {
        Self {
            width,
            theme,
            lines: Vec::new(),
            current: Vec::new(),
            styles: vec![Style::default().fg(theme.text())],
            lists: Vec::new(),
            quote_depth: 0,
            links: Vec::new(),
            rendered_links: Vec::new(),
            image: None,
            highlights,
            block: 0,
            highlighted: 0,
        }
    }

    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) => self.text(&text),
            Event::Code(code) => self.span(
                &code,
                Style::default()
                    .fg(self.theme.code_text())
                    .bg(self.theme.code_background()),
            ),
            Event::InlineMath(math) => self.span(
                &format!("${}$", sanitize(&math)),
                Style::default().fg(self.theme.thinking_high()),
            ),
            Event::DisplayMath(math) => {
                self.flush();
                self.span(
                    &format!("  $${}$$", sanitize(&math)),
                    Style::default().fg(self.theme.thinking_high()),
                );
                self.flush();
                self.blank();
            }
            Event::Html(html) | Event::InlineHtml(html) => {
                self.span(&html, Style::default().fg(self.theme.muted()));
            }
            Event::FootnoteReference(reference) => self.span(
                &format!("[{}]", sanitize(&reference)),
                Style::default().fg(self.theme.thinking_medium()),
            ),
            Event::SoftBreak => self.text(" "),
            Event::HardBreak => self.flush(),
            Event::Rule => {
                self.flush();
                self.lines.push(Line::from(Span::styled(
                    "─".repeat(usize::from(self.width)),
                    Style::default().fg(self.theme.muted()),
                )));
                self.blank();
            }
            Event::TaskListMarker(checked) => self.span(
                if checked { "✓ " } else { "□ " },
                Style::default().fg(if checked {
                    self.theme.thinking_medium()
                } else {
                    self.theme.muted()
                }),
            ),
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => self.ensure_prefix(),
            Tag::Heading { level, .. } => {
                self.flush();
                let style = heading_style(level, self.theme);
                self.push_unlinked(Span::styled("▍ ", style));
                self.push_style(style);
            }
            Tag::BlockQuote(_) => {
                self.flush();
                self.quote_depth = self.quote_depth.saturating_add(1);
                self.push_style(Style::default().fg(self.theme.muted()));
            }
            Tag::List(start) => self.lists.push(ListState { next: start }),
            Tag::Item => {
                self.flush();
                self.ensure_quote_prefix();
                let depth = self.lists.len().saturating_sub(1);
                self.push_unlinked(Span::raw("  ".repeat(depth)));
                let marker = self.lists.last_mut().map_or_else(
                    || "• ".to_owned(),
                    |list| match &mut list.next {
                        Some(next) => {
                            let marker = format!("{next}. ");
                            *next = next.saturating_add(1);
                            marker
                        }
                        None => "• ".to_owned(),
                    },
                );
                self.push_unlinked(Span::styled(
                    marker,
                    Style::default().fg(self.theme.accent()),
                ));
            }
            Tag::Emphasis => self.push_style(Style::default().add_modifier(Modifier::ITALIC)),
            Tag::Strong => self.push_style(Style::default().add_modifier(Modifier::BOLD)),
            Tag::Strikethrough => self.push_style(
                Style::default()
                    .fg(self.theme.muted())
                    .add_modifier(Modifier::CROSSED_OUT),
            ),
            Tag::Superscript => self.text("^"),
            Tag::Subscript => self.text("~"),
            Tag::Link { dest_url, .. } => {
                self.links.push(LinkState {
                    destination: Arc::from(sanitize(&dest_url)),
                    label: String::new(),
                });
                self.push_style(
                    Style::default()
                        .fg(self.theme.accent())
                        .add_modifier(Modifier::UNDERLINED),
                );
            }
            Tag::Image { dest_url, .. } => {
                self.image = Some(LinkState {
                    destination: Arc::from(sanitize(&dest_url)),
                    label: String::new(),
                });
            }
            Tag::FootnoteDefinition(label) => {
                self.flush();
                self.push_unlinked(Span::styled(
                    format!("[{}] ", sanitize(&label)),
                    Style::default().fg(self.theme.thinking_medium()),
                ));
            }
            Tag::DefinitionListTitle => {
                self.flush();
                self.push_style(Style::default().add_modifier(Modifier::BOLD));
            }
            Tag::DefinitionListDefinition => {
                self.flush();
                self.push_unlinked(Span::styled(
                    "  : ",
                    Style::default().fg(self.theme.muted()),
                ));
            }
            Tag::HtmlBlock | Tag::MetadataBlock(_) => {
                self.flush();
                self.push_style(Style::default().fg(self.theme.muted()));
            }
            Tag::CodeBlock(_)
            | Tag::Table(_)
            | Tag::TableHead
            | Tag::TableRow
            | Tag::TableCell
            | Tag::DefinitionList => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => {
                self.flush();
                self.blank();
            }
            TagEnd::Heading(_) => {
                self.pop_style();
                self.flush();
                self.blank();
            }
            TagEnd::BlockQuote(_) => {
                self.flush();
                self.quote_depth = self.quote_depth.saturating_sub(1);
                self.pop_style();
                self.blank();
            }
            TagEnd::List(_) => {
                self.lists.pop();
                if self.lists.is_empty() {
                    self.blank();
                }
            }
            TagEnd::Item => self.flush(),
            TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough => self.pop_style(),
            TagEnd::Superscript => self.text("^"),
            TagEnd::Subscript => self.text("~"),
            TagEnd::Link => {
                self.pop_style();
                if let Some(link) = self.links.pop()
                    && link.label.trim() != link.destination.as_ref()
                {
                    self.push_linked(
                        Span::styled(
                            format!(" ↗ {}", link.destination),
                            Style::default()
                                .fg(self.theme.accent())
                                .add_modifier(Modifier::DIM),
                        ),
                        link.destination,
                    );
                }
            }
            TagEnd::Image => {
                if let Some(image) = self.image.take() {
                    self.render_image_link(image);
                }
            }
            TagEnd::FootnoteDefinition
            | TagEnd::DefinitionListTitle
            | TagEnd::DefinitionListDefinition
            | TagEnd::HtmlBlock
            | TagEnd::MetadataBlock(_) => {
                self.pop_style();
                self.flush();
                self.blank();
            }
            TagEnd::CodeBlock
            | TagEnd::DefinitionList
            | TagEnd::Table
            | TagEnd::TableHead
            | TagEnd::TableRow
            | TagEnd::TableCell => {}
        }
    }

    fn text(&mut self, text: &str) {
        let text = sanitize(text);
        if let Some(image) = &mut self.image {
            image.label.push_str(&text);
            return;
        }
        if let Some(link) = self.links.last_mut() {
            link.label.push_str(&text);
        }
        self.ensure_prefix();
        self.push_current(Span::styled(text, self.style()));
    }

    fn span(&mut self, text: &str, style: Style) {
        let text = sanitize(text);
        if let Some(image) = &mut self.image {
            image.label.push_str(&text);
            return;
        }
        self.ensure_prefix();
        self.push_current(Span::styled(text, self.style().patch(style)));
    }

    fn code_block(
        &mut self,
        kind: CodeBlockKind<'_>,
        events: &mut std::iter::Peekable<Parser<'_>>,
    ) {
        let language = match kind {
            CodeBlockKind::Indented => None,
            CodeBlockKind::Fenced(language) if language.is_empty() => None,
            CodeBlockKind::Fenced(language) => Some(sanitize(&language)),
        };
        let mut code = String::new();
        for event in events.by_ref() {
            match event {
                Event::End(TagEnd::CodeBlock) => break,
                Event::Text(text) | Event::Code(text) => code.push_str(&sanitize(&text)),
                Event::SoftBreak | Event::HardBreak => code.push('\n'),
                _ => {}
            }
        }
        let is_diff = language.as_deref().is_some_and(is_diff_language);
        if is_diff {
            self.lines
                .extend(super::diff::render(&code, self.width, self.theme));
            self.blank();
            return;
        }
        let block = self.highlight(code, language.as_deref());
        if self.width < 6 {
            self.narrow_code_block(block);
            self.blank();
            return;
        }

        let border = Style::default().fg(self.theme.border());
        self.lines.push(code_block_header(
            language.as_deref(),
            self.width,
            self.theme,
        ));
        let content_width = self.width.saturating_sub(4).max(1);
        for highlighted in &self.highlights.blocks[block].lines {
            for (index, mut spans) in
                super::highlight::wrap_spans(highlighted, content_width.saturating_sub(2).max(1))
                    .into_iter()
                    .enumerate()
            {
                if index > 0 {
                    spans.insert(
                        0,
                        Span::styled("↪ ", Style::default().fg(self.theme.muted())),
                    );
                }
                let used = spans.iter().map(Span::width).sum::<usize>();
                let padding = usize::from(content_width).saturating_sub(used);
                let mut body = Vec::with_capacity(spans.len() + 3);
                body.push(Span::styled("│ ", border));
                body.extend(spans);
                body.push(Span::raw(" ".repeat(padding)));
                body.push(Span::styled(" │", border));
                self.lines.push(Line::from(body));
            }
        }
        self.lines.push(Line::from(Span::styled(
            format!(
                "╰{}╯",
                "─".repeat(usize::from(self.width.saturating_sub(2)))
            ),
            border,
        )));
        self.blank();
    }

    fn highlight(&mut self, code: String, language: Option<&str>) -> usize {
        let block = self.block;
        self.block += 1;
        let cached = self
            .highlights
            .blocks
            .get_mut(block)
            .filter(|cached| cached.language.as_deref() == language);
        if let Some(cached) = &cached
            && cached.code == code
        {
            return block;
        }
        self.highlighted += 1;
        let assets = super::highlight::assets();
        let theme = super::highlight::theme();
        let source = code.trim_end_matches('\n');
        let resumed = cached.and_then(|cached| {
            let resume = cached.resume.take()?;
            (source.len() > resume.offset
                && source.as_bytes()[..resume.offset] == cached.code.as_bytes()[..resume.offset])
                .then(|| {
                    let mut lines = std::mem::take(&mut cached.lines);
                    lines.truncate(resume.lines);
                    (
                        lines,
                        resume.offset,
                        HighlightLines::from_state(theme, resume.highlight, resume.parse),
                    )
                })
        });
        let (mut lines, offset, mut highlighter) = resumed.unwrap_or_else(|| {
            let syntax = language.map_or_else(
                || assets.syntaxes.find_syntax_plain_text(),
                |language| super::highlight::syntax_for_token(&assets.syntaxes, language),
            );
            (Vec::new(), 0, HighlightLines::new(syntax, theme))
        });
        let (complete, last) = source[offset..]
            .rsplit_once('\n')
            .unwrap_or(("", &source[offset..]));
        if offset < source.len() - last.len() {
            lines.extend(complete.split('\n').map(|source_line| {
                super::highlight::line(&mut highlighter, source_line, &assets.syntaxes)
            }));
        }
        let (highlight, parse) = highlighter.state();
        let resume = Resume {
            offset: source.len() - last.len(),
            lines: lines.len(),
            highlight: highlight.clone(),
            parse: parse.clone(),
        };
        let mut highlighter = HighlightLines::from_state(theme, highlight, parse);
        lines.push(super::highlight::line(
            &mut highlighter,
            last,
            &assets.syntaxes,
        ));
        let highlighted = HighlightedBlock {
            language: language.map(str::to_owned),
            code,
            lines,
            resume: Some(resume),
        };
        if let Some(slot) = self.highlights.blocks.get_mut(block) {
            *slot = highlighted;
        } else {
            self.highlights.blocks.push(highlighted);
        }
        block
    }

    fn narrow_code_block(&mut self, block: usize) {
        let gutter = Style::default().fg(self.theme.border());
        let content_width = self.width.saturating_sub(2).max(1);
        for highlighted in &self.highlights.blocks[block].lines {
            for spans in super::highlight::wrap_spans(highlighted, content_width) {
                let mut line = vec![Span::styled("┃ ", gutter)];
                line.extend(spans);
                self.lines.push(Line::from(line));
            }
        }
    }

    fn table(&mut self, events: &mut std::iter::Peekable<Parser<'_>>) {
        let mut rows = Vec::<Vec<String>>::new();
        let mut row = Vec::<String>::new();
        let mut cell = String::new();
        let mut in_cell = false;
        let mut header_rows = 0_usize;
        let mut in_header = false;
        for event in events.by_ref() {
            match event {
                Event::Start(Tag::TableHead) => in_header = true,
                Event::End(TagEnd::TableHead) => {
                    if !row.is_empty() {
                        rows.push(std::mem::take(&mut row));
                        header_rows = header_rows.saturating_add(1);
                    }
                    in_header = false;
                }
                Event::Start(Tag::TableCell) => {
                    cell.clear();
                    in_cell = true;
                }
                Event::End(TagEnd::TableCell) => {
                    row.push(cell.trim().to_owned());
                    in_cell = false;
                }
                Event::End(TagEnd::TableRow) => {
                    if in_header {
                        header_rows = header_rows.saturating_add(1);
                    }
                    rows.push(std::mem::take(&mut row));
                }
                Event::End(TagEnd::Table) => break,
                Event::Text(text) | Event::Code(text) if in_cell => {
                    cell.push_str(&sanitize(&text));
                }
                Event::SoftBreak | Event::HardBreak if in_cell => cell.push(' '),
                _ => {}
            }
        }
        self.lines
            .extend(render_table(&rows, header_rows, self.width, self.theme));
        self.blank();
    }

    fn ensure_prefix(&mut self) {
        if !self.current.is_empty() {
            return;
        }
        self.ensure_quote_prefix();
    }

    fn ensure_quote_prefix(&mut self) {
        for _ in 0..self.quote_depth {
            self.push_unlinked(Span::styled("▌ ", Style::default().fg(self.theme.accent())));
        }
    }

    fn push_style(&mut self, style: Style) {
        self.styles.push(self.style().patch(style));
    }

    fn pop_style(&mut self) {
        if self.styles.len() > 1 {
            self.styles.pop();
        }
    }

    fn style(&self) -> Style {
        self.styles.last().copied().unwrap_or_default()
    }

    fn push_current(&mut self, span: Span<'static>) {
        let link = self.links.last().map(|link| Arc::clone(&link.destination));
        self.current.push(TaggedSpan { span, link });
    }

    fn push_unlinked(&mut self, span: Span<'static>) {
        self.current.push(TaggedSpan { span, link: None });
    }

    fn push_linked(&mut self, span: Span<'static>, destination: Arc<str>) {
        self.current.push(TaggedSpan {
            span,
            link: Some(destination),
        });
    }

    fn render_image_link(&mut self, image: LinkState) {
        self.ensure_prefix();
        let label = if image.label.is_empty() {
            image.destination.to_string()
        } else {
            image.label
        };
        let include_destination = label.trim() != image.destination.as_ref();
        self.push_linked(
            Span::styled(
                label,
                Style::default()
                    .fg(self.theme.accent())
                    .add_modifier(Modifier::UNDERLINED),
            ),
            Arc::clone(&image.destination),
        );
        if include_destination {
            self.push_linked(
                Span::styled(
                    format!(" ↗ {}", image.destination),
                    Style::default()
                        .fg(self.theme.accent())
                        .add_modifier(Modifier::DIM),
                ),
                image.destination,
            );
        }
    }

    fn flush(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let first_line = self.lines.len();
        for (offset, (line, links)) in wrap_tagged_spans(&self.current, self.width, true)
            .into_iter()
            .enumerate()
        {
            self.lines.push(line);
            self.rendered_links.extend(
                links
                    .into_iter()
                    .map(|link| (first_line.saturating_add(offset), link)),
            );
        }
        self.current.clear();
    }

    fn blank(&mut self) {
        if self.lines.last().is_some_and(|line| line.width() == 0) {
            return;
        }
        self.lines.push(Line::default());
    }

    fn finish(mut self) -> Layout {
        self.flush();
        self.highlights.blocks.truncate(self.block);
        while self.lines.last().is_some_and(|line| line.width() == 0) {
            self.lines.pop();
        }
        let mut links = vec![Vec::new(); self.lines.len()];
        for (line, link) in self.rendered_links {
            if let Some(line_links) = links.get_mut(line) {
                line_links.push(link);
            }
        }
        Layout {
            lines: self.lines,
            links,
        }
    }
}

fn is_diff_language(language: &str) -> bool {
    let language = language.split_ascii_whitespace().next().unwrap_or_default();
    language.eq_ignore_ascii_case("diff") || language.eq_ignore_ascii_case("patch")
}

fn heading_style(level: HeadingLevel, theme: &Theme) -> Style {
    let color = match level {
        HeadingLevel::H1 => theme.thinking_max(),
        HeadingLevel::H2 => theme.accent(),
        HeadingLevel::H3 => theme.thinking_medium(),
        HeadingLevel::H4 => theme.thinking_high(),
        HeadingLevel::H5 => theme.thinking_xhigh(),
        HeadingLevel::H6 => theme.muted(),
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

pub fn wrap_spans(spans: &[Span<'static>], width: u16, prefer_words: bool) -> Vec<Line<'static>> {
    wrap_spans_with_whitespace(spans, width, prefer_words, false)
}

fn wrap_spans_with_whitespace(
    spans: &[Span<'_>],
    width: u16,
    prefer_words: bool,
    preserve_whitespace: bool,
) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    if let Some(lines) = wrap_ascii_spans(
        spans
            .iter()
            .map(|span| (span.content.as_ref(), span.style, None)),
        width,
        prefer_words,
        preserve_whitespace,
    ) {
        return lines.into_iter().map(|(line, _)| line).collect();
    }
    let graphemes = spans
        .iter()
        .flat_map(|span| {
            graphemes(&span.content).map(|text| StyledGrapheme {
                text,
                style: span.style,
                link: None,
                width: u16::try_from(UnicodeWidthStr::width(text)).unwrap_or(u16::MAX),
                whitespace: text.chars().all(char::is_whitespace),
            })
        })
        .collect::<Vec<_>>();
    wrap_graphemes(&graphemes, width, prefer_words, preserve_whitespace)
        .into_iter()
        .map(|(line, _)| line)
        .collect()
}

fn wrap_tagged_spans(
    spans: &[TaggedSpan],
    width: u16,
    prefer_words: bool,
) -> Vec<(Line<'static>, Vec<LinkSpan>)> {
    if width == 0 {
        return Vec::new();
    }
    if let Some(lines) = wrap_ascii_spans(
        spans.iter().map(|tagged| {
            (
                tagged.span.content.as_ref(),
                tagged.span.style,
                tagged.link.as_ref(),
            )
        }),
        width,
        prefer_words,
        false,
    ) {
        return lines;
    }
    let graphemes = spans
        .iter()
        .flat_map(|tagged| {
            graphemes(&tagged.span.content).map(|text| StyledGrapheme {
                text,
                style: tagged.span.style,
                link: tagged.link.as_ref(),
                width: u16::try_from(UnicodeWidthStr::width(text)).unwrap_or(u16::MAX),
                whitespace: text.chars().all(char::is_whitespace),
            })
        })
        .collect::<Vec<_>>();
    wrap_graphemes(&graphemes, width, prefer_words, false)
}

fn graphemes(text: &str) -> impl Iterator<Item = &str> {
    let ascii = text.bytes().all(|byte| matches!(byte, b' '..=b'~' | b'\n'));
    let mut segmented = text.graphemes(true);
    let mut offset = 0;
    std::iter::from_fn(move || {
        if ascii {
            let grapheme = text.get(offset..offset + 1)?;
            offset += 1;
            Some(grapheme)
        } else {
            segmented.next()
        }
    })
}

fn wrap_graphemes(
    graphemes: &[StyledGrapheme<'_>],
    width: u16,
    prefer_words: bool,
    preserve_whitespace: bool,
) -> Vec<(Line<'static>, Vec<LinkSpan>)> {
    if graphemes.is_empty() {
        return vec![(Line::default(), Vec::new())];
    }
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, grapheme) in graphemes.iter().enumerate() {
        if grapheme.text == "\n" {
            lines.extend(wrap_visual_line(
                &graphemes[start..index],
                width,
                prefer_words,
                preserve_whitespace,
            ));
            start = index + 1;
        }
    }
    if start < graphemes.len()
        || graphemes
            .last()
            .is_some_and(|grapheme| grapheme.text == "\n")
    {
        lines.extend(wrap_visual_line(
            &graphemes[start..],
            width,
            prefer_words,
            preserve_whitespace,
        ));
    }
    lines
}

fn wrap_visual_line(
    graphemes: &[StyledGrapheme<'_>],
    width: u16,
    prefer_words: bool,
    preserve_whitespace: bool,
) -> Vec<(Line<'static>, Vec<LinkSpan>)> {
    if graphemes.is_empty() {
        return vec![(Line::default(), Vec::new())];
    }

    let mut lines = Vec::new();
    let mut start = 0_usize;
    while start < graphemes.len() {
        if preserve_whitespace && start > 0 && graphemes[start].text == " " {
            start += 1;
        }
        if !preserve_whitespace {
            while start < graphemes.len() && graphemes[start].whitespace {
                start += 1;
            }
        }
        if start == graphemes.len() {
            break;
        }
        let mut end = start;
        let mut used = 0_u16;
        let mut word_break = None;
        let first_content = graphemes[start..]
            .iter()
            .position(|grapheme| !grapheme.whitespace)
            .map_or(start, |offset| start.saturating_add(offset));
        while end < graphemes.len() {
            let next = used.saturating_add(graphemes[end].width);
            if next > width && end > start {
                break;
            }
            if graphemes[end].whitespace && (!preserve_whitespace || end > first_content) {
                word_break = Some(end);
            }
            used = next;
            end += 1;
            if used >= width {
                break;
            }
        }
        let split = if prefer_words && end < graphemes.len() {
            word_break.filter(|&index| index > start).unwrap_or(end)
        } else {
            end
        };
        lines.push(graphemes_to_line(
            &graphemes[start..split],
            preserve_whitespace,
        ));
        start = split.max(start + 1);
    }
    lines
}

struct StyledGrapheme<'a> {
    text: &'a str,
    style: Style,
    link: Option<&'a Arc<str>>,
    width: u16,
    whitespace: bool,
}

fn graphemes_to_line(
    graphemes: &[StyledGrapheme<'_>],
    preserve_whitespace: bool,
) -> (Line<'static>, Vec<LinkSpan>) {
    let mut spans = Vec::<Span<'static>>::new();
    let mut links = Vec::<LinkSpan>::new();
    let mut column = 0_u16;
    let rendered_len = if preserve_whitespace {
        graphemes.len()
    } else {
        graphemes
            .iter()
            .rposition(|grapheme| !grapheme.whitespace)
            .map_or(0, |index| index.saturating_add(1))
    };
    for run in graphemes[..rendered_len].chunk_by(|left, right| left.style == right.style) {
        let mut content =
            String::with_capacity(run.iter().map(|grapheme| grapheme.text.len()).sum());
        for grapheme in run {
            content.push_str(grapheme.text);
        }
        spans.push(Span::styled(content, run[0].style));
    }
    for grapheme in &graphemes[..rendered_len] {
        if let Some(destination) = grapheme.link
            && grapheme.width > 0
        {
            let end = column.saturating_add(grapheme.width);
            if let Some(last) = links.last_mut()
                && last.destination == *destination
                && last.end == column
            {
                last.end = end;
            } else {
                links.push(LinkSpan {
                    destination: Arc::clone(destination),
                    start: column,
                    end,
                });
            }
        }
        column = column.saturating_add(grapheme.width);
    }
    (Line::from(spans), links)
}

fn wrap_ascii_spans<'a>(
    spans: impl Iterator<Item = (&'a str, Style, Option<&'a Arc<str>>)>,
    width: u16,
    prefer_words: bool,
    preserve_whitespace: bool,
) -> Option<Vec<(Line<'static>, Vec<LinkSpan>)>> {
    if width == 0 {
        return Some(Vec::new());
    }
    let spans = spans
        .filter(|(text, _, _)| !text.is_empty())
        .collect::<Vec<_>>();
    if spans
        .iter()
        .any(|(text, _, _)| crate::text::ascii_prefix(text, true) != text.len())
    {
        return None;
    }
    let text = if spans.len() == 1 {
        std::borrow::Cow::Borrowed(spans[0].0)
    } else {
        std::borrow::Cow::Owned(spans.iter().map(|(text, _, _)| *text).collect::<String>())
    };
    let mut index = 0;
    let mut offset = 0;
    let mut lines = Vec::new();
    for range in
        crate::text::ascii_rows(&text, usize::from(width), prefer_words, preserve_whitespace)
    {
        while index < spans.len() && offset + spans[index].0.len() <= range.start {
            offset += spans[index].0.len();
            index += 1;
        }
        let mut position = range.start;
        let mut rendered = Vec::<Span<'static>>::new();
        let mut links = Vec::<LinkSpan>::new();
        while position < range.end {
            let (content, style, link) = spans[index];
            let end = range.end.min(offset + content.len());
            let slice = &text[position..end];
            if let Some(last) = rendered.last_mut()
                && last.style == style
            {
                last.content.to_mut().push_str(slice);
            } else {
                rendered.push(Span::styled(slice.to_owned(), style));
            }
            if let Some(destination) = link {
                let start = (position - range.start) as u16;
                let end = (end - range.start) as u16;
                if let Some(last) = links.last_mut()
                    && last.destination == *destination
                    && last.end == start
                {
                    last.end = end;
                } else {
                    links.push(LinkSpan {
                        destination: Arc::clone(destination),
                        start,
                        end,
                    });
                }
            }
            position = end;
            if position == offset + content.len() {
                offset += content.len();
                index += 1;
            }
        }
        lines.push((Line::from(rendered), links));
    }
    Some(lines)
}

fn hard_wrap(text: &str, width: u16) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    if width > 0 && crate::text::ascii_prefix(text, false) == text.len() {
        return text
            .as_bytes()
            .chunks(usize::from(width))
            .scan(0, |offset, chunk| {
                let line = text[*offset..*offset + chunk.len()].to_owned();
                *offset += chunk.len();
                Some(line)
            })
            .collect();
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    let mut used = 0_u16;
    for grapheme in text.graphemes(true) {
        let grapheme_width = u16::try_from(UnicodeWidthStr::width(grapheme)).unwrap_or(u16::MAX);
        if used.saturating_add(grapheme_width) > width && !line.is_empty() {
            lines.push(std::mem::take(&mut line));
            used = 0;
        }
        line.push_str(grapheme);
        used = used.saturating_add(grapheme_width);
    }
    lines.push(line);
    lines
}

fn code_block_header(language: Option<&str>, width: u16, theme: &Theme) -> Line<'static> {
    let border = Style::default().fg(theme.border());
    let Some(language) = language else {
        return Line::from(Span::styled(
            format!("╭{}╮", "─".repeat(usize::from(width.saturating_sub(2)))),
            border,
        ));
    };
    let available = width.saturating_sub(5);
    let language = truncate_graphemes(language, available);
    let language_width =
        u16::try_from(UnicodeWidthStr::width(language.as_str())).unwrap_or(u16::MAX);
    let fill = width.saturating_sub(language_width.saturating_add(5));
    Line::from(vec![
        Span::styled("╭─ ", border),
        Span::styled(
            language,
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!(" {}╮", "─".repeat(usize::from(fill))), border),
    ])
}

fn truncate_graphemes(text: &str, width: u16) -> String {
    if crate::text::ascii_prefix(text, false) == text.len() {
        return text[..text.len().min(usize::from(width))].to_owned();
    }
    let mut rendered = String::new();
    let mut used = 0_u16;
    for grapheme in text.graphemes(true) {
        let grapheme_width = u16::try_from(UnicodeWidthStr::width(grapheme)).unwrap_or(u16::MAX);
        if used.saturating_add(grapheme_width) > width {
            break;
        }
        rendered.push_str(grapheme);
        used = used.saturating_add(grapheme_width);
    }
    rendered
}

fn render_table(
    rows: &[Vec<String>],
    header_rows: usize,
    width: u16,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    if columns == 0 || width < 4 {
        return Vec::new();
    }
    let border_cells = u16::try_from(columns.saturating_add(1)).unwrap_or(u16::MAX);
    let available = width.saturating_sub(border_cells);
    if available < u16::try_from(columns.saturating_mul(3)).unwrap_or(u16::MAX) {
        return render_stacked_table(rows, header_rows, width, theme);
    }
    let mut widths = vec![3_u16; columns];
    for row in rows {
        for (column, cell) in row.iter().enumerate() {
            widths[column] = widths[column]
                .max(u16::try_from(UnicodeWidthStr::width(cell.as_str())).unwrap_or(u16::MAX));
        }
    }
    while widths.iter().copied().sum::<u16>() > available {
        let Some((index, _)) = widths.iter().enumerate().max_by_key(|(_, width)| **width) else {
            break;
        };
        if widths[index] <= 3 {
            break;
        }
        widths[index] -= 1;
    }

    let mut lines = Vec::new();
    lines.push(table_rule('╭', '┬', '╮', &widths, theme));
    for (row_index, row) in rows.iter().enumerate() {
        let wrapped = widths
            .iter()
            .enumerate()
            .map(|(column, &cell_width)| {
                hard_wrap(row.get(column).map_or("", String::as_str), cell_width)
            })
            .collect::<Vec<_>>();
        let height = wrapped.iter().map(Vec::len).max().unwrap_or(1);
        for line_index in 0..height {
            let mut spans = vec![Span::styled("│", Style::default().fg(theme.border()))];
            for (column, &cell_width) in widths.iter().enumerate() {
                let text = wrapped[column].get(line_index).map_or("", String::as_str);
                let padding = usize::from(cell_width).saturating_sub(UnicodeWidthStr::width(text));
                let style = if row_index < header_rows {
                    Style::default()
                        .fg(theme.accent())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(theme.text())
                };
                spans.push(Span::styled(
                    format!("{text}{}", " ".repeat(padding)),
                    style,
                ));
                spans.push(Span::styled("│", Style::default().fg(theme.border())));
            }
            lines.push(Line::from(spans));
        }
        if row_index + 1 < rows.len() {
            lines.push(table_rule('├', '┼', '┤', &widths, theme));
        }
    }
    lines.push(table_rule('╰', '┴', '╯', &widths, theme));
    lines
}

fn table_rule(
    left: char,
    middle: char,
    right: char,
    widths: &[u16],
    theme: &Theme,
) -> Line<'static> {
    let mut rule = left.to_string();
    for (index, width) in widths.iter().enumerate() {
        rule.push_str(&"─".repeat(usize::from(*width)));
        rule.push(if index + 1 == widths.len() {
            right
        } else {
            middle
        });
    }
    Line::from(Span::styled(rule, Style::default().fg(theme.border())))
}

fn render_stacked_table(
    rows: &[Vec<String>],
    header_rows: usize,
    width: u16,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let Some(headers) = rows.first() else {
        return Vec::new();
    };
    let mut lines = vec![Line::from(Span::styled(
        format!("╭{}╮", "─".repeat(usize::from(width.saturating_sub(2)))),
        Style::default().fg(theme.border()),
    ))];
    for row in rows.iter().skip(header_rows.max(1)) {
        for (column, value) in row.iter().enumerate() {
            let header = headers.get(column).map_or("Value", String::as_str);
            let content = format!("{header}: {value}");
            for line in hard_wrap(&content, width.saturating_sub(4).max(1)) {
                let padding = usize::from(width.saturating_sub(4))
                    .saturating_sub(UnicodeWidthStr::width(line.as_str()));
                lines.push(Line::from(vec![
                    Span::styled("│ ", Style::default().fg(theme.border())),
                    Span::styled(line, Style::default().fg(theme.text())),
                    Span::raw(" ".repeat(padding)),
                    Span::styled(" │", Style::default().fg(theme.border())),
                ]));
            }
        }
        if row.as_ptr() != rows.last().map_or(row.as_ptr(), Vec::as_ptr) {
            lines.push(Line::from(Span::styled(
                format!("├{}┤", "─".repeat(usize::from(width.saturating_sub(2)))),
                Style::default().fg(theme.border()),
            )));
        }
    }
    lines.push(Line::from(Span::styled(
        format!("╰{}╯", "─".repeat(usize::from(width.saturating_sub(2)))),
        Style::default().fg(theme.border()),
    )));
    lines
}
