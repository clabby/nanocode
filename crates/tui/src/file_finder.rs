use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};

use crate::theme::Theme;

#[derive(Clone, Debug, Default)]
pub struct FileFinder {
    files: Vec<String>,
    query: String,
    normalized: Vec<(String, usize)>,
    matches: Vec<usize>,
    selected: usize,
}

impl FileFinder {
    #[must_use]
    pub fn new(files: Vec<String>) -> Self {
        let mut finder = Self {
            normalized: files
                .iter()
                .map(|path| {
                    let normalized = path.to_lowercase();
                    let name_start = normalized.rfind('/').map_or(0, |index| index + 1);
                    (normalized, name_start)
                })
                .collect(),
            files,
            ..Self::default()
        };
        finder.refresh();
        finder
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.files.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn set_query(&mut self, query: &str) {
        if self.query != query {
            query.clone_into(&mut self.query);
            self.refresh();
        }
    }

    #[must_use]
    pub const fn matches(&self) -> usize {
        self.matches.len()
    }

    #[must_use]
    pub fn selected(&self) -> Option<&str> {
        self.matches
            .get(self.selected)
            .map(|index| self.files[*index].as_str())
    }

    pub const fn select_next(&mut self) {
        if !self.matches.is_empty() {
            self.selected = (self.selected + 1) % self.matches.len();
        }
    }

    pub const fn select_previous(&mut self) {
        if !self.matches.is_empty() {
            self.selected = (self.selected + self.matches.len() - 1) % self.matches.len();
        }
    }

    fn refresh(&mut self) {
        let _span = tracing::info_span!(
            "tui.finder.filter",
            files = self.files.len(),
            query.bytes = self.query.len()
        )
        .entered();
        let query = self.query.to_lowercase();
        let mut scored = self
            .normalized
            .iter()
            .enumerate()
            .filter_map(|(index, (path, name_start))| {
                score(path, *name_start, &query).map(|score| (score, index))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| self.files[left.1].len().cmp(&self.files[right.1].len()))
                .then_with(|| self.files[left.1].cmp(&self.files[right.1]))
        });
        self.matches = scored.into_iter().map(|(_, index)| index).collect();
        self.selected = 0;
    }

    #[must_use]
    pub fn lines(&self, rows: usize, theme: &Theme) -> Vec<Line<'static>> {
        let offset = (self.selected + 1).saturating_sub(rows);
        let mut lines = self
            .matches
            .iter()
            .enumerate()
            .skip(offset)
            .take(rows)
            .map(|(position, index)| {
                let path = &self.files[*index];
                let (directory, name) = path
                    .rsplit_once('/')
                    .map_or(("", path.as_str()), |(directory, name)| (directory, name));
                let selected = position == self.selected;
                let marker = if selected { "› " } else { "  " };
                let mut name_style = Style::default().fg(theme.text());
                if selected {
                    name_style = name_style.fg(theme.accent()).add_modifier(Modifier::BOLD);
                }
                let mut spans = vec![
                    Span::styled(marker, Style::default().fg(theme.accent())),
                    Span::styled(name.to_owned(), name_style),
                ];
                if !directory.is_empty() {
                    spans.push(Span::styled(
                        format!("  {directory}/"),
                        Style::default().fg(theme.muted()),
                    ));
                }
                Line::from(spans)
            })
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(Line::styled(
                "  no matching files",
                Style::default().fg(theme.muted()),
            ));
        }
        lines
    }
}

fn score(path: &str, name_start: usize, query: &str) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }
    let bytes = path.as_bytes();
    let mut score = 0_i64;
    let mut position = 0;
    let mut previous: Option<usize> = None;
    for character in query.chars() {
        let found = if character.is_ascii() {
            memchr::memchr(character as u8, &bytes[position..])?
        } else {
            path[position..].find(character)?
        } + position;
        score += 1;
        if previous.is_some_and(|previous| previous + 1 == found) {
            score += 5;
        }
        if found >= name_start {
            score += 3;
        }
        if found == name_start || found > 0 && matches!(bytes[found - 1], b'/' | b'_' | b'-' | b'.')
        {
            score += 4;
        }
        previous = Some(found);
        position = found + character.len_utf8();
    }
    if path[name_start..].starts_with(query) {
        score += 20;
    }
    Some(score)
}
