use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RenderScheduler {
    frame_interval: Duration,
    last_presented: Option<Instant>,
    deadline: Option<Instant>,
}

impl RenderScheduler {
    #[must_use]
    pub const fn new(frame_interval: Duration, now: Instant) -> Self {
        Self {
            frame_interval,
            last_presented: None,
            deadline: Some(now),
        }
    }

    pub fn request_streaming(&mut self, now: Instant) {
        let next_frame = self
            .last_presented
            .map_or(now, |presented| presented + self.frame_interval);
        let deadline = next_frame.max(now);
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
    }

    pub fn request_immediate(&mut self, now: Instant) {
        let deadline = self.deadline.map_or(now, |deadline| deadline.min(now));
        self.deadline = Some(deadline);
    }

    pub fn request_at(&mut self, at: Instant) {
        let deadline = self.deadline.map_or(at, |deadline| deadline.min(at));
        self.deadline = Some(deadline);
    }

    #[must_use]
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    #[must_use]
    pub fn next_frame_at(&self, now: Instant) -> Instant {
        self.last_presented
            .map(|presented| presented + self.frame_interval)
            .filter(|next| *next > now)
            .unwrap_or(now + self.frame_interval)
    }

    #[must_use]
    pub fn is_due(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| deadline <= now)
    }

    pub const fn presented(&mut self, now: Instant) {
        self.deadline = None;
        self.last_presented = Some(now);
    }
}
