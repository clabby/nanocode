use std::{future::Future, pin::Pin, sync::Arc};

use arc_swap::{ArcSwap, ArcSwapOption};
use futures_util::future::{FutureExt, Shared};
use tokio::sync::{mpsc, oneshot};

pub struct Signal {
    finish: ArcSwapOption<oneshot::Sender<()>>,
    finished: Shared<oneshot::Receiver<()>>,
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}

impl Signal {
    #[must_use]
    pub fn new() -> Self {
        let (finish, finished) = oneshot::channel();
        Self {
            finish: ArcSwapOption::from_pointee(finish),
            finished: finished.shared(),
        }
    }

    pub fn fire(&self) {
        drop(self.finish.swap(None));
    }

    #[must_use]
    pub fn is_fired(&self) -> bool {
        self.finish.load().is_none()
    }

    pub async fn wait(&self) {
        let _ = self.finished.clone().await;
    }
}

pub struct Atom<T>(ArcSwap<T>);

impl<T: Default> Default for Atom<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Atom<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("Atom").field(&self.0.load()).finish()
    }
}

impl<T> Atom<T> {
    pub fn new(value: T) -> Self {
        Self(ArcSwap::from_pointee(value))
    }

    #[must_use]
    pub fn get(&self) -> Arc<T> {
        self.0.load_full()
    }

    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        read(&self.0.load())
    }

    pub fn set(&self, value: T) -> Arc<T> {
        self.0.swap(Arc::new(value))
    }
}

impl<T: Clone> Atom<T> {
    pub fn update<R>(&self, mut change: impl FnMut(&mut T) -> R) -> R {
        let mut current = self.0.load_full();
        loop {
            let mut next = T::clone(&current);
            let output = change(&mut next);
            let previous = self.0.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return output;
            }
            current = arc_swap::Guard::into_inner(previous);
        }
    }

    pub fn try_update<R>(&self, mut change: impl FnMut(&mut T) -> Option<R>) -> Option<R> {
        let mut current = self.0.load_full();
        loop {
            let mut next = T::clone(&current);
            let output = change(&mut next)?;
            let previous = self.0.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return Some(output);
            }
            current = arc_swap::Guard::into_inner(previous);
        }
    }
}

pub struct Slot<T>(ArcSwapOption<T>);

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self(ArcSwapOption::empty())
    }
}

impl<T> Slot<T> {
    #[must_use]
    pub fn new(value: Option<T>) -> Self {
        Self(ArcSwapOption::from(value.map(Arc::new)))
    }

    pub fn put(&self, value: T) -> Option<T> {
        self.0
            .swap(Some(Arc::new(value)))
            .and_then(|previous| Arc::try_unwrap(previous).ok())
    }

    pub fn take(&self) -> Option<T> {
        self.0
            .swap(None)
            .and_then(|value| Arc::try_unwrap(value).ok())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.load().is_none()
    }
}

struct Node<T> {
    value: T,
    next: Option<Arc<Self>>,
}

pub struct Inbox<T> {
    head: ArcSwapOption<Node<T>>,
}

impl<T> Default for Inbox<T> {
    fn default() -> Self {
        Self {
            head: ArcSwapOption::empty(),
        }
    }
}

impl<T> Drop for Inbox<T> {
    fn drop(&mut self) {
        let mut next = self.head.swap(None);
        while let Some(node) = next.take() {
            if let Ok(node) = Arc::try_unwrap(node) {
                next = node.next;
            }
        }
    }
}

impl<T: Clone> Inbox<T> {
    pub fn push(&self, value: T) {
        let mut node = Arc::new(Node { value, next: None });
        loop {
            let head = self.head.load_full();
            if let Some(unpublished) = Arc::get_mut(&mut node) {
                unpublished.next.clone_from(&head);
            }
            let previous = self.head.compare_and_swap(&head, Some(Arc::clone(&node)));
            if previous.as_ref().map(Arc::as_ptr) == head.as_ref().map(Arc::as_ptr) {
                return;
            }
        }
    }

    pub fn drain(&self) -> Vec<T> {
        let mut values = Vec::new();
        let mut next = self.head.swap(None);
        while let Some(node) = next.take() {
            match Arc::try_unwrap(node) {
                Ok(node) => {
                    values.push(node.value);
                    next = node.next;
                }
                Err(shared) => {
                    values.push(shared.value.clone());
                    next.clone_from(&shared.next);
                }
            }
        }
        values.reverse();
        values
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.head.load().is_none()
    }

    pub fn any(&self, mut predicate: impl FnMut(&T) -> bool) -> bool {
        let mut next = self.head.load_full();
        while let Some(node) = next.take() {
            if predicate(&node.value) {
                return true;
            }
            next.clone_from(&node.next);
        }
        false
    }
}

struct Flight<T> {
    done: Signal,
    result: ArcSwapOption<T>,
}

pub struct SingleFlight<T> {
    running: ArcSwapOption<Flight<T>>,
}

impl<T> Default for SingleFlight<T> {
    fn default() -> Self {
        Self {
            running: ArcSwapOption::empty(),
        }
    }
}

struct Leading<'a, T> {
    owner: &'a SingleFlight<T>,
    flight: Arc<Flight<T>>,
}

impl<T> Drop for Leading<'_, T> {
    fn drop(&mut self) {
        self.owner.running.rcu(|running| match running {
            Some(current) if Arc::ptr_eq(current, &self.flight) => None,
            other => other.clone(),
        });
        self.flight.done.fire();
    }
}

impl<T: Clone> SingleFlight<T> {
    pub async fn run<F, Fut>(&self, start: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut start = Some(start);
        loop {
            let flight = Arc::new(Flight {
                done: Signal::new(),
                result: ArcSwapOption::empty(),
            });
            let previous = self
                .running
                .rcu(|running| running.clone().or_else(|| Some(Arc::clone(&flight))));
            if let Some(running) = previous {
                running.done.wait().await;
                if let Some(result) = running.result.load_full() {
                    return T::clone(&result);
                }
                continue;
            }
            let Some(start) = start.take() else {
                continue;
            };
            let leading = Leading {
                owner: self,
                flight: Arc::clone(&flight),
            };
            let outcome = start().await;
            flight.result.store(Some(Arc::new(outcome.clone())));
            drop(leading);
            return outcome;
        }
    }
}

#[cfg(not(target_family = "wasm"))]
type Job<S> =
    Box<dyn for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send>;
#[cfg(target_family = "wasm")]
type Job<S> = Box<dyn for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = ()> + 'a>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the owning task stopped")
    }
}

impl std::error::Error for Stopped {}

pub struct Actor<S> {
    jobs: mpsc::UnboundedSender<Job<S>>,
}

impl<S> Clone for Actor<S> {
    fn clone(&self) -> Self {
        Self {
            jobs: self.jobs.clone(),
        }
    }
}

impl<S: 'static> Actor<S> {
    #[cfg(not(target_family = "wasm"))]
    pub fn spawn(state: S) -> std::io::Result<Self>
    where
        S: Send,
    {
        let runtime = tokio::runtime::Handle::try_current().map_err(std::io::Error::other)?;
        let (jobs, received) = mpsc::unbounded_channel();
        drop(runtime.spawn(Self::serve(state, received)));
        Ok(Self { jobs })
    }

    #[cfg(target_family = "wasm")]
    pub fn spawn(state: S) -> std::io::Result<Self> {
        let (jobs, received) = mpsc::unbounded_channel();
        nanocode_platform::spawn_local(Self::serve(state, received));
        Ok(Self { jobs })
    }

    async fn serve(mut state: S, mut jobs: mpsc::UnboundedReceiver<Job<S>>) {
        while let Some(job) = jobs.recv().await {
            job(&mut state).await;
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn run<R, F>(&self, job: F) -> Result<R, Stopped>
    where
        S: Send,
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
            + Send
            + 'static,
    {
        let (mut sender, receiver) = oneshot::channel();
        let job: Job<S> = Box::new(move |state| {
            Box::pin(async move {
                let work = job(state);
                let output = {
                    let abandoned = Box::pin(sender.closed());
                    match futures_util::future::select(work, abandoned).await {
                        futures_util::future::Either::Left((output, _)) => Some(output),
                        futures_util::future::Either::Right(_) => None,
                    }
                };
                if let Some(output) = output {
                    drop(sender.send(output));
                }
            })
        });
        self.jobs.send(job).map_err(|_| Stopped)?;
        receiver.await.map_err(|_| Stopped)
    }

    #[cfg(target_family = "wasm")]
    pub async fn run<R, F>(&self, job: F) -> Result<R, Stopped>
    where
        R: 'static,
        F: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + 'a>> + 'static,
    {
        let (mut sender, receiver) = oneshot::channel();
        let job: Job<S> = Box::new(move |state| {
            Box::pin(async move {
                let work = job(state);
                let output = {
                    let abandoned = Box::pin(sender.closed());
                    match futures_util::future::select(work, abandoned).await {
                        futures_util::future::Either::Left((output, _)) => Some(output),
                        futures_util::future::Either::Right(_) => None,
                    }
                };
                if let Some(output) = output {
                    drop(sender.send(output));
                }
            })
        });
        self.jobs.send(job).map_err(|_| Stopped)?;
        receiver.await.map_err(|_| Stopped)
    }

    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.jobs.is_closed()
    }
}
