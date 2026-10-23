use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

thread_local! {
    static CURRENT: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };
}

#[must_use]
pub fn current() -> Option<Arc<str>> {
    CURRENT.with_borrow(Clone::clone)
}

pub fn scope<F: Future>(origin: Option<Arc<str>>, future: F) -> Scoped<F> {
    Scoped {
        origin,
        future: Box::pin(future),
    }
}

pub struct Scoped<F> {
    origin: Option<Arc<str>>,
    future: Pin<Box<F>>,
}

struct Entered<'a> {
    origin: &'a mut Option<Arc<str>>,
    previous: Option<Arc<str>>,
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        *self.origin = CURRENT.replace(self.previous.take());
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let previous = CURRENT.replace(this.origin.take());
        let _entered = Entered {
            origin: &mut this.origin,
            previous,
        };
        this.future.as_mut().poll(cx)
    }
}
