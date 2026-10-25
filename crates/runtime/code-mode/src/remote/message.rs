use std::{fmt::Display, marker::PhantomData, sync::Arc};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::Mutex;

use super::{PEER_CLOSED, RemoteDelivery, RemoteFrame, RemoteLink};
use crate::embedded::{CodeModeHostError, HostFuture};

pub struct MessageLink<St, Si, M> {
    stream: Mutex<St>,
    sink: Arc<Mutex<Si>>,
    max_frame_bytes: Option<usize>,
    resumes: Option<String>,
    message: PhantomData<fn(String) -> M>,
}

impl<St, Si, M> MessageLink<St, Si, M> {
    pub fn new(
        stream: St,
        sink: Arc<Mutex<Si>>,
        max_frame_bytes: Option<usize>,
        resumes: Option<String>,
    ) -> Self {
        Self {
            stream: Mutex::new(stream),
            sink,
            max_frame_bytes,
            resumes,
            message: PhantomData,
        }
    }
}

impl<St, Si, M, E> RemoteLink for MessageLink<St, Si, M>
where
    St: Stream<Item = Result<String, E>> + Unpin + Send + 'static,
    E: Display,
    Si: Sink<M> + Unpin + Send + 'static,
    M: From<String> + Send + 'static,
{
    fn send(
        &self,
        frames: Vec<RemoteFrame>,
        _: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let failed = |_| CodeModeHostError::new("socket write failed");
            let limit = self.max_frame_bytes.unwrap_or(usize::MAX);
            let mut sink = self.sink.lock().await;
            for frame in frames {
                let text = frame
                    .encode(limit)
                    .map_err(|error| CodeModeHostError::unsent(error.to_string()))?;
                sink.feed(M::from(text)).await.map_err(failed)?;
            }
            sink.flush().await.map_err(failed)
        })
    }

    fn receive(&self) -> HostFuture<'_, Result<Vec<RemoteFrame>, CodeModeHostError>> {
        Box::pin(async move {
            match self.stream.lock().await.next().await {
                Some(Ok(text)) => Ok(vec![RemoteFrame::decode(
                    &text,
                    self.max_frame_bytes.unwrap_or(usize::MAX),
                )?]),
                Some(Err(error)) => Err(CodeModeHostError::new(error.to_string())),
                None => Err(CodeModeHostError::new(PEER_CLOSED)),
            }
        })
    }

    fn close<'a>(&'a self, _: &'a str) -> HostFuture<'a, ()> {
        Box::pin(async {})
    }

    fn max_frame_bytes(&self) -> Option<usize> {
        self.max_frame_bytes
    }

    fn resumes(&self) -> Option<String> {
        self.resumes.clone()
    }
}

pub enum SocketMessage<M> {
    Text(String),
    Ping(M),
    Pong,
    Close,
    Invalid(&'static str),
}

#[cfg(not(target_family = "wasm"))]
#[expect(clippy::type_complexity, reason = "the link's boxed message stream")]
pub fn websocket<St, Si, M, E>(
    stream: St,
    sink: Arc<Mutex<Si>>,
    max_frame_bytes: Option<usize>,
    resumes: Option<String>,
    classify: fn(M) -> SocketMessage<M>,
    ping: fn() -> M,
    (interval, timeout): (std::time::Duration, std::time::Duration),
) -> (
    MessageLink<futures_util::stream::BoxStream<'static, Result<String, &'static str>>, Si, M>,
    impl Future<Output = &'static str> + Send,
)
where
    St: Stream<Item = Result<M, E>> + Send + 'static,
    Si: Sink<M> + Unpin + Send + 'static,
    M: Send + 'static,
{
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::Instant;

    let start = Instant::now();
    let answered = Arc::new(AtomicU64::new(0));
    let (writer, pong) = (Arc::clone(&sink), Arc::clone(&answered));
    let messages = stream.filter_map(move |message| {
        let (sink, pong) = (Arc::clone(&writer), Arc::clone(&pong));
        let message = message.map(classify).map_err(drop);
        async move {
            match message {
                Ok(SocketMessage::Text(text)) => Some(Ok(text)),
                Ok(SocketMessage::Ping(reply)) => {
                    drop(sink.lock().await.send(reply).await);
                    None
                }
                Ok(SocketMessage::Pong) => {
                    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                    pong.store(elapsed, Ordering::Relaxed);
                    None
                }
                Ok(SocketMessage::Invalid(error)) => Some(Err(error)),
                Ok(SocketMessage::Close) => Some(Err(PEER_CLOSED)),
                Err(_) => Some(Err("socket read failed")),
            }
        }
    });
    let link = MessageLink::new(
        messages.boxed(),
        Arc::clone(&sink),
        max_frame_bytes,
        resumes,
    );
    let heartbeat = async move {
        let mut heartbeat = tokio::time::interval(interval);
        loop {
            heartbeat.tick().await;
            let last = start + std::time::Duration::from_millis(answered.load(Ordering::Relaxed));
            if last.elapsed() >= timeout {
                return "heartbeat lost";
            }
            if sink.lock().await.send(ping()).await.is_err() {
                return "socket write failed";
            }
        }
    };
    (link, heartbeat)
}
