use std::{fmt::Display, marker::PhantomData, sync::Arc};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::Mutex;

use super::{PEER_CLOSED, RemoteDelivery, RemoteFrame, RemoteLink};
use crate::embedded::{CodeModeHostError, HostFuture};

/// A [`RemoteLink`] over a message connection such as a server's WebSocket. `stream` yields the
/// peer's text messages, or an error that fails the link, and ends when the peer closes; `sink`
/// takes text messages as `M` and may be shared with the host's own writes, such as pings.
/// `close` writes nothing: the host closes the connection, with a reason of its own.
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
