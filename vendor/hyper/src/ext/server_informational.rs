use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use http::{HeaderMap, Response, StatusCode};

#[derive(Default)]
struct Queue {
    heads: VecDeque<Response<()>>,
    reader: Option<Waker>,
    accepted: u8,
    closed: bool,
}

/// Sends informational heads on the HTTP/1 connection for this request.
///
/// This extension is available on HTTP/1 server requests. Accepted heads are
/// written in order before the final service response. A request may queue at
/// most nine pending heads, and its sender closes when the service completes.
#[derive(Clone)]
pub struct InformationalSender(Arc<Mutex<Queue>>);

impl fmt::Debug for InformationalSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InformationalSender")
            .finish_non_exhaustive()
    }
}

pub(crate) struct InformationalReceiver(Arc<Mutex<Queue>>);

impl InformationalSender {
    pub(crate) fn channel() -> (Self, InformationalReceiver) {
        let queue = Arc::new(Mutex::new(Queue::default()));
        (Self(queue.clone()), InformationalReceiver(queue))
    }

    /// Queues a non-upgrade 1xx response without a body.
    ///
    /// Returns false for invalid statuses, a reached limit, or an ended request.
    /// A 101 upgrade belongs to the service's normal final response.
    pub fn try_send(&self, status: StatusCode, headers: HeaderMap) -> bool {
        if !status.is_informational() || status == StatusCode::SWITCHING_PROTOCOLS {
            return false;
        }
        let reader = {
            let mut queue = self.0.lock().expect("informational response queue");
            if queue.closed || queue.accepted == 9 {
                return false;
            }
            let mut head = Response::new(());
            *head.status_mut() = status;
            *head.headers_mut() = headers;
            queue.heads.push_back(head);
            queue.accepted += 1;
            queue.reader.take()
        };
        if let Some(reader) = reader {
            reader.wake();
        }
        true
    }
}

impl InformationalReceiver {
    pub(crate) fn close(&self) {
        let mut queue = self.0.lock().expect("informational response queue");
        queue.closed = true;
        queue.reader = None;
    }

    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<Response<()>>> {
        let mut queue = self.0.lock().expect("informational response queue");
        if let Some(head) = queue.heads.pop_front() {
            Poll::Ready(Some(head))
        } else if queue.closed {
            Poll::Ready(None)
        } else {
            queue.reader = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for InformationalReceiver {
    fn drop(&mut self) {
        let mut queue = self.0.lock().expect("informational response queue");
        queue.closed = true;
        queue.reader = None;
        queue.heads.clear();
    }
}
