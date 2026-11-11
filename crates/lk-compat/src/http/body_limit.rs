//! Lazy configured API body limit, corresponding to Go http.MaxBytesReader.
//! Registration precedes authentication, but no body is polled until its actual
//! downstream consumer reads. The original Content-Length early gate stays out
//! front. Prefix bytes remain observable before a later overflow error.
use axum::{
    BoxError,
    body::{Body, Bytes},
};
use futures_util::{StreamExt, stream};
use std::{error::Error, fmt};

#[derive(Debug)]
pub(crate) struct ApiBodyLimitExceeded {
    pub(crate) limit: u64,
}
impl fmt::Display for ApiBodyLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("http: request body too large")
    }
}
impl Error for ApiBodyLimitExceeded {}

/// Inspect the actual error source chain, never a display-string heuristic.
pub(crate) fn exceeded_limit(mut error: &(dyn Error + 'static)) -> Option<u64> {
    loop {
        if let Some(overflow) = error.downcast_ref::<ApiBodyLimitExceeded>() {
            return Some(overflow.limit);
        }
        error = error.source()?;
    }
}

/// One lazy stream per request. No full collection, timer or shared cache.
/// Exactly N bytes followed by EOF succeeds. An extra byte fails only on a read;
/// a consumer that has already decoded its complete value need not keep reading.
pub(super) fn wrap(body: Body, limit: u64) -> Body {
    if limit == 0 {
        return body;
    }
    let source = body.into_data_stream();
    let bounded = stream::unfold(
        (source, limit, false, false),
        move |(mut source, mut remaining, pending, done)| async move {
            if done {
                return None;
            }
            if pending {
                return Some((
                    Err::<Bytes, BoxError>(Box::new(ApiBodyLimitExceeded { limit })),
                    (source, remaining, false, true),
                ));
            }
            match source.next().await {
                None => None,
                Some(Err(error)) => Some((
                    Err(Box::new(error) as BoxError),
                    (source, remaining, false, true),
                )),
                Some(Ok(bytes)) => {
                    if bytes.len() as u64 <= remaining {
                        remaining -= bytes.len() as u64;
                        Some((Ok(bytes), (source, remaining, false, false)))
                    } else if remaining == 0 {
                        Some((
                            Err(Box::new(ApiBodyLimitExceeded { limit }) as BoxError),
                            (source, remaining, false, true),
                        ))
                    } else {
                        // Conversion is bounded by this actual Bytes length, not arbitrary u64.
                        let prefix = bytes.slice(..remaining as usize);
                        Some((Ok(prefix), (source, 0, true, false)))
                    }
                }
            }
        },
    );
    Body::from_stream(bounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn exact_limit_and_disabled_leave_actual_bytes_intact() {
        for limit in [0, 5, 6] {
            let bytes = axum::body::to_bytes(wrap(Body::from("hello"), limit), usize::MAX)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), b"hello");
        }
    }
    #[tokio::test]
    async fn actual_prefix_then_typed_error_without_overflow_bytes() {
        let mut bytes = wrap(Body::from("abcdef"), 3).into_data_stream();
        assert_eq!(bytes.next().await.unwrap().unwrap().as_ref(), b"abc");
        let error = bytes.next().await.unwrap().unwrap_err();
        assert_eq!(exceeded_limit(&error), Some(3));
        assert_eq!(error.to_string(), "http: request body too large");
        assert!(bytes.next().await.is_none());
    }
    #[tokio::test]
    async fn cross_chunk_limit_counts_actual_bytes_and_exact_eof() {
        for extra in [false, true] {
            let mut chunks = vec![
                Ok::<_, std::io::Error>(Bytes::from_static(b"ab")),
                Ok(Bytes::new()),
                Ok(Bytes::from_static(b"cd")),
            ];
            if extra {
                chunks.push(Ok(Bytes::from_static(b"e")));
            }
            let result =
                axum::body::to_bytes(wrap(Body::from_stream(stream::iter(chunks)), 4), usize::MAX)
                    .await;
            if extra {
                assert_eq!(exceeded_limit(&result.unwrap_err()), Some(4));
            } else {
                assert_eq!(result.unwrap().as_ref(), b"abcd");
            }
        }
    }
    #[tokio::test]
    async fn no_early_poll_and_real_transport_error_is_not_a_limit() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let source = stream::once(async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Err::<Bytes, _>(std::io::Error::other("owned transport failure"))
        });
        let limited = wrap(Body::from_stream(source), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error = axum::body::to_bytes(limited, usize::MAX).await.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(exceeded_limit(&error), None);
        assert!(error.to_string().contains("owned transport failure"));
    }
}
