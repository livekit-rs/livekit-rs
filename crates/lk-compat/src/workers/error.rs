//! Ordinary versus typed IO errors; preserve the actual source boundary code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Ordinary,
    Typed,
    IngressNotFound,
    EgressNotFound,
    IngressOutOfDate,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoError {
    kind: Kind,
    pub code: &'static str,
    pub message: String,
}
impl IoError {
    pub fn ordinary(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Ordinary,
            code: "unknown",
            message: message.into(),
        }
    }
    pub fn typed(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Typed,
            code,
            message: message.into(),
        }
    }
    pub fn redis(error: redis::RedisError) -> Self {
        let message = match (error.code(), error.detail()) {
            (Some(code), Some(detail)) if !detail.is_empty() => format!("{code} {detail}"),
            (Some(code), _) => code.into(),
            _ => error.to_string(),
        };
        Self::ordinary(message)
    }
    pub fn egress_not_found() -> Self {
        Self {
            kind: Kind::EgressNotFound,
            code: "not_found",
            message: "egress does not exist".into(),
        }
    }
    pub fn ingress_not_found() -> Self {
        Self {
            kind: Kind::IngressNotFound,
            code: "not_found",
            message: "ingress does not exist".into(),
        }
    }
    pub fn out_of_date() -> Self {
        Self {
            kind: Kind::IngressOutOfDate,
            code: "failed_precondition",
            message: "trying to ovewrite an ingress with an older version".into(),
        }
    }
    pub fn context(mut self, context: &str) -> Self {
        if self.kind == Kind::IngressOutOfDate {
            self.kind = Kind::Typed;
        }
        self.message = format!("{context}: {}", self.message);
        self
    }
    pub fn is_ingress_out_of_date(&self) -> bool {
        self.kind == Kind::IngressOutOfDate
    }
    pub fn is_ingress_not_found(&self) -> bool {
        self.kind == Kind::IngressNotFound
    }
}
impl From<IoError> for crate::service_bus::rpc::RpcError {
    fn from(e: IoError) -> Self {
        Self::new(e.code, e.message)
    }
}
impl std::fmt::Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for IoError {}

#[cfg(test)]
mod ingress_identity_tests {
    use super::*;
    #[test]
    fn local_singleton_identity_is_not_recovered_from_wrapped_or_equal_errors() {
        assert!(IoError::out_of_date().is_ingress_out_of_date());
        assert!(
            !IoError::out_of_date()
                .context("wrapped")
                .is_ingress_out_of_date()
        );
        assert!(
            !IoError::typed("failed_precondition", IoError::out_of_date().message)
                .is_ingress_out_of_date()
        );
        assert!(!IoError::ordinary(IoError::out_of_date().message).is_ingress_out_of_date());
    }
}
