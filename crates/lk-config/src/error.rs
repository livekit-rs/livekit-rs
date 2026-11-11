//! Error type returned by [`crate::parse`]. `Display` is the exact one-line message
//! the binary prints before exiting. Ordinary errors use stdout/1; genuine
//! child ExitErrors implement upstream cli.ExitCoder and retain stderr/status.

/// Preserve both formatted bytes and the source error origin where observable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// Message already formatted with the upstream text (e.g.
    /// `could not validate RTC config: ...`, `open x.yaml: no such file or directory`).
    #[error("{0}")]
    Message(String),

    /// A real child process ExitError, handled by urfave cli.ExitCoder.
    #[error("{message}")]
    ProcessExit { message: String, code: i32 },

    /// Exact non-Unicode public output. The binary writes these bytes directly.
    #[error("non-Unicode CLI diagnostic (use stdout_bytes)")]
    Raw(Vec<u8>),

    /// YAML decode failure; upstream wraps it as `could not parse config: <err>`.
    #[error("could not parse config: {0}")]
    Parse(String),

    /// A configuration value would enable a capability this build does not
    /// implement (CLI-C08). `path` is the YAML path (or flag name) of the field.
    #[error("unsupported configuration \"{path}\": {reason}")]
    Unsupported { path: String, reason: String },
}

impl ConfigError {
    /// Exact error output excluding main's final newline.
    pub fn stdout_bytes(&self) -> Vec<u8> {
        match self {
            Self::Raw(bytes) => bytes.clone(),
            _ => self.to_string().into_bytes(),
        }
    }
    pub(crate) fn raw(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(text) => Self::Message(text),
            Err(error) => Self::Raw(error.into_bytes()),
        }
    }
    /// Only genuine child exit errors select stderr; wording is not classification.
    pub fn diagnostic_to_stderr(&self) -> bool {
        matches!(self, Self::ProcessExit { .. })
    }
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::ProcessExit { code, .. } => *code as u8,
            _ => 1,
        }
    }
    pub(crate) fn process_exit(message: String, code: i32) -> Self {
        Self::ProcessExit { message, code }
    }

    pub(crate) fn msg(s: impl Into<String>) -> Self {
        ConfigError::Message(s.into())
    }

    pub(crate) fn unsupported(path: impl Into<String>, reason: impl Into<String>) -> Self {
        ConfigError::Unsupported {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod process_exit_tests {
    use super::*;
    #[test]
    fn process_error_origin_preserves_stream_and_status_without_message_inference() {
        let ordinary = ConfigError::msg("exit status 7");
        assert!(!ordinary.diagnostic_to_stderr());
        assert_eq!(ordinary.exit_code(), 1);
        for (code, expected) in [(1, 1), (7, 7), (-1, 255)] {
            let actual = ConfigError::process_exit("actual child diagnostic".into(), code);
            assert!(actual.diagnostic_to_stderr());
            assert_eq!(actual.exit_code(), expected);
            assert_eq!(actual.stdout_bytes(), b"actual child diagnostic");
        }
    }
}
