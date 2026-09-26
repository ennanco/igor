//! Worker and supervisor runtime and local protocol for Igor.

mod protocol;
mod runtime;
mod supervisor;
mod systemd;
mod telegram;
mod worker;

pub use protocol::{
    DaemonRole, DatabaseStatus, Health, PROTOCOL_VERSION, ProtocolError, ProtocolErrorKind,
    Request, RequestEnvelope, Response, ResponseEnvelope, Version,
};
pub use runtime::{Client, ClientError, DaemonError, run, run_until};
pub use telegram::{SendResult, TelegramClient, format_notification};
