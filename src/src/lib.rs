pub mod api;
pub mod app;
pub mod config;
pub mod domain;
pub mod health;
pub mod metadata;
pub mod parser;
pub mod probe;
pub mod publisher;
pub mod scheduler;
pub mod selection;
pub mod storage;
pub mod upstream;
pub mod xray;

pub const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");
