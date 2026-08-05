use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureClass {
    InvalidConfig,
    XrayStartFailed,
    DnsFailure,
    TcpTimeout,
    ConnectionRefused,
    TlsTimeout,
    RelayTimeout,
    HttpFailure,
    DownloadTimeout,
    DownloadTooSlow,
    LocalOverload,
    EndpointFailure,
    Success,
}

impl FailureClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::XrayStartFailed => "XRAY_START_FAILED",
            Self::DnsFailure => "DNS_FAILURE",
            Self::TcpTimeout => "TCP_TIMEOUT",
            Self::ConnectionRefused => "CONNECTION_REFUSED",
            Self::TlsTimeout => "TLS_TIMEOUT",
            Self::RelayTimeout => "RELAY_TIMEOUT",
            Self::HttpFailure => "HTTP_FAILURE",
            Self::DownloadTimeout => "DOWNLOAD_TIMEOUT",
            Self::DownloadTooSlow => "DOWNLOAD_TOO_SLOW",
            Self::LocalOverload => "LOCAL_OVERLOAD",
            Self::EndpointFailure => "ENDPOINT_FAILURE",
            Self::Success => "SUCCESS",
        }
    }

    pub const fn inconclusive(self) -> bool {
        matches!(self, Self::LocalOverload | Self::EndpointFailure)
    }
    pub const fn hard_failure(self) -> bool {
        matches!(
            self,
            Self::InvalidConfig
                | Self::XrayStartFailed
                | Self::ConnectionRefused
                | Self::DownloadTooSlow
        )
    }
}

impl std::str::FromStr for FailureClass {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "INVALID_CONFIG" => Self::InvalidConfig,
            "XRAY_START_FAILED" => Self::XrayStartFailed,
            "DNS_FAILURE" => Self::DnsFailure,
            "TCP_TIMEOUT" => Self::TcpTimeout,
            "CONNECTION_REFUSED" => Self::ConnectionRefused,
            "TLS_TIMEOUT" => Self::TlsTimeout,
            "RELAY_TIMEOUT" => Self::RelayTimeout,
            "HTTP_FAILURE" => Self::HttpFailure,
            "DOWNLOAD_TIMEOUT" => Self::DownloadTimeout,
            "DOWNLOAD_TOO_SLOW" => Self::DownloadTooSlow,
            "LOCAL_OVERLOAD" => Self::LocalOverload,
            "ENDPOINT_FAILURE" => Self::EndpointFailure,
            "SUCCESS" => Self::Success,
            _ => return Err("unknown failure class"),
        })
    }
}
