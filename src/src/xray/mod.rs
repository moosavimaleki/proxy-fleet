use std::{ops::RangeInclusive, path::PathBuf, time::Duration};

use anyhow::Context;
use tokio::{
    net::TcpStream,
    process::{Child, Command},
};
use tracing::warn;

use crate::parser::{ParsedProxy, xray_outbound};

pub mod runtime;

pub struct XraySession {
    child: Child,
    config_path: PathBuf,
    pub socks_port: u16,
}

/// A single Xray process with one SOCKS inbound per candidate.  The routing
/// table pins each inbound to its own outbound, avoiding a process-per-proxy
/// storm while retaining an unambiguous observation for every candidate.
pub struct XrayBatchSession {
    child: Child,
    config_path: PathBuf,
    pub socks_ports: Vec<u16>,
}

impl XraySession {
    pub async fn start(binary: &str, proxy: &ParsedProxy, socks_port: u16) -> anyhow::Result<Self> {
        Self::start_with_listen(binary, proxy, socks_port, "127.0.0.1").await
    }

    pub async fn start_with_listen(
        binary: &str,
        proxy: &ParsedProxy,
        socks_port: u16,
        listen: &str,
    ) -> anyhow::Result<Self> {
        let outbound = xray_outbound(proxy)?;
        let config = serde_json::json!({
            "log":{"loglevel":"warning"},
            "inbounds":[{"listen":listen,"port":socks_port,"protocol":"socks","settings":{"udp":true}}],
            "outbounds":[outbound]
        });
        let config_path = std::env::temp_dir().join(format!(
            "proxy-fleet-xray-{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        tokio::fs::write(&config_path, serde_json::to_vec(&config)?).await?;
        let child = Command::new(binary)
            .arg("run")
            .arg("-config")
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            // We do not consume a child pipe here.  Keeping stderr piped can
            // deadlock a noisy Xray process once its pipe buffer fills.
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("starting Xray binary {binary}"))?;
        let mut session = Self {
            child,
            config_path,
            socks_port,
        };
        if let Err(error) = session.wait_ready().await {
            session.stop().await;
            return Err(error);
        }
        Ok(session)
    }

    async fn wait_ready(&mut self) -> anyhow::Result<()> {
        for _ in 0..30 {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!("Xray exited during startup: {status}");
            }
            if TcpStream::connect(("127.0.0.1", self.socks_port))
                .await
                .is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("Xray SOCKS inbound did not become ready")
    }

    pub async fn stop(&mut self) {
        terminate_and_reap(&mut self.child, "Xray child").await;
        let _ = tokio::fs::remove_file(&self.config_path).await;
    }
}

impl XrayBatchSession {
    pub async fn start(
        binary: &str,
        proxies: &[ParsedProxy],
        socks_ports: Vec<u16>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            proxies.len() == socks_ports.len() && !proxies.is_empty(),
            "invalid Xray batch"
        );
        let mut inbounds = Vec::with_capacity(proxies.len());
        let mut outbounds = Vec::with_capacity(proxies.len());
        let mut rules = Vec::with_capacity(proxies.len());
        for (index, (proxy, port)) in proxies.iter().zip(&socks_ports).enumerate() {
            let inbound_tag = format!("in-{index}");
            let outbound_tag = format!("out-{index}");
            inbounds.push(serde_json::json!({"tag":inbound_tag,"listen":"127.0.0.1","port":port,"protocol":"socks","settings":{"udp":false}}));
            let mut outbound = xray_outbound(proxy)?;
            outbound["tag"] = serde_json::Value::String(outbound_tag.clone());
            outbounds.push(outbound);
            rules.push(serde_json::json!({"type":"field","inboundTag":[format!("in-{index}")],"outboundTag":outbound_tag}));
        }
        let config = serde_json::json!({
            "log":{"loglevel":"warning"},
            "inbounds":inbounds,
            "outbounds":outbounds,
            "routing":{"domainStrategy":"AsIs","rules":rules}
        });
        let config_path = std::env::temp_dir().join(format!(
            "proxy-fleet-xray-batch-{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        tokio::fs::write(&config_path, serde_json::to_vec(&config)?).await?;
        let child = Command::new(binary)
            .arg("run")
            .arg("-config")
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            // See the single-session startup path above: do not leave an
            // unread pipe attached to a long-lived batch process.
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("starting Xray binary {binary}"))?;
        let mut session = Self {
            child,
            config_path,
            socks_ports,
        };
        if let Err(error) = session.wait_ready().await {
            session.stop().await;
            return Err(error);
        }
        Ok(session)
    }

    async fn wait_ready(&mut self) -> anyhow::Result<()> {
        for _ in 0..30 {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!("Xray batch exited during startup: {status}");
            }
            let mut ready = true;
            for port in &self.socks_ports {
                if TcpStream::connect(("127.0.0.1", *port)).await.is_err() {
                    ready = false;
                    break;
                }
            }
            if ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("Xray batch SOCKS inbounds did not become ready")
    }

    pub async fn stop(&mut self) {
        terminate_and_reap(&mut self.child, "Xray batch child").await;
        let _ = tokio::fs::remove_file(&self.config_path).await;
    }
}

/// Stop a child without leaking a zombie. Xray receives SIGTERM first so it
/// can close sockets cleanly; only a process that ignores the grace period is
/// force-killed. `wait` is always called after either path.
async fn terminate_and_reap(child: &mut Child, label: &str) {
    match child.try_wait() {
        Ok(Some(_)) => return,
        Err(error) => {
            warn!(%error, %label, "could not inspect Xray child state");
        }
        Ok(None) => {}
    }

    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: `pid` is supplied by Tokio for this exact child process;
        // sending SIGTERM does not dereference memory or widen process scope.
        let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if result != 0 {
            warn!(error = %std::io::Error::last_os_error(), %label, "could not send SIGTERM to Xray child");
        }
    }

    match tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
        Ok(Ok(_)) => return,
        Ok(Err(error)) => {
            warn!(%error, %label, "could not reap Xray child after SIGTERM");
            return;
        }
        Err(_) => {}
    }
    if let Err(error) = child.start_kill() {
        warn!(%error, %label, "could not force-stop Xray child");
        return;
    }
    if let Err(error) = child.wait().await {
        warn!(%error, %label, "could not reap Xray child after force stop");
    }
}

impl Drop for XrayBatchSession {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

impl Drop for XraySession {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

pub async fn allocate_port(range: RangeInclusive<u16>) -> anyhow::Result<u16> {
    for port in range {
        if tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return Ok(port);
        }
    }
    anyhow::bail!("no available test port")
}

pub async fn allocate_ports(range: RangeInclusive<u16>, count: usize) -> anyhow::Result<Vec<u16>> {
    let mut ports = Vec::with_capacity(count);
    for port in range {
        if ports.len() == count {
            break;
        }
        if tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .is_ok()
        {
            ports.push(port);
        }
    }
    anyhow::ensure!(ports.len() == count, "not enough available test ports");
    Ok(ports)
}
