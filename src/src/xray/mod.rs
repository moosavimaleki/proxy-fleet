use std::{
    collections::HashSet,
    ops::RangeInclusive,
    path::PathBuf,
    sync::{LazyLock, Mutex},
    time::Duration,
};

use anyhow::Context;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpStream,
    process::{Child, Command},
    task::JoinHandle,
};
use tracing::{debug, warn};

#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;

use crate::parser::{ParsedProxy, xray_outbound};

pub mod runtime;

static PORT_RESERVATIONS: LazyLock<Mutex<HashSet<u16>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

const XRAY_OUTPUT_LIMIT_BYTES: usize = 32 * 1024;
const XRAY_CONFIG_PREFIX: &str = "proxy-fleet-xray-";

/// Drain child pipes continuously while retaining only a bounded tail for
/// startup diagnostics. This avoids both pipe backpressure and unbounded logs.
#[derive(Clone, Default)]
struct ProcessLogs {
    stdout: std::sync::Arc<Mutex<Vec<u8>>>,
    stderr: std::sync::Arc<Mutex<Vec<u8>>>,
}

impl ProcessLogs {
    fn append(&self, is_stderr: bool, bytes: &[u8]) {
        let target = if is_stderr {
            &self.stderr
        } else {
            &self.stdout
        };
        let mut target = target.lock().expect("Xray output mutex is not poisoned");
        target.extend_from_slice(bytes);
        if target.len() > XRAY_OUTPUT_LIMIT_BYTES {
            let excess = target.len() - XRAY_OUTPUT_LIMIT_BYTES;
            target.drain(..excess);
        }
    }

    fn summary(&self) -> String {
        let stderr = self
            .stderr
            .lock()
            .expect("Xray output mutex is not poisoned");
        let stdout = self
            .stdout
            .lock()
            .expect("Xray output mutex is not poisoned");
        let bytes = if stderr.is_empty() {
            &*stdout
        } else {
            &*stderr
        };
        String::from_utf8_lossy(bytes)
            .trim()
            .chars()
            .take(2_000)
            .collect()
    }
}

fn drain_output<R>(mut reader: R, logs: ProcessLogs, is_stderr: bool) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => return,
                Ok(read) => logs.append(is_stderr, &buffer[..read]),
                Err(error) => {
                    debug!(%error, "could not drain Xray child output");
                    return;
                }
            }
        }
    })
}

pub async fn detect_version(binary: &str) -> anyhow::Result<String> {
    let output = tokio::time::timeout(
        Duration::from_secs(3),
        Command::new(binary)
            .arg("version")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("timed out while reading Xray version")??;
    anyhow::ensure!(
        output.status.success(),
        "Xray version command exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let version = String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();
    anyhow::ensure!(
        !version.is_empty(),
        "Xray version command produced no version"
    );
    Ok(version)
}

/// Remove only Xray processes and temporary configs created by this project
/// after an ungraceful restart. The config filename is an ownership marker;
/// no generic process name or port scan is ever used.
pub fn cleanup_project_orphans() -> anyhow::Result<usize> {
    #[cfg(not(target_os = "linux"))]
    {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    {
        let mut stopped = 0;
        for entry in std::fs::read_dir("/proc").context("reading /proc for owned Xray processes")? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            let cmdline = match std::fs::read(entry.path().join("cmdline")) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if owned_xray_command(&cmdline) && terminate_orphan(pid) {
                stopped += 1;
            }
        }
        let temporary = std::env::temp_dir();
        if let Ok(entries) = std::fs::read_dir(temporary) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name.to_string_lossy().starts_with(XRAY_CONFIG_PREFIX) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Ok(stopped)
    }
}

fn owned_xray_command(cmdline: &[u8]) -> bool {
    let args: Vec<_> = cmdline
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .collect();
    args.windows(2).any(|pair| {
        pair[0] == b"-config"
            && std::path::Path::new(std::ffi::OsStr::from_bytes(pair[1]))
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(XRAY_CONFIG_PREFIX))
    })
}

#[cfg(target_os = "linux")]
fn terminate_orphan(pid: i32) -> bool {
    // SAFETY: `pid` comes from /proc and `owned_xray_command` has verified a
    // project-specific config marker. The negative form targets the owned
    // group created by this binary; the direct fallback covers older groups.
    let group_result = unsafe { libc::kill(-pid, libc::SIGTERM) };
    if group_result == 0 {
        return true;
    }
    // SAFETY: same scoped ownership predicate as above.
    unsafe { libc::kill(pid, libc::SIGTERM) == 0 }
}

fn spawn_xray(
    binary: &str,
    config_path: &PathBuf,
) -> anyhow::Result<(Child, ProcessLogs, Vec<JoinHandle<()>>)> {
    let mut command = Command::new(binary);
    command
        .arg("run")
        .arg("-config")
        .arg(config_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Xray and any helper it spawns are contained in this group.
        command.as_std_mut().process_group(0);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("starting Xray binary {binary}"))?;
    let logs = ProcessLogs::default();
    let mut drains = Vec::with_capacity(2);
    if let Some(stdout) = child.stdout.take() {
        drains.push(drain_output(stdout, logs.clone(), false));
    }
    if let Some(stderr) = child.stderr.take() {
        drains.push(drain_output(stderr, logs.clone(), true));
    }
    Ok((child, logs, drains))
}

/// A process-local reservation closes the race where concurrent testers both
/// observe the same free TCP port between probe bind and Xray spawn. The OS
/// still owns the definitive bind, but a failed Xray start is now isolated
/// rather than caused by another fleet worker selecting the same port.
pub struct PortReservation {
    port: u16,
}

impl PortReservation {
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for PortReservation {
    fn drop(&mut self) {
        let mut reservations = PORT_RESERVATIONS
            .lock()
            .expect("port reservation mutex is not poisoned");
        reservations.remove(&self.port);
    }
}

pub struct XraySession {
    child: Child,
    config_path: PathBuf,
    logs: ProcessLogs,
    drain_tasks: Vec<JoinHandle<()>>,
    pub socks_port: u16,
    _reservation: Option<PortReservation>,
}

/// A single Xray process with one SOCKS inbound per candidate.  The routing
/// table pins each inbound to its own outbound, avoiding a process-per-proxy
/// storm while retaining an unambiguous observation for every candidate.
pub struct XrayBatchSession {
    child: Child,
    config_path: PathBuf,
    logs: ProcessLogs,
    drain_tasks: Vec<JoinHandle<()>>,
    pub socks_ports: Vec<u16>,
    _reservations: Vec<PortReservation>,
}

impl XraySession {
    pub async fn start(
        binary: &str,
        proxy: &ParsedProxy,
        reservation: PortReservation,
    ) -> anyhow::Result<Self> {
        Self::start_with_listen(binary, proxy, reservation, "127.0.0.1").await
    }

    pub async fn start_with_listen(
        binary: &str,
        proxy: &ParsedProxy,
        reservation: PortReservation,
        listen: &str,
    ) -> anyhow::Result<Self> {
        Self::start_at_port(binary, proxy, reservation.port(), Some(reservation), listen).await
    }

    pub async fn start_fixed_with_listen(
        binary: &str,
        proxy: &ParsedProxy,
        socks_port: u16,
        listen: &str,
    ) -> anyhow::Result<Self> {
        Self::start_at_port(binary, proxy, socks_port, None, listen).await
    }

    async fn start_at_port(
        binary: &str,
        proxy: &ParsedProxy,
        socks_port: u16,
        reservation: Option<PortReservation>,
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
        let (child, logs, drain_tasks) = spawn_xray(binary, &config_path)?;
        let mut session = Self {
            child,
            config_path,
            logs,
            drain_tasks,
            socks_port,
            _reservation: reservation,
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
                anyhow::bail!(
                    "Xray exited during startup: {status}; {}",
                    self.logs.summary()
                );
            }
            if TcpStream::connect(("127.0.0.1", self.socks_port))
                .await
                .is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "Xray SOCKS inbound did not become ready; {}",
            self.logs.summary()
        )
    }

    pub async fn stop(&mut self) {
        terminate_and_reap(&mut self.child, "Xray child").await;
        join_output_drains(&mut self.drain_tasks).await;
        let _ = tokio::fs::remove_file(&self.config_path).await;
    }
}

impl XrayBatchSession {
    pub async fn start(
        binary: &str,
        proxies: &[ParsedProxy],
        reservations: Vec<PortReservation>,
    ) -> anyhow::Result<Self> {
        let socks_ports: Vec<_> = reservations.iter().map(PortReservation::port).collect();
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
        let (child, logs, drain_tasks) = spawn_xray(binary, &config_path)?;
        let mut session = Self {
            child,
            config_path,
            logs,
            drain_tasks,
            socks_ports,
            _reservations: reservations,
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
                anyhow::bail!(
                    "Xray batch exited during startup: {status}; {}",
                    self.logs.summary()
                );
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
        anyhow::bail!(
            "Xray batch SOCKS inbounds did not become ready; {}",
            self.logs.summary()
        )
    }

    pub async fn stop(&mut self) {
        terminate_and_reap(&mut self.child, "Xray batch child").await;
        join_output_drains(&mut self.drain_tasks).await;
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
        // SAFETY: `spawn_xray` creates a new process group whose ID is the
        // child PID. A negative PID confines this signal to that owned group.
        let result = unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
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
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: see the SIGTERM rationale above; only the owned group gets
        // SIGKILL after its bounded grace period has expired.
        let result = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        if result != 0 {
            warn!(error = %std::io::Error::last_os_error(), %label, "could not send SIGKILL to Xray process group");
        }
    }
    if let Err(error) = child.start_kill() {
        warn!(%error, %label, "could not force-stop Xray child");
        return;
    }
    if let Err(error) = child.wait().await {
        warn!(%error, %label, "could not reap Xray child after force stop");
    }
}

async fn join_output_drains(tasks: &mut Vec<JoinHandle<()>>) {
    for task in tasks.drain(..) {
        let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
    }
}

impl Drop for XrayBatchSession {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: the child was created in its own Xray process group.
            let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
        let _ = self.child.start_kill();
        for task in &self.drain_tasks {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.config_path);
    }
}

impl Drop for XraySession {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: see the batch Drop implementation above.
            let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
        let _ = self.child.start_kill();
        for task in &self.drain_tasks {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.config_path);
    }
}

pub async fn allocate_port(range: RangeInclusive<u16>) -> anyhow::Result<PortReservation> {
    for port in range {
        {
            let reservations = PORT_RESERVATIONS
                .lock()
                .expect("port reservation mutex is not poisoned");
            if reservations.contains(&port) {
                continue;
            }
        }
        if tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .is_ok()
        {
            let mut reservations = PORT_RESERVATIONS
                .lock()
                .expect("port reservation mutex is not poisoned");
            if reservations.insert(port) {
                return Ok(PortReservation { port });
            }
        }
    }
    anyhow::bail!("no available test port")
}

pub async fn allocate_ports(
    range: RangeInclusive<u16>,
    count: usize,
) -> anyhow::Result<Vec<PortReservation>> {
    let mut ports = Vec::with_capacity(count);
    for port in range {
        if ports.len() == count {
            break;
        }
        {
            let reservations = PORT_RESERVATIONS
                .lock()
                .expect("port reservation mutex is not poisoned");
            if reservations.contains(&port) {
                continue;
            }
        }
        if tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .is_ok()
        {
            let mut reservations = PORT_RESERVATIONS
                .lock()
                .expect("port reservation mutex is not poisoned");
            if reservations.insert(port) {
                ports.push(PortReservation { port });
            }
        }
    }
    anyhow::ensure!(ports.len() == count, "not enough available test ports");
    Ok(ports)
}

#[cfg(test)]
mod tests {
    use super::{ProcessLogs, allocate_port, owned_xray_command};

    #[tokio::test]
    async fn concurrent_allocations_do_not_reserve_the_same_port() {
        let range = 45_000..=45_200;
        let (left, right) = tokio::join!(allocate_port(range.clone()), allocate_port(range));
        let left = left.expect("first reservation");
        let right = right.expect("second reservation");
        assert_ne!(left.port(), right.port());
    }

    #[test]
    fn output_tail_is_bounded_and_prefers_stderr() {
        let logs = ProcessLogs::default();
        logs.append(false, b"stdout");
        logs.append(true, &vec![b'x'; super::XRAY_OUTPUT_LIMIT_BYTES + 16]);
        assert_eq!(
            logs.stderr.lock().expect("output lock").len(),
            super::XRAY_OUTPUT_LIMIT_BYTES
        );
        assert!(logs.summary().starts_with('x'));
    }

    #[test]
    fn orphan_cleanup_requires_our_config_marker() {
        assert!(owned_xray_command(
            b"/usr/local/bin/xray\0run\0-config\0/tmp/proxy-fleet-xray-batch-a.json\0"
        ));
        assert!(!owned_xray_command(
            b"/usr/local/bin/xray\0run\0-config\0/tmp/another-service.json\0"
        ));
    }
}
