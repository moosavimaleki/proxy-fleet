//! Lease-based public subscription publisher. It never writes a blank feed merely because
//! the new tester has not produced a successful observation yet.

use std::{path::Path, time::Duration};

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD};
use tokio::process::Command;

use crate::{config::PublishingConfig, storage::Store};

#[derive(Debug, Clone, serde::Serialize)]
pub struct PublishResult {
    pub active_count: usize,
    pub changed: bool,
    pub committed: bool,
    pub pushed: bool,
    pub commit: String,
}

pub async fn publish(
    store: &Store,
    database_path: &str,
    config: &PublishingConfig,
) -> anyhow::Result<PublishResult> {
    if !config.enabled {
        return Ok(PublishResult {
            active_count: 0,
            changed: false,
            committed: false,
            pushed: false,
            commit: String::new(),
        });
    }
    let snapshot = store.publication_snapshot().await?;
    let configs = snapshot.raw_configs;
    // Do not replace a public feed during initial migration before any valid lease exists.
    if configs.is_empty() {
        return Ok(PublishResult {
            active_count: 0,
            changed: false,
            committed: false,
            pushed: false,
            commit: String::new(),
        });
    }
    let raw = format!("{}\n", configs.join("\n")).into_bytes();
    let encoded = format!("{}\n", STANDARD.encode(&raw)).into_bytes();
    let data_dir = Path::new(database_path)
        .parent()
        .unwrap_or_else(|| Path::new("data"));
    let snapshot_dir = data_dir.join("publish");
    let changed = write_if_changed(&snapshot_dir.join("active-raw.txt"), &raw).await?
        | write_if_changed(&snapshot_dir.join("active.txt"), &encoded).await?;
    let repo_dir = data_dir.join("publisher-repo");
    ensure_repo(&repo_dir, config).await?;
    rebase_to_remote(&repo_dir, &config.git_branch).await?;
    let repo_changed = write_if_changed(&repo_dir.join("subscriptions/active-raw.txt"), &raw)
        .await?
        | write_if_changed(&repo_dir.join("subscriptions/active.txt"), &encoded).await?;
    let mut committed = false;
    if repo_changed {
        run_git(
            &repo_dir,
            &[
                "add",
                "--",
                "subscriptions/active-raw.txt",
                "subscriptions/active.txt",
            ],
        )
        .await?;
        run_git(
            &repo_dir,
            &[
                "commit",
                "-m",
                &format!(
                    "chore(subscription): publish {} leased proxies (generation {})",
                    configs.len(),
                    snapshot.generation,
                ),
            ],
        )
        .await?;
        committed = true;
    }
    let pending = run_git(
        &repo_dir,
        &[
            "rev-list",
            "--count",
            &format!("origin/{}..HEAD", config.git_branch),
        ],
    )
    .await?;
    let pushed = pending.trim().parse::<u64>().unwrap_or(0) > 0;
    if pushed {
        push_with_rebase_retry(&repo_dir, &config.git_branch).await?;
    }
    let commit = run_git(&repo_dir, &["rev-parse", "--short", "HEAD"])
        .await
        .unwrap_or_default()
        .trim()
        .to_owned();
    Ok(PublishResult {
        active_count: configs.len(),
        changed,
        committed,
        pushed,
        commit,
    })
}

/// A publisher shares its branch with application code. A concurrent code or
/// subscription commit can make the first push non-fast-forward; fetch and
/// rebase once, never reset, then retry the exact HEAD that was rendered.
async fn push_with_rebase_retry(repo_dir: &Path, branch: &str) -> anyhow::Result<()> {
    let push_args = ["push", "origin", &format!("HEAD:{branch}")];
    match run_git(repo_dir, &push_args).await {
        Ok(_) => Ok(()),
        Err(first_error) => {
            rebase_to_remote(repo_dir, branch).await.with_context(|| {
                format!("push rejected and safe rebase retry could not be prepared: {first_error}")
            })?;
            run_git(repo_dir, &push_args)
                .await
                .context("publisher push failed after safe rebase retry")?;
            Ok(())
        }
    }
}

async fn rebase_to_remote(repo_dir: &Path, branch: &str) -> anyhow::Result<()> {
    run_git(repo_dir, &["fetch", "origin", branch]).await?;
    let target = format!("origin/{branch}");
    if let Err(error) = run_git(repo_dir, &["rebase", &target]).await {
        // A rebase abort returns the working tree to the exact pre-rebase
        // commit; unlike reset it never throws away a rendered publication.
        let _ = run_git(repo_dir, &["rebase", "--abort"]).await;
        return Err(error).context("publisher rebase failed");
    }
    Ok(())
}

async fn ensure_repo(repo_dir: &Path, config: &PublishingConfig) -> anyhow::Result<()> {
    if !repo_dir.join(".git").is_dir() {
        let parent = repo_dir.parent().expect("publisher repo has parent");
        tokio::fs::create_dir_all(parent).await?;
        if repo_dir.exists()
            && tokio::fs::read_dir(repo_dir)
                .await?
                .next_entry()
                .await?
                .is_some()
        {
            anyhow::bail!(
                "publisher directory is not an empty Git repository: {}",
                repo_dir.display()
            );
        }
        run_command(
            parent,
            &[
                "git",
                "clone",
                "--branch",
                &config.git_branch,
                "--single-branch",
                &config.git_remote,
                &repo_dir.to_string_lossy(),
            ],
        )
        .await?;
    }
    run_git(
        repo_dir,
        &["remote", "set-url", "origin", &config.git_remote],
    )
    .await?;
    run_git(repo_dir, &["config", "user.name", &config.author_name]).await?;
    run_git(repo_dir, &["config", "user.email", &config.author_email]).await?;
    Ok(())
}

async fn run_git(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let mut command = vec!["git"];
    command.extend_from_slice(args);
    run_command(cwd, &command).await
}

async fn run_command(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let mut command = Command::new(args[0]);
    command
        .args(&args[1..])
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0");
    command.env(
        "GIT_SSH_COMMAND",
        std::env::var("GIT_SSH_COMMAND")
            .unwrap_or_else(|_| "ssh -o BatchMode=yes -o ConnectTimeout=10".to_owned()),
    );
    let output = tokio::time::timeout(Duration::from_secs(45), command.output())
        .await
        .map_err(|_| anyhow::anyhow!("publisher command timed out"))??;
    if !output.status.success() {
        anyhow::bail!(
            "publisher command failed: {}",
            String::from_utf8_lossy(&output.stderr)
                .replace('\n', " ")
                .chars()
                .take(500)
                .collect::<String>()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn write_if_changed(path: &Path, bytes: &[u8]) -> anyhow::Result<bool> {
    if tokio::fs::read(path).await.ok().as_deref() == Some(bytes) {
        return Ok(false);
    }
    let parent = path.parent().expect("output has parent");
    tokio::fs::create_dir_all(parent).await?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("output"),
        uuid::Uuid::new_v4().simple()
    ));
    tokio::fs::write(&temporary, bytes).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::{path::Path, process::Command, time::Duration};

    use crate::{
        config::PublishingConfig,
        domain::{evidence::TestStage, failure::FailureClass},
        parser::parse_share_url,
        storage::{Store, TestEventInput},
    };

    use super::{publish, push_with_rebase_retry, write_if_changed};

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    }

    fn init_remote(temp: &tempfile::TempDir) -> std::path::PathBuf {
        let remote = temp.path().join("remote.git");
        git(
            temp.path(),
            &[
                "init",
                "--bare",
                "--initial-branch=main",
                remote.to_str().expect("remote path"),
            ],
        );
        let seed = temp.path().join("seed");
        std::fs::create_dir(&seed).expect("seed directory");
        git(&seed, &["init", "--initial-branch=main"]);
        git(&seed, &["config", "user.name", "test"]);
        git(&seed, &["config", "user.email", "test@example.test"]);
        std::fs::create_dir_all(seed.join("subscriptions")).expect("subscriptions directory");
        std::fs::write(seed.join("subscriptions/active.txt"), "\n").expect("seed encoded feed");
        std::fs::write(seed.join("subscriptions/active-raw.txt"), "\n").expect("seed raw feed");
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-m", "seed"]);
        git(
            &seed,
            &[
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&seed, &["push", "-u", "origin", "main"]);
        remote
    }

    async fn publishable_store(temp: &tempfile::TempDir) -> (Store, String) {
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).expect("data directory");
        let store = Store::connect(data.join("app.db")).await.expect("store");
        store.migrate().await.expect("migration");
        let proxy = parse_share_url(
            "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls&sni=example.com#fixture",
            "test",
        )
        .expect("proxy");
        store.ingest_proxy(&proxy, 1).await.expect("ingest");
        let id = sqlx::query_scalar::<_, String>("SELECT id FROM nodes WHERE config_hash = ?")
            .bind(proxy.config_hash)
            .fetch_one(store.pool())
            .await
            .expect("node id");
        store
            .apply_test_event(
                TestEventInput {
                    proxy_id: id,
                    run_id: "download".to_owned(),
                    stage: TestStage::Download,
                    class: FailureClass::Success,
                    fast_download: true,
                    latency_ms: Some(10.0),
                    download_bps: Some(1_000_000.0),
                    bytes_transferred: Some(1_000_000),
                    duration_ms: Some(1_000),
                    endpoint: Some("https://example.test".to_owned()),
                    system_pressure: Some(0.1),
                    incident_id: None,
                    detail_json: serde_json::json!({}),
                },
                Duration::from_secs(10),
                Duration::from_secs(300),
                Duration::from_secs(1_800),
            )
            .await
            .expect("activation");
        (store, data.join("app.db").to_string_lossy().to_string())
    }

    #[tokio::test]
    async fn publisher_output_is_atomic_and_noops_for_identical_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("subscriptions/active-raw.txt");
        assert!(
            write_if_changed(&output, b"one\n")
                .await
                .expect("initial write")
        );
        assert!(
            !write_if_changed(&output, b"one\n")
                .await
                .expect("no-op write")
        );
        assert!(
            write_if_changed(&output, b"two\n")
                .await
                .expect("changed write")
        );
        assert_eq!(tokio::fs::read(output).await.expect("read"), b"two\n");
    }

    #[tokio::test]
    async fn publisher_commits_pushes_changes_and_noops_on_identical_snapshot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let remote = init_remote(&temp);
        let (store, database_path) = publishable_store(&temp).await;
        let config = PublishingConfig {
            enabled: true,
            git_remote: remote.to_string_lossy().to_string(),
            ..PublishingConfig::default()
        };
        let first = publish(&store, &database_path, &config)
            .await
            .expect("first publication");
        assert!(first.changed && first.committed && first.pushed);
        let second = publish(&store, &database_path, &config)
            .await
            .expect("identical publication");
        assert!(!second.changed && !second.committed && !second.pushed);
        let raw = git(
            temp.path(),
            &[
                "--git-dir",
                remote.to_str().expect("remote path"),
                "show",
                "main:subscriptions/active-raw.txt",
            ],
        );
        assert!(raw.starts_with("vless://"));
    }

    #[tokio::test]
    async fn rejected_push_is_rebased_and_retried_without_resetting_publication() {
        let temp = tempfile::tempdir().expect("tempdir");
        let remote = init_remote(&temp);
        let working = temp.path().join("working");
        let concurrent = temp.path().join("concurrent");
        git(
            temp.path(),
            &[
                "clone",
                remote.to_str().expect("remote path"),
                working.to_str().expect("working path"),
            ],
        );
        git(
            temp.path(),
            &[
                "clone",
                remote.to_str().expect("remote path"),
                concurrent.to_str().expect("concurrent path"),
            ],
        );
        for repo in [&working, &concurrent] {
            git(repo, &["config", "user.name", "test"]);
            git(repo, &["config", "user.email", "test@example.test"]);
        }
        std::fs::write(working.join("working.txt"), "publication\n").expect("working edit");
        git(&working, &["add", "working.txt"]);
        git(&working, &["commit", "-m", "publication"]);
        std::fs::write(concurrent.join("concurrent.txt"), "concurrent\n").expect("concurrent edit");
        git(&concurrent, &["add", "concurrent.txt"]);
        git(&concurrent, &["commit", "-m", "concurrent"]);
        git(&concurrent, &["push", "origin", "main"]);
        push_with_rebase_retry(&working, "main")
            .await
            .expect("safe push retry");
        for path in ["working.txt", "concurrent.txt"] {
            let shown = git(
                temp.path(),
                &[
                    "--git-dir",
                    remote.to_str().expect("remote path"),
                    "show",
                    &format!("main:{path}"),
                ],
            );
            assert!(!shown.is_empty(), "remote lost {path}");
        }
    }
}
