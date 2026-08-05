//! Lease-based public subscription publisher. It never writes a blank feed merely because
//! the new tester has not produced a successful observation yet.

use std::{path::Path, time::Duration};

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
    let configs = store.list_publishable_raw_configs().await?;
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
    run_git(&repo_dir, &["fetch", "origin", &config.git_branch]).await?;
    run_git(
        &repo_dir,
        &["rebase", &format!("origin/{}", config.git_branch)],
    )
    .await?;
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
                    "chore(subscription): publish {} leased proxies",
                    configs.len()
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
        run_git(
            &repo_dir,
            &["push", "origin", &format!("HEAD:{}", config.git_branch)],
        )
        .await?;
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
