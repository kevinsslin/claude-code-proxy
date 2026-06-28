use assert_cmd::Command;
use predicates::str::contains;
use tempfile::TempDir;

/// Run a codex auth command with a temp config dir that isolates
/// from the real user config. Overrides HOME so the legacy config
/// fallback also resolves within the temp dir.
fn codex_cmd() -> (Command, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("claude-code-proxy").unwrap();
    cmd.args(["codex", "auth", "status"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.env("HOME", temp.path());
    (cmd, temp)
}

#[test]
fn codex_auth_status_reads_stored_auth() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cmd, temp) = codex_cmd();
    let auth_dir = temp.path().join("codex");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000,"accountId":"acct_1"}"#,
    )?;
    cmd.assert().success().stdout(contains("Account: acct_1"));
    Ok(())
}

#[test]
fn codex_auth_status_reads_legacy_account_id_key() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cmd, temp) = codex_cmd();
    let auth_dir = temp.path().join("codex");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000,"account_id":"acct_2"}"#,
    )?;
    cmd.assert().success().stdout(contains("Account: acct_2"));
    Ok(())
}

#[test]
fn codex_auth_status_no_auth() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cmd, _temp) = codex_cmd();
    let output = cmd.output()?;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8(output.stdout)?.contains("Not authenticated"));
    Ok(())
}

#[test]
fn codex_auth_status_shows_auth_path() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cmd, temp) = codex_cmd();
    let auth_dir = temp.path().join("codex");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000,"accountId":"acct_3"}"#,
    )?;
    cmd.assert().success().stdout(contains("Auth path:"));
    Ok(())
}

#[test]
fn codex_auth_status_no_account_id_shows_no_account_line() -> Result<(), Box<dyn std::error::Error>>
{
    let (mut cmd, temp) = codex_cmd();
    let auth_dir = temp.path().join("codex");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000}"#,
    )?;
    let output = cmd.output()?;
    let out = String::from_utf8(output.stdout)?;
    assert!(output.status.success());
    assert!(out.contains("Authenticated: true"));
    assert!(!out.contains("Account:"));
    Ok(())
}
