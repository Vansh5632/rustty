/// Tests for the CLI/REPL command parsing and execution.
/// We test by invoking the binary with --demo to verify it runs end-to-end.
use std::process::Command;

#[test]
fn test_demo_mode_runs_successfully() {
    let output = Command::new(env!("CARGO_BIN_EXE_Rustdb"))
        .arg("--demo")
        .arg("--data-dir")
        .arg(tempfile::TempDir::new().unwrap().path().to_str().unwrap())
        .output()
        .expect("Failed to execute binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Demo mode should exit successfully.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("RustDB Demo"),
        "Demo output should contain header"
    );
    assert!(
        stdout.contains("Created table 'users'"),
        "Demo should create users table"
    );
    assert!(
        stdout.contains("Inserted row"),
        "Demo should insert rows"
    );
    assert!(
        stdout.contains("Demo complete"),
        "Demo should complete"
    );
}

#[test]
fn test_help_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_Rustdb"))
        .arg("--help")
        .output()
        .expect("Failed to execute binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("LSM-tree") || stdout.contains("RustDB") || stdout.contains("rustdb"));
}

#[test]
fn test_version_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_Rustdb"))
        .arg("--version")
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("0.1.0"));
}
