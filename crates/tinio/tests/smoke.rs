//! Binary smoke test: the facade binary must exit successfully.

use std::process::Command;

#[test]
fn binary_exits_success() {
    let status = Command::new(env!("CARGO_BIN_EXE_tinio")).status().unwrap();
    assert!(status.success());
}
