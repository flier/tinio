//! Binary smoke test: the facade binary must exit successfully.

#[test]
fn binary_exits_success() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_tinio"))
        .status()
        .unwrap();
    assert!(status.success());
}
