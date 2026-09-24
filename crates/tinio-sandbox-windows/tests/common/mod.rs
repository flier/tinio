//! Shared harness for the Windows confinement integration tests.
//!
//! Each integration binary is its own crate and calls a subset of these
//! helpers. The allows cover that split: a name another binary calls is not
//! dead, and the re-exports stay so call sites keep `common::`.

#![cfg(windows)]

#[allow(dead_code)]
mod launch;
#[allow(dead_code)]
mod roles;
#[allow(dead_code)]
mod signals;
#[allow(dead_code)]
mod tree;
#[allow(dead_code)]
mod wait;

#[allow(unused_imports)]
pub use launch::{
    Run, launch_confined, launch_confined_at, run_confined, run_confined_with, sid_for,
};
#[allow(unused_imports)]
pub use roles::{append_to_report, child_role, child_role_body, launcher_role, probe_dir};
#[allow(unused_imports)]
pub use signals::{
    ARMED, BEAT, STOPPED, STOPPED_EXIT, child_role_with_stop_hook, spawn_launcher,
    spawn_unrelayed_child_in_own_group, unrelayed_child_role,
};
#[allow(unused_imports)]
pub use tree::{Tree, icacls, one_volume, tree, volumes};
#[allow(unused_imports)]
pub use wait::{TIMEOUT, answer, wait_for_exit};
