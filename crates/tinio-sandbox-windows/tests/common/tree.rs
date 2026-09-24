//! Volumes and the on-disk tree a confinement test grants.

use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
};

use tinio_sandbox_windows::acl;

/// The volumes the suite exercises.
///
/// `TINIO_SANDBOX_TEST_VOLUMES` (semicolon separated) selects them; without it,
/// the volume holding the system temp directory. The design's evidence shows
/// ReFS and NTFS agree on every probed ACL case, so a regression that broke one
/// would be invisible on a single-volume run — this is how both are covered.
///
/// **A selection that names no usable volume fails rather than answering an
/// empty list**, because every caller either loops over this or takes its first
/// entry: an empty answer would make those tests pass by doing nothing, which is
/// exactly the shape of green this phase refuses.
pub fn volumes() -> Vec<PathBuf> {
    let Ok(raw) = env::var("TINIO_SANDBOX_TEST_VOLUMES") else {
        return vec![env::temp_dir()];
    };
    let volumes: Vec<PathBuf> = raw
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty() && Path::new(entry).is_dir())
        .map(PathBuf::from)
        .collect();
    assert!(
        !volumes.is_empty(),
        "TINIO_SANDBOX_TEST_VOLUMES={raw:?} names no directory that exists on this host, so \
         every test looping over the volumes would run zero cases and pass"
    );
    volumes
}

/// The first volume, for the tests that exercise one's worth of tree.
pub fn one_volume() -> PathBuf {
    volumes()
        .into_iter()
        .next()
        .expect("volumes() answers with at least one volume or fails")
}

/// One tree's worth of probes: what the child reads and what it writes.
pub struct Tree {
    /// The tree root, on the volume under test.
    pub path: PathBuf,
    /// The served root. Exposed with the caller's access mode.
    pub root: PathBuf,
    /// A sibling with no package-SID ACE, carrying `Users` instead.
    pub denied: PathBuf,
    /// A directory exposed read-write, where the child writes its report.
    pub report: PathBuf,
    /// The address the child should try to reach, when a test sets one. It
    /// travels in the probe file, so the confined child needs no environment
    /// channel to learn it.
    pub target: Option<SocketAddr>,
}

impl Tree {
    /// The tree whose directories sit under `path`, without touching the disk.
    ///
    /// The one rule `tree()` and [`launch_confined_at`] share: `root`, `denied`
    /// and `report` are `path`'s three children, in that order, and nothing about
    /// the layout is carried in a value a second process cannot receive. A
    /// launcher role is a fresh process that got a path in an environment
    /// variable and nothing else, so it rebuilds the same three directories from
    /// it rather than being handed a `Tree`.
    pub fn at(path: &Path) -> Self {
        Self {
            root: path.join("root"),
            denied: path.join("denied"),
            report: path.join("report"),
            path: path.to_path_buf(),
            target: None,
        }
    }

    /// The probe text, without writing it.
    pub fn probe_text(&self) -> String {
        let mut text = format!(
            "root={}\ndenied={}\nreport={}\n",
            self.root.display(),
            self.denied.display(),
            self.report.join("report.txt").display()
        );
        if let Some(target) = self.target {
            text.push_str(&format!("target={target}\n"));
        }
        text
    }

    /// Write the probe file and return its path — what the payload's config
    /// bytes carry.
    ///
    /// **It goes into the report directory and not into `path`**, and that is
    /// the difference between a suite that tests the confinement and one that
    /// tests nothing. `path` is the tree's own directory and is exposed to
    /// nobody; the confined child is an AppContainer, so its reads are checked
    /// against the package SID and the user's own ACEs do not help it — a probe
    /// written as a sibling of `root` and `report` is a file the child cannot
    /// open, and every role that reads it panics on its first line, taking the
    /// confined cases of Tasks 8, 9, 10 and 12 with it. The report directory is
    /// the one path every test here grants read-write, which is what makes it the
    /// only place a probe can live.
    pub fn probe(&self) -> PathBuf {
        let probe = self.report.join("probe.txt");
        fs::write(&probe, self.probe_text()).expect("write the probe file");
        probe
    }

    /// What the child reported, if it got that far.
    pub fn report_text(&self) -> String {
        fs::read_to_string(self.report.join("report.txt")).unwrap_or_default()
    }
}

/// Build the standard tree under `volume`.
///
/// The `denied` sibling carries `Users` and no package-SID ACE, which is the
/// verified row every confinement test reproduces: those ACEs do not help.
pub fn tree(volume: &Path) -> (tempfile::TempDir, Tree) {
    let dir = tempfile::TempDir::new_in(volume).expect("temp dir on the volume");
    let tree = Tree::at(&dir.path().join("tree"));
    fs::create_dir_all(&tree.root).expect("root");
    fs::create_dir_all(&tree.denied).expect("denied");
    fs::create_dir_all(&tree.report).expect("report");
    // Seeded *before* the grant: this is design §6's inheritance question as the
    // confinement suite sees it — Task 4 settled the ACL shape, and this is the
    // functional half of the same answer.
    fs::write(tree.root.join("granted.txt"), b"granted").expect("seed");
    fs::write(tree.denied.join("secret.txt"), b"secret").expect("seed");
    let granted = Command::new(acl::icacls_binary())
        .arg(&tree.denied)
        .args(["/grant", r"*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users on the denied tree");
    assert!(
        granted.status.success(),
        "{}",
        String::from_utf8_lossy(&granted.stderr)
    );
    (dir, tree)
}

/// Run `icacls` the way a human would, and hand back its rendered output — a
/// re-export, because the suites that assert on the DACL text and the crate's
/// own tests assert the same way, and the rule they share is stated once in
/// [`tinio_sandbox_windows::testing`].
#[allow(unused_imports)] // a re-export: the binaries that call it are the readers
pub use tinio_sandbox_windows::testing::icacls;
