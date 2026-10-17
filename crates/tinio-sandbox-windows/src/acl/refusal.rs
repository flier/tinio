//! **The two refusals a whitelisted path can earn: it is a reparse point, or it is not the
//! kind of object the grant's shape addresses.** Each is one sentence, and both are built
//! through one constructor because the sites that meet one are three — the whitelist's walk
//! over a spelled path's components, its sibling check, and the grant's own handle check — so
//! one operator rule reads one way wherever it fires.

use std::path::Path;

use crate::_core::Error;

/// **The sentence a reparse point is refused with, and the one place it is
/// spelled.** Three sites meet one: the whitelist's `refuse_reparse_points` — a
/// link anywhere in a *spelled* path, which is where the ACE would land on the
/// link's target — its `refuse_a_reparse_sibling`, and [`Object::verify`](super::descriptor::Object::verify), the
/// handle check every grant now passes.
///
/// **What it states is what a grant does through a link, and that is not one
/// thing — the wording says so because the measurement did** (this host,
/// 2026-09-27): `icacls /grant:r` on a path with a junction **above** the last
/// component resolves it and writes the ACE on the target, outside the exposed
/// tree (`…\hop\tinio` wrote `…\real\tinio`'s descriptor, and the pre-existing
/// file beneath it inherited the ACE); on a path whose **last** component is a
/// junction the ACE lands on the link's own object rather than on the target; on a
/// **file symlink** it lands on the target — the sibling-DLL escape the name-based
/// enumeration closes. None of the three is the ACE the caller asked for, and two
/// of them can be outside the exposure.
pub(crate) const REPARSE_POINT: &str = "is a reparse point (a junction or a symlink), and a grant through it lands on the link's own object or on whatever it resolves to — never on the path the caller named, and not necessarily inside the exposed tree";

/// **The refusal a grant's shape earns on an object of the wrong kind** — the
/// opened handle's own answer, and the replacement for the one failure `icacls`
/// reported as success: an inheritance-flagged ACE on a file is a silent no-op
/// (design §6's evidence row), and a plain ACE aimed at a directory leaves every
/// descendant ungranted, so a shape that disagrees with the object is refused
/// rather than written.
const NOT_THE_GRANTED_KIND: &str = "is not the kind of object this grant's shape addresses: an inherited (OI)(CI) grant and the program directory's plain grant need a directory, and a file grant needs one that is not a directory — on the wrong kind the ACE is a silent no-op when a tool writes it and reaches nothing when this crate does";

/// **The one constructor both refusals are built through**: an `Error::BadPath` whose field is
/// the path, emitted where it is built, so the event and the error cannot disagree.
///
/// **Hand-written for the same reason the caller's-file refusal is**: the error has to exist
/// before `refused` could be handed it, and `fail` would either drop the path or turn this into
/// a `Launch`. The event carries the core's fields — `step`, `path`, `error` — and is a security
/// decision, so an operator has to see it in the log rather than infer it from a startup failure
/// (shared §7's rule).
fn refusal(path: &Path, step: &'static str, reason: &'static str) -> Error {
    let error = Error::BadPath {
        path: path.to_path_buf(),
        reason,
    };
    tracing::error!(
        step,
        path = %path.display(),
        error = %error,
        "sandbox step failed"
    );
    error
}

/// The refusal a reparse point earns.
///
/// **The step is the caller's**, because the sites fail at different stages of a launch while
/// the *finding* is one: `step` names where in the launch the refusal happened, and
/// [`REPARSE_POINT`] is what happened.
pub(crate) fn reparse_refusal(path: &Path, step: &'static str) -> Error {
    refusal(path, step, REPARSE_POINT)
}

/// The refusal a shape that disagrees with the object earns, in [`reparse_refusal`]'s shape and
/// for its reason.
pub(super) fn kind_refusal(path: &Path) -> Error {
    refusal(path, "verify the granted path", NOT_THE_GRANTED_KIND)
}

#[cfg(test)]
mod tests {
    // The allowance is grant.rs's test module's, for the one `Arc` this module holds: shared
    // ownership inside one thread, which is what the code under test is given.
    #![allow(
        clippy::arc_with_non_send_sync,
        reason = "test context: `PackageSid` owns a raw `PSID` and is deliberately neither `Send` nor `Sync`, and the `Arc` is shared ownership inside one thread"
    )]

    use std::{fs, sync::Arc};

    use crate::{
        _core::AccessMode,
        acl::{
            Grants, Target,
            descriptor::{OwnedSid, acl_names},
            grant::{grant_to, has_ace},
            test_support::standing,
        },
        sid::ALL_APPLICATION_PACKAGES,
        testing::{icacls, junction, sid_for},
    };

    /// **The other direction of the kind check**: a directory-shaped grant on a
    /// file is refused rather than written.
    ///
    /// Both directory shapes are wrong on a file and wrong differently:
    /// `(OI)(CI)` is inherited by nothing, so the ACE reaches no one, and the
    /// program-directory form is the plain one, which the child's loader would read
    /// as an ACE on a file that is not the loader's. `icacls` writes neither (exit 0,
    /// nothing stored — design §6's evidence row); this is the same rule answered by
    /// the handle, and it is the check `backend::grant_whitelist` used to make with
    /// `is_dir` for decision 22's entry alone — the handle makes it for every shape,
    /// against the object the write would reach.
    #[test]
    fn a_directory_shaped_grant_on_a_file_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file = dir.path().join("plain.txt");
        fs::write(&file, b"x").expect("write");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");

        for target in [Target::Directory, Target::ProgramDirectory] {
            let refused = grant_to(&file, target, AccessMode::ReadOnly, standing(&sid))
                .expect_err("a directory-shaped grant needs a directory");
            assert!(
                refused.to_string().contains("not the kind of object"),
                "{target:?}: {refused}"
            );
            assert!(
                !acl_names(&file, sid.raw()).expect("read the DACL"),
                "{target:?} wrote an ACE on a file it refused"
            );
        }
    }

    /// **The object the grant checks is the object it writes**, which is the defect
    /// the handle route closed: a junction is planted at the path the whitelist
    /// resolved — the window Fix 1 is about — and the grant refuses it rather than
    /// following it to wherever it points.
    ///
    /// **What the refusal protects is measured rather than asserted** (this host,
    /// 2026-09-27): `icacls /grant:r` through a junction **above** the last component
    /// resolved it and wrote the ACE on the target, outside the exposed tree, and the
    /// target's pre-existing children inherited it. The second half of this test is
    /// that no ACE reached the target: the crate's own predicate *and* the tool's own
    /// rendering both say so, because an assertion that rested on `has_ace` alone
    /// would also pass for a DACL nothing had looked at.
    #[test]
    fn a_grant_refuses_a_junction_instead_of_following_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let real = dir.path().join("real");
        fs::create_dir_all(&real).expect("create the junction's target");
        let link = dir.path().join("swapped");
        junction(&link, &real);
        let sid = Arc::new(sid_for(dir.path()));
        let sid_text = sid.as_str().to_string();

        let grants = Grants::new(Arc::clone(&sid));
        let err = grants
            .apply(&link, Target::Directory, AccessMode::ReadOnly)
            .expect_err("a junction is refused, not followed");
        assert!(err.to_string().contains("is a reparse point"), "{err}");

        assert!(
            !has_ace(&real, &sid).expect("read the DACL"),
            "the ACE landed on the junction's target: {}",
            icacls(&real, &[])
        );
        assert!(
            !icacls(&real, &[]).contains(&sid_text),
            "the target carries no package ACE: {}",
            icacls(&real, &[])
        );

        // **The junction's own object is untouched too**, which is the half the old
        // wildcard-style route could take: a grant that follows the link writes there
        // instead of nowhere, and the confined child cannot use an ACE on a link
        // object. A DACL read of the link's own object is what sees it.
        let internal = has_ace(&link, &sid).expect("read the link's own DACL");
        assert!(!internal, "nothing was written on the link's object either");
    }
}
