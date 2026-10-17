//! The workspace's one ACE traversal: a live DACL's entries, handed to a caller's own verdict.
//!
//! **This is the walk the dissolved `tinio-winsec` crate held until it went, and the claim it
//! carried travelled with it**: `tinio_sandbox_windows::acl` is the walk's one reader — by name
//! for its test oracle ([`has_ace`](crate::acl::has_ace)) and by handle for the grant's skip
//! (`Object::granted`) — and it hands this walk a predicate and the SID to compare against, so
//! the Win32 sequence below has no second copy in the workspace.
//!
//! **The `unsafe` here is the walk's own, and this module carries the exception that scopes
//! it**: `GetAce` over the ACL's own count, the ACE-type test before any field is read, and the
//! `EqualSid` comparison — every block carrying its own `// SAFETY:` argument (design decision
//! 14's enumerated sites).
//!
//! **A failed `GetAce` is reported as its own status and the attribution is the caller's**: the
//! shared sentence the dissolved crate built its failures with lives in `tinio-util` now
//! ([`tinio_util::win32::error_message`]), so what leaves this module is the call's
//! [`io::Error`] and `acl::carries` wraps it in the step and the path the failure belongs to.

#![allow(unsafe_code)]

use std::{io, ops::ControlFlow, ptr};

use tinio_util::win32::error_message;
use windows_sys::Win32::{
    Foundation::GetLastError,
    Security::{ACCESS_ALLOWED_ACE, ACL, EqualSid, GetAce, PSID},
};

/// `ACCESS_ALLOWED_ACE_TYPE` (`winnt.h`, `0`): the only ACE type a restricted DACL may carry.
///
/// `windows-sys` declares it in `Win32::System::SystemServices`, and the fixed ABI value is
/// written here instead — one constant is not worth reaching into a 5338-generated-line module
/// for, and this way the number the walk compares against and the number this crate's fixtures
/// build their ACEs with are the same spelling.
///
/// **Public because it is one fact with more than one reader**: [`walk_dacl`] has to tell an
/// access-allowed entry from every other kind before it reads the entry's fields, and this
/// crate's fixtures build ACEs carrying the same type code rather than transcribing the zero
/// beside it. Exported under the arm's own name rather than re-named, so a reader meets the
/// header's spelling.
pub const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

/// One ACE of a DACL, as [`walk_dacl`] hands it to a caller's verdict.
///
/// **The ACE types share their header and nothing else**, so this value names which case a
/// reader has instead of leaving it to a cast: an access-allowed entry is the one layout this
/// crate reads past the header — a mask, and then the trustee's SID inline — while the body of
/// any other type is a different structure.
///
/// **The two allowed cases are apart because their verdicts are.** A reader asking whether some
/// entry names its own SID folds them together; a reader asking whether the DACL is *nothing
/// but* that SID's grants — the question a current-user-only verdict answers — has to see
/// another trustee's entry to refuse it, and this crate's skip reads both for the same reason:
/// an inherited entry for a well-known SID is in most DACLs on this platform and says nothing
/// about the grant it is looking for.
///
/// **The three cases are pinned by the tests below rather than by a doctest**: this type is
/// `pub(crate)`, and a doctest is compiled as an external crate that cannot name it.
pub enum Ace<'a> {
    /// An access-allowed ACE that names `sid`, the SID the walk was given.
    Named(&'a ACCESS_ALLOWED_ACE),
    /// An access-allowed ACE that names any other trustee: the same layout as
    /// [`Named`](Self::Named), so the entry's mask, flags and SID are readable too.
    ///
    /// **The field is read by the test oracle and not by the production path**, which asks only
    /// whether some entry names the SID itself — the `expect` says that rather than leaving an
    /// unread field to a reader's guess.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the walk's only production reader asks about `Named`; the test oracle renders this entry's mask, and carrying the entry is the variant's point"
        )
    )]
    OtherTrustee(&'a ACCESS_ALLOWED_ACE),
    /// An ACE of any other type — a deny, an audit, an object-typed entry, a mandatory
    /// label. The walk reports the fact and reads no field past the header, which is the
    /// only part every ACE layout has.
    Other,
}

/// Walks one live DACL's entries and hands each to `visit`, the caller's own verdict.
///
/// **The one ACE walk the workspace has.** Reading a Windows DACL is always the same
/// sequence — `GetAce` over the ACL's own count, the ACE-type test before any field is read,
/// `EqualSid` against the trustee the question is about — and `tinio_sandbox_windows::acl` is
/// where it is asked: the ACL whitelist reads by name for its oracle and by handle for its
/// skip. What lives here is the walk and the verdict stays the caller's — "some entry is the
/// package SID's grant of this shape" — so `visit` is called once per ACE in the DACL's own
/// order with [`Ace`] saying which of the three cases it is, and answers
/// [`ControlFlow::Break`] to end the walk there or [`ControlFlow::Continue`] for the next
/// entry, which is how a caller expresses its quantifier without a second traversal.
///
/// **The return value is the walk's own end**: the break `visit` asked for, or
/// [`ControlFlow::Continue`] when the DACL ran out of ACEs. The two are different answers —
/// a walk that ran out was one a DACL with no entries also produces — so a caller whose "yes"
/// needs an entry to be there reads the count too.
///
/// **A `GetAce` that fails is an error and not a `Continue`, and that is a decision rather
/// than the shape the code happened to take.** The walk as this crate first wrote it skipped
/// such an entry silently — as it skipped a `GetAce` that returned success and left a null
/// pointer, a shape a successful call does not produce and this walk therefore does not test
/// for; merging the workspace's two traversals into one kept the `Err`, because an entry
/// nothing could read is not an entry the caller's verdict may be asked about, and answering
/// "no such ACE" for it would let a caller act on a DACL it never saw.
/// It is also the rule the workspace already writes down elsewhere — a failed read is an error
/// and not an `Ok(false)`, in this crate's `has_ace` as many words — and it is **fail-closed
/// for the walk's reader**: `Object::granted` travels the `Err` out as a failed launch
/// *before* any grant is called satisfied, never as a skip, so an entry nothing could read
/// never becomes a satisfied grant.
///
/// **No test covers this path, and the record is worth more than the gap.** `GetAce` fails for
/// an index at or past the ACL's own `AceCount` — which this loop cannot ask for, since the
/// count it reads is the bound it walks — or for an ACL that is not valid, which nothing the
/// security APIs hand out can be: the kernel validates a DACL when it is set, and every
/// producer the readers here call (`GetNamedSecurityInfoW`, `GetSecurityInfo`, and the
/// `GetSecurityDescriptorDacl` that resolves the ACL inside the descriptor they return) gives
/// back the object's own. Reaching the failure therefore means calling this function on a
/// hand-built, malformed ACL, which the `# Safety` section below does not permit: the branch is
/// a contract-violation path rather than a reviewed one. Written down so that the next reader
/// does not take its absence from the coverage for an oversight, and so that a change which
/// makes it reachable has to bring a test with it.
///
/// **The failure carries no path**: the sentence names the call and the status, and the caller
/// attributes it — [`Object::granted`](super::descriptor::Object) wraps this error in the core's
/// `launch` with the path it already holds, so the operator reads the path once rather than
/// twice.
///
/// # Safety///
/// **`dacl` must name a live ACL inside a descriptor the caller owns, and `sid` must name a
/// live SID; both must stay live for the whole call.** Concretely: `dacl` is a pointer
/// `GetNamedSecurityInfoW`, `GetSecurityInfo` or `GetSecurityDescriptorDacl` wrote into its
/// out-parameter — an ACL that lies inside the descriptor the same call returned, which the
/// same owner keeps alive — and not one whose descriptor is already `LocalFree`d, not one
/// that has moved since its address was taken, and not an arbitrary address. Everything the
/// walk reaches through it must stay live for the same call: it reads the ACL's own
/// `AceCount`, then each ACE that count covers — the entry's header, and for an
/// access-allowed entry its access mask and the SID inline in it — so the ACEs must still lie
/// inside the same live allocation when the walk returns. `sid` is read by `EqualSid` for the
/// length its own header declares, so it must be a SID that is live for the call: a null,
/// freed or arbitrary pointer is the caller's undefined behaviour, never "no entry names it".
/// This call writes nothing, allocates nothing and frees nothing, and the ACL is never
/// released on its own (`LocalFree` takes the descriptor that holds it). A caller that cannot
/// show all of that about its pointers must not call this function; this crate's readers take
/// the path instead, and read the descriptor the ACL lies in themselves.
///
/// **The call sequence a caller writes is pinned by the tests below rather than by a doctest**:
/// this function is `pub(crate)`, and a doctest is compiled as an external crate that cannot
/// name it. The tests build a real DACL with `SetEntriesInAclW`, read it back through
/// `GetNamedSecurityInfoW`, classify every entry the walk hands over, and release the
/// descriptor once — which is the contract this doc states.
pub unsafe fn walk_dacl(
    dacl: *const ACL,
    sid: PSID,
    visit: &mut dyn FnMut(Ace<'_>) -> ControlFlow<()>,
) -> io::Result<ControlFlow<()>> {
    // SAFETY: `dacl` is the caller's live ACL — this call's contract — so its header is in
    // bounds and aligned, which is what reading the count it covers asks for.
    let count = unsafe { (*dacl).AceCount };
    for index in 0..u32::from(count) {
        let mut raw = ptr::null_mut();
        // SAFETY: `dacl` is alive and `index` is below the count just read, which is this
        // call's own bound; `raw` is a valid out-parameter.
        if unsafe { GetAce(dacl, index, &mut raw) } == 0 {
            // SAFETY: a thread-local read with no preconditions, as the first call after the
            // failure.
            return Err(io::Error::other(error_message("GetAce", unsafe {
                GetLastError()
            })));
        }
        // The type first, through the raw pointer: a deny or audit ACE is not read as an
        // access-allowed one, and the caller's verdict is asked what its presence means
        // rather than being handed a layout it does not have.
        //
        // SAFETY: `GetAce` returned a pointer to this ACE inside the caller's live ACL, so its
        // header is readable and the cast is aligned. Only the header is read here; the
        // structure is read in full below, once this test has established which one it is —
        // the ACE layouts agree on the header and differ after it.
        let ace_type = unsafe { (*raw.cast::<ACCESS_ALLOWED_ACE>()).Header.AceType };
        let control = if ace_type != ACCESS_ALLOWED_ACE_TYPE {
            visit(Ace::Other)
        } else {
            // SAFETY: the type test above is what makes this structure's layout the right one
            // for this pointer — an `ACCESS_ALLOWED_ACE` inside the ACL the caller owns and
            // keeps alive for the whole walk.
            let ace = unsafe { &*raw.cast::<ACCESS_ALLOWED_ACE>() };
            // SAFETY: `ace` is an access-allowed ACE — the test two lines up is what
            // established it — so `SidStart` is the first byte of the SID that layout puts
            // inline, inside that ACE. `EqualSid` reads it for the length the SID's own header
            // declares, and `sid` is the caller's live SID.
            let names = unsafe { EqualSid(ptr::addr_of!(ace.SidStart) as PSID, sid) } != 0;
            visit(if names {
                Ace::Named(ace)
            } else {
                Ace::OtherTrustee(ace)
            })
        };
        if control.is_break() {
            return Ok(control);
        }
    }
    Ok(ControlFlow::Continue(()))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, ptr};

    use tinio_util::wide::to_wide;
    use windows_sys::{
        Win32::{
            Foundation::{ERROR_SUCCESS, HLOCAL, LocalFree},
            Security::{
                ACE_OBJECT_TYPE_PRESENT, ACL,
                Authorization::{
                    DENY_ACCESS, EXPLICIT_ACCESS_W, GRANT_ACCESS, OBJECTS_AND_SID, SE_FILE_OBJECT,
                    SET_ACCESS, SET_AUDIT_SUCCESS, SetEntriesInAclW, SetNamedSecurityInfoW,
                    TRUSTEE_IS_OBJECTS_AND_SID, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
                },
                CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, NO_INHERITANCE,
                OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSID,
            },
            Storage::FileSystem::FILE_ALL_ACCESS,
        },
        core::GUID,
    };

    use super::Ace;
    use crate::{
        acl::{descriptor::OwnedSid, test_support::dacl_reading},
        sid::ALL_APPLICATION_PACKAGES,
    };

    /// **A deny entry naming the SID the walk was given is not the allow it resembles**, and the
    /// judgement it pins is the one [`Ace::Other`] exists for: an `ACCESS_DENIED_ACE`
    /// (type 1) has the same header, mask and trustee layout an access-allowed entry
    /// has, and the one this test installs is `FILE_ALL_ACCESS` (measured on this host
    /// 2026-09-28: raw type 1, flags 0, mask `0x1F_01FF`) — the mask the grant this crate
    /// writes carries — so a read that did not ask the entry's type would read a denial as
    /// the very grant it is looking for.
    ///
    /// **Both entries name one SID on purpose.** The allow is there so that the
    /// classification below cannot come from a DACL that holds nothing the walk would
    /// accept — the entry it is looking for is present, and the deny beside it is what has
    /// to be handed over as another type. Which SID that is makes no difference to the
    /// walk's own question: the two entries differ in their type alone, and the SID is the
    /// one the test hands the walk.
    ///
    /// **The DACL is built through `SetEntriesInAclW`, and the tool cannot build this
    /// one** (measured on this host 2026-09-28): `icacls /deny` for a trustee that
    /// already holds an explicit allow *replaces* that entry — a file whose DACL was
    /// `[allow]` came back from `/deny *<sid>:(F)` as `[deny]`, and the `/remove:d` that
    /// followed left it empty — so the pair this test is about, an allow the walk wants
    /// beside a deny it must classify as another type, is not a state the tool produces.
    /// `SetEntriesInAclW` takes the same two entries — `SET_ACCESS` and `DENY_ACCESS` —
    /// and the kernel keeps both.
    ///
    /// The classification is what shows the branch was reached: the walk has to hand the
    /// deny over as [`Ace::Other`] and the allow as [`Ace::Named`], in the ACL's own
    /// order (deny entries come first).
    #[test]
    fn a_deny_entry_for_the_walks_sid_is_not_the_allow_it_resembles() {
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        let denied = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            // The deny flavour of the same list: the entry it builds is an
            // `ACCESS_DENIED_ACE` where `SET_ACCESS` builds an `ACCESS_ALLOWED_ACE`.
            grfAccessMode: DENY_ACCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: sid.raw().cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(denied, dir.path(), sid.raw());

        assert_eq!(
            seen,
            ["other", "named"],
            "the DACL carries the walk's SID's allow and a deny entry beside it, and the walk \
             has to hand the deny over as an ACE of another type rather than read it as the \
             access-allowed entry it is shaped like"
        );
    }

    /// **An audit entry is not the grant the walk is looking for**, and it is one of the types
    /// [`Ace::Other`] is named for: a `SYSTEM_AUDIT_ACE` (type 2) carries the same mask
    /// and the same trustee an access-allowed entry does, so a walk that skipped the
    /// type test would hand the caller a `FILE_ALL_ACCESS` entry naming the SID it was
    /// given — the very entry it is looking for.
    ///
    /// **Windows never puts an audit ACE in a DACL, and the API still accepts one**,
    /// which is why this is a test and not a paragraph: `SetEntriesInAclW`'s audit
    /// access mode builds the entry and `SetNamedSecurityInfoW` installs it in the DACL
    /// slot, and the descriptor read back carries it as a type-2 entry, measured on
    /// this host 2026-09-28. The tool cannot write one there at all — `icacls /audit`
    /// is a SACL write — and a SACL never reaches this walk, since every caller hands
    /// it the DACL `GetSecurityDescriptorDacl` resolved (this crate's readers resolve it
    /// the same way), so this is the only route to the branch with an audit-typed entry
    /// on it.
    #[test]
    fn an_audit_entry_is_not_the_allow_it_resembles() {
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        let audited = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            // The audit flavour of the same list: the entry it builds is a
            // `SYSTEM_AUDIT_ACE` where `SET_ACCESS` builds an `ACCESS_ALLOWED_ACE`.
            grfAccessMode: SET_AUDIT_SUCCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: sid.raw().cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(audited, dir.path(), sid.raw());

        assert_eq!(
            seen,
            ["other", "named"],
            "the DACL carries the walk's SID's allow and an audit entry, and the walk has to \
             hand the second over as an ACE of another type rather than read it as an \
             access-allowed one"
        );
    }

    /// **An object-typed ACE is not the grant the walk is looking for, and its layout is the
    /// reason the type test has to come first**: an `ACCESS_ALLOWED_OBJECT_ACE` (type 5)
    /// carries the object-type GUIDs between its mask and its SID, so the `SidStart` an
    /// access-allowed entry's trustee is read at is a GUID's first bytes here — a read
    /// that skipped the type test would compare a SID that is not one, for a length
    /// those bytes happen to claim.
    ///
    /// **The entry is built through `SetEntriesInAclW`'s object form**
    /// (`TRUSTEE_IS_OBJECTS_AND_SID` with `ACE_OBJECT_TYPE_PRESENT` in the inheritance
    /// word, which is the pair that form requires), and Windows stores it in the file's
    /// DACL — measured on this host 2026-09-28, as the audit case above is. The GUID is
    /// this test's own: nothing resolves an object type on a file, and what the entry
    /// has to be is an object-typed ACE naming the SID the walk is handed.
    #[test]
    fn an_object_typed_ace_is_not_the_allow_it_resembles() {
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        let mut objects = OBJECTS_AND_SID {
            ObjectsPresent: ACE_OBJECT_TYPE_PRESENT,
            ObjectTypeGuid: GUID::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff),
            pSid: sid.raw().cast(),
            ..Default::default()
        };
        let extra = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: GRANT_ACCESS,
            grfInheritance: OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | ACE_OBJECT_TYPE_PRESENT,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_OBJECTS_AND_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: ptr::from_mut(&mut objects).cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(extra, dir.path(), sid.raw());

        assert_eq!(
            seen,
            ["named", "other"],
            "the DACL carries the walk's SID's allow and an object-typed entry, and the walk \
             has to hand the second over as an ACE of another type: reading its `SidStart` \
             would be reading a GUID"
        );
    }

    /// **An allow naming any other trustee is not the grant the walk was asked about**, which
    /// is the case [`Ace::OtherTrustee`] exists for: a DACL carrying `BUILTIN\Users` at full
    /// control must not answer "this SID holds the grant", and folding the two allowed cases
    /// together would answer yes for most DACLs on this platform.
    ///
    /// **The pair goes in through one `SetEntriesInAclW` pass** — the walk's SID at full
    /// control beside `BUILTIN\Users` at the same mask — so the only difference the walk can
    /// see is the trustee, and the classification has to be `["named", "other-trustee"]`.
    #[test]
    fn another_trustees_allow_is_not_the_grant_the_walk_was_asked_about() {
        let dir = tempfile::tempdir().expect("temp dir");
        let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, dir.path()).expect("a SID");
        let users = OwnedSid::parse("S-1-5-32-545", dir.path()).expect("a SID");
        let other = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: SET_ACCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: users.raw().cast::<u16>(),
                ..Default::default()
            },
        };
        let seen = dacl_with_an_extra_ace(other, dir.path(), sid.raw());

        assert_eq!(
            seen,
            ["named", "other-trustee"],
            "the entry for another trustee has to arrive as its own case: folding it into \
             `Named` would answer yes for every DACL that grants Users at all"
        );
    }

    /// **One file carrying the SID the walk was given at full control and one ACE of another
    /// type on top**, built the way this crate's own writers build a DACL — `SetEntriesInAclW`
    /// merges an `EXPLICIT_ACCESS_W` list, `SetNamedSecurityInfoW` installs it with the DACL
    /// protected, so the file carries exactly the entries this test named and none the temp
    /// directory's own ACL would otherwise have contributed as inherited entries.
    ///
    /// The `extra` entry is the caller's, because that is the point: a deny, an audit ACE
    /// and an object-typed ACE are three different lists to merge and one question for the
    /// walk. What comes back is [`classification`]'s reading of the DACL that landed, so
    /// the tests assert on what the object carries rather than on what they asked for.
    fn dacl_with_an_extra_ace(
        extra: EXPLICIT_ACCESS_W,
        dir: &Path,
        sid: PSID,
    ) -> Vec<&'static str> {
        let path = dir.join("state.tmp");
        let _ = fs::remove_file(&path);
        fs::write(&path, b"the token a `state` file holds").expect("the layer");
        let allow = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: SET_ACCESS,
            grfInheritance: NO_INHERITANCE,
            Trustee: TRUSTEE_W {
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_USER,
                ptstrName: sid.cast::<u16>(),
                ..Default::default()
            },
        };
        let entries = [allow, extra];
        let mut acl: *mut ACL = ptr::null_mut();
        // SAFETY: `entries` is the two initialized `EXPLICIT_ACCESS_W` values above and is
        // alive for the call; each trustee pointer they carry is a live value the caller
        // keeps alive — `sid` for the first, and whatever `extra` was built over — and the
        // form/type pair beside each says which kind that pointer is, which is what makes
        // the API read it as one. The null old ACL is the documented "no existing list to
        // merge with", and `acl` is a valid out-parameter. On success the value is this
        // function's own allocation: `SetEntriesInAclW` allocates it and documents
        // `LocalFree` as its release.
        let status = unsafe { SetEntriesInAclW(2, entries.as_ptr(), ptr::null_mut(), &mut acl) };
        assert_eq!(status, ERROR_SUCCESS, "SetEntriesInAclW");
        let wide = to_wide(&path);
        // SAFETY: `wide` is the NUL-terminated path built above and alive for the call;
        // `acl` is the list the call above returned and is released below, after this call
        // has copied it into the object's own descriptor, which is what lets the free
        // follow it. `PROTECTED_DACL_SECURITY_INFORMATION` is what keeps the directory's
        // inheritable ACEs out, so the entries the walk reads are the ones named here. The
        // owner, group and SACL arguments are null, the documented "not asked for".
        let status = unsafe {
            SetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                acl,
                ptr::null_mut(),
            )
        };
        // SAFETY: `acl` is this function's own `SetEntriesInAclW` allocation and this is
        // its one free, after the status has been read.
        unsafe { LocalFree(acl as HLOCAL) };
        assert_eq!(status, ERROR_SUCCESS, "SetNamedSecurityInfoW");
        classification(&path, sid)
    }

    /// How [`walk_dacl`] classified every entry of `path`'s DACL, in the DACL's own
    /// order — the three cases [`Ace`] names, as text a failed assertion can print.
    ///
    /// **This is the branch's own measurement**: the verdicts these tests assert answer
    /// `false` for reasons a broken walk could also produce, and what tells the two apart
    /// is that the entry arrived as [`Ace::Other`] rather than as the allow it resembles.
    /// How [`walk_dacl`] classified every entry of `path`'s DACL, in the DACL's own
    /// order — the three cases [`Ace`] names, as text a failed assertion can print.
    ///
    /// **This is the branch's own measurement**: the verdicts these tests assert answer
    /// `false` for reasons a broken walk could also produce, and what tells the two apart
    /// is that the entry arrived as [`Ace::Other`] rather than as the allow it resembles.
    /// The descriptor read is [`dacl_reading`]'s, so this test trusts the same read the
    /// grant's own oracle does rather than spelling a second one.
    fn classification(path: &Path, sid: PSID) -> Vec<&'static str> {
        dacl_reading(path, sid, |entry, seen| {
            seen.push(match entry {
                Ace::Named(_) => "named",
                Ace::OtherTrustee(_) => "other-trustee",
                Ace::Other => "other",
            });
        })
        .1
    }
}
