//! The `acl` directory's test helpers: the readings a cluster's tests are made of — the mask
//! table, the descriptor shape, the DACL read, the ACE lines, the SID renderers and the text
//! oracle — plus the three thread-local seams (`opens`, `overlap`, `writes`) the production call
//! sites consult, gathered here rather than widened into the production modules. **`cfg(test)`
//! and not `pub(crate)`**: the alternative is widening a production item so a test two modules
//! away can reach it, which is the cost D10 rules out.
// **The `cfg(test)` gate is the parent's**: `acl/mod.rs` declares this module inside one, so a
// second `#![cfg(test)]` here would be the same rule written twice.
// **The one `#![allow]` boundary a test module needs**: the descriptor's control word and the
// walk over the ACL inside it are two `unsafe` calls, each carrying its own `// SAFETY:`
// argument. The production permits stay where the production unsafe is (`descriptor.rs`,
// `walk.rs`), and no build but a test one compiles this module.
#![allow(unsafe_code)]

use std::{ffi::c_void, mem::MaybeUninit, ops::ControlFlow, path::Path, ptr::addr_of};

use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, GetSecurityDescriptorControl, INHERITED_ACE, PSID,
};

use super::{
    descriptor::{Object, OwnedSid, free_descriptor, read_dacl},
    grant::GrantSid,
    spec::Target,
    walk::{Ace, walk_dacl},
};
use crate::{
    _core::AccessMode,
    sid::{ALL_APPLICATION_PACKAGES, sid_text},
};

/// The masks of a path's access-allowed ACEs, in the DACL's own order.
///
/// **`explicit` decides whether the entries the tool did not write are
/// counted, and the mask measurement below needs "no".** A `%TEMP%`
/// directory inherits `(OI)(CI)(RX)` for `BUILTIN\Users` from the volume's
/// own DACL — the same SID those grants name — so a reading of every entry
/// answers "RX is there" for a directory where nothing was granted at all:
/// the false pass this parameter removes. `INHERITED_ACE` is the flag such
/// an entry carries and the one the tool's own entry does not.
///
/// **One walk, two readings**: this is [`descriptor_shape`]'s entry list with
/// the masks kept, so a second traversal of the same ACL — and the second
/// `unsafe` block that came with it — has no reason to exist.
pub(super) fn masks(path: &Path, explicit: bool, sid: PSID) -> Vec<u32> {
    let (_, entries) = descriptor_shape(path, sid);
    entries
        .into_iter()
        .filter(|(flags, _)| !(explicit && u32::from(*flags) & INHERITED_ACE != 0))
        .map(|(_, mask)| mask)
        .collect()
}

/// [`descriptor_shape`] for **the trustee the comparison is about** — the control word and the
/// entries that name `sid`, in the DACL's own order.
///
/// **The host decides every other trustee's entries, so a comparison that keeps them compares
/// hosts.** A route writes the ACE it was asked for; what was already there comes from the
/// volume's DACL and from how `icacls` rewrites one — measured on this workspace's CI, where the
/// `icacls` half of the route comparison carried an explicit copy of every inherited entry and
/// the handle half did not, on two trees of the same shape. Neither entry is what the routes
/// disagree about, and the trustee filter is what says so.
///
/// `Ace::Named` is the walk's case for an entry whose trustee is the `sid` handed in, so this is
/// the same one read as [`descriptor_shape`] with the other-trustee entries dropped.
pub(super) fn named_shape(path: &Path, sid: PSID) -> (u16, Vec<(u8, u32)>) {
    dacl_reading(path, sid, |entry, entries| {
        if let Ace::Named(ace) = entry {
            entries.push((ace.Header.AceFlags, ace.Mask));
        }
    })
}

/// A descriptor as the comparison below needs it: **the control word and the
/// sequence of access-allowed entries, each as its flags and its mask.**
///
/// This is the whole of what "the same descriptor" means here — owner, group
/// and SACL are not asked for by either writer, and a byte comparison would
/// include an ACL revision that is not a behaviour.
///
/// **The ACE walk is [`walk_dacl`](super::walk::walk_dacl)'s, like every other read in this module**,
/// and this measurement puts the two access-allowed cases it hands back
/// together again: the entry list it compares is every such entry whatever
/// trustee it names, so the SID handed to the walk only decides which case an
/// entry arrives as, never whether it is kept.
pub(super) fn descriptor_shape(path: &Path, sid: PSID) -> (u16, Vec<(u8, u32)>) {
    dacl_reading(path, sid, |entry, entries| {
        if let Ace::Named(ace) | Ace::OtherTrustee(ace) = entry {
            entries.push((ace.Header.AceFlags, ace.Mask));
        }
    })
}

/// **The one descriptor read and ACE walk the two readings above are made of**:
/// a path's control word, and whatever `project` keeps of each access-allowed
/// entry. `read_dacl` and `walk_dacl` are the module's own, so what either
/// reading says is what a grant's read would see.
///
/// `None`-shaped entries are dropped by the walk's own contract; the two
/// projections differ only in what they keep of the two access-allowed cases.
pub(super) fn dacl_reading<T>(
    path: &Path,
    sid: PSID,
    mut project: impl FnMut(Ace<'_>, &mut Vec<T>),
) -> (u16, Vec<T>) {
    let (dacl, descriptor) = read_dacl(path).expect("read").expect("a DACL");
    let mut control = MaybeUninit::<u16>::uninit();
    let mut revision = MaybeUninit::<u32>::uninit();
    // SAFETY: `descriptor` is live until the free below, and both
    // out-parameters are writable.
    let read = unsafe {
        GetSecurityDescriptorControl(descriptor, control.as_mut_ptr(), revision.as_mut_ptr())
    };
    assert_ne!(read, 0, "the control word of {}", path.display());
    // SAFETY: the call above reported success, and a successful
    // `GetSecurityDescriptorControl` writes both out-parameters. The revision
    // is deliberately dropped: it is not a behaviour either reading compares.
    let (control, _) = unsafe { (control.assume_init(), revision.assume_init()) };
    let mut entries = Vec::new();
    // SAFETY: `dacl` is the ACL inside the descriptor `read_dacl` returned and
    // `sid` is the caller's live SID; both outlive this walk, and `descriptor` is
    // released once below, after it.
    let walked = unsafe {
        walk_dacl(dacl, sid, &mut |entry| {
            project(entry, &mut entries);
            ControlFlow::Continue(())
        })
    };
    free_descriptor(descriptor);
    assert!(walked.is_ok(), "walk the DACL of {}", path.display());
    (control, entries)
}

/// The entry lines of an `icacls` listing, with the path the tool prefixes
/// the first one with and its own count line dropped.
///
/// **The path has to come off, and a test that forgot it is how this helper
/// exists**: a temporary directory's own name can contain something that looks
/// like a SID or a permission, so a search of the whole listing for a SID can
/// match the *path* rather than an ACE.
pub(super) fn ace_lines(listing: &str) -> Vec<&str> {
    listing
        .lines()
        .filter(|line| line.contains(':') && !line.contains("Successfully processed"))
        .collect()
}

/// The old read's question, asked the way the old read asked it: does an
/// *entry* line of the listing carry the literal SID?
pub(super) fn text_mentions_sid_among_the_entries(listing: &str, sid: &str) -> bool {
    ace_lines(listing).iter().any(|line| line.contains(sid))
}

/// What the skip asks of an object: whether its DACL already carries the
/// grant, read the way `grant_to` reads it — **[`Object::granted`](super::descriptor::Object::granted) on an
/// opened handle**, so the answer is about the object the write would reach.
pub(super) fn satisfied(path: &Path, target: Target, access: AccessMode) -> bool {
    let sid = OwnedSid::parse(ALL_APPLICATION_PACKAGES, path).expect("a SID");
    let object = Object::open(path).expect("open the object");
    object.verify(target).expect("a plain object");
    object
        .granted(sid.raw(), target, access)
        .expect("read the DACL")
}

/// The standing SID as the [`GrantSid`](super::grant::GrantSid) pair a test hands `grant_to` — resolved
/// once here, exactly as the production route resolves it, rather than per path.
pub(super) fn standing(sid: &OwnedSid) -> GrantSid<'_> {
    GrantSid {
        raw: sid.raw(),
        text: ALL_APPLICATION_PACKAGES,
    }
}

/// Every access-allowed entry of `path`'s DACL, each with the trustee's SID
/// rendered — **the reading the profile-directory measurement is made of**.
/// The walk and the descriptor read are the module's own (`read_dacl`,
/// `walk_dacl`), so what the report says is what a grant's read would see.
pub(super) fn dacl_entries(path: &Path, sid: PSID) -> (u16, Vec<String>) {
    dacl_reading(path, sid, |entry, entries| {
        let rendered = match entry {
            Ace::Named(ace) => render_ace(ace, "(this SID)"),
            Ace::OtherTrustee(ace) => {
                // The trustee's SID is inline in the ACE, so its address is
                // the one the conversion below reads — and it stays live
                // for as long as the descriptor this walk borrows.
                let trustee = addr_of!(ace.SidStart).cast_mut().cast::<c_void>();
                render_ace(ace, &render_sid(trustee))
            }
            Ace::Other => "an ACE of another type".to_string(),
        };
        entries.push(rendered);
    })
}

/// One access-allowed entry as its mask and flags.
pub(super) fn render_ace(ace: &ACCESS_ALLOWED_ACE, trustee: &str) -> String {
    format!(
        "allow {trustee} mask=0x{:08X} flags=0x{:02X}",
        ace.Mask, ace.Header.AceFlags
    )
}

/// A SID's `S-1-…` text — [`crate::sid::sid_text`]'s conversion, with this
/// module's own reading of a refusal.
pub(super) fn render_sid(sid: PSID) -> String {
    sid_text(sid).unwrap_or_else(|| "a SID that would not render".to_string())
}

/// The access masks this thread's opens have asked for, in order, for the test that
/// pins the grant's two-open split.
pub(in crate::acl) mod opens {
    use std::{cell::RefCell, mem};

    thread_local! {
        static OPENS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    }

    /// Record one open's requested access.
    pub(crate) fn note(access: u32) {
        OPENS.with(|opens| opens.borrow_mut().push(access));
    }

    /// The masks since the last call, and empty from here on.
    pub(crate) fn take() -> Vec<u32> {
        OPENS.with(|opens| mem::take(&mut *opens.borrow_mut()))
    }
}

/// The read-to-write gap [`grant_to`](super::grant::grant_to) holds open for a test, so that
/// the race the per-path lock closes can be reproduced on purpose.
///
/// **A thread-local, armed by the thread it applies to**, because that is exactly
/// the scope the test needs: two threads armed and released together from a
/// barrier have certainly both read their DACL before either writes, which is the
/// interleaving that loses an ACE when [`AclLock`](super::lock::AclLock) is not there. Nothing
/// arms it in a production build — the module is `#[cfg(test)]` and the call site compiles
/// to nothing without it — and no other test in this module arms it, so no other
/// test pays the sleep.
pub(in crate::acl) mod overlap {
    use std::{cell::Cell, thread, time::Duration};

    thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
    }

    /// How long an armed thread holds the gap open: long enough that a peer
    /// thread started from the same barrier is inside its own gap whatever the
    /// scheduler does with it, and short enough that the test that uses it stays
    /// fast.
    const SETTLE: Duration = Duration::from_millis(50);

    /// Hold this thread's next read-to-write gap open.
    pub(crate) fn arm() {
        ARMED.with(|armed| armed.set(true));
    }

    /// Hold the gap open, if this thread was armed.
    pub(crate) fn settle() {
        if ARMED.with(Cell::get) {
            thread::sleep(SETTLE);
        }
    }
}

/// The descriptor writes this thread has made, for the tests that tell a skip from
/// a write that happened to leave the same ACE.
///
/// **A thread-local in a `cfg(test)` module of its own**, because it exists for
/// one test's question and because the counter's `Cell` is a path this module has
/// no other use for: the crate lints `clippy::absolute_paths`, so a `std::cell::Cell`
/// spelled inline in a `thread_local!` is a warning rather than a device.
pub(in crate::acl) mod writes {
    use std::cell::Cell;

    thread_local! {
        static DACL_WRITES: Cell<u32> = const { Cell::new(0) };
    }

    /// Count one `SetSecurityInfo`.
    pub(crate) fn note() {
        DACL_WRITES.with(|writes| writes.set(writes.get().saturating_add(1)));
    }

    /// The count since the last call, and zero from here on.
    pub(crate) fn take() -> u32 {
        DACL_WRITES.with(|writes| writes.replace(0))
    }
}
