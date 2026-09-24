//! The seccomp filter that denies outbound connection setup.
//!
//! The filter is installed exactly once, after every mount and after
//! `PR_SET_NO_NEW_PRIVS`, and is irreversible (design decisions 13, 26).
//! Opcodes, return codes, `AUDIT_ARCH_*` and `seccomp_data` field offsets
//! are `linux-raw-sys`'s UAPI constants; the program is assembled here.
//!
//! **What it denies is the syscall *paths*, and that is why the io_uring ring
//! calls are in the list beside `connect`/`sendto`/`sendmsg`/`sendmmsg`.** A
//! ring submit never reaches those syscalls: `IORING_OP_CONNECT`,
//! `IORING_OP_SENDMSG` and (5.19+) `IORING_OP_SOCKET` are executed by the
//! kernel from `io_uring_enter`, so a process that owns a ring has an outbound
//! path the four network denials cannot see (design §11's Risks row). The
//! filter is a `seccomp_data.nr` comparison and has no argument dereference, so
//! it can neither inspect an SQE nor an address family; refusing the ring calls
//! is the only closure available at this layer.
//!
//! **Every denial returns `EPERM` and the tail allows** (`SECCOMP_RET_ERRNO |
//! EPERM`, `SECCOMP_RET_ALLOW`). `ENOSYS` was considered for the ring calls,
//! because a library that probes `io_uring_setup` and falls back on an absent
//! syscall would keep working rather than fail — and rejected for consistency:
//! the filter has one refusal spelling, `ENOSYS` is a second one to reason
//! about per call site, and a dependency that treats a refused ring as fatal
//! would already be treating the *unavailable* case as fatal on the kernels
//! that have no `io_uring` at all.

use std::{io, mem::offset_of, ptr};

/// `struct sock_filter` — one BPF instruction.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, SockFilter, filter_program};
///
/// let program: Vec<SockFilter> = filter_program(Arch::X86_64);
/// assert_eq!(program.len(), 19, "one struct per instruction");
/// assert_eq!(program[0].jt, 0, "a load jumps nowhere");
/// ```
pub use linux_raw_sys::ptrace::sock_filter as SockFilter;
#[doc(inline)]
pub use linux_raw_sys::{
    errno::EPERM,
    ptrace::{
        AUDIT_ARCH_AARCH64, AUDIT_ARCH_X86_64, BPF_ABS, BPF_JEQ, BPF_JMP, BPF_K, BPF_LD, BPF_RET,
        BPF_W, SECCOMP_RET_ALLOW, SECCOMP_RET_ERRNO, SECCOMP_RET_KILL_PROCESS,
    },
};
use linux_raw_sys::{
    prctl::PR_SET_SECCOMP,
    ptrace::{SECCOMP_MODE_FILTER, seccomp_data, sock_fprog},
};
use tracing::{Span, field, instrument, warn};

use crate::{
    _core::{Error, Result},
    mechanism::fail,
};

/// Truncate a UAPI opcode (`u32` in `linux-raw-sys`) to `sock_filter.code`.
const fn bpf_code(op: u32) -> u16 {
    op as u16
}

const SECCOMP_DATA_NR: u32 = offset_of!(seccomp_data, nr) as u32;
const SECCOMP_DATA_ARCH: u32 = offset_of!(seccomp_data, arch) as u32;

/// The architecture the filter is compiled for.
///
/// A process of a different architecture has different syscall numbers, so a
/// mismatch is fatal rather than filtered — the filter cannot safely interpret
/// it.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, SECCOMP_RET_KILL_PROCESS, filter_program};
///
/// // The guard compares the loaded arch: equal skips the kill by one
/// // instruction, unequal falls into it — which is what makes a process of
/// // the other architecture killed rather than filtered.
/// let program = filter_program(Arch::Aarch64);
/// assert_eq!(program[1].k, Arch::Aarch64.audit_arch());
/// assert_eq!((program[1].jt, program[1].jf), (1, 0));
/// assert_eq!(program[2].k, SECCOMP_RET_KILL_PROCESS);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    /// `x86_64`
    X86_64,
    /// `aarch64`
    Aarch64,
}

/// The syscalls the filter denies.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, Syscall};
///
/// // Every variant has a number on both tables, which is what the program
/// // compares the loaded `nr` against.
/// assert_eq!(Arch::X86_64.syscall_nr(Syscall::SendTo), 44);
/// assert_eq!(Arch::Aarch64.syscall_nr(Syscall::SendTo), 206);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Syscall {
    /// `connect` — the only way to start an outbound TCP connection.
    Connect,
    /// `sendto` — datagram exfiltration without `connect`.
    SendTo,
    /// `sendmsg` — the same, via an iovec.
    SendMsg,
    /// `sendmmsg` — the same, batched. Denied for the same reason as
    /// [`Syscall::SendMsg`]: it is a separate syscall with its own number
    /// (307 on `x86_64`, 269 on `aarch64`) and reaches the network without
    /// ever calling `connect`.
    SendMmsg,
    /// `io_uring_setup` — creates the ring a later `io_uring_enter` submits
    /// opcodes through.
    ///
    /// **Denied because the opcodes are outbound syscalls the filter never
    /// sees.** `IORING_OP_CONNECT`, `IORING_OP_SENDMSG` and `IORING_OP_SEND`
    /// run in the kernel from the ring, so no `connect`/`sendto`/`sendmsg`
    /// reaches `seccomp_data`; `IORING_OP_SOCKET` (5.19+) is stronger still —
    /// it creates a socket, `AF_VSOCK` included, without the `socket` syscall.
    /// Measured on the acceptance kernel (`6.18.33.2-microsoft-standard-WSL2`)
    /// before this denial: `io_uring_setup(8)` returned a descriptor from
    /// inside the confinement.
    IoUringSetup,
    /// `io_uring_enter` — submits the opcodes above **and is denied separately
    /// from the setup**, because this process confines itself *in place* after
    /// its own startup: a ring a dependency created before the filter landed
    /// is still usable afterwards, and only the submit path can refuse it.
    IoUringEnter,
    /// `io_uring_register` — the third ring call, denied on the same terms as
    /// [`Syscall::IoUringEnter`]: it is part of the same ring surface and a
    /// refusal costs nothing once the other two are refused, while leaving it
    /// open would keep a registered buffer or file set usable on a ring whose
    /// submit path was only half closed.
    IoUringRegister,
}

impl Arch {
    /// The architecture of the running process, when the filter supports it.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::seccomp::Arch;
    ///
    /// // `None` where this crate has no table, so `install` skips rather than
    /// // guessing at numbers it does not have.
    /// assert_eq!(
    ///     Arch::current().is_some(),
    ///     cfg!(any(target_arch = "x86_64", target_arch = "aarch64"))
    /// );
    /// ```
    pub const fn current() -> Option<Self> {
        #[cfg(target_arch = "x86_64")]
        {
            Some(Self::X86_64)
        }
        #[cfg(target_arch = "aarch64")]
        {
            Some(Self::Aarch64)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            None
        }
    }

    /// The `AUDIT_ARCH_*` value `seccomp_data::arch` carries.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::seccomp::{AUDIT_ARCH_AARCH64, AUDIT_ARCH_X86_64, Arch};
    ///
    /// // `AUDIT_ARCH_*`: the ELF machine, with the 64-bit flag on top.
    /// assert_eq!(Arch::X86_64.audit_arch(), AUDIT_ARCH_X86_64);
    /// assert_eq!(Arch::Aarch64.audit_arch(), AUDIT_ARCH_AARCH64);
    /// ```
    pub const fn audit_arch(self) -> u32 {
        match self {
            Self::X86_64 => AUDIT_ARCH_X86_64,
            Self::Aarch64 => AUDIT_ARCH_AARCH64,
        }
    }

    /// The syscall number for this architecture.
    ///
    /// Dual tables, because `linux-raw-sys` only publishes `__NR_*` for the
    /// compiling architecture and both numbers have to be named here.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::seccomp::{Arch, Syscall};
    ///
    /// // The number belongs to the pair, which is what makes the table
    /// // necessary rather than convenient.
    /// assert_eq!(Arch::X86_64.syscall_nr(Syscall::Connect), 42);
    /// assert_eq!(Arch::Aarch64.syscall_nr(Syscall::Connect), 203);
    /// ```
    pub const fn syscall_nr(self, syscall: Syscall) -> u32 {
        match (self, syscall) {
            (Self::X86_64, Syscall::Connect) => 42,
            (Self::X86_64, Syscall::SendTo) => 44,
            (Self::X86_64, Syscall::SendMsg) => 46,
            (Self::X86_64, Syscall::SendMmsg) => 307,
            (Self::X86_64, Syscall::IoUringSetup) => 425,
            (Self::X86_64, Syscall::IoUringEnter) => 426,
            (Self::X86_64, Syscall::IoUringRegister) => 427,
            (Self::Aarch64, Syscall::Connect) => 203,
            (Self::Aarch64, Syscall::SendTo) => 206,
            (Self::Aarch64, Syscall::SendMsg) => 211,
            (Self::Aarch64, Syscall::SendMmsg) => 269,
            // **The same three numbers on both tables, and not by coincidence**:
            // 425-427 were assigned from the generic syscall table, which
            // `arm64` uses for every post-4.0 addition — unlike `connect` and
            // the `send*` family above, whose numbers are per-architecture.
            (Self::Aarch64, Syscall::IoUringSetup) => 425,
            (Self::Aarch64, Syscall::IoUringEnter) => 426,
            (Self::Aarch64, Syscall::IoUringRegister) => 427,
        }
    }
}

/// Build the filter program. Pure — the whole denial policy is testable
/// without installing anything.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, filter_program};
///
/// assert_eq!(filter_program(Arch::X86_64).len(), 19);
/// ```
pub fn filter_program(arch: Arch) -> Vec<SockFilter> {
    let load = |offset: u32| SockFilter {
        code: bpf_code(BPF_LD | BPF_W | BPF_ABS),
        jt: 0,
        jf: 0,
        k: offset,
    };
    let jump = |nr: u32| SockFilter {
        code: bpf_code(BPF_JMP | BPF_JEQ | BPF_K),
        jt: 0,
        jf: 1,
        k: nr,
    };
    let deny = SockFilter {
        code: bpf_code(BPF_RET | BPF_K),
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_ERRNO | EPERM,
    };
    let ret = |k: u32| SockFilter {
        code: bpf_code(BPF_RET | BPF_K),
        jt: 0,
        jf: 0,
        k,
    };

    // **What this program cannot see is the x32 ABI, and it is a real gap.**
    // The arch guard compares `seccomp_data.arch` and nothing else, and on
    // `x86_64` an x32 process reports the same `AUDIT_ARCH_X86_64` as every
    // other one: the ABI lives in the *syscall number* (`__X32_SYSCALL_BIT`
    // `0x4000_0000` OR'd into it — `connect` is `0x4000_002a`, not 42), so an
    // x32 `connect` meets the tail below and is allowed. Closing it means a
    // mask and a second compare ahead of the seven `JEQ`s, which is a different
    // program with a different instruction count and a different test; the
    // task's prose records it beside the design's other filter boundaries
    // rather than pretending the guard covers it.
    //
    // **`io_uring` is not in that same category any more.** It used to be
    // described here as a boundary this program cannot see, on the reasoning
    // that seccomp cannot dereference an SQE: true, and irrelevant once the
    // ring calls themselves are the denied numbers, because a ring cannot be
    // created or submitted through without them. What remains unseen is an
    // *address family* (`socket(AF_VSOCK)` and `connect(AF_UNIX)` are the same
    // two numbers as any other family), which is why the whole `connect`
    // family is denied rather than the ones that look outbound.
    vec![
        load(SECCOMP_DATA_ARCH),
        SockFilter {
            code: bpf_code(BPF_JMP | BPF_JEQ | BPF_K),
            jt: 1,
            jf: 0,
            k: arch.audit_arch(),
        },
        ret(SECCOMP_RET_KILL_PROCESS),
        load(SECCOMP_DATA_NR),
        jump(arch.syscall_nr(Syscall::Connect)),
        deny,
        jump(arch.syscall_nr(Syscall::SendTo)),
        deny,
        jump(arch.syscall_nr(Syscall::SendMsg)),
        deny,
        jump(arch.syscall_nr(Syscall::SendMmsg)),
        deny,
        jump(arch.syscall_nr(Syscall::IoUringSetup)),
        deny,
        jump(arch.syscall_nr(Syscall::IoUringEnter)),
        deny,
        jump(arch.syscall_nr(Syscall::IoUringRegister)),
        deny,
        ret(SECCOMP_RET_ALLOW),
    ]
}

// The `prctl` declaration is **not** here, and that is not a matter of style:
// under edition 2024 an `extern` block is itself an unsafe construct, so at
// module scope it is a fourth one that no `#[allow(unsafe_code)]` on a sibling
// item covers — and the crate is `deny`. It is declared inside `install`
// instead, where that function's own attribute covers the declaration as well
// as the call, which is what keeps this crate's enumerated count at three
// sites rather than four constructs.

/// What [`install`] did.
///
/// **A return value *as well as* the `warn!` below, and the two are not in
/// tension.** The value exists because the decision to *act* on the degradation
/// belongs to the caller: a library that announced it with `eprintln!` could
/// neither be tested nor silenced, and a returned value cannot be dropped on
/// the floor because the caller is holding it. The event is a different
/// instrument on purpose — `tracing` emits nothing at all until a subscriber is
/// installed, and this crate installs none, so the `warn!` costs a caller
/// nothing and reaches a log only where the process that owns the runtime has
/// asked for one (shared §7).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::Seccomp;
///
/// let skipped = Seccomp::SkippedUnsupportedArch;
/// assert!(!matches!(skipped, Seccomp::Installed));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seccomp {
    /// The filter is live and irreversible for this thread.
    Installed,
    /// This architecture has no syscall table here, so nothing was installed.
    /// The directory boundary is unaffected; the network guarantee is not
    /// delivered, and the caller is expected to say so.
    SkippedUnsupportedArch,
}

/// Install the filter on the calling thread.
///
/// Must run before any thread exists: a seccomp filter applies per-thread
/// unless `SECCOMP_FILTER_FLAG_TSYNC` is used, and there is nothing to
/// synchronise while the process is single-threaded (design §8).
///
/// `ret` is the Info event for the install (shared §7); the skip is still a
/// `warn!`.
///
/// # Errors
///
/// [`Error::Step`] when the kernel refuses the filter. **A refusal is fatal,
/// and an unknown architecture is not** — the two are deliberately different
/// outcomes: an architecture this crate has no table for is a limitation of
/// the table, while a refused `prctl` means the filter the code believes is
/// installed is not, which is exactly the silent downgrade the design refuses
/// everywhere else.
///
/// [`Error::Unsupported`] when the program does not fit the `u16` length the
/// kernel's `struct sock_fprog` carries — **unreachable by construction**,
/// because [`filter_program`] always returns nineteen instructions; it is
/// listed so the conversion's failure mode is named rather than left to be
/// discovered at the `u16::try_from`.
///
/// # Safety
///
/// This is one of the crate's three authorised `unsafe` sites (design decision
/// 14). Its invariants: `program` outlives the call — `fprog` borrows it and
/// nothing stores either — `fprog` is `linux_raw_sys::ptrace::sock_fprog`, the
/// kernel's own layout, and the process is single-threaded so no task can
/// observe a half-installed filter.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_linux::seccomp::install;
///
/// // `no_run`: the filter is irreversible once it lands, and a kernel that
/// // refuses it reports the refusal rather than degrading silently.
/// let installed = install().unwrap();
/// # let _ = installed;
/// ```
#[instrument(skip_all, ret, fields(arch = field::Empty, instructions = field::Empty))]
#[allow(unsafe_code)]
pub fn install() -> Result<Seccomp> {
    // Declared here, not at module scope: edition 2024 makes an `extern` block
    // unsafe in its own right, and this attribute covers the body's items while
    // a sibling item's would not (see the note at the end of `filter_program`).
    unsafe extern "C" {
        fn prctl(option: i32, ...) -> i32;
    }

    let Some(arch) = Arch::current() else {
        // The one documented degradation (design §11) and this crate's only
        // `warn!`: the directory boundary still holds, the network guarantee
        // does not, and that is a sentence an operator has to be given rather
        // than left to infer from a filter that was never installed.
        warn!(
            step = "seccomp",
            "this architecture has no syscall table, so outbound connections are NOT blocked"
        );
        return Ok(Seccomp::SkippedUnsupportedArch);
    };
    Span::current().record("arch", field::debug(arch));
    let mut program = filter_program(arch);
    Span::current().record("instructions", program.len() as u64);
    let len = u16::try_from(program.len()).map_err(|_| Error::Unsupported {
        detail: "seccomp program too long",
    })?;
    let fprog = sock_fprog {
        len,
        filter: program.as_mut_ptr(),
    };

    // SAFETY: `program` is alive for the whole call and `fprog` points at its
    // first element; PR_SET_SECCOMP copies the program and retains nothing.
    let rc = unsafe {
        prctl(
            PR_SET_SECCOMP as i32,
            SECCOMP_MODE_FILTER as usize,
            ptr::from_ref(&fprog),
            0usize,
            0usize,
        )
    };
    if rc != 0 {
        // Through the crate's one failure event rather than an inline
        // `Error::Step` and an `error!` of its own (shared §7): a refused
        // `prctl` means the filter this code believes is installed is not,
        // which is the silent downgrade the design refuses everywhere else,
        // and it is the same kind of failure as every other one this crate
        // reports.
        return Err(fail("seccomp", io::Error::last_os_error()));
    }
    Ok(Seccomp::Installed)
}

#[cfg(test)]
mod tests {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    use linux_raw_sys::general::{
        __NR_connect, __NR_io_uring_enter, __NR_io_uring_register, __NR_io_uring_setup,
        __NR_sendmmsg, __NR_sendmsg, __NR_sendto,
    };

    use super::*;

    #[test]
    fn the_program_has_nineteen_instructions() {
        assert_eq!(filter_program(Arch::X86_64).len(), 19);
        assert_eq!(filter_program(Arch::Aarch64).len(), 19);
    }

    #[test]
    fn the_seven_denials_return_eperm() {
        let program = filter_program(Arch::X86_64);
        for index in [5, 7, 9, 11, 13, 15, 17] {
            assert_eq!(program[index].code, bpf_code(BPF_RET | BPF_K));
            assert_eq!(program[index].k, SECCOMP_RET_ERRNO | EPERM);
        }
    }

    #[test]
    fn the_ring_calls_are_denied_beside_the_network_ones() {
        // **This is the io_uring closure, and it is a deny list rather than an
        // argument test on purpose.** The filter loads `nr` and nothing else,
        // so it cannot see an SQE: what it *can* do is refuse the three ring
        // syscalls, and a ring cannot reach the network without them
        // (`IORING_OP_CONNECT`/`IORING_OP_SENDMSG`/`IORING_OP_SOCKET` are
        // submitted through `io_uring_enter` and never leave it). Measured on
        // the acceptance kernel before this denial: `io_uring_setup(8)`
        // returned a descriptor inside the confinement, so the path was
        // reachable, not theoretical.
        let program = filter_program(Arch::X86_64);
        for (index, syscall) in [
            (12, Syscall::IoUringSetup),
            (14, Syscall::IoUringEnter),
            (16, Syscall::IoUringRegister),
        ] {
            assert_eq!(program[index].k, Arch::X86_64.syscall_nr(syscall));
            assert_eq!(program[index + 1].k, SECCOMP_RET_ERRNO | EPERM);
        }
        // The three numbers are consecutive and identical on both tables: they
        // come from the generic table `arm64` also uses, unlike `connect`.
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::IoUringSetup), 425);
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::IoUringEnter), 426);
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::IoUringRegister), 427);
        assert_eq!(
            Arch::Aarch64.syscall_nr(Syscall::IoUringSetup),
            Arch::X86_64.syscall_nr(Syscall::IoUringSetup)
        );
    }

    #[test]
    fn the_tail_allows_everything_else() {
        // **`recvfrom`, `recvmsg`, `recvmmsg`, `socketpair` and `sendfile` are
        // on this side of the tail, and that is the outbound-only rule rather
        // than an omission.** Nothing about receiving reaches the network, a
        // `socketpair` needs no `connect`, and `sendfile`/`splice` copy into an
        // already-connected descriptor — which no syscall filter can unmake
        // (the process could equally `write` to it) and which the server needs
        // for its own file path. What the filter answers is *starting* an
        // outbound connection, not the reachability of a socket the process was
        // already handed.
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[18].code, bpf_code(BPF_RET | BPF_K));
        assert_eq!(program[18].k, SECCOMP_RET_ALLOW);
    }

    #[test]
    fn the_arch_guard_kills_a_mismatched_process() {
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[1].k, Arch::X86_64.audit_arch());
        assert_eq!(program[1].jt, 1, "a matching arch skips the kill");
        assert_eq!(program[1].jf, 0, "a mismatched arch falls into it");
        assert_eq!(program[2].k, SECCOMP_RET_KILL_PROCESS);
    }

    #[test]
    fn the_syscall_number_is_loaded_first_and_the_arch_second() {
        // `seccomp_data` is `{ int nr; __u32 arch; ... }`, so the offsets are
        // fixed and getting them backwards would filter the wrong words.
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[0].k, SECCOMP_DATA_ARCH, "arch is the second u32");
        assert_eq!(program[0].code, bpf_code(BPF_LD | BPF_W | BPF_ABS));
        assert_eq!(program[3].k, SECCOMP_DATA_NR, "nr is the first u32");
    }

    #[test]
    fn jump_offsets_land_on_each_denial_or_skip_it() {
        let program = filter_program(Arch::X86_64);
        for index in [4, 6, 8, 10, 12, 14, 16] {
            assert_eq!(program[index].code, bpf_code(BPF_JMP | BPF_JEQ | BPF_K));
            assert_eq!(program[index].jt, 0, "equal falls through to the denial");
            assert_eq!(program[index].jf, 1, "unequal skips it");
        }
        assert_eq!(program[4].k, Arch::X86_64.syscall_nr(Syscall::Connect));
        assert_eq!(program[6].k, Arch::X86_64.syscall_nr(Syscall::SendTo));
        assert_eq!(program[8].k, Arch::X86_64.syscall_nr(Syscall::SendMsg));
        assert_eq!(program[10].k, Arch::X86_64.syscall_nr(Syscall::SendMmsg));
        assert_eq!(
            program[12].k,
            Arch::X86_64.syscall_nr(Syscall::IoUringSetup)
        );
        assert_eq!(
            program[14].k,
            Arch::X86_64.syscall_nr(Syscall::IoUringEnter)
        );
        assert_eq!(
            program[16].k,
            Arch::X86_64.syscall_nr(Syscall::IoUringRegister)
        );
    }

    #[test]
    fn syscall_numbers_are_per_architecture() {
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::Connect), 42);
        assert_eq!(Arch::Aarch64.syscall_nr(Syscall::Connect), 203);
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::SendMmsg), 307);
        assert_eq!(Arch::Aarch64.syscall_nr(Syscall::SendMmsg), 269);
        assert_ne!(
            Arch::X86_64.syscall_nr(Syscall::SendMsg),
            Arch::Aarch64.syscall_nr(Syscall::SendMsg)
        );
    }

    #[test]
    fn the_running_architecture_is_known_on_the_supported_targets() {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(Arch::current().is_some());
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    #[test]
    fn syscall_numbers_match_the_host_uapi() {
        let arch = Arch::current().expect("this test is gated to a table we have");
        assert_eq!(arch.syscall_nr(Syscall::Connect), __NR_connect);
        assert_eq!(arch.syscall_nr(Syscall::SendTo), __NR_sendto);
        assert_eq!(arch.syscall_nr(Syscall::SendMsg), __NR_sendmsg);
        assert_eq!(arch.syscall_nr(Syscall::SendMmsg), __NR_sendmmsg);
        // The io_uring names are generic-table numbers, so this leg is what
        // says a kernel's own UAPI agrees with the literals above on both
        // architectures rather than only on `x86_64`.
        assert_eq!(arch.syscall_nr(Syscall::IoUringSetup), __NR_io_uring_setup);
        assert_eq!(arch.syscall_nr(Syscall::IoUringEnter), __NR_io_uring_enter);
        assert_eq!(
            arch.syscall_nr(Syscall::IoUringRegister),
            __NR_io_uring_register
        );
    }
}
