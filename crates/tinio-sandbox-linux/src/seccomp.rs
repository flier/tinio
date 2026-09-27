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
//!
//! **The ptrace family is denied beside the network paths, and it is a second
//! guarantee rather than a second spelling of the first** (follow-ups §6(a)).
//! `ptrace`, `process_vm_readv` and `process_vm_writev` have nothing to do with
//! outbound traffic: they are what a confined same-uid process uses to reach a
//! process *outside* the jail — which the user namespace does not confine — and
//! to follow `/proc/<pid>/root` across the mount view this program's own
//! confinement just established. The cost is real and tooling-visible (`gdb`,
//! `perf` and `strace` all attach, so none of them works inside the jail) and it
//! is recorded at [`Syscall::Ptrace`]. **It does not replace the
//! address-family note below and is not a network guarantee**: `connect` is
//! denied whatever family it names, and this addition denies three more
//! syscalls, not a capability.
//!
//! **The x32 ABI used to be a third boundary this program could not see, and it
//! is closed by one mask.** On `x86_64` an x32 process reports the same
//! `AUDIT_ARCH_X86_64` as every other one, so the architecture guard cannot
//! distinguish it; the ABI lives in the syscall number, and
//! [`filter_program`] now refuses any number with `__X32_SYSCALL_BIT` set
//! before the equality tests run. **The mask denies every x32 syscall, not only
//! the ten the list names**, and that is the shape the follow-ups plan ratified:
//! the program compares `seccomp_data.nr` once and has no second comparison to
//! key on both the ABI bit and a number, while nothing in this workspace builds
//! or runs an x32 binary — so refusing the whole ABI costs nothing anything here
//! does.

use std::{io, mem::offset_of, ptr};

/// `struct sock_filter` — one BPF instruction.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, SockFilter, filter_program};
///
/// let program: Vec<SockFilter> = filter_program(Arch::X86_64);
/// assert_eq!(program.len(), 27, "one struct per instruction");
/// assert_eq!(program[0].jt, 0, "a load jumps nowhere");
/// ```
pub use linux_raw_sys::ptrace::sock_filter as SockFilter;
#[doc(inline)]
pub use linux_raw_sys::{
    errno::EPERM,
    ptrace::{
        AUDIT_ARCH_AARCH64, AUDIT_ARCH_X86_64, BPF_ABS, BPF_JEQ, BPF_JMP, BPF_JSET, BPF_K, BPF_LD,
        BPF_RET, BPF_W, SECCOMP_RET_ALLOW, SECCOMP_RET_ERRNO, SECCOMP_RET_KILL_PROCESS,
    },
};
use linux_raw_sys::{
    prctl::PR_SET_SECCOMP,
    ptrace::{SECCOMP_MODE_FILTER, seccomp_data, sock_fprog},
};
use tracing::{Span, field, info, instrument, warn};

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

/// `__X32_SYSCALL_BIT`: the bit an x32 process ORs into every syscall number it
/// makes, which is the whole of what distinguishes the ABI — `seccomp_data.arch`
/// is `AUDIT_ARCH_X86_64` for both.
///
/// **Spelled here rather than imported, because the UAPI constant is
/// `x86`-only.** `linux-raw-sys` publishes `__X32_SYSCALL_BIT` in its `x86_64`,
/// `x86` and `x32` modules and in no `aarch64` one, while `filter_program` takes
/// the architecture as a *parameter* — a test builds this host's x86_64 program
/// and the aarch64 one side by side — so the value has to be nameable on either
/// target. The host-UAPI test below is what keeps it equal to
/// `__X32_SYSCALL_BIT` on the target that has one.
const X32_SYSCALL_BIT: u32 = 0x4000_0000;

/// The deny list, in program order.
///
/// **One array rather than a hand-written pair of instructions per syscall**, so
/// that a variant added to [`Syscall`] cannot be left out of the program it
/// exists for: `the_program_covers_every_denied_syscall` derives the length it
/// expects from this constant, and `Syscall`'s own `match` in
/// [`Arch::syscall_nr`] is exhaustive, so the two together are a compile error
/// plus a failing test rather than a silently unfinished filter.
const DENIED: [Syscall; 10] = [
    Syscall::Connect,
    Syscall::SendTo,
    Syscall::SendMsg,
    Syscall::SendMmsg,
    Syscall::IoUringSetup,
    Syscall::IoUringEnter,
    Syscall::IoUringRegister,
    Syscall::Ptrace,
    Syscall::ProcessVmReadv,
    Syscall::ProcessVmWritev,
];

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
    /// `ptrace` — attaching to any process this uid may attach to, inside the
    /// jail or outside it.
    ///
    /// **Denied as one family with the two `process_vm_*` calls, and the cost is
    /// tooling-visible rather than theoretical** (follow-ups §6(a)): `gdb`,
    /// `perf` and `strace` all work by attaching, so none of them works inside
    /// the jail any more. What that buys is the escape the user namespace does
    /// not cover: the peer process is *outside* the confinement and is not
    /// filtered, so an unconstrained `ptrace` is a way to have that process do
    /// the reading, writing or connecting — and `/proc/<pid>/root` follows a
    /// neighbour's mount view straight back out of the jail the mounts just
    /// built. Measured on the acceptance kernel with the filter installed and
    /// this rule absent: `ptrace(PTRACE_TRACEME)` returned `0`.
    Ptrace,
    /// `process_vm_readv` — the same read as [`Syscall::Ptrace`] without
    /// attaching, and denied for the same reason at the same tooling cost.
    /// Measured with the rule absent: `process_vm_readv(getpid(), …)` copied 5
    /// bytes from inside the confinement.
    ProcessVmReadv,
    /// `process_vm_writev` — the injection half, denied on [`Syscall::Ptrace`]'s
    /// terms. Reading a neighbour's memory out is the exfiltration half; writing
    /// into one is what turns a bug inside the jail into code running outside
    /// it, and neither needs the peer's cooperation.
    ProcessVmWritev,
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
            // **Per-architecture, like `connect` and the `send*` family above
            // and unlike the ring calls below**: the ptrace family was assigned
            // long before the generic table existed, so `ptrace` is 101 here and
            // 117 there, and the two `process_vm_*` numbers differ by the same
            // history. The host-UAPI test is what says these literals are the
            // kernel's rather than this file's.
            (Self::X86_64, Syscall::Ptrace) => 101,
            (Self::X86_64, Syscall::ProcessVmReadv) => 310,
            (Self::X86_64, Syscall::ProcessVmWritev) => 311,
            (Self::Aarch64, Syscall::Connect) => 203,
            (Self::Aarch64, Syscall::SendTo) => 206,
            (Self::Aarch64, Syscall::SendMsg) => 211,
            (Self::Aarch64, Syscall::SendMmsg) => 269,
            (Self::Aarch64, Syscall::Ptrace) => 117,
            (Self::Aarch64, Syscall::ProcessVmReadv) => 270,
            (Self::Aarch64, Syscall::ProcessVmWritev) => 271,
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
/// **Twenty-seven instructions on `x86_64` and twenty-five on `aarch64`**, and
/// the difference is the x32 mask: it is `x86_64`'s alone, because that ABI is
/// the only one with a second syscall table reachable from a 64-bit process
/// (see the module doc).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, filter_program};
///
/// assert_eq!(filter_program(Arch::X86_64).len(), 27);
/// assert_eq!(filter_program(Arch::Aarch64).len(), 25);
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

    // **The x32 mask comes first, ahead of every equality test, and that order
    // is the rule rather than a style.** Each number in the list below is far
    // below `X32_SYSCALL_BIT`, so a masked `nr` can never equal one of them: the
    // comparisons would not match and the tail would allow the call, which is
    // exactly the gap this pair closes. Put after the tests, the mask would
    // still be reached by every number the tests miss — and the tests cannot
    // miss a masked X32 number for the reason just given, so the two orders are
    // equivalent in effect and only this one reads as "the ABI is answered for
    // before the list is".
    //
    // **`io_uring` is no longer described here as a boundary this program cannot
    // see.** It was, on the reasoning that seccomp cannot dereference an SQE:
    // true, and irrelevant once the ring calls themselves are the denied
    // numbers, because a ring cannot be created or submitted through without
    // them. What remains unseen is an *address family* (`socket(AF_VSOCK)` and
    // `connect(AF_UNIX)` are the same two numbers as any other family), which is
    // why the whole `connect` family is denied rather than the ones that look
    // outbound.
    let mut program = vec![
        load(SECCOMP_DATA_ARCH),
        SockFilter {
            code: bpf_code(BPF_JMP | BPF_JEQ | BPF_K),
            jt: 1,
            jf: 0,
            k: arch.audit_arch(),
        },
        ret(SECCOMP_RET_KILL_PROCESS),
        load(SECCOMP_DATA_NR),
    ];
    if arch == Arch::X86_64 {
        program.push(SockFilter {
            code: bpf_code(BPF_JMP | BPF_JSET | BPF_K),
            jt: 0,
            jf: 1,
            k: X32_SYSCALL_BIT,
        });
        program.push(deny);
    }
    for syscall in DENIED {
        program.push(jump(arch.syscall_nr(syscall)));
        program.push(deny);
    }
    program.push(ret(SECCOMP_RET_ALLOW));
    program
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
/// `ret` is the Info event for the install (shared §7), emitted by
/// [`announce_installed`]; the skip is still a `warn!`.
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
/// because [`filter_program`] returns a fixed twenty-seven instructions on
/// `x86_64` and twenty-five on `aarch64`; it is listed so the conversion's
/// failure mode is named rather than left to be discovered at the
/// `u16::try_from`.
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
    announce_installed(arch, program.len());
    Ok(Seccomp::Installed)
}

/// The Info event for a filter that landed (shared §7).
///
/// **A function rather than an inline `info!`, and the reason is the one thing
/// this module cannot otherwise state: `install` is one-way.** The filter is
/// irreversible the moment `prctl` accepts it, so no test can drive the site —
/// and §7's check is that "it logs" is a property a test can *lose*, which means
/// the event has to be reachable from one. Everything the event carries is an
/// argument, so what a recording sees is what the kernel was handed rather than
/// what some other function later thought it had handed over.
///
/// **The skip's `warn!` stays inline, and it is the one event here no test
/// reads.** It records a state (`Seccomp::SkippedUnsupportedArch`) rather than an
/// install, it is emitted on the path that returns before a program exists, and
/// the process that reaches it is the confined child — which installs no
/// subscriber — so §7's rule that a degradation be named where it is decided is
/// what that line satisfies, and `Seccomp`'s returned variant is what the caller
/// acts on.
fn announce_installed(arch: Arch, instructions: usize) {
    info!(
        step = "seccomp",
        arch = ?arch,
        instructions,
        "the seccomp filter is installed"
    );
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "x86_64")]
    use linux_raw_sys::general::__X32_SYSCALL_BIT;
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    use linux_raw_sys::general::{
        __NR_connect, __NR_io_uring_enter, __NR_io_uring_register, __NR_io_uring_setup,
        __NR_process_vm_readv, __NR_process_vm_writev, __NR_ptrace, __NR_sendmmsg, __NR_sendmsg,
        __NR_sendto,
    };

    use super::*;
    use crate::testing::{assert_step, record_events};

    /// Evaluate `program` over one synthetic `seccomp_data`, the way the kernel's
    /// BPF interpreter does.
    ///
    /// **It exists because "what does this program answer for syscall number X"
    /// is the whole content of the policy, and nothing else can ask it.**
    /// `install` is irreversible, so no test can put the program in front of a
    /// real syscall; the rules this file gained on 2026-09-26 are decisions about
    /// *numbers* (a mask on `nr`, three more equalities), and the live
    /// measurement that proves the kernel agrees with them is the WSL2 script's
    /// prober, which installs these same compiled bytes.
    ///
    /// Only the four opcodes this crate emits are interpreted, and an unknown
    /// one panics rather than being skipped: a second opcode would mean the
    /// program grew a shape this interpreter would otherwise read as policy. The
    /// argument words of `seccomp_data` are not modelled because no rule here
    /// reads them (`seccomp_data.nr` and `.arch` are the first two `u32`s and
    /// the only two that matter).
    fn verdict(program: &[SockFilter], arch: u32, nr: u32) -> u32 {
        let mut accumulator = 0u32;
        let mut pc = 0usize;
        loop {
            let instruction = program[pc];
            match instruction.code {
                code if code == bpf_code(BPF_LD | BPF_W | BPF_ABS) => {
                    accumulator = match instruction.k {
                        SECCOMP_DATA_NR => nr,
                        SECCOMP_DATA_ARCH => arch,
                        other => panic!("the program loads no such offset: {other}"),
                    };
                    pc += 1;
                }
                code if code == bpf_code(BPF_JMP | BPF_JEQ | BPF_K) => {
                    let offset = if accumulator == instruction.k {
                        instruction.jt
                    } else {
                        instruction.jf
                    };
                    pc += usize::from(offset) + 1;
                }
                code if code == bpf_code(BPF_JMP | BPF_JSET | BPF_K) => {
                    let offset = if accumulator & instruction.k != 0 {
                        instruction.jt
                    } else {
                        instruction.jf
                    };
                    pc += usize::from(offset) + 1;
                }
                code if code == bpf_code(BPF_RET | BPF_K) => return instruction.k,
                other => panic!("this crate's program has no opcode {other:#x}"),
            }
        }
    }

    #[test]
    fn the_x32_abi_does_not_reach_the_allow_tail() {
        // **The gap, as a verdict per number.** An x32 process reports
        // `AUDIT_ARCH_X86_64`, so the architecture guard cannot see the ABI: the
        // ABI lives in the syscall number (`__X32_SYSCALL_BIT` OR'd in), and no
        // equality test in this program can match one of those numbers — which
        // is why they used to reach the allow tail. Measured live before the
        // mask landed, with this program installed by `prctl` and a listener the
        // same process held: `x32 connect: ALLOWED (result=0) ->
        // ('127.0.0.1', 35596)`.
        let program = filter_program(Arch::X86_64);
        for nr in [42, 44, 46, 307, 425, 426, 427, 0, 1, 101, 310, 311] {
            let x32 = X32_SYSCALL_BIT | nr;
            assert_eq!(
                verdict(&program, AUDIT_ARCH_X86_64, x32),
                SECCOMP_RET_ERRNO | EPERM,
                "{x32:#x} must not reach the tail"
            );
        }
        // **And the numbers the list below *does* name, masked, would have
        // passed every equality test.** That is the whole shape of the old gap:
        // `42 != 0x4000_002a`, so no `JEQ` could match, and the tail is the only
        // instruction left. Asserting the unmasked half too is what separates
        // "the mask answers for the ABI" from "the mask happens to deny
        // everything".
        for syscall in DENIED {
            let plain = Arch::X86_64.syscall_nr(syscall);
            assert_eq!(
                verdict(&program, AUDIT_ARCH_X86_64, plain),
                SECCOMP_RET_ERRNO | EPERM,
                "the unmasked number is denied by its own comparison: {plain}"
            );
        }
        // The rule is a mask and not a list, so it covers numbers this crate
        // never names as well — including ones past the table it knows.
        assert_eq!(
            verdict(&program, AUDIT_ARCH_X86_64, X32_SYSCALL_BIT | 0xffff),
            SECCOMP_RET_ERRNO | EPERM
        );
        // **`aarch64` has no x32 ABI and gets no such rule**: its syscall
        // numbers come from the generic table and none of them carries the bit,
        // so the instruction would be dead code there — and it would be one more
        // instruction on a table that cannot reach it.
        let arm = filter_program(Arch::Aarch64);
        assert_eq!(
            verdict(&arm, AUDIT_ARCH_AARCH64, X32_SYSCALL_BIT | 203),
            SECCOMP_RET_ALLOW
        );
    }

    #[test]
    fn the_ptrace_family_is_denied() {
        // **The escape the user namespace does not cover** (follow-ups §6(a)): a
        // confined same-uid process can still attach to a process outside the
        // jail, read or write its memory, or follow `/proc/<pid>/root` across
        // the mount view. Measured live before these rules landed, with this
        // program installed by `prctl`: `ptrace TRACEME: ALLOWED (result=0)`,
        // `process_vm_readv (self): ALLOWED (result=5)`.
        for arch in [Arch::X86_64, Arch::Aarch64] {
            let program = filter_program(arch);
            for syscall in [
                Syscall::Ptrace,
                Syscall::ProcessVmReadv,
                Syscall::ProcessVmWritev,
            ] {
                let nr = arch.syscall_nr(syscall);
                assert_eq!(
                    verdict(&program, arch.audit_arch(), nr),
                    SECCOMP_RET_ERRNO | EPERM,
                    "{syscall:?} on {arch:?} must be denied"
                );
            }
        }
        // The family's numbers are per-architecture, so the two tables disagree
        // about them — which is the half of this rule a single-architecture
        // assertion would miss.
        assert_ne!(
            Arch::X86_64.syscall_nr(Syscall::Ptrace),
            Arch::Aarch64.syscall_nr(Syscall::Ptrace)
        );
    }

    #[test]
    fn the_program_length_is_fixed_per_architecture() {
        // **The length is derived from the list rather than written twice.** The
        // four instructions before the budget are the architecture load, the
        // guard's compare, the kill and the `nr` load; each entry of `DENIED`
        // contributes a compare and a denial; the x32 mask is one more pair on
        // the architecture that has the ABI; and the tail allows. A variant added
        // to `Syscall` and forgotten in `DENIED` is a failing length here rather
        // than a filter that denies nine of ten paths.
        assert_eq!(
            filter_program(Arch::X86_64).len(),
            4 + 2 * DENIED.len() + 2 + 1
        );
        assert_eq!(filter_program(Arch::X86_64).len(), 27);
        assert_eq!(
            filter_program(Arch::Aarch64).len(),
            4 + 2 * DENIED.len() + 1
        );
        assert_eq!(filter_program(Arch::Aarch64).len(), 25);
    }

    #[test]
    fn every_denial_returns_eperm() {
        // Every `RET` in the program that is not the allow tail must be the one
        // refusal spelling, and the positions pin the layout the offset tests
        // below index into.
        let program = filter_program(Arch::X86_64);
        let denials: Vec<usize> = program
            .iter()
            .enumerate()
            .filter(|(_, instruction)| {
                instruction.code == bpf_code(BPF_RET | BPF_K)
                    && instruction.k == SECCOMP_RET_ERRNO | EPERM
            })
            .map(|(index, _)| index)
            .collect();
        assert_eq!(
            denials,
            vec![5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25],
            "one per `DENIED` entry, plus the x32 mask"
        );
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
            (14, Syscall::IoUringSetup),
            (16, Syscall::IoUringEnter),
            (18, Syscall::IoUringRegister),
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
        let tail = program.len() - 1;
        assert_eq!(program[tail].code, bpf_code(BPF_RET | BPF_K));
        assert_eq!(program[tail].k, SECCOMP_RET_ALLOW);
        // **The four the comment above names are asserted by verdict and not by
        // position**: `recvmsg`, `socketpair`, `write` and `sendfile` are not in
        // the deny list, so the interpreter walks past every `JEQ` and the mask
        // and answers from the tail — which is the property "the filter denies
        // these paths", not "the filter denies everything".
        for nr in [47, 53, 1, 40] {
            assert_eq!(
                verdict(&program, AUDIT_ARCH_X86_64, nr),
                SECCOMP_RET_ALLOW,
                "{nr} must still reach the network the confinement left it"
            );
        }
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
        for index in [6, 8, 10, 12, 14, 16, 18, 20, 22, 24] {
            assert_eq!(program[index].code, bpf_code(BPF_JMP | BPF_JEQ | BPF_K));
            assert_eq!(program[index].jt, 0, "equal falls through to the denial");
            assert_eq!(program[index].jf, 1, "unequal skips it");
        }
        // **The list is the source of the order**, so this walks it rather than
        // repeating its entries: an entry moved in `DENIED` and not in the
        // program (or the reverse) is a failing assertion here.
        for (position, syscall) in DENIED.iter().enumerate() {
            let index = 6 + 2 * position;
            assert_eq!(
                program[index].k,
                Arch::X86_64.syscall_nr(*syscall),
                "{syscall:?} at {index}"
            );
        }
        // **The x32 mask is a `JSET` and not an equality, and its offsets are
        // the mirror of the comparisons'**: a set bit falls *through* to the
        // denial below it, and a clear bit skips that denial into the list.
        assert_eq!(program[4].code, bpf_code(BPF_JMP | BPF_JSET | BPF_K));
        assert_eq!(program[4].k, X32_SYSCALL_BIT);
        assert_eq!((program[4].jt, program[4].jf), (0, 1));
        // ...and the aarch64 program has no such instruction at all, which is
        // why its list starts two instructions earlier.
        assert_eq!(
            filter_program(Arch::Aarch64)[4].code,
            bpf_code(BPF_JMP | BPF_JEQ | BPF_K)
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
        // The ptrace family is per-architecture, so this leg is the one that
        // says the literals above are the running kernel's own numbers on
        // whichever of the two tables this target is.
        assert_eq!(arch.syscall_nr(Syscall::Ptrace), __NR_ptrace);
        assert_eq!(
            arch.syscall_nr(Syscall::ProcessVmReadv),
            __NR_process_vm_readv
        );
        assert_eq!(
            arch.syscall_nr(Syscall::ProcessVmWritev),
            __NR_process_vm_writev
        );
        // **The x32 mask's literal is checkable on exactly one target.** The UAPI
        // publishes `__X32_SYSCALL_BIT` in its `x86_64`, `x86` and `x32` modules
        // and in no `aarch64` one, so this line is what keeps the constant in
        // this file equal to the kernel's on the architecture that has the ABI —
        // and there is nothing to check it against on the one that does not.
        #[cfg(target_arch = "x86_64")]
        assert_eq!(X32_SYSCALL_BIT, __X32_SYSCALL_BIT);
    }

    #[test]
    fn the_install_is_traced_with_its_step() {
        // Shared §7's check, on the one event this module must emit and which no
        // test can drive through its site: `install` is irreversible, so the
        // event is asserted through the function `install` calls with the values
        // the kernel is handed. The `warn!` on the skip is not assertable at all
        // and `announce_installed`'s doc says why.
        let (_, logged) = record_events(|| announce_installed(Arch::X86_64, 27));
        assert_step(&logged, "seccomp");
        assert!(logged.contains("arch=X86_64"), "{logged}");
        assert!(logged.contains("instructions=27"), "{logged}");
    }
}
