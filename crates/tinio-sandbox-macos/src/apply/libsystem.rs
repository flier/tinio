//! The `libSystem` declarations, and the safe call that wraps them.
//!
//! **This is the macOS crate's FFI `#[allow(unsafe_code)]` site** (design
//! decision 14). The crate's other two are `launch::inherited_fd` — the inherited
//! descriptor rebuild — and `apply::validator`'s `fork`, which is `unsafe` for a
//! reason of its own (a child of a threaded process is not something a signature
//! can promise) and is therefore counted separately.
//!
//! **Neither `rustix` nor `libc` ships a binding for these symbols**, which is why
//! the declarations are this crate's: `libc` 0.2.189's `apple` module carries no
//! `sandbox_*` name at all (measured), and `rustix` has no `sandbox_init` wrapper.
//! The declaration is Apple's ABI rather than a layout this crate reasons about.
//!
//! **Everything above this module is safe.** [`init`] is the only entry point, it
//! takes Rust types, and it owns the two rules the raw call needs: the caller's
//! strings outlive the call, and the error string the callee allocates is freed
//! exactly once — by `sandbox_free_error`, the only deallocator libSystem
//! publishes for it.
#![allow(unsafe_code)]

use std::{
    ffi::{CStr, CString, c_char, c_int},
    ptr,
};

// `sandbox_init_with_parameters` takes a C string, a flags word this crate always
// passes as `0`, a NULL-terminated array of alternating name/value C strings, an
// out-parameter this crate always passes as NULL, and an out-parameter that is
// either NULL or an error string the caller frees with the second symbol.
#[link(name = "System")]
unsafe extern "C" {
    /// `int sandbox_init_with_parameters(const char *profile, uint64_t flags,
    /// char *const parameters[], char **errorbuf, char **errmsg);`
    ///
    /// Non-zero means refusal, and `*errmsg` then owns a string the caller frees
    /// with [`sandbox_free_error_ffi`].
    #[link_name = "sandbox_init_with_parameters"]
    fn sandbox_init_with_parameters_ffi(
        profile: *const c_char,
        flags: u64,
        parameters: *const *const c_char,
        errorbuf: *mut *mut c_char,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    /// `void sandbox_free_error(char *errorbuf);`
    #[link_name = "sandbox_free_error"]
    fn sandbox_free_error_ffi(errmsg: *mut c_char);
}

/// Apply a Seatbelt profile to **this process**, and hand back the kernel's
/// diagnostic when it refuses.
///
/// **The raw call's two obligations are discharged here**, which is what leaves
/// the rest of the crate safe: the strings are held in `pairs` for the duration of
/// the call (the array borrows them and the callee reads them only then), and the
/// error string is copied before it is freed.
///
/// **No example, by design, and the amended doc-example rule is why**: this is a pure
/// FFI wrapper, and the only honest example would be one that must not run — it
/// applies a profile to the process it is called in, one-shot and irreversibly, so a
/// doctest that executed it would confine the doctest process for good and one that
/// did not would demonstrate nothing. [`crate::apply::apply`] is the safe call a
/// caller writes, and it is the one that carries the example.
///
/// # Errors
///
/// The kernel's own message when the profile is refused, and a fixed sentence when
/// it refuses while reporting no message — there is then nothing to diagnose with
/// and inventing one would be worse than saying so.
pub fn init(profile: &str, params: &[(String, String)]) -> Result<(), String> {
    let profile =
        CString::new(profile).map_err(|_| String::from("the profile holds an interior NUL"))?;
    // The names and the values, kept alive across the call: a parameter whose value
    // holds a quote, a newline or a whole SBPL rule is passed through as the value
    // it is (measured), because `sandbox_init_with_parameters` reads the array and
    // never parses a value as SBPL.
    let mut pairs: Vec<CString> = Vec::with_capacity(params.len() * 2);
    for (name, value) in params {
        for text in [name, value] {
            pairs.push(
                CString::new(text.as_str())
                    .map_err(|_| format!("the parameter `{name}` holds an interior NUL"))?,
            );
        }
    }
    // The NULL-terminated array the ABI takes: one pointer per string, then the
    // terminator — pushed first, so an empty parameter list is still an array with
    // a terminator in it rather than a NULL pointer.
    let mut flat: Vec<*const c_char> = Vec::with_capacity(pairs.len() + 1);
    flat.extend(pairs.iter().map(|pair| pair.as_ptr()));
    flat.push(ptr::null());

    let mut message: *mut c_char = ptr::null_mut();
    // SAFETY: the profile is a live `CString`, `flat` is a live NULL-terminated
    // array of live pointers into `pairs`, and both out-parameters are valid slots
    // this function owns. `message` starts NULL, which is the contract's "no
    // error" input; the callee writes it only on refusal, and only then is it
    // freed below — once, by `sandbox_free_error`, the only deallocator libSystem
    // publishes for it.
    let rc = unsafe {
        sandbox_init_with_parameters_ffi(
            profile.as_ptr(),
            0,
            flat.as_ptr(),
            ptr::null_mut(),
            &mut message,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    if message.is_null() {
        return Err(String::from(
            "the kernel refused the profile and reported no diagnostic",
        ));
    }
    // SAFETY: the callee returned non-zero, which is the documented case in which
    // `message` points at a NUL-terminated string it allocated; nothing else has
    // read or freed it, and the free takes the only ownership there is immediately
    // after the copy.
    let text = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: the same pointer, freed once, by the only function that may.
    unsafe { sandbox_free_error_ffi(message) };
    Err(text)
}
