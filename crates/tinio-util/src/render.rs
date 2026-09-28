//! The one place a Handlebars renderer is built.
//!
//! **Strict mode and the escape function are decisions about *rendering*, not about the text
//! being rendered** — a missing field is an error rather than an empty string, and the outputs
//! (a status report, a systemd unit, a Seatbelt profile) are not markup, so escaping would
//! corrupt them. Three call sites in two crates spelled these two lines identically before this
//! module existed, which is how one of them could have gained a third line the others did not.
//!
//! **Behind the `render` feature** rather than a plain dependency: `handlebars` is a real crate
//! and most consumers of this one never render a template, so the edge is opt-in. Both crates
//! that do render already depend on `handlebars` themselves, so enabling it adds no crate to
//! their build.

use handlebars::{Handlebars, no_escape};

/// A renderer for a template whose output is text a human or a supervisor reads, never markup.
///
/// The returned value is owned, so a caller that renders more than once keeps it rather than
/// rebuilding — and one that renders once calls through the temporary.
///
/// # Examples
///
/// ```
/// use tinio_util::render::renderer;
///
/// // A template naming no field renders as it stands…
/// assert_eq!(
///     renderer().render_template("tinio", &()).expect("no fields"),
///     "tinio"
/// );
/// // …and one naming a field the view does not carry is an error rather than an empty
/// // string, which is the strict mode this module exists to set.
/// assert!(renderer().render_template("{{missing}}", &()).is_err());
/// ```
pub fn renderer() -> Handlebars<'static> {
    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);
    handlebars.register_escape_fn(no_escape);
    handlebars
}
