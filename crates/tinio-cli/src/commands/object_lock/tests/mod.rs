//! The three Object Lock groups' tests: the parser's surface, every verb's plane call, the
//! no-server refusal, `--default`'s scope rules, `Nd`/`Ny`, and the `mc` JSON shapes.
//!
//! The verbs' request/answer half is generic over [`Plane`], so a recording fake stands in for
//! the control channel and every assertion is about the bytes the CLI would send (OL30).
//!
//! The files split by verb group: `bucket` owns the bucket configuration (`enable`/`info`/
//! `clear`), `retention` and `legalhold` own their keyed verbs, and `arguments` owns the
//! free-form parse and scope tests. `fixtures` owns the parser entry points, the recording
//! plane, and the stopped-root helper.
//!
//! [`Plane`]: crate::commands::plane::Plane

mod arguments;
mod bucket;
mod fixtures;
mod legalhold;
mod retention;
