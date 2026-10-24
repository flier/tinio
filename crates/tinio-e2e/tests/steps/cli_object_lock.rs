//! The CLI Object Lock steps (FR-043): the `--json` documents the three
//! verb groups mirror from `mc`.
//!
//! **The human lines and exit codes are the shared process steps'**
//! (`cli.rs`): a verb's stdout sentence is `its stdout contains`, and its
//! contract code is `the run fails with exit code`. What the corpus cannot
//! spell is the JSON document, because Gherkin's quoted argument cannot
//! carry the document's own quotes — so the shape is a data table here.
//!
//! **The field set is exact and not merely sampled.** `--json` is an output
//! shape (OL30), so a renamed, added or dropped field is a contract change
//! and must fail: the table names every key the document carries.

use cucumber::{gherkin::Step, then};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::cli::cli;

/// The last run's stdout as the JSON document a verb rendered, field by
/// field.
///
/// The first table row is the header; each later row names one field and the
/// value it must hold. Two spellings are assertions rather than omissions:
/// `<any>` accepts any scalar and `<rfc3339>` accepts an instant the format
/// parses — the computed retention `until` is the value that needs the
/// second.
#[then("its stdout is the JSON document")]
async fn stdout_is_json_document(world: &mut super::World, step: &Step) {
    let run = cli(world).last();
    let document: Value = serde_json::from_str(run.stdout.trim()).unwrap_or_else(|err| {
        panic!(
            "the run's stdout is not one JSON document ({err}):\n{}",
            run.stdout
        )
    });
    let object = document
        .as_object()
        .unwrap_or_else(|| panic!("the run's stdout is not a JSON object:\n{}", run.stdout));
    let table = step
        .table()
        .expect("the step carries the document's fields");
    let mut expected: Vec<&str> = Vec::new();
    for row in table.rows.iter().skip(1) {
        let field = row[0].trim();
        let want = row.get(1).map_or("", |cell| cell.trim());
        expected.push(field);
        let got = object
            .get(field)
            .unwrap_or_else(|| panic!("the JSON document lacks `{field}`:\n{}", run.stdout));
        match want {
            "<any>" => {}
            "<rfc3339>" => {
                let instant = scalar(got);
                OffsetDateTime::parse(&instant, &Rfc3339).unwrap_or_else(|err| {
                    panic!("`{field}` is not an RFC3339 instant ({err}): {instant:?}")
                });
            }
            _ => assert_eq!(scalar(got), want, "`{field}` in:\n{}", run.stdout),
        }
    }
    let mut actual: Vec<&str> = object.keys().map(String::as_str).collect();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(
        actual, expected,
        "the JSON document's field set:\n{}",
        run.stdout
    );
}

/// One JSON scalar as the table spells it: a string as itself, a boolean as
/// `true`/`false`, a number as its digits, `null` as the empty string. The
/// verbs render no array or object, so that shape fails rather than producing
/// a spelling a table could accidentally match.
fn scalar(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Null => String::new(),
        other => panic!("a scalar field, not {other}"),
    }
}
