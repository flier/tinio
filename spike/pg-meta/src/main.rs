//! The D21 spike gate driver.
//!
//! Legs 3 to 5 run the shared conformance suites (the store's public
//! contract); the rest drive what the suites cannot: the encoding/collation
//! pin, the schema seed, the 8 MiB staged write with its full and ranged read,
//! the raw-SQL self-heal plant, and the dsn redaction rule.
//!
//! Gate: `TINIO_PG_DSN` set (the dsn `docker-compose.yml` pins),
//! `cargo run --manifest-path spike/pg-meta/Cargo.toml`, exit 0.

use std::{
    error::Error as StdError,
    process::ExitCode,
    time::{Duration, SystemTime},
};

use bytes::Bytes;

use tinio_core::{
    ByteRange, ETag,
    acl::Acl,
    bucket, collect_body,
    content::{ContentKey, ObjectContent, StagedObject},
    meta::{ObjectMeta, object::Row as ObjectRow},
    object,
};
use tinio_util::testing::metadata::{
    assert_content_conformance, assert_metadata_conformance, assert_version_conformance,
};

use pg_meta::{CONTENT_CHUNK_BYTES, PgStore, publish_plan};

/// The legs the driver runs.
const LEGS: usize = 8;

type Fallible = Result<(), Box<dyn StdError>>;

#[tokio::main]
async fn main() -> ExitCode {
    let dsn = match pg_meta::dsn_from_env() {
        Ok(dsn) => dsn,
        Err(message) => {
            eprintln!("pg-meta gate: {message}");
            return ExitCode::FAILURE;
        }
    };
    println!("pg-meta gate (P1 Task 2, D21)");
    match run(&dsn).await {
        Ok(legs) => {
            println!("\ngate: {legs}/{LEGS} legs green -- exit 0");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("\ngate FAILED: {err}");
            ExitCode::FAILURE
        }
    }
}

fn leg(index: usize, name: &str) {
    println!("[leg {index:02}/{LEGS}] {name}");
}

async fn run(dsn: &str) -> Result<usize, Box<dyn StdError>> {
    let store = PgStore::connect(dsn).await?;
    let mut done = 0usize;

    // Leg 1 -- the connection, the encoding pin and the collation pin.
    leg(1, "connect + encoding/collation pin");
    pin_encoding_and_collation(&store).await?;
    done += 1;

    // Leg 2 -- the schema seed the store applied on connect.
    leg(2, "schema presence");
    pin_schema(&store).await?;
    done += 1;

    // Legs 3-5 -- the shared contract suite.
    leg(3, "assert_metadata_conformance");
    assert_metadata_conformance(&store).await;
    println!("  metadata suite green");
    done += 1;

    leg(4, "assert_version_conformance");
    assert_version_conformance(&store).await;
    println!("  version suite green");
    done += 1;

    leg(5, "assert_content_conformance");
    assert_content_conformance(&store).await;
    println!("  content suite green");
    done += 1;

    // Leg 6 -- the spike's own content load: 8 MiB staged in 64 KiB writes.
    leg(
        6,
        "8 MiB staged write (64 KiB chunks) + full and ranged read",
    );
    staged_8mib(&store).await?;
    done += 1;

    // Leg 7 -- a corrupt row planted with raw SQL.
    leg(7, "raw-SQL self-heal plant");
    raw_self_heal(&store).await?;
    done += 1;

    // Leg 8 -- no password reaches a rendering.
    leg(8, "dsn redaction");
    dsn_redaction().await?;
    done += 1;

    Ok(done)
}

async fn pin_encoding_and_collation(store: &PgStore) -> Fallible {
    let rows = store
        .raw_query(
            r#"SELECT current_setting('server_version'),
                      current_setting('server_encoding'),
                      (SELECT datcollate FROM pg_database WHERE datname = current_database())"#,
            &[],
        )
        .await?;
    let version: String = rows[0].get(0);
    let encoding: String = rows[0].get(1);
    let datcollate: String = rows[0].get(2);
    println!("  server_version={version} server_encoding={encoding} datcollate={datcollate}");
    if !encoding.eq_ignore_ascii_case("UTF8") {
        return Err(format!("PG4: server_encoding is {encoding}, not UTF8").into());
    }

    // PG18: every ordered read is byte-ordered only because the statement
    // spells COLLATE "C"; the database's own collation is not C.
    let byte_order = store
        .raw_query(
            r#"SELECT k FROM (VALUES ('data'), ('data-2'), ('data/'), ('data/leaf')) AS t(k)
               ORDER BY k COLLATE "C""#,
            &[],
        )
        .await?;
    let byte_order: Vec<String> = byte_order.iter().map(|row| row.get(0)).collect();
    let expected = vec!["data", "data-2", "data/", "data/leaf"];
    if byte_order != expected {
        return Err(format!("COLLATE \"C\" ordered {byte_order:?}, expected {expected:?}").into());
    }
    // The same values under the database's own collation and under an ICU
    // collation: either can reorder them, which is why every ordered store
    // statement spells COLLATE "C" instead of trusting the server's default.
    let default_order = store
        .raw_query(
            r#"SELECT k FROM (VALUES ('data'), ('data-2'), ('data/'), ('data/leaf')) AS t(k)
               ORDER BY k"#,
            &[],
        )
        .await?;
    let default_order: Vec<String> = default_order.iter().map(|row| row.get(0)).collect();
    let icu_order = store
        .raw_query(
            r#"SELECT k FROM (VALUES ('B'), ('a'), ('_'), ('0')) AS t(k)
               ORDER BY k COLLATE "en-US-x-icu""#,
            &[],
        )
        .await?;
    let icu_order: Vec<String> = icu_order.iter().map(|row| row.get(0)).collect();
    let c_order = store
        .raw_query(
            r#"SELECT k FROM (VALUES ('B'), ('a'), ('_'), ('0')) AS t(k)
               ORDER BY k COLLATE "C""#,
            &[],
        )
        .await?;
    let c_order: Vec<String> = c_order.iter().map(|row| row.get(0)).collect();
    if c_order != vec!["0", "B", "_", "a"] {
        return Err(format!("COLLATE \"C\" ordered {c_order:?}").into());
    }
    if c_order == icu_order {
        return Err("the ICU probe did not diverge: the collation pin is untested".into());
    }
    println!("  datcollate order {default_order:?}");
    println!(
        "  COLLATE \"C\" {c_order:?} vs COLLATE \"en-US-x-icu\" {icu_order:?} on the same values"
    );
    Ok(())
}

async fn pin_schema(store: &PgStore) -> Fallible {
    let expected = [
        "buckets",
        "object_meta",
        "object_parts",
        "uploads",
        "parts",
        "part_checksums",
        "upload_checksums",
        "bucket_versioning",
        "version_rows",
        "version_index",
        "current_slots",
        "content_heads",
        "content",
        "blocks",
        "schema_version",
    ];
    let rows = store
        .raw_query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
            &[],
        )
        .await?;
    let present: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    for table in expected {
        if !present.iter().any(|name| name == table) {
            return Err(format!("schema.sql did not create table {table}").into());
        }
    }
    println!("  {} tables: {}", present.len(), present.join(", "));

    // The brief pins part numbers as bigint; the store binds them as i64.
    let rows = store
        .raw_query(
            "SELECT table_name, data_type FROM information_schema.columns \
             WHERE table_schema = 'public' AND column_name = 'number' \
             ORDER BY table_name",
            &[],
        )
        .await?;
    let numbers: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect();
    if numbers.len() != 3 {
        return Err(format!("expected 3 part-number columns, found {}", numbers.len()).into());
    }
    for (table, data_type) in &numbers {
        if data_type != "bigint" {
            return Err(format!("{table}.number is {data_type}; the brief pins bigint").into());
        }
    }
    println!("  3 part-number columns are bigint");
    Ok(())
}

async fn staged_8mib(store: &PgStore) -> Fallible {
    const TOTAL: usize = 8 * 1024 * 1024;
    const STAGE_CHUNK: usize = 64 * 1024;

    let bucket = bucket::name("gate-staged-8mib")?;
    store.create_bucket(&bucket).await?;
    let key = object::key("eight-mib.bin")?;
    let content_key = ContentKey::Object(key.clone());

    let expected: Vec<u8> = (0..TOTAL)
        .map(|index| ((index * 31 + 7) % 251) as u8)
        .collect();
    let plan = publish_plan(TOTAL as u64);
    println!(
        "  {TOTAL} bytes staged in {} x {} KiB writes; publish plan: {} chunk rows in {} statements (store chunk {} KiB, batch {} MiB)",
        TOTAL.div_ceil(STAGE_CHUNK),
        STAGE_CHUNK / 1024,
        plan.chunks,
        plan.batches,
        CONTENT_CHUNK_BYTES / 1024,
        pg_meta::PUBLISH_BATCH_BYTES / (1024 * 1024),
    );

    let mut staged = store.stage(&bucket, &content_key).await?;
    for chunk in expected.chunks(STAGE_CHUNK) {
        staged.write(Bytes::copy_from_slice(chunk)).await?;
    }
    store.publish(&bucket, staged, &content_key).await?;

    let whole = collect_body(store.open(&bucket, &content_key, None).await?).await?;
    if whole != expected {
        return Err(format!("the full read served {} bytes, not {TOTAL}", whole.len()).into());
    }

    let first = 1_000_003u64;
    let last = 3_500_000u64;
    let window = collect_body(
        store
            .open(
                &bucket,
                &content_key,
                Some(ByteRange::Inclusive(first, last)),
            )
            .await?,
    )
    .await?;
    if window != expected[first as usize..=last as usize] {
        return Err(format!(
            "the ranged read served {} bytes, not {}",
            window.len(),
            last - first + 1
        )
        .into());
    }

    let entry = store
        .stat(&bucket, &key)
        .await?
        .ok_or("stat after publish answered None")?;
    if entry.len != TOTAL as u64 {
        return Err(format!("stat len {} is not {TOTAL}", entry.len).into());
    }
    println!(
        "  full read {} bytes; ranged read {first}..={last} -> {} bytes; stat len {} (identity {}, mtime {:?})",
        whole.len(),
        window.len(),
        entry.len,
        entry.identity,
        entry.mtime,
    );

    ObjectContent::remove(store, &bucket, &content_key).await?;
    if !store.is_empty(&bucket).await? {
        return Err("the bucket is not empty after remove".into());
    }
    store.remove_bucket(&bucket).await?;
    Ok(())
}

async fn raw_self_heal(store: &PgStore) -> Fallible {
    let bucket = bucket::name("gate-self-heal")?;
    store.create_bucket(&bucket).await?;
    let good = object::key("good.bin")?;
    let bad = object::key("bad.bin")?;

    // A failed earlier run must not poison this one: the leg cleans its own
    // keys before and after.
    ObjectMeta::remove(store, &bucket, &good).await?;
    ObjectMeta::remove(store, &bucket, &bad).await?;

    let row = ObjectRow {
        etag: ETag::from_content(b"good"),
        size: 4,
        mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        identity: 0,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    };
    ObjectMeta::put(store, &bucket, &good, &row).await?;

    // The plant: a domain-invalid etag written straight into the table, past
    // every guard the store's own writes carry.
    let bucket_wire = bucket.to_string();
    let bad_wire = bad.to_string();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&bucket_wire, &bad_wire];
    let planted = store
        .raw_execute(
            r#"INSERT INTO object_meta
                   (bucket, key, etag, size, mtime, identity, tags, checksum, owner, acl)
               VALUES ($1, $2, 'not-an-etag', 0, 0, 0, '', '', '', '')
               ON CONFLICT (bucket, key) DO UPDATE SET etag = EXCLUDED.etag"#,
            params,
        )
        .await?;
    if planted != 1 {
        return Err("the raw plant inserted no row".into());
    }

    // The point read self-heals it to absent.
    if ObjectMeta::get(store, &bucket, &bad).await?.is_some() {
        return Err("the point read served a corrupt row".into());
    }
    // The strict traversal fails on it.
    if ObjectMeta::walk(store, &bucket).await.is_ok() {
        return Err("the strict walk served a corrupt row".into());
    }
    // The healing traversal reports it as None and repairs it.
    let healed = ObjectMeta::walk_healing(store, &bucket).await?;
    if !healed.iter().any(|(key, row)| key == &bad && row.is_none()) {
        return Err("walk_healing did not report the corrupt key as None".into());
    }
    let after = ObjectMeta::walk(store, &bucket).await?;
    if after.len() != 1 || after[0].0 != good {
        return Err(format!("the strict walk after the heal saw {} rows", after.len()).into());
    }
    println!(
        "  garbage etag -> get None, walk Err, walk_healing (bad -> None), walk after the heal: {} row",
        after.len()
    );
    ObjectMeta::remove(store, &bucket, &good).await?;
    ObjectMeta::remove(store, &bucket, &bad).await?;
    Ok(())
}

async fn dsn_redaction() -> Fallible {
    const DSN: &str = "postgres://tinio:sup3rsecret@127.0.0.1:55432/tinio";
    let redacted = pg_meta::redact_dsn(DSN, DSN);
    if redacted.contains("sup3rsecret") {
        return Err(format!("redact_dsn left the password in {redacted}").into());
    }
    let err = match PgStore::connect("postgres://tinio:sup3rsecret@127.0.0.1:1/tinio").await {
        Ok(_) => return Err("a dead port connected".into()),
        Err(err) => err,
    };
    let message = err.to_string();
    if message.contains("sup3rsecret") {
        return Err(format!("the connect error printed the password: {message}").into());
    }
    println!("  redact_dsn -> {redacted}; failed connect -> {message}");
    Ok(())
}
