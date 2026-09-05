//! Store-level lifecycle test: a bucket → multipart upload → part
//! checksums → complete → object-meta/object-parts → read path, driven
//! through the shared store's tables over one in-memory database. The
//! point is the cross-table contract (spec 2026-09-03): the rows of a
//! completed upload are the same rows the backends commit and read.

use std::time::SystemTime;

use redb::Database;
use tinio_core::{
    checksum::{Algorithm, Part, Recorded, Type as ChecksumType, Value},
    etag::ETag,
    object::{self, Tags},
};
use tinio_store::{
    bucket, meta, object_part, part, part_checksum, part_data, part_meta, upload, upload_checksum,
    store::Handle, ensure_all,
};

fn handle() -> Handle {
    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    let handle = Handle::new(db);
    handle.write(ensure_all).unwrap();
    handle
}

#[test]
fn object_lifecycle_across_all_row_tables() {
    let h = handle();
    let now = SystemTime::UNIX_EPOCH;
    let key = object::key("big.bin").unwrap();
    let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();

    // 1. Create the bucket.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        bucket::Table::open(txn)?.put("demo", now)?;
        Ok(())
    })
    .unwrap();

    // 2. Open a multipart upload and record its checksum spec + parts
    //    (etag, checksum, content bytes, stat) — each table in its own
    //    scope so the write-transaction handle is not shared.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        {
            let mut u = upload::Table::open(txn)?;
            u.put("demo", "u1", &key, now, "env=prod")?;
        }
        {
            let mut uc = upload_checksum::Table::open(txn)?;
            uc.put("demo", "u1", "SHA256", "FULL_OBJECT")?;
        }
        {
            let mut p = part::Table::open(txn)?;
            p.put("demo", "u1", 1, &etag)?;
        }
        {
            let mut pc = part_checksum::Table::open(txn)?;
            pc.put("demo", "u1", 1, "SHA256", "QjI0Ng==")?;
        }
        {
            let mut pd = part_data::Table::open(txn)?;
            pd.put("demo", "u1", 1, b"abc")?;
        }
        {
            let mut pm = part_meta::Table::open(txn)?;
            pm.put("demo", "u1", 1, 3, 0)?;
        }
        Ok(())
    })
    .unwrap();

    // 3. Complete the upload: the object meta row + the retained part
    //    list (one transaction, the same atomicity the backends use).
    let completed = meta::Stored {
        etag: etag.clone(),
        size: 3,
        mtime: 0,
        file_identity: 0,
        tags: Tags::from_pairs([("env".into(), "prod".into())]).unwrap(),
        checksum: Some(Recorded {
            part: Part {
                algorithm: Algorithm::Sha256,
                value: Value("QjI0Ng==".into()),
            },
            kind: ChecksumType::FullObject,
        }),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        {
            let mut m = meta::Table::open(txn)?;
            m.put("demo", &key, &completed)?;
        }
        {
            let mut op = object_part::Table::open(txn)?;
            op.put("demo", &key, 1, 3, "SHA256", "QjI0Ng==")?;
        }
        Ok(())
    })
    .unwrap();

    // 4. Read the whole thing back through one read transaction.
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let b = bucket::Table::open_readonly(txn)?;
        assert!(b.exists("demo")?);

        let u = upload::Table::open_readonly(txn)?;
        assert!(u.key_matches("demo", &key, "u1")?);
        let (got_key, _, tags) = u.get_matching("demo", &key, "u1")?.unwrap();
        assert_eq!((got_key, tags), ("big.bin".to_string(), "env=prod".to_string()));

        let uc = upload_checksum::Table::open_readonly(txn)?;
        assert_eq!(uc.get("demo", "u1")?, Some(("SHA256".into(), "FULL_OBJECT".into())));

        let p = part::Table::open_readonly(txn)?;
        assert_eq!(p.get_hex("demo", "u1", 1)?, Some(etag.to_string()));
        let pc = part_checksum::Table::open_readonly(txn)?;
        assert!(pc.has_upload("demo", "u1")?);

        let pd = part_data::Table::open_readonly(txn)?;
        assert_eq!(pd.total_len("demo", "u1")?, 3);
        let pm = part_meta::Table::open_readonly(txn)?;
        assert_eq!(pm.get("demo", "u1", 1)?, Some((3, 0)));

        let m = meta::Table::open_readonly(txn)?;
        assert_eq!(m.get("demo", &key)?, Some(completed));
        let op = object_part::Table::open_readonly(txn)?;
        assert_eq!(op.list("demo", &key)?, vec![(1, 3, "SHA256".into(), "QjI0Ng==".into())]);
        Ok(())
    })
    .unwrap();
}
