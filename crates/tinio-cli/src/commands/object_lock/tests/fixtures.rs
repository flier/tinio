//! The suite's shared fixtures: the parser's entry points, the recording plane, and the
//! stopped-root helper.

use std::{collections::VecDeque, sync::Mutex};

use async_trait::async_trait;
use clap::{CommandFactory, FromArgMatches};
use serde_json::Value;
use tempfile::TempDir;

use crate::{
    _api::client::RawResponse,
    Error,
    cli::{Cli, Command, Discovery, Provenance},
    commands::{legalhold, object_lock, plane::Plane, retention},
    resolve::{Located, locate},
};

/// The parse as the process does it.
pub fn parse(argv: &[&str]) -> Result<Command, String> {
    let matches = Cli::command()
        .try_get_matches_from(argv)
        .map_err(|err| err.to_string())?;
    Cli::from_arg_matches(&matches)
        .map(|cli| cli.command)
        .map_err(|err| err.to_string())
}

/// The arm every parse below must land in, or a panic naming what parsed instead.
pub fn command(argv: &[&str]) -> Command {
    parse(argv).expect("the command line parses")
}

pub fn retention_verb(argv: &[&str]) -> retention::Verb {
    let Command::Retention(args) = command(argv) else {
        panic!("{argv:?} is not the retention arm");
    };
    args.verb
}

pub fn legalhold_verb(argv: &[&str]) -> legalhold::Verb {
    let Command::LegalHold(args) = command(argv) else {
        panic!("{argv:?} is not the legalhold arm");
    };
    args.verb
}

pub fn object_lock_verb(argv: &[&str]) -> object_lock::Verb {
    let Command::ObjectLock(args) = command(argv) else {
        panic!("{argv:?} is not the object-lock arm");
    };
    args.verb
}

/// One request the CLI made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    BucketGet(String),
    BucketPut(String, String),
    ObjectGet(String, String),
    ObjectPut(String, String),
}

/// A recording plane: each method logs its call and answers the next canned response.
#[derive(Default)]
pub struct Fake {
    calls: Mutex<Vec<Call>>,
    bucket_get: Mutex<VecDeque<RawResponse>>,
    bucket_put: Mutex<VecDeque<RawResponse>>,
    object_get: Mutex<VecDeque<RawResponse>>,
    object_put: Mutex<VecDeque<RawResponse>>,
}

impl Fake {
    pub fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .clone()
    }

    pub fn bucket_get_answers(&self, status: u16, body: &str) {
        self.bucket_get
            .lock()
            .expect("the fixture is not poisoned")
            .push_back(answer(status, body));
    }

    pub fn bucket_put_answers(&self, status: u16, body: &str) {
        self.bucket_put
            .lock()
            .expect("the fixture is not poisoned")
            .push_back(answer(status, body));
    }

    pub fn object_get_answers(&self, status: u16, body: &str) {
        self.object_get
            .lock()
            .expect("the fixture is not poisoned")
            .push_back(answer(status, body));
    }

    pub fn object_put_answers(&self, status: u16, body: &str) {
        self.object_put
            .lock()
            .expect("the fixture is not poisoned")
            .push_back(answer(status, body));
    }
}

fn answer(status: u16, body: &str) -> RawResponse {
    RawResponse {
        status,
        body: body.to_owned(),
    }
}

fn next(queue: &Mutex<VecDeque<RawResponse>>) -> RawResponse {
    queue
        .lock()
        .expect("the fixture is not poisoned")
        .pop_front()
        .expect("the test canned this answer")
}

#[async_trait]
impl Plane for Fake {
    async fn bucket_get(&self, bucket: &str) -> Result<RawResponse, Error> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .push(Call::BucketGet(bucket.to_owned()));
        Ok(next(&self.bucket_get))
    }

    async fn bucket_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .push(Call::BucketPut(bucket.to_owned(), body.to_owned()));
        Ok(next(&self.bucket_put))
    }

    async fn object_get(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .push(Call::ObjectGet(bucket.to_owned(), body.to_owned()));
        Ok(next(&self.object_get))
    }

    async fn object_put(&self, bucket: &str, body: &str) -> Result<RawResponse, Error> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .push(Call::ObjectPut(bucket.to_owned(), body.to_owned()));
        Ok(next(&self.object_put))
    }
}

/// A request body as the test reads it.
pub fn body(raw: &str) -> Value {
    serde_json::from_str(raw).expect("the request is JSON")
}

/// The one object `PUT` the fake recorded.
pub fn object_put_call(calls: &[Call]) -> (&str, Value) {
    match calls {
        [Call::ObjectPut(bucket, raw)] => (bucket, body(raw)),
        other => panic!("expected one object PUT, got {other:?}"),
    }
}

/// A temp root and its resolution, with no state file — the stopped-server shape.
pub fn no_server_root() -> (TempDir, Located) {
    let dir = tempfile::tempdir().expect("a scratch root");
    let located = locate(
        Discovery {
            dir: Some(dir.path().to_path_buf()),
            config: None,
            read_only: false,
            state_dir: Some(dir.path().join("state")),
            pg_dsn: None,
        },
        &Provenance::default(),
    )
    .expect("the temp root resolves");
    (dir, located)
}
