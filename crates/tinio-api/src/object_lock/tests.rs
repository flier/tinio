use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use http_body_util::BodyExt;
use secrecy::ExposeSecret;
use tokio::sync::watch;
use tower::ServiceExt;

use super::{ObjectLockAdmin, UpdateObjectLock, VersionAddress};
use crate::{
    _core::{
        bucket,
        mode::AccessMode,
        object, object_lock as model, storage,
        version::{self, Address},
    },
    State,
    router::{Auth, Context, Shared, router},
};

const BUCKET: &str = "/buckets/logs/object-lock";
const OBJECTS: &str = "/buckets/logs/object-lock/objects";
const FUTURE: &str = "2030-01-01T00:00:00Z";

fn state() -> State {
    State::new(
        "tinio-0123456789abcdef.sock",
        Some(9000),
        Path::new("/srv/data"),
    )
    .expect("state")
}

fn config() -> model::Config {
    model::Config {
        enabled: true,
        rule: Some(model::Rule {
            default_retention: model::DefaultRetention {
                mode: model::Mode::Governance,
                period: model::Period::Days(30),
            },
        }),
    }
}

fn retention() -> model::Retention {
    model::Retention {
        mode: model::Mode::Compliance,
        retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000),
    }
}

/// What the admin answers instead of `Ok`.
#[derive(Debug, Clone, Copy, Default)]
enum Failure {
    #[default]
    None,
    /// A store that refuses the writes as read-only — the race a mode change after admission
    /// leaves, or a backend whose own access is read-only.
    ReadOnly,
    /// A protected change (COMPLIANCE, or governance without the bypass).
    Locked,
    /// The bucket's versioning or configuration refuses the write.
    Suspended,
    /// The bucket does not exist.
    NoBucket,
    /// The addressed version does not exist.
    NoVersion,
    /// A failure the mapping has no code for: `500`, and the detail stays in the log.
    Unexpected,
}

/// One observed call, so the set/clear/leave distinction is asserted at the admin boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Config(model::Config),
    Retention {
        address: Address,
        retention: Option<model::Retention>,
        bypass: model::Bypass,
    },
    Hold {
        address: Address,
        hold: model::LegalHold,
    },
    /// The combined update: both members travel in one call, so the store can
    /// apply them in one transaction.
    Update {
        address: Address,
        retention: Option<Option<model::Retention>>,
        legal_hold: Option<model::LegalHold>,
        bypass: model::Bypass,
    },
}

/// The CLI's admin in miniature: the stored configuration, one lock view, and the refusal
/// every call answers with when it is not `Ok`.
#[derive(Default)]
struct Admin {
    stored: Mutex<Option<model::Config>>,
    view: Mutex<Option<model::Lock>>,
    read_addresses: Mutex<Vec<Address>>,
    calls: Mutex<Vec<Call>>,
    failure: Failure,
}

impl Admin {
    fn storing(config: model::Config) -> Self {
        Self {
            stored: Mutex::new(Some(config)),
            ..Self::default()
        }
    }

    fn failing(failure: Failure) -> Self {
        Self {
            stored: Mutex::new(Some(config())),
            failure,
            ..Self::default()
        }
    }

    fn with_view(view: model::Lock) -> Self {
        Self {
            stored: Mutex::new(Some(config())),
            view: Mutex::new(Some(view)),
            ..Self::default()
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .expect("the fixture is not poisoned")
            .clone()
    }

    fn read_addresses(&self) -> Vec<Address> {
        self.read_addresses
            .lock()
            .expect("the fixture is not poisoned")
            .clone()
    }

    fn view(&self) -> Option<model::Lock> {
        self.view
            .lock()
            .expect("the fixture is not poisoned")
            .clone()
    }
}

fn read_refusal(failure: Failure, bucket: &bucket::Name) -> Option<storage::Error> {
    match failure {
        Failure::None | Failure::ReadOnly | Failure::Locked | Failure::Suspended => None,
        Failure::NoBucket => Some(storage::no_such_bucket(bucket)),
        Failure::NoVersion => Some(storage::no_such_version("abc")),
        Failure::Unexpected => Some(storage::io(io::Error::other(
            "/srv/data/.tinio/meta.redb is unreadable",
        ))),
    }
}

fn write_refusal(failure: Failure, bucket: &bucket::Name) -> Option<storage::Error> {
    match failure {
        Failure::None => None,
        Failure::ReadOnly => Some(storage::access_denied_bucket(bucket)),
        Failure::Locked => Some(storage::locked(model::RefusalReason::Compliance)),
        Failure::Suspended => Some(storage::invalid_bucket_state(bucket)),
        Failure::NoBucket => Some(storage::no_such_bucket(bucket)),
        Failure::NoVersion => Some(storage::no_such_version("abc")),
        Failure::Unexpected => Some(storage::io(io::Error::other(
            "/srv/data/.tinio/meta.redb is unreadable",
        ))),
    }
}

#[async_trait]
impl ObjectLockAdmin for Admin {
    async fn bucket_config(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<model::Config>, storage::Error> {
        match read_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => Ok(self
                .stored
                .lock()
                .expect("the fixture is not poisoned")
                .clone()),
        }
    }

    async fn put_bucket_config(
        &self,
        bucket: &bucket::Name,
        config: &model::Config,
    ) -> Result<(), storage::Error> {
        match write_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => {
                self.calls
                    .lock()
                    .expect("the fixture is not poisoned")
                    .push(Call::Config(config.clone()));
                *self.stored.lock().expect("the fixture is not poisoned") = Some(config.clone());
                Ok(())
            }
        }
    }

    async fn object_lock(
        &self,
        bucket: &bucket::Name,
        _key: &object::Key,
        address: &Address,
    ) -> Result<Option<model::Lock>, storage::Error> {
        match read_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => {
                self.read_addresses
                    .lock()
                    .expect("the fixture is not poisoned")
                    .push(address.clone());
                Ok(self
                    .view
                    .lock()
                    .expect("the fixture is not poisoned")
                    .clone())
            }
        }
    }

    async fn put_retention(
        &self,
        bucket: &bucket::Name,
        _key: &object::Key,
        address: &Address,
        retention: Option<&model::Retention>,
        bypass: model::Bypass,
    ) -> Result<(), storage::Error> {
        match write_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => {
                self.calls
                    .lock()
                    .expect("the fixture is not poisoned")
                    .push(Call::Retention {
                        address: address.clone(),
                        retention: retention.cloned(),
                        bypass,
                    });
                Ok(())
            }
        }
    }

    async fn put_legal_hold(
        &self,
        bucket: &bucket::Name,
        _key: &object::Key,
        address: &Address,
        hold: model::LegalHold,
    ) -> Result<(), storage::Error> {
        match write_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => {
                self.calls
                    .lock()
                    .expect("the fixture is not poisoned")
                    .push(Call::Hold {
                        address: address.clone(),
                        hold,
                    });
                Ok(())
            }
        }
    }

    /// The combined update in miniature: one call carries both members, one
    /// model decision applies them, and a refusal changes nothing.
    async fn update_lock(
        &self,
        bucket: &bucket::Name,
        _key: &object::Key,
        address: &Address,
        retention: Option<Option<&model::Retention>>,
        legal_hold: Option<model::LegalHold>,
        bypass: model::Bypass,
    ) -> Result<(), storage::Error> {
        match write_refusal(self.failure, bucket) {
            Some(err) => Err(err),
            None => {
                self.calls
                    .lock()
                    .expect("the fixture is not poisoned")
                    .push(Call::Update {
                        address: address.clone(),
                        retention: retention.map(|retention| retention.copied()),
                        legal_hold,
                        bypass,
                    });
                let mut view = self.view.lock().expect("the fixture is not poisoned");
                let updated = model::apply_lock(
                    view.as_ref(),
                    retention,
                    legal_hold,
                    bypass,
                    SystemTime::now(),
                )
                .map_err(storage::locked)?;
                *view = updated;
                Ok(())
            }
        }
    }
}

fn plane(auth: Auth, admin: Option<Arc<dyn ObjectLockAdmin>>) -> (Shared, String) {
    plane_with(auth, AccessMode::ReadWrite, admin)
}

fn plane_with(
    auth: Auth,
    access: AccessMode,
    admin: Option<Arc<dyn ObjectLockAdmin>>,
) -> (Shared, String) {
    let (tx, _rx) = watch::channel(false);
    let shared = Arc::new(
        Context::new(state(), "127.0.0.1", auth, tx)
            .with_access(access)
            .with_object_lock(admin),
    );
    let token = shared.state.token.expose_secret().as_str().to_owned();
    (shared, token)
}

fn request(method: &str, path: &str, token: Option<&str>, body: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(path).method(method);
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_owned())))
        .expect("request")
}

async fn body_text(response: Response) -> String {
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8(body.to_vec()).expect("utf8")
}

async fn call(
    shared: &Shared,
    method: &str,
    path: &str,
    token: &str,
    body: Option<&str>,
) -> (StatusCode, String) {
    let response = router(shared.clone())
        .oneshot(request(method, path, Some(token), body))
        .await
        .expect("response");
    let status = response.status();
    (status, body_text(response).await)
}

/// A valid bucket `PUT` body: governed, 30 days.
fn bucket_body() -> String {
    serde_json::to_string(&config()).expect("json")
}

fn address_body(version: &str) -> String {
    format!(r#"{{"key":"report.txt","version":"{version}"}}"#)
}

fn read_set_body() -> String {
    format!(
        r#"{{"key":"report.txt","version":"current","retention":{{"mode":"COMPLIANCE","retain_until":"{FUTURE}"}}}}"#
    )
}

#[tokio::test]
async fn the_bucket_configuration_round_trips_the_stored_json() {
    // The body is the model's own JSON, and the `GET` answers exactly what was stored (OL30):
    // the enabled bit, the rule, the stored `GOVERNANCE` spelling and the `{"days": 30}` period.
    let admin = Arc::new(Admin::default());
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let body = bucket_body();
    let (status, _) = call(&shared, "PUT", BUCKET, &token, Some(&body)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(admin.calls(), vec![Call::Config(config())]);

    let (status, text) = call(&shared, "GET", BUCKET, &token, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, body, "the stored configuration, field for field");
    let parsed: model::Config = serde_json::from_str(&text).expect("the model's own JSON");
    assert_eq!(parsed, config());
}

#[tokio::test]
async fn a_disabled_configuration_is_refused() {
    // OL23: the setter only enables or updates the rule; it never disables. A disabled body is
    // `400` and nothing reaches the admin.
    let admin = Arc::new(Admin::default());
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let (status, text) = call(
        &shared,
        "PUT",
        BUCKET,
        &token,
        Some(r#"{"enabled":false,"rule":null}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("enables or updates"), "{text}");
    assert!(admin.calls().is_empty(), "nothing was stored");
}

#[test]
fn the_version_address_spells_all_three_states() {
    // OL29: `current`, `null`, an id — never an `Option` where absent and null collapse.
    for (address, wire) in [
        (VersionAddress::Current, r#""current""#),
        (VersionAddress::Null, r#""null""#),
        (VersionAddress::Id("abc".to_string()), r#""abc""#),
    ] {
        assert_eq!(serde_json::to_string(&address).expect("json"), wire);
        assert_eq!(
            serde_json::from_str::<VersionAddress>(wire).expect("json"),
            address
        );
    }
}

#[tokio::test]
async fn the_object_get_resolves_all_three_addresses() {
    // The body's address is handed to the admin as the contract's one model: `current` is the
    // current slot, `null` the null version, anything else an id.
    let admin = Arc::new(Admin::with_view(model::Lock {
        retention: Some(retention()),
        legal_hold: model::LegalHold::Off,
    }));
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    for version in ["current", "null", "abc"] {
        let body = address_body(version);
        let (status, text) = call(&shared, "GET", OBJECTS, &token, Some(&body)).await;
        assert_eq!(status, StatusCode::OK, "{version}: {text}");
    }
    assert_eq!(
        admin.read_addresses(),
        vec![
            Address::Current,
            Address::Null,
            Address::Id(version::id("abc").expect("an id")),
        ]
    );
}

#[tokio::test]
async fn retention_set_clear_and_leave_are_independent() {
    // OL23: absent leaves, `null` clears, an object sets; the legal hold is independent, so
    // clearing the retention never clears the hold and vice versa.
    let admin = Arc::new(Admin::storing(config()));
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let current = Address::Current;
    let set = format!(
        r#"{{"key":"report.txt","version":"current","retention":{{"mode":"GOVERNANCE","retain_until":"{FUTURE}"}}}}"#
    );

    // Set.
    let (status, _) = call(&shared, "PUT", OBJECTS, &token, Some(&set)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Clear: a governance retention needs the bypass to be removed.
    let clear = r#"{"key":"report.txt","version":"current","retention":null,"bypass":true}"#;
    let (status, _) = call(&shared, "PUT", OBJECTS, &token, Some(clear)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // A legal hold only: no retention call at all.
    let hold = r#"{"key":"report.txt","version":"current","legal_hold":"ON"}"#;
    let (status, _) = call(&shared, "PUT", OBJECTS, &token, Some(hold)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // A retention only, with the explicit governance bypass: no hold call.
    let bypassed = format!(
        r#"{{"key":"report.txt","version":"current","retention":{{"mode":"GOVERNANCE","retain_until":"{FUTURE}"}},"bypass":true}}"#
    );
    let (status, _) = call(&shared, "PUT", OBJECTS, &token, Some(&bypassed)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Neither member: a no-op, and still `204`.
    let noop = r#"{"key":"report.txt","version":"current"}"#;
    let (status, _) = call(&shared, "PUT", OBJECTS, &token, Some(noop)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    assert_eq!(
        admin.calls(),
        vec![
            Call::Update {
                address: current.clone(),
                retention: Some(Some(model::Retention {
                    mode: model::Mode::Governance,
                    retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000),
                })),
                legal_hold: None,
                bypass: model::Bypass::None,
            },
            Call::Update {
                address: current.clone(),
                retention: Some(None),
                legal_hold: None,
                bypass: model::Bypass::Governance,
            },
            Call::Update {
                address: current.clone(),
                retention: None,
                legal_hold: Some(model::LegalHold::On),
                bypass: model::Bypass::None,
            },
            Call::Update {
                address: current,
                retention: Some(Some(model::Retention {
                    mode: model::Mode::Governance,
                    retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000),
                })),
                legal_hold: None,
                bypass: model::Bypass::Governance,
            },
        ]
    );
}

#[tokio::test]
async fn a_refused_retention_leaves_the_legal_hold_unapplied() {
    // OL23: the present members are one store transaction. A compliance
    // shortening is refused while a legal-hold member is present: the answer
    // is 403 and the stored lock is unchanged — the hold never lands.
    let current = model::Lock {
        retention: Some(retention()),
        legal_hold: model::LegalHold::Off,
    };
    let admin = Arc::new(Admin::with_view(current.clone()));
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let body = r#"{"key":"report.txt","version":"current","retention":{"mode":"COMPLIANCE","retain_until":"2029-01-01T00:00:00Z"},"legal_hold":"ON"}"#;
    let (status, text) = call(&shared, "PUT", OBJECTS, &token, Some(body)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{text}");
    assert_eq!(
        admin.view(),
        Some(current),
        "the refused retention applied neither member: the hold is not ON"
    );
    assert_eq!(
        admin.calls(),
        vec![Call::Update {
            address: Address::Current,
            retention: Some(Some(model::Retention {
                mode: model::Mode::Compliance,
                retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(1_861_920_000),
            })),
            legal_hold: Some(model::LegalHold::On),
            bypass: model::Bypass::None,
        }],
        "both members travelled in one combined call"
    );
}

#[tokio::test]
async fn a_past_retention_is_refused_before_the_admin() {
    let admin = Arc::new(Admin::storing(config()));
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let past = r#"{"key":"report.txt","version":"current","retention":{"mode":"COMPLIANCE","retain_until":"2000-01-01T00:00:00Z"}}"#;
    let (status, text) = call(&shared, "PUT", OBJECTS, &token, Some(past)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("future"), "{text}");
    assert!(admin.calls().is_empty(), "nothing was written");
}

#[tokio::test]
async fn the_object_answer_is_the_models_json() {
    let admin = Arc::new(Admin::with_view(model::Lock {
        retention: Some(retention()),
        legal_hold: model::LegalHold::On,
    }));
    let (shared, token) = plane(Auth::Local, Some(admin));
    let (status, text) = call(
        &shared,
        "GET",
        OBJECTS,
        &token,
        Some(&address_body("current")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: model::Lock = serde_json::from_str(&text).expect("the model's JSON");
    assert_eq!(body.legal_hold, model::LegalHold::On);
    let stored = body.retention.expect("a retention");
    assert_eq!(stored.mode, model::Mode::Compliance);
    assert_eq!(stored.retain_until, retention().retain_until);
}

#[tokio::test]
async fn the_errors_map_to_their_statuses() {
    // OL23's table: `Locked` 403, `InvalidBucketState` 409, `NoSuchBucket` 404,
    // `NoSuchVersion` 404, everything else 500 with the detail off the wire.
    let cases = [
        (
            Failure::NoBucket,
            "GET",
            BUCKET,
            None,
            StatusCode::NOT_FOUND,
            "no such bucket",
        ),
        (
            Failure::Suspended,
            "PUT",
            BUCKET,
            Some(bucket_body()),
            StatusCode::CONFLICT,
            "invalid bucket state",
        ),
        (
            Failure::Locked,
            "PUT",
            OBJECTS,
            Some(read_set_body()),
            StatusCode::FORBIDDEN,
            "access denied",
        ),
        (
            Failure::NoVersion,
            "PUT",
            OBJECTS,
            Some(read_set_body()),
            StatusCode::NOT_FOUND,
            "no such version",
        ),
        (
            Failure::Unexpected,
            "GET",
            BUCKET,
            None,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error",
        ),
    ];
    for (failure, method, path, body, expected, needle) in cases {
        let admin: Arc<dyn ObjectLockAdmin> = Arc::new(Admin::failing(failure));
        let (shared, token) = plane(Auth::Local, Some(admin));
        let (status, text) = call(&shared, method, path, &token, body.as_deref()).await;
        assert_eq!(status, expected, "{failure:?} {method}: {text}");
        assert!(text.contains(needle), "{failure:?} {method}: {text}");
        assert!(!text.contains("meta.redb"), "the detail stays in the log");
    }
}

#[tokio::test]
async fn a_read_only_plane_serves_the_reads_and_refuses_the_writes() {
    // OL23: the plane's access mode is the admission. A read-only plane serves both `GET`s and
    // answers `403` for both `PUT`s without reaching the admin at all.
    let admin = Arc::new(Admin::storing(config()));
    let (shared, token) = plane_with(Auth::Local, AccessMode::ReadOnly, Some(admin.clone()));
    let (status, _) = call(&shared, "GET", BUCKET, &token, None).await;
    assert_eq!(status, StatusCode::OK, "the read is served");
    for (path, body) in [(BUCKET, bucket_body()), (OBJECTS, read_set_body())] {
        let (status, text) = call(&shared, "PUT", path, &token, Some(&body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {text}");
        assert!(text.contains("access denied"), "{path}: {text}");
    }
    assert!(admin.calls().is_empty(), "nothing reached the admin");
}

#[tokio::test]
async fn a_stores_read_only_refusal_is_403_on_a_writable_plane() {
    // The admission lives on the plane, but the store can still refuse a write it admitted — a
    // mode change after the admission, or a backend whose own access is read-only. The shared
    // `failure` mapping answers that `403`, and the reads still serve.
    let admin: Arc<dyn ObjectLockAdmin> = Arc::new(Admin::failing(Failure::ReadOnly));
    let (shared, token) = plane(Auth::Local, Some(admin));
    let (status, _) = call(&shared, "GET", BUCKET, &token, None).await;
    assert_eq!(status, StatusCode::OK, "the read is served");
    for (path, body) in [(BUCKET, bucket_body()), (OBJECTS, read_set_body())] {
        let (status, text) = call(&shared, "PUT", path, &token, Some(&body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {text}");
        assert!(text.contains("access denied"), "{path}: {text}");
    }
}

#[tokio::test]
async fn the_read_only_plane_refuses_both_puts_before_the_body() {
    // OL23: the admission runs after the admin is taken and before the body is parsed, so a
    // read-only plane answers both `PUT`s `403` — even a malformed body and the object route's
    // otherwise-`204` no-op — the double is untouched, and the body is the plane's own
    // `{"error": …}` shape.
    let admin = Arc::new(Admin::storing(config()));
    let (shared, token) = plane_with(Auth::Local, AccessMode::ReadOnly, Some(admin.clone()));
    for (path, body) in [
        (BUCKET, bucket_body()),
        (BUCKET, r#"{"enabled":"yes"}"#.to_string()),
        (
            OBJECTS,
            r#"{"key":"report.txt","version":"current"}"#.to_string(),
        ),
    ] {
        let (status, text) = call(&shared, "PUT", path, &token, Some(&body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {text}");
        let answer: serde_json::Value =
            serde_json::from_str(&text).expect("the plane's error body");
        assert_eq!(answer["error"], "access denied", "{path}: {text}");
    }
    assert!(admin.calls().is_empty(), "nothing reached the double");
    assert_eq!(
        *admin.stored.lock().expect("the fixture is not poisoned"),
        Some(config()),
        "the stored configuration is untouched"
    );
}

#[tokio::test]
async fn the_404_body_distinguishes_its_causes() {
    // OL23: "no such bucket" is its own answer; "not enabled" and "no retention" are the object
    // route's two absences, told apart in the body.
    let (missing, token) = plane(
        Auth::Local,
        Some(Arc::new(Admin::failing(Failure::NoBucket))),
    );
    let (status, text) = call(&missing, "GET", BUCKET, &token, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("no such bucket"), "{text}");

    let (empty, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
    let (status, text) = call(&empty, "GET", BUCKET, &token, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("no object lock configuration"), "{text}");

    let (status, text) = call(
        &empty,
        "GET",
        OBJECTS,
        &token,
        Some(&address_body("current")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("not enabled"), "{text}");

    let (configured, token) = plane(Auth::Local, Some(Arc::new(Admin::storing(config()))));
    let (status, text) = call(
        &configured,
        "GET",
        OBJECTS,
        &token,
        Some(&address_body("current")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("no retention"), "{text}");
}

#[tokio::test]
async fn no_admin_answers_404_even_with_a_malformed_body() {
    // The admin is taken before the body is parsed, so a plane carrying none answers its `404`
    // whatever the body is. The same malformed body with an admin present is the parser's own
    // `422`, which is what makes this ordering rather than a blanket.
    let (absent, token) = plane(Auth::Local, None);
    for (method, path, body) in [
        ("GET", BUCKET, None),
        ("PUT", BUCKET, Some(r#"{"enabled":"yes"}"#)),
        ("GET", OBJECTS, Some(r#"{"key":1}"#)),
        ("PUT", OBJECTS, Some(r#"{"key":1}"#)),
    ] {
        let (status, text) = call(&absent, method, path, &token, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {text}");
        assert!(text.contains("object lock is not served here"), "{text}");
    }

    let (present, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
    let (status, _) = call(
        &present,
        "PUT",
        BUCKET,
        &token,
        Some(r#"{"enabled":"yes"}"#),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "the parser's own status, once the route is served"
    );
}

#[tokio::test]
async fn the_object_lock_routes_need_the_token() {
    // Registered above the token layer like every other route, so the path rule covers both
    // pairs on every transport. The tokened read below is what makes these the guard's `401`s
    // rather than an unregistered path's `404`s.
    let admin: Arc<dyn ObjectLockAdmin> = Arc::new(Admin::storing(config()));
    for auth in [Auth::Local, Auth::Loopback, Auth::Tcp] {
        let (shared, token) = plane(auth, Some(Arc::clone(&admin)));
        for (method, path, body) in [
            ("GET", BUCKET, None),
            ("PUT", BUCKET, Some(bucket_body())),
            ("GET", OBJECTS, Some(address_body("current"))),
            ("PUT", OBJECTS, Some(read_set_body())),
        ] {
            let response = router(shared.clone())
                .oneshot(request(method, path, None, body.as_deref()))
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{auth:?} {method}"
            );
        }
        let (status, _) = call(&shared, "GET", BUCKET, &token, None).await;
        assert_eq!(status, StatusCode::OK, "{auth:?}");
    }
}

#[tokio::test]
async fn an_invalid_key_or_version_is_400() {
    let admin = Arc::new(Admin::storing(config()));
    let (shared, token) = plane(Auth::Local, Some(admin));
    for body in [
        r#"{"key":"","version":"current"}"#,
        r#"{"key":"report.txt","version":""}"#,
    ] {
        let (status, text) = call(&shared, "GET", OBJECTS, &token, Some(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
    }
    // A missing member is the parser's `422`, not a fabricated default.
    let (status, _) = call(
        &shared,
        "GET",
        OBJECTS,
        &token,
        Some(r#"{"key":"report.txt"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn an_invalid_rule_is_400_with_the_models_reason() {
    // The plane is a write path, so the model validates before anything is stored.
    let admin = Arc::new(Admin::default());
    let (shared, token) = plane(Auth::Local, Some(admin.clone()));
    let zero_days = r#"{"enabled":true,"rule":{"default_retention":{"mode":"GOVERNANCE","period":{"days":0}}}}"#;
    let (status, text) = call(&shared, "PUT", BUCKET, &token, Some(zero_days)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("between 1 and 36500"), "{text}");
    assert!(admin.calls().is_empty(), "nothing was stored");
}

#[tokio::test]
async fn a_bad_bucket_name_is_404() {
    let (shared, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
    let (status, text) = call(&shared, "GET", "/buckets/!!/object-lock", &token, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("no such bucket"), "{text}");
}

#[test]
fn the_update_model_keeps_absent_apart_from_null() {
    // The set/clear/leave distinction is the wire's own: an absent member is `None`, a `null`
    // is `Some(None)`, and a value is `Some(Some(_))`.
    let absent: UpdateObjectLock =
        serde_json::from_str(r#"{"key":"k","version":"current"}"#).expect("json");
    assert_eq!(absent.retention, None);
    assert_eq!(absent.legal_hold, None);
    let cleared: UpdateObjectLock =
        serde_json::from_str(r#"{"key":"k","version":"null","retention":null}"#).expect("json");
    assert_eq!(cleared.retention, Some(None));
    assert_eq!(cleared.version, VersionAddress::Null);
    let set: UpdateObjectLock = serde_json::from_str(&format!(
        r#"{{"key":"k","version":"abc","retention":{{"mode":"GOVERNANCE","retain_until":"{FUTURE}"}}}}"#
    ))
    .expect("json");
    assert_eq!(set.version, VersionAddress::Id("abc".to_string()));
    assert!(matches!(set.retention, Some(Some(_))));
}

#[test]
fn the_update_model_serializes_leave_as_an_omitted_member() {
    // The write side must not turn "leave" into "clear": an absent member is omitted, while an
    // explicit `null` (the clear) survives serialization as `null`.
    let leave = UpdateObjectLock {
        key: "k".to_string(),
        version: VersionAddress::Current,
        retention: None,
        legal_hold: None,
        bypass: false,
    };
    let wire = serde_json::to_value(&leave).expect("json");
    let object = wire.as_object().expect("an object");
    assert!(!object.contains_key("retention"), "{wire}");
    assert!(!object.contains_key("legal_hold"), "{wire}");
    assert_eq!(
        serde_json::from_value::<UpdateObjectLock>(wire).expect("json"),
        leave,
        "the omitted members round-trip as leave"
    );

    let clear = UpdateObjectLock {
        retention: Some(None),
        ..leave.clone()
    };
    let wire = serde_json::to_value(&clear).expect("json");
    assert_eq!(wire["retention"], serde_json::Value::Null);

    let hold_only = UpdateObjectLock {
        legal_hold: Some(model::LegalHold::On),
        ..leave
    };
    let wire = serde_json::to_value(&hold_only).expect("json");
    assert!(
        !wire
            .as_object()
            .expect("an object")
            .contains_key("retention"),
        "a legal-hold-only body must not carry a retention member: {wire}"
    );
    assert_eq!(wire["legal_hold"], "ON");
}
