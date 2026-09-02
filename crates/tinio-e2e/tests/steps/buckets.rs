use cucumber::{given, then, when};

#[given(expr = "I create bucket {string}")]
#[when(expr = "I create bucket {string}")]
async fn create_bucket(world: &mut super::World, name: String) {
    world.last = world
        .client
        .request("PUT", &format!("/{name}"), &[], &[])
        .await;
}

#[given(expr = "I delete bucket {string}")]
#[then(expr = "I delete bucket {string}")]
async fn delete_bucket(world: &mut super::World, name: String) {
    world.last = world
        .client
        .request("DELETE", &format!("/{name}"), &[], &[])
        .await;
}

/// The bucket listing (GET /) is empty: no `<Name>` entries at all.
#[then("the bucket listing is empty")]
async fn listing_is_empty(world: &mut super::World) {
    let resp = world.client.request("GET", "/", &[], &[]).await;
    let text = String::from_utf8_lossy(&resp.body).into_owned();
    assert!(!text.contains("<Name>"), "bucket listing not empty: {text}");
}

#[then(regex = r#"the bucket listing contains "([^"]+)" and "([^"]+)""#)]
async fn listing_contains(world: &mut super::World, a: String, b: String) {
    let resp = world.client.request("GET", "/", &[], &[]).await;
    let text = String::from_utf8_lossy(&resp.body).into_owned();
    for name in [&a, &b] {
        assert!(
            text.contains(&format!("<Name>{name}</Name>")),
            "bucket {name} missing from listing: {text}"
        );
    }
}
