# The interim data-plane `GET /metrics` (F10, the dev-harness exposure
# recorded in contracts/config.md) is retired: the data-plane listener no
# longer intercepts the path, so it is routed as the bucket named
# `metrics` like any other first path segment. What this scenario asserts
# is the interception's ABSENCE, which is the regression — the suite
# plants no bucket called `metrics`, so the request answers 404 in THIS
# fixture. That is a consequence of the fixture, not a promise of the
# contract: a root with a real bucket by that name would answer 200, and
# a future fixture that adds one must stay free to pass.
#
# The scrape endpoint now lives on the management plane (tinio-api's
# token-gated `/metrics`, contracts/management-api.md). SC-008's three-layer
# assertion (HTTP, S3 operations, storage) is carried by `tinio-api`'s inline
# tests — `src/transport.rs`, `src/router.rs`, `src/client.rs` and
# `src/openapi.rs`, where the retired `tests/management.rs` legs moved on
# 2026-10-01 (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`) —
# the open form on the local channel, the registry gathered without an injected source, and the
# wrap-vs-scan counting — and **not here**: plan B's harness migration moved
# this suite onto the `tinio` binary, and the control-channel scrape did not
# come with it, so this file keeps the interception's absence alone.
@T075 @SC-008
Feature: Metrics endpoint

  Scenario: The data-plane /metrics path is no longer intercepted
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I get object "data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/metrics"
    Then the response status is 404

  # The wire-error family, observed end to end through the in-process plane:
  # `MetricS3`'s Err arm records the code the client receives, and the plane
  # writes the same process-global registry the cucumber binary links. The
  # assertion is the delta from the sample taken before the request — never
  # an absolute value, because the registry is shared with every concurrent
  # scenario.
  Scenario: A failed S3 request increments the wire-error counter
    Given I sample the "tinio_s3_errors_total" counter for op "GetObject" and code "NoSuchBucket"
    When I send a "GET" request to "/ghost/key"
    Then the response status is 404
    And the "tinio_s3_errors_total" counter for op "GetObject" and code "NoSuchBucket" has increased
