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
# token-gated `/metrics`, contracts/management-api.md), so SC-008's
# three-layer assertion (HTTP, S3 operations, storage) rides Task 10's
# management-plane integration suite for now; the control-channel form of
# this scenario returns with plan B's harness migration, which puts this
# suite on the `tinio` binary instead of the `serve` example.
@T075 @SC-008
Feature: Metrics endpoint

  Scenario: The data-plane /metrics path is no longer intercepted
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I get object "data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/metrics"
    Then the response status is 404
