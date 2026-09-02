# derived from specs/001-s3-local-server/contracts/s3-surface.md (buckets)
@SC-001 @FR-002
Feature: Buckets

  Scenario: Create and delete a bucket
    Given I create bucket "demo"
    Then the response status is 200
    And I delete bucket "demo"
    Then the response status is 204
    And the bucket listing is empty

  Scenario: Duplicate bucket creation answers BucketAlreadyOwnedByYou
    Given I create bucket "demo"
    And I create bucket "demo"
    Then the error code is "BucketAlreadyOwnedByYou"

  Scenario: Bucket listing shows created buckets
    Given I create bucket "alpha"
    And I create bucket "beta"
    Then the bucket listing contains "alpha" and "beta"

  # GetBucketLocation always answers us-east-1 (contracts/s3-surface.md,
  # buckets group) — the operation is implemented but had no scenario.
  Scenario: GetBucketLocation answers us-east-1
    Given I create bucket "demo"
    When I send a "GET" request to "/demo?location"
    Then the response status is 200
    And the response body contains "<LocationConstraint"
    And the response body contains "us-east-1"

  # HeadBucket: 200 on an existing bucket, 404 NoSuchBucket on a missing
  # one (the s3s framework maps the storage error).
  Scenario: HeadBucket distinguishes existing from missing buckets
    Given I create bucket "demo"
    When I send a "HEAD" request to "/demo"
    Then the response status is 200
    When I send a "HEAD" request to "/missing"
    Then the response status is 404

  # F07: DeleteBucket answers 204 before the tree is gone (async purge on
  # the fs backend), so a bucket recreated under the same name is live
  # again immediately — no client-visible tombstone window.
  Scenario: A deleted bucket name is reusable immediately
    Given I create bucket "recycled"
    When I send a "DELETE" request to "/recycled"
    Then the response status is 204
    When I create bucket "recycled"
    Then the response status is 200
    And I upload "recycled/fresh.txt" with body "y"
    Then the response status is 200
    When I send a "GET" request to "/recycled/fresh.txt"
    Then the response status is 200
    And the response body is "y"

  # US1-AS1: a directory placed directly in the storage root is a bucket
  # (FR-001/FR-002 — buckets map to top-level subdirectories; the
  # out-of-band mirror semantics of SC-006).
  @fs
  Scenario: A directory placed directly in the served root is a bucket
    Given I create the directory "photos" in the served root
    When I send a "GET" request to "/"
    Then the response status is 200
    And the response body contains "<Name>photos</Name>"
    When I send a "PUT" request to "/photos/hello.txt" with body "x"
    Then the response status is 200
