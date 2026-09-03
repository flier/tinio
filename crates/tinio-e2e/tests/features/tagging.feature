# replaces the tagging legs of tinio-server/tests/coverage_gaps.rs
# (the FR-003 keep-legs below: GetObjectTagging empty-set, DeleteObjects
# quiet mode). The 2026-08-31 tagging-ops surface scenarios carry @FR-030
# (contracts/s3-surface.md §Object tagging).
Feature: Object tagging

  @FR-003
  Scenario: GetObjectTagging answers an empty set
    Given I create bucket "data"
    And I upload "data/tagged.txt" with body "x"
    When I send a "GET" request to "/data/tagged.txt?tagging"
    Then the response status is 200
    And the response body contains "<TagSet>"
    When I send a "GET" request to "/data/missing.txt?tagging"
    Then the response status is 404
    And the error code is "NoSuchKey"

  @FR-003
  Scenario: DeleteObjects quiet mode suppresses Deleted entries
    Given I create bucket "data"
    And I upload "data/a.txt" with body "a"
    And I upload "data/b.txt" with body "b"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Quiet>true</Quiet><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>"
      | Content-Type | application/xml |
    Then the response status is 200
    And the response body does not contain "<Deleted>"
    And the response body does not contain "<Error>"
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 404
    And I send a "GET" request to "/data/b.txt"
    Then the response status is 404
    Given I upload "data/c.txt" with body "c"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>c.txt</Key></Object></Delete>"
      | Content-Type | application/xml |
    Then the response status is 200
    And the response body contains "<Deleted>"
    And the response body contains "<Key>c.txt</Key>"

  # Task 11 (2026-08-31 s3-tagging-ops): the tagging surface of the
  # tagging spec — the ?tagging ops round trip (object AND bucket trios;
  # the bucket legs were missing at first and landed 2026-09-03),
  # x-amz-tagging on put/copy/multipart-create, the COPY/REPLACE
  # directives, and the toggle-off gate. Wire forms: PUT ?tagging
  # carries the <Tagging><TagSet> XML, x-amz-tagging is the URL-encoded
  # `k=v&k2=v2` header, CopyObject rides x-amz-tagging-directive, and
  # GetObject answers x-amz-tagging-count only while the object carries
  # tags.
  # Task 12 (2026-09-03): these scenarios carry @FR-030 (contracts/
  # s3-surface.md §Object tagging / §Bucket tagging) — per-scenario
  # tags, because the two keep-legs above stay @FR-003.

  @FR-030
  Scenario: Object tagging round trip, replace-all, and delete
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data/a.txt?tagging" with headers and body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>prod</Value>"
    # Put replaces the whole set — never a merge…
    When I send a "PUT" request to "/data/a.txt?tagging" with headers and body "<Tagging><TagSet><Tag><Key>team</Key><Value>core</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response body contains "<Key>team</Key>"
    And the response body contains "<Value>core</Value>"
    And the response body does not contain "<Key>env</Key>"
    # …and Delete clears the set back to empty…
    When I send a "DELETE" request to "/data/a.txt?tagging"
    Then the response status is 204
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<TagSet>"
    And the response body does not contain "<Key>"
    # …while the tagging ops never touch the object body.
    When I get object "data/a.txt"
    Then the response status is 200
    And the object body is "hello"

  @FR-030
  Scenario: PutObject with x-amz-tagging records the tags
    Given I create bucket "data"
    When I send a "PUT" request to "/data/a.txt" with headers
      | x-amz-tagging | env=prod&team=core |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Key>team</Key>"
    And the response body contains "<Value>core</Value>"
    # GetObject echoes the count header while tags are recorded…
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the response header "x-amz-tagging-count" is "2"
    # …and drops it once the tags are gone.
    When I send a "DELETE" request to "/data/a.txt?tagging"
    Then the response status is 204
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the response header "x-amz-tagging-count" is absent

  @FR-030
  Scenario: CopyObject with the default COPY directive carries the tags
    Given I create bucket "data"
    And I send a "PUT" request to "/data/a.txt" with headers
      | x-amz-tagging | env=prod |
    Then the response status is 200
    When I send a "PUT" request to "/data/b.txt" with headers
      | x-amz-copy-source | /data/a.txt |
    Then the response status is 200
    When I send a "GET" request to "/data/b.txt?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>prod</Value>"

  @FR-030
  Scenario: CopyObject REPLACE overrides the source tags
    Given I create bucket "data"
    And I send a "PUT" request to "/data/a.txt" with headers
      | x-amz-tagging | env=prod |
    Then the response status is 200
    When I send a "PUT" request to "/data/b.txt" with headers
      | x-amz-copy-source       | /data/a.txt |
      | x-amz-tagging-directive | REPLACE      |
      | x-amz-tagging           | env=dev      |
    Then the response status is 200
    When I send a "GET" request to "/data/b.txt?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>dev</Value>"
    And the response body does not contain "<Value>prod</Value>"
    # The directive never rewrites the source's own tags.
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<Value>prod</Value>"

  @FR-030
  Scenario: Multipart completion carries the create-time tags
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin" with header "x-amz-tagging" "env=prod"
    And I upload part 1 with body "hello"
    When I complete the multipart upload
    Then the response status is 200
    When I send a "GET" request to "/data/big.bin?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>prod</Value>"

  @FR-030
  Scenario: A malformed x-amz-tagging value answers InvalidTag
    Given I create bucket "data"
    When I send a "PUT" request to "/data/a.txt" with headers
      | x-amz-tagging | k=v&broken |
    Then the response status is 400
    And the error code is "InvalidTag"
    # The rejection is request-shape — it fires before the body is
    # staged, so the failed put never creates the object.
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"

  @FR-030
  Scenario: Bucket tagging round trip, replace-all, and delete
    Given I create bucket "data"
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 200
    When I send a "GET" request to "/data?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>prod</Value>"
    # Put replaces the whole set — never a merge…
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>team</Key><Value>core</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 200
    When I send a "GET" request to "/data?tagging"
    Then the response body contains "<Key>team</Key>"
    And the response body contains "<Value>core</Value>"
    And the response body does not contain "<Key>env</Key>"
    # …and Delete clears the set back to empty.
    When I send a "DELETE" request to "/data?tagging"
    Then the response status is 204
    When I send a "GET" request to "/data?tagging"
    Then the response status is 200
    And the response body contains "<TagSet>"
    And the response body does not contain "<Key>"
    # The bucket and object tag sets are independent surfaces: an
    # object tag never shows up in the bucket's set, and vice versa.
    When I send a "PUT" request to "/data/a.txt" with headers
      | x-amz-tagging | env=obj |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<Value>obj</Value>"
    When I send a "GET" request to "/data?tagging"
    Then the response status is 200
    And the response body does not contain "<Value>obj</Value>"

  @tagging-off
  @FR-030
  Scenario: Disabled tagging answers NotImplemented on the object trio
    Given I create bucket "data"
    # Every ?tagging op is gated on the same toggle.
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "PUT" request to "/data/a.txt?tagging" with headers and body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "DELETE" request to "/data/a.txt?tagging"
    Then the response status is 501
    And the error code is "NotImplemented"

  @tagging-off
  @FR-030
  Scenario: Disabled tagging answers NotImplemented on the bucket trio
    Given I create bucket "data"
    When I send a "GET" request to "/data?tagging"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "DELETE" request to "/data?tagging"
    Then the response status is 501
    And the error code is "NotImplemented"
