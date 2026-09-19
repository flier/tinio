# Read-only mode (FR-023): every mutating operation answers AccessDenied and
# reads are served unchanged. The other half of FR-023 — the relocated home
# state dir and a genuinely read-only root — is not observable through the
# wire and is asserted in crates/tinio-server/tests/read_only.rs.
#
# Fixtures are planted on disk: in this mode `Given I create bucket` and
# `Given I upload …` are themselves refused. Reads stop at what a planted
# root can express (buckets, objects); tagging / ACL / multipart read state
# lives in `meta.redb` and is covered by the Rust suites.
@FR-023
Feature: Read-only mode

  @fs @read-only
  Scenario: Reads are served unchanged
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "HEAD" request to "/data"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the response body contains "hello"
    When I send a "HEAD" request to "/data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/data?list-type=2"
    Then the response status is 200
    And the response body contains "a.txt"

  @fs @read-only
  Scenario: Bucket writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/other"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data"
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Object writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data/b.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/a.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/b.txt" with headers
      | x-amz-copy-source | /data/a.txt |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Batch delete is refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>a.txt</Key></Object></Delete>"
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Multipart writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data/b.txt?uploads"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/b.txt?uploadId=anything"
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Tagging and ACL writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/a.txt?acl" with headers
      | x-amz-acl | private |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: A write to a missing bucket is refused too
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/ghost/a.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
