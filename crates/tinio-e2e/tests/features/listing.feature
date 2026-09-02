# derived from specs/001-s3-local-server/contracts/s3-surface.md (listing,
# @SC-001); replaces the listing legs of tinio-server/tests/data_plane.rs
@SC-001 @FR-004
Feature: Listing

  Scenario: Prefix and delimiter split the listing
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I upload "data/b.txt" with 1 bytes
    And I upload "data/sub/c.txt" with 1 bytes
    And I upload "data/x/d.txt" with 1 bytes
    When I list objects under "data/"
    Then the listing shows 4 keys
    When I list objects under "data/" with delimiter "/"
    Then the listing shows 2 keys
    And the listing prefixes are "sub/" and "x/"
    When I list objects under "data/sub/"
    Then the listing shows 1 key

  Scenario: Pagination walks all keys
    Given I create bucket "data"
    And I upload "data/k0.txt" with 1 bytes
    And I upload "data/k1.txt" with 1 bytes
    And I upload "data/k2.txt" with 1 bytes
    And I upload "data/k3.txt" with 1 bytes
    When I list objects under "data/" with max-keys 2
    Then the listing shows 2 keys
    And a truncated listing resumes with the next page

  # replaces the v1 leg of tinio-server/tests/coverage_gaps.rs: the
  # path-style GET without list-type — Name echo, delimiter common-prefix
  # grouping, and marker pagination (the capped page walks the marker via
  # the raw request, exactly as the old test's wire call did)
  Scenario: ListObjects v1 walks delimiter and marker
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I upload "data/b.txt" with 1 bytes
    And I upload "data/dir/c.txt" with 1 bytes
    When I list v1 objects under "data/" with marker "" and delimiter ""
    Then the response status is 200
    And the response body contains "<Name>data</Name>"
    And the listing shows 3 keys
    And the listing contains "a.txt"
    And the listing contains "b.txt"
    And the listing contains "dir/c.txt"
    And the response body contains "<IsTruncated>false</IsTruncated>"
    When I list v1 objects under "data/" with marker "" and delimiter "/"
    Then the listing shows 2 keys
    And the response body contains "<Prefix>dir/</Prefix>"
    When I list v1 objects under "data/" with marker "a.txt" and delimiter "/"
    Then the response body contains "<Marker>a.txt</Marker>"
    And the listing shows 1 key
    When I send a "GET" request to "/data?marker=a.txt&max-keys=1"
    Then the response body contains "<IsTruncated>true</IsTruncated>"
    And the response body contains "<NextMarker>b.txt</NextMarker>"

  # Task 8: the fs listing unit suite's prefix/delimiter/pagination
  # semantics (full_listing_is_lexicographic,
  # list_objects_delimiter_groups_and_resumes_after_common_prefix, the
  # v1/v2 max-keys validation).

  Scenario: The full listing is lexicographic
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I upload "data/b.txt" with 1 bytes
    And I upload "data/dir/c.txt" with 1 bytes
    And I upload "data/dir/e.txt" with 1 bytes
    And I upload "data/dir/sub/d.txt" with 1 bytes
    When I list objects under "data/"
    Then the listing shows 5 keys
    And the listing keys in order are
      | a.txt         |
      | b.txt         |
      | dir/c.txt     |
      | dir/e.txt     |
      | dir/sub/d.txt |
    When I list objects under "data/b.txt"
    Then the listing shows 1 key
    When I list objects under "data/dir/"
    Then the listing shows 3 keys

  Scenario: Delimiter pagination resumes past common prefixes
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I upload "data/b.txt" with 1 bytes
    And I upload "data/dir/c.txt" with 1 bytes
    And I upload "data/dir/e.txt" with 1 bytes
    And I upload "data/z.txt" with 1 bytes
    When I list objects under "data/" with delimiter "/" and max-keys 2
    Then the listing shows 2 keys
    And a truncated listing resumes with the next page

  Scenario Outline: Listing rejects max-keys below one
    Given I create bucket "data"
    When I send a "GET" request to "/data?<query>"
    Then the response status is 400
    And the error code is "InvalidArgument"

    Examples:
      | query                  |
      | max-keys=0             |
      | max-keys=-1            |
      | list-type=2&max-keys=0 |
      | list-type=2&max-keys=-1 |
