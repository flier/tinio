# Object versioning (spec 2026-10-03-s3-versioning): the three bucket states,
# the version response headers, the delete-marker rules and the version
# listing, driven through the surface. The corpus is `contracts/s3-surface.md`
# §Object versioning (`data-model.md`/`fs-backend.md` own the storage side);
# the tags resolve FR-037 through FR-041 plus FR-023 amended, the read-only
# leg (history proven *served* by a read-only run, not only unit-tested).
#
# Listings are bucket-addressed (`GET /<bucket>?versions&prefix=…`) — the one
# shape the contract records, so an object-addressed `?versions` would be
# served as that key's bytes.
#
# The design's remaining feature-matrix cells are derivable, not scenarios
# (the contract's e2e coverage row names them per cell). Folder markers sit
# outside the matrix by the design's non-goal.
#
# The `@cli` leg is shaped by the harness: its spawned binary serves the
# identity-enforced plane (`signed as "root"`), and the read-write run must
# end before the read-only one opens the root.
Feature: Object versioning

  @FR-037
  Scenario: An overwrite keeps the previous version retrievable
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    And I upload "data/notes.txt" with body "second"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 200
    And the response body is "second"
    When I send a "GET" request to "/data?versions&prefix=notes.txt"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers
    And the first version in the listing is the latest

  @FR-038
  Scenario: A delete marker hides the object but keeps every version
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    When I send a "DELETE" request to "/data/notes.txt"
    Then the response status is 204
    And the response header "x-amz-delete-marker" is "true"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 404
    And the response header "x-amz-delete-marker" is "true"
    And the response header "content-type" is "application/xml"
    # The 404 above proves the marker hides the key; the listing proves the
    # version it hid is still there — the "keeps every version" half of the
    # scenario name.
    When I send a "GET" request to "/data?versions"
    Then the response status is 200
    And the version listing has 1 versions and 1 delete markers

  @FR-038
  Scenario: The version-addressed delete journey restores and purges one version
    Given I create bucket "data"
    And I enable versioning on "data"
    When I upload "data/notes.txt" with body "first"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "older"
    When I upload "data/notes.txt" with body "second"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "newer"
    # A plain DELETE stacks a marker: the key is hidden, and the marker's own
    # identity rides the response.
    When I send a "DELETE" request to "/data/notes.txt"
    Then the response status is 204
    And the response header "x-amz-delete-marker" is "true"
    And the response header "x-amz-version-id" is stored as "marker"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 404
    And the response header "x-amz-delete-marker" is "true"
    And the response header "content-type" is "application/xml"
    # A GET addressed at the marker itself is the 405 with the marker's own
    # headers — and the XML error body's content type survives the header
    # map the error carries (2026-10-06 review).
    When I send a "GET" request to "/data/notes.txt?versionId={marker}"
    Then the response status is 405
    And the response header "x-amz-delete-marker" is "true"
    And the response header "content-type" is "application/xml"
    # Deleting the marker itself is a version-addressed delete: the version it
    # hid serves again.
    When I send a "DELETE" request to "/data/notes.txt?versionId={marker}"
    Then the response status is 204
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 200
    And the response body is "second"
    # Purging one content version is version-addressed too: the listing loses
    # it, and the other version still serves by its id.
    When I send a "DELETE" request to "/data/notes.txt?versionId={older}"
    Then the response status is 204
    When I send a "GET" request to "/data?versions&prefix=notes.txt"
    Then the response status is 200
    And the version listing has 1 versions and 0 delete markers
    When I send a "GET" request to "/data/notes.txt?versionId={newer}"
    Then the response status is 200
    And the response body is "second"

  @versioning-off @FR-039
  Scenario: A version-addressed request is refused when versioning is off
    Given I create bucket "data"
    And I upload "data/notes.txt" with body "first"
    When I send a "GET" request to "/data/notes.txt?versionId=v1"
    Then the response status is 501
    And the error code is "NotImplemented"

  @FR-040
  Scenario: A suspended bucket writes the null version in place
    Given I create bucket "data"
    And I enable versioning on "data"
    And I upload "data/notes.txt" with body "first"
    And I suspend versioning on "data"
    And I upload "data/notes.txt" with body "second"
    When I send a "GET" request to "/data/notes.txt"
    Then the response status is 200
    And the response body is "second"
    And the response header "x-amz-version-id" is "null"
    When I send a "GET" request to "/data?versions&prefix=notes.txt"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers

  @FR-041
  Scenario: The null version is addressable by its literal
    Given I create bucket "data"
    And I upload "data/notes.txt" with body "before"
    When I send a "GET" request to "/data/notes.txt?versionId=null"
    Then the response status is 200
    And the response body is "before"
    And the response header "x-amz-version-id" is "null"

  @cli @FR-023
  Scenario: A read-only run serves the history it can read
    Given a storage root
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data?versioning" signed as "root" with headers and body "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
      | Content-Type | application/xml |
    Then the response status is 200
    When I send a "PUT" request to "/data/notes.txt" signed as "root" with headers and body "first"
      | Content-Type | text/plain |
    Then the response status is 200
    When I send a "PUT" request to "/data/notes.txt" signed as "root" with headers and body "second"
      | Content-Type | text/plain |
    Then the response status is 200
    When I stop the server through its control channel
    And the run ends
    When I run "tinio server {root} --port 0 --read-only"
    And the client is bound to the run's endpoint
    When I send a "GET" request to "/data?versions" signed as "root"
    Then the response status is 200
    And the version listing has 2 versions and 0 delete markers
