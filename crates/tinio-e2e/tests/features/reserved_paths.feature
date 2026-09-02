# derived from specs/001-s3-local-server/contracts/s3-surface.md (reserved
# paths, FR-020, T026); replaces tinio-server/tests/reserved_paths.rs
@FR-020
Feature: Reserved .tinio paths

  @fs
  Scenario: Writes to .tinio are denied and reads answer NoSuchKey
    Given I create bucket "data"
    Given I upload "data/.tinio" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    Given I upload "data/.tinio/state" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    Given I upload "data/a/.tinio/x" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    Given I upload "data/a/b/.tinio/c" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    # F11: the backslash-separator leg of the reserved-key rule — a
    # single-backslash key with a .tinio segment is reserved on every
    # platform (the `\` split is unconditional, not Windows-only).
    Given I send a "PUT" request to "/data/a%5C.tinio%5Cb" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    Given I send a "GET" request to "/data/.tinio"
    Then the response status is 404
    And the error code is "NoSuchKey"
    Given I send a "GET" request to "/data/.tinio/state"
    Then the response status is 404
    And the error code is "NoSuchKey"
    Given I send a "GET" request to "/data/a/.tinio/x"
    Then the response status is 404
    And the error code is "NoSuchKey"
    Given I send a "GET" request to "/data/a/b/.tinio/c"
    Then the response status is 404
    And the error code is "NoSuchKey"
    Given I send a "GET" request to "/data?list-type=2"
    Then the response status is 200
    And the listing is empty and omits the reserved entries
    And the served root contains only the state dir and the bucket

  @fs
  Scenario: Nested roots never serve the outer state
    Given I create bucket "inner-root"
    And I write "secret" to "inner-root/.tinio/state" in the served root
    And I send a "GET" request to "/inner-root/.tinio/state"
    Then the response status is 404
    And the error code is "NoSuchKey"
    When I send a "PUT" request to "/inner-root/.tinio/state" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    And the file "inner-root/.tinio/state" in the served root contains "secret"
    Given I send a "GET" request to "/inner-root?list-type=2"
    Then the listing omits the reserved entries
    Given I write "public" to "inner-root/public.txt" in the served root
    And I send a "GET" request to "/inner-root/public.txt"
    Then the response status is 200
    And the response body is "public"

  # Symlink policy (spec Edge Cases, s3-surface.md): with the default
  # `follow_symlinks = false`, access resolving through a link is refused
  # and link entries are excluded from listings — a link inside a bucket
  # cannot escape the storage root.
  @fs
  Scenario: Access through a symlink is refused and links are excluded from listings
    Given I create bucket "data"
    And I upload "data/real.txt" with body "inside"
    And I create a directory link "data/linkdir" in the served root
    When I send a "GET" request to "/data/linkdir/secret.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/linkdir/new.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I list objects under "data/" with delimiter "/"
    Then the listing omits "linkdir"
    When I get object "data/real.txt"
    Then the response status is 200
    And the object body is "inside"
