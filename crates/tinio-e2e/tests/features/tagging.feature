# replaces the tagging legs of tinio-server/tests/coverage_gaps.rs
# (GetObjectTagging, DeleteObjects quiet mode)
@FR-003
Feature: Object tagging

  Scenario: GetObjectTagging answers an empty set
    Given I create bucket "data"
    And I upload "data/tagged.txt" with body "x"
    When I send a "GET" request to "/data/tagged.txt?tagging"
    Then the response status is 200
    And the response body contains "<TagSet>"
    When I send a "GET" request to "/data/missing.txt?tagging"
    Then the response status is 404
    And the error code is "NoSuchKey"

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
