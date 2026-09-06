# derived from specs/001-s3-local-server/contracts/s3-surface.md
# (SelectObjectContent, FR-034). The scenarios run on both backends as the
# suite's default: the CI mem leg via TINIO_E2E_BACKEND=mem, the local fs
# default (docs/tests.md — the backend tags are scenario-level and would
# force one backend; @fs at feature level would also exclude this feature
# from the CI mem leg's `not @fs` filter).
@FR-034
Feature: S3 Select over objects

  Scenario: Filter CSV by column position
    Given a bucket
    And an object "data.csv" with content:
      """
      a,b
      1,x
      2,y
      """
    When I select over object "data.csv" with query "SELECT s._1, s._2 FROM S3Object s WHERE s._1 = '1'"
    Then the select results contain "1,x"

  Scenario: Count rows over JSON lines
    Given a bucket
    And an object "data.jsonl" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":40}
      """
    When I select over object "data.jsonl" with query "SELECT count(*) FROM S3Object s"
    Then the select results contain "2"

  Scenario: LIMIT caps results
    Given a bucket
    And an object "data.csv" with content:
      """
      1,2
      3,4
      5,6
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s LIMIT 2"
    Then the select results contain "1,2" and "3,4" but not "5,6"

  Scenario: GZIP compressed CSV input
    Given a bucket
    And a gzip-compressed object "data.csv.gz" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.gz" with query "SELECT * FROM S3Object s"
    Then the select results contain "1,x"
