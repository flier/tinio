# derived from specs/001-s3-local-server/contracts/s3-surface.md
# (SelectObjectContent, FR-034). The scenarios run on both backends as the
# suite's default: the CI mem leg via TINIO_E2E_BACKEND=mem, the local fs
# default (docs/tests.md — the backend tags are scenario-level and would
# force one backend; @fs at feature level would also exclude this feature
# from the CI mem leg's `not @fs` filter).
@FR-034
Feature: S3 Select over objects

  Scenario: Filter CSV by column position
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      a,b
      1,x
      2,y
      """
    When I select over object "data.csv" with query "SELECT s._1, s._2 FROM S3Object s WHERE s._1 = '1'"
    Then the select results contain "1,x"

  Scenario: Count rows over JSON lines
    Given I create bucket "select"
    And an object "data.jsonl" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":40}
      """
    When I select over object "data.jsonl" with query "SELECT count(*) FROM S3Object s"
    Then the select results contain "2"

  Scenario: LIMIT caps results
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,2
      3,4
      5,6
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s LIMIT 2"
    Then the select results contain "1,2" and "3,4" but not "5,6"

  Scenario: GZIP compressed CSV input
    Given I create bucket "select"
    And a gzip-compressed object "data.csv.gz" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.gz" with query "SELECT * FROM S3Object s"
    Then the select results contain "1,x"

  Scenario: AWS doc sample with named headers
    Given I create bucket "select"
    And an object "cities.csvh" with content:
      """
      country,city
      US,Seattle
      US,Portland
      UK,London
      """
    When I select over object "cities.csvh" with query "SELECT s.country, s.city FROM S3Object s WHERE s.city = 'Seattle'"
    Then the select results contain "US,Seattle"
    And the select results do not contain "US,Portland"

  Scenario: CSV aggregates over one row
    Given I create bucket "select"
    And an object "nums.csv" with content:
      """
      3
      7
      1
      """
    When I select over object "nums.csv" with query "SELECT count(*), min(s._1), max(s._1), avg(s._1) FROM S3Object s"
    Then the select results contain "3,1,7,3.6666666667"

  Scenario: CSV WHERE filter with LIKE
    Given I create bucket "select"
    And an object "words.csv" with content:
      """
      alpha
      beta
      amber
      """
    When I select over object "words.csv" with query "SELECT s._1 FROM S3Object s WHERE s._1 LIKE 'a%'"
    Then the select results contain "alpha" and "amber" but not "beta"

  Scenario: JSON LINES projection and filter
    Given I create bucket "select"
    And an object "people.jsonl" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":40}
      {"name":"carol","age":25}
      """
    When I select over object "people.jsonl" with query "SELECT s.name FROM S3Object s WHERE s.age > 28"
    Then the select results contain "alice" and "bob" but not "carol"

  Scenario: JSON DOCUMENT root array
    Given I create bucket "select"
    And an object "items.jsond" with content:
      """
      [{"id":1},{"id":2}]
      """
    When I select over object "items.jsond" with query "SELECT s.id FROM S3Object s"
    Then the select results contain "1" and "2"

  Scenario: JSON output with aliased projection
    Given I create bucket "select"
    And an object "people.jsonl" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":25}
      """
    When I select over object "people.jsonl" with query "SELECT s.name AS n FROM S3Object s WHERE s.age > 28" as JSON output
    Then the select results contain "alice"
    And the select results do not contain "bob"

  Scenario: ScanRange window over CSV
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      0,1
      2,3
      4,5
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s" within scan range "4" to "8"
    Then the select results contain "2,3" and "4,5" but not "0,1"

  Scenario: Bad SQL is a request-level 400 S3QueryParsingError
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,x
      """
    When I try select over object "data.csv" with query "SELECT s._1 FROM S3Object s JOIN S3Object t"
    Then the select fails with HTTP 400 and code "S3QueryParsingError"

  Scenario: LIMIT stops a filter that would otherwise scan past it
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,a
      1,b
      1,c
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s WHERE s._1 = '1' LIMIT 2"
    Then the select results contain "1,a" and "1,b" but not "1,c"

  Scenario: CSV GZIP input with JSON output
    Given I create bucket "select"
    And a gzip-compressed object "data.csv.gz" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.gz" with query "SELECT s._2 AS v FROM S3Object s WHERE s._1 = '2'" as JSON output
    Then the select results contain "y"
    And the select results do not contain "x"

  Scenario: CSV BZIP2 input
    Given I create bucket "select"
    And a bzip2-compressed object "data.csv.bz2" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.bz2" with query "SELECT * FROM S3Object s"
    Then the select results contain "1,x"

  Scenario: CSV BZIP2 input with JSON output
    Given I create bucket "select"
    And a bzip2-compressed object "data.csv.bz2" with content:
      """
      1,x
      2,y
      """
    When I select over object "data.csv.bz2" with query "SELECT s._2 AS v FROM S3Object s WHERE s._1 = '1'" as JSON output
    Then the select results contain "x"

  Scenario: JSON LINES GZIP input
    Given I create bucket "select"
    And a gzip-compressed object "people.jsonl.gz" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":40}
      {"name":"carol","age":25}
      """
    When I select over object "people.jsonl.gz" with query "SELECT s.name FROM S3Object s WHERE s.age > 28"
    Then the select results contain "alice" and "bob" but not "carol"

  Scenario: JSON LINES GZIP input with JSON output
    Given I create bucket "select"
    And a gzip-compressed object "people.jsonl.gz" with content:
      """
      {"name":"alice","age":30}
      """
    When I select over object "people.jsonl.gz" with query "SELECT s.name AS n FROM S3Object s" as JSON output
    Then the select results contain "alice"

  Scenario: JSON LINES BZIP2 input
    Given I create bucket "select"
    And a bzip2-compressed object "people.jsonl.bz2" with content:
      """
      {"name":"alice","age":30}
      {"name":"bob","age":25}
      """
    When I select over object "people.jsonl.bz2" with query "SELECT s.name FROM S3Object s WHERE s.age > 28"
    Then the select results contain "alice"
    And the select results do not contain "bob"

  Scenario: JSON DOCUMENT with JSON output
    Given I create bucket "select"
    And an object "items.jsond" with content:
      """
      [{"id":1},{"id":2}]
      """
    When I select over object "items.jsond" with query "SELECT s.id AS i FROM S3Object s" as JSON output
    Then the select results contain "1" and "2"

  Scenario: Compound WHERE with arithmetic projections
    Given I create bucket "select"
    And an object "nums.csv" with content:
      """
      2,10,6
      3,7,7
      1,4,2
      """
    When I select over object "nums.csv" with query "SELECT s._1 + s._3 AS sum_, s._2 / 2 AS half FROM S3Object s WHERE s._1 + s._3 > 6"
    Then the select results contain "8,5" and "10,3.5" but not "3,1.5"

  Scenario: IN and BETWEEN in one predicate
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,a
      2,b
      3,c
      4,d
      5,e
      """
    When I select over object "data.csv" with query "SELECT s._2 FROM S3Object s WHERE s._1 IN ('2', '3') AND s._2 BETWEEN 'b' AND 'd'"
    Then the select results contain "b" and "c" but not "d"

  Scenario: NOT IN and NOT BETWEEN
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      1,a
      2,b
      3,c
      4,d
      5,e
      """
    When I select over object "data.csv" with query "SELECT s._2 FROM S3Object s WHERE s._1 NOT IN ('1', '5') AND s._2 NOT BETWEEN 'a' AND 'b'"
    Then the select results contain "c" and "d" but not "b"

  Scenario: JSON nested path access with IS NOT MISSING
    Given I create bucket "select"
    And an object "people.jsonl" with content:
      """
      {"name":"alice","projects":[{"budget":100}]}
      {"name":"bob","projects":[]}
      """
    When I select over object "people.jsonl" with query "SELECT s.name, s.projects[0].budget AS b FROM S3Object s WHERE s.projects[0].budget IS NOT MISSING"
    Then the select results contain "alice,100"
    And the select results do not contain "bob"

  Scenario: Traversal wildcard with aggregate
    Given I create bucket "select"
    And an object "items.jsond" with content:
      """
      {"items":[{"price":10},{"price":20}]}
      """
    When I select over object "items.jsond" with query "SELECT count(*), sum(s.price) AS total FROM S3Object[*].items[*] s"
    Then the select results contain "2,30"

  Scenario: NULL and MISSING are distinct
    Given I create bucket "select"
    And an object "people.jsonl" with content:
      """
      {"name":"a","optional":null}
      {"name":"b","optional":"x"}
      """
    When I select over object "people.jsonl" with query "SELECT s.name, s.absent AS m FROM S3Object s WHERE s.optional IS NULL AND s.absent IS MISSING"
    Then the select results contain "a,"
    And the select results do not contain "b,"

  Scenario: Empty CSV input yields no records
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s"
    Then the select results are empty

  Scenario: IGNORE header drops the first row
    Given I create bucket "select"
    And an object "data.csvi" with content:
      """
      id,name
      1,alice
      2,bob
      """
    When I select over object "data.csvi" with query "SELECT s._1, s._2 FROM S3Object s"
    Then the select results contain "1,alice"
    And the select results do not contain "id,name"

  Scenario: CSV comment lines are skipped
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      # generated file
      1,x
      # trailing note
      2,y
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s"
    Then the select results contain "1,x" and "2,y"
    And the select results do not contain "generated"

  Scenario: JSON nested value under CSV output is a compact cell
    Given I create bucket "select"
    And an object "items.jsonl" with content:
      """
      {"name":"a","tags":[1,2]}
      """
    When I select over object "items.jsonl" with query "SELECT s.name, s.tags AS t FROM S3Object s"
    Then the select results contain "[1,2]"

  Scenario: MISSING projected under JSON output is an empty object
    Given I create bucket "select"
    And an object "people.jsonl" with content:
      """
      {"name":"alice"}
      """
    When I select over object "people.jsonl" with query "SELECT s.absent AS a FROM S3Object s" as JSON output
    Then the select results contain "{}"

  Scenario: Missing object is a request-level 404 NoSuchKey
    Given I create bucket "select"
    When I try select over object "missing.csv" with query "SELECT * FROM S3Object s"
    Then the select fails with HTTP 404 and code "NoSuchKey"

  Scenario: ScanRange start-only reads from the byte offset
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      0,1
      2,3
      4,5
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s" from scan range "4"
    Then the select results contain "2,3" and "4,5" but not "0,1"

  Scenario: ScanRange end-only reads the last N bytes
    Given I create bucket "select"
    And an object "data.csv" with content:
      """
      0,1
      2,3
      4,5
      """
    When I select over object "data.csv" with query "SELECT * FROM S3Object s" over the last "4" bytes
    Then the select results contain "4,5"
    And the select results do not contain "0,1"
