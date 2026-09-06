# Spec 2026-09-05 (s3-acl-owner), Task 15. The four ACL operations and
# the enforced authorization pipeline (ConfigAuth + AclAccess over
# DataPlane::new_with_acl) driven by the fixture identity the harness
# spawns with: root — the configured [auth] pair "ROOTAK" presenting the
# [owner] element (canonical ID ffeeddcc…221100, display "ops") — and
# user-b — the [[users]] member "USERB" (canonical ID 8b3873bb…23b6c,
# derived from the access key). Authenticate-requiring requests sign as
# one of the two (`signed as "root"` / `signed as "user-b"`); every
# anonymous leg stays driver-level unsigned. The @acl-off scenario runs
# the legacy no-identity plane with the acl capability off — the
# @minimal-caps path (each ACL op answers 501 NotImplemented).

Feature: Bucket and object ACL policy

  @acl
  Scenario: A public-read object serves anonymous GETs
    When I send a "PUT" request to "/data" signed as "root" with headers
      | x-amz-acl | public-read |
    Then the response status is 200
    When I send a "PUT" request to "/data/a.txt" signed as "root" with headers and body "hello"
      | x-amz-acl | public-read |
    Then the response status is 200
    When I get object "data/a.txt"
    Then the response status is 200
    And the object body is "hello"

  @acl
  Scenario: A private object denies anonymous GETs
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data/secret.txt" signed as "root" with headers and body "top-secret"
      | x-amz-acl | private |
    Then the response status is 200
    When I get object "data/secret.txt"
    Then the response status is 403
    And the error code is "AccessDenied"

  @acl
  Scenario: A READ grant lets the grantee's signed GET succeed
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data/share.txt" signed as "root" with headers and body "shared"
      | x-amz-grant-read | id="8b3873bb48d62959c9316b8bc5a1c11dcb3e14dce9426cc76ce9c67e00523b6c" |
    Then the response status is 200
    When I get object "data/share.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "GET" request to "/data/share.txt" signed as "user-b"
    Then the response status is 200
    And the object body is "shared"

  @acl
  Scenario: expected-bucket-owner mismatch answers 403
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data/a.txt" signed as "root" with headers and body "hello"
      | x-amz-acl | public-read |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt" signed as "root" with headers
      | x-amz-expected-bucket-owner | ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100 |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt" signed as "root" with headers
      | x-amz-expected-bucket-owner | 0000000000000000000000000000000000000000000000000000000000000000 |
    Then the response status is 403
    And the error code is "AccessDenied"

  @acl
  Scenario: ListBuckets returns only the requester's buckets
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/other" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/work" signed as "user-b"
    Then the response status is 200
    When I send a "GET" request to "/" signed as "root"
    Then the response status is 200
    And the response body contains "<Name>data</Name>"
    And the response body contains "<Name>other</Name>"
    And the response body does not contain "<Name>work</Name>"
    When I send a "GET" request to "/" signed as "user-b"
    Then the response status is 200
    And the response body contains "<Name>work</Name>"
    And the response body does not contain "<Name>data</Name>"
    And the response body does not contain "<Name>other</Name>"

  @acl
  Scenario: Public-write denies overwriting an existing object
    When I send a "PUT" request to "/pub" signed as "root" with headers
      | x-amz-acl | public-read-write |
    Then the response status is 200
    When I send a "PUT" request to "/pub/a.txt" signed as "root" with headers and body "root's"
      | x-amz-acl | private |
    Then the response status is 200
    When I send a "PUT" request to "/pub/a.txt" with body "overwrite"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/pub/new.txt" with body "anonymous"
    Then the response status is 200
    When I get object "pub/new.txt"
    Then the response status is 200
    And the object body is "anonymous"

  @acl
  Scenario: DeleteObjects without a grant is denied as a whole
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data/a.txt" signed as "root" with headers and body "hello"
      | x-amz-acl | private |
    Then the response status is 200
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>a.txt</Key></Object></Delete>"
      | Content-Type | application/xml |
    Then the response status is 403
    And the error code is "AccessDenied"

  @acl
  Scenario: PutBucketAcl without Content-MD5 answers 400
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data?acl" signed as "root" with headers
      | x-amz-acl | private |
    Then the response status is 400
    And the error code is "InvalidRequest"
    And the response body contains "Missing required header for this request: Content-MD5"
    When I send a "PUT" request to "/data?acl" signed as "root" with headers
      | x-amz-acl | private |
      | Content-MD5 | 1B2M2Y8AsgTpgAmY7PhCfg== |
    Then the response status is 200

  @acl-off
  Scenario: ACL operations answer NotImplemented when the capability is off
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data?acl"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "PUT" request to "/data?acl"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "GET" request to "/data/a.txt?acl"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "PUT" request to "/data/a.txt?acl"
    Then the response status is 501
    And the error code is "NotImplemented"
