# replaces the /metrics leg of tinio-server/tests/coverage_gaps.rs (F10:
# the data-plane listener serves the Prometheus text format for local
# scraping). The HTTP/S3 families register on the first served request,
# so the bucket create and upload precede the scrape.
@T075 @SC-008
Feature: Metrics endpoint

  Scenario: The /metrics endpoint serves the three-layer metric set
    Given I create bucket "data"
    And I upload "data/a.txt" with 1 bytes
    And I get object "data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/metrics"
    Then the response status is 200
    And the response header "Content-Type" is "text/plain; version=0.0.4"
    # SC-008: the three layers — HTTP, S3 operations, storage. The
    # storage families here are the streaming-path counters; the
    # full-scan gauges (buckets/objects/bytes) are the management
    # plane's T075 work (see metrics.rs).
    And the response body contains "tinio_http_requests_total"
    And the response body contains "tinio_s3_operations_total"
    And the response body contains "tinio_storage_upload_bytes_total"
    And the response body contains "tinio_storage_download_bytes_total"
