import argparse
import csv
import io
import os
import re
import subprocess
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlsplit


def bucket(name, region="eu-central-1"):
    return (
        f"<Bucket><Name>{name}</Name><BucketRegion>{region}</BucketRegion>"
        "<CreationDate>2024-11-14T23:32:47Z</CreationDate></Bucket>"
    )


def page(buckets, token=""):
    return (
        '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Buckets>{buckets}</Buckets><ContinuationToken>{token}</ContinuationToken>"
        "</ListAllMyBucketsResult>"
    )


@contextmanager
def mock_s3(responses):
    pending = deque(responses)
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            credential = re.search(r"Credential=([^, ]+)", self.headers.get("Authorization", ""))
            requests.append(
                {
                    "path": urlsplit(self.path).path,
                    "query": parse_qs(urlsplit(self.path).query),
                    "credential": credential.group(1).split("/") if credential else [],
                }
            )
            status, body = pending.popleft() if pending else (400, "<Error><Code>UnexpectedRequest</Code></Error>")
            encoded = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_sql(args, endpoint, sql, secret_type="aws"):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("AWS_")
        and key != "AI_AGENT"
        and key.lower() not in {"http_proxy", "https_proxy", "all_proxy"}
    }
    env.update(
        AWS_ENDPOINT_URL_S3=endpoint,
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE=os.devnull,
        AWS_SHARED_CREDENTIALS_FILE=os.devnull,
        AWS_MAX_ATTEMPTS="1",
        NO_PROXY="127.0.0.1",
        no_proxy="127.0.0.1",
    )
    setup = f"LOAD {sql_string(args.httpfs)}; LOAD {sql_string(args.extension)};"
    setup += f"""
        SET aws_network_calls_via_duckdb = {str(args.via_duckdb).lower()};
        SET aws_default_credentials = false;
        CREATE SECRET listing (
            TYPE {secret_type}, KEY_ID 'listing-test-key', SECRET 'listing-test-secret', REGION 'us-west-2'
        );
    """
    return subprocess.run(
        [
            str(args.duckdb),
            "-unsigned",
            "-batch",
            "-bail",
            "-csv",
            "-noheader",
            "-cmd",
            f".output {os.devnull}",
            "-cmd",
            setup,
            "-cmd",
            ".output stdout",
            "-c",
            sql,
        ],
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
    )


def rows(result):
    assert result.returncode == 0, result.stderr
    return list(csv.reader(io.StringIO(result.stdout)))


def test_paginated_account_listing(args):
    first = "".join(bucket(f"bucket-{i}") for i in range(1000))
    second = "".join(bucket(f"bucket-{i}", "us-east-1") for i in range(1000, 2000))
    third = "".join(bucket(f"bucket-{i}") for i in range(2000, 2501))
    responses = [
        (200, page(first, "page+two/=")),
        (200, page("", "page-three")),
        (200, page(second, "page-four")),
        (200, page(third)),
    ]
    with mock_s3(responses) as (endpoint, requests):
        result = run_sql(
            args,
            endpoint,
            """
            SELECT count(*), count(DISTINCT name),
                   count(*) FILTER (WHERE region = 'eu-central-1'),
                   count(*) FILTER (WHERE region = 'us-east-1'),
                   min(epoch_ms(created_at)), max(epoch_ms(created_at))
            FROM s3_buckets();
            """,
        )
        assert rows(result) == [["2501", "2501", "1501", "1000", "1731627167000", "1731627167000"]], result.stdout
        assert [request["query"] for request in requests] == [
            {"max-buckets": ["1000"]},
            {"max-buckets": ["1000"], "continuation-token": ["page+two/="]},
            {"max-buckets": ["1000"], "continuation-token": ["page-three"]},
            {"max-buckets": ["1000"], "continuation-token": ["page-four"]},
        ]
        assert all(request["path"] == "/" for request in requests)
        assert all(request["credential"][2] == "us-west-2" for request in requests)


def test_region_and_named_secret(args):
    with mock_s3([(200, page(bucket("regional-bucket")))]) as (endpoint, requests):
        result = run_sql(
            args,
            endpoint,
            """
            CREATE SECRET other (TYPE s3, KEY_ID 'other-test-key', SECRET 'other-test-secret');
            SELECT name, region FROM s3_buckets(region := 'eu-central-1', secret := 'listing');
            """,
            secret_type="s3",
        )
        assert rows(result) == [["true"], ["regional-bucket", "eu-central-1"]], result.stdout
        assert len(requests) == 1
        assert requests[0]["query"] == {"max-buckets": ["1000"], "bucket-region": ["eu-central-1"]}
        assert requests[0]["credential"][0] == "listing-test-key"
        assert requests[0]["credential"][2:4] == ["eu-central-1", "s3"]


def test_empty_and_optional_metadata(args):
    with mock_s3([(200, page("")), (200, page("<Bucket><Name>minimal-bucket</Name></Bucket>"))]) as (
        endpoint,
        requests,
    ):
        result = run_sql(
            args,
            endpoint,
            """
            SELECT count(*) FROM s3_buckets();
            SELECT name, region IS NULL, created_at IS NULL FROM s3_buckets();
            """,
        )
        assert rows(result) == [["0"], ["minimal-bucket", "true", "true"]], result.stdout
        assert len(requests) == 2


def test_repeated_execution(args):
    with mock_s3([(200, page(bucket("first-query"))), (200, page(bucket("second-query")))]) as (endpoint, requests):
        result = run_sql(
            args,
            endpoint,
            """
            PREPARE list_buckets AS SELECT name FROM s3_buckets(secret := 'listing');
            EXECUTE list_buckets;
            CREATE OR REPLACE SECRET listing (
                TYPE aws, KEY_ID 'replacement-test-key', SECRET 'replacement-test-secret', REGION 'eu-west-1'
            );
            EXECUTE list_buckets;
            """,
        )
        assert rows(result) == [["first-query"], ["true"], ["second-query"]], result.stdout
        assert [request["credential"][0] for request in requests] == ["listing-test-key", "replacement-test-key"]
        assert [request["credential"][2] for request in requests] == ["us-west-2", "eu-west-1"]


def test_errors_are_not_empty_results(args):
    responses = [
        (200, page(bucket("partial-result"), "next-page")),
        (403, "<Error><Code>AccessDenied</Code><Message>listing denied</Message></Error>"),
    ]
    with mock_s3(responses) as (endpoint, requests):
        result = run_sql(args, endpoint, "SELECT count(*) FROM s3_buckets();")
        assert result.returncode != 0
        assert "S3 ListBuckets failed: AccessDenied" in result.stderr, result.stderr
        assert "listing denied" in result.stderr
        assert len(requests) == 2

    with mock_s3([(200, page("", "same-token")), (200, page("", "same-token"))]) as (endpoint, requests):
        result = run_sql(args, endpoint, "SELECT count(*) FROM s3_buckets();")
        assert result.returncode != 0
        assert "repeated continuation token" in result.stderr, result.stderr
        assert len(requests) == 2


def test_no_network_during_binding_or_when_disabled(args):
    with mock_s3([]) as (endpoint, requests):
        result = run_sql(
            args,
            endpoint,
            """
            DESCRIBE SELECT * FROM s3_buckets(secret := 'does-not-exist');
            PREPARE list_buckets AS SELECT * FROM s3_buckets(secret := 'listing');
            SET enable_external_access = false;
            EXECUTE list_buckets;
            """,
        )
        assert result.returncode != 0
        assert "listing S3 buckets is disabled through configuration" in result.stderr, result.stderr
        assert requests == []


def main():
    parser = argparse.ArgumentParser(
        description="Test s3_buckets against a local HTTP fixture, without AWS credentials"
    )
    parser.add_argument("duckdb", type=Path)
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--httpfs", type=Path)
    args = parser.parse_args()
    args.duckdb = args.duckdb.resolve()
    args.extension = (args.extension or args.duckdb.parent / "extension/aws/aws.duckdb_extension").resolve()
    args.httpfs = (args.httpfs or args.duckdb.parent / "extension/httpfs/httpfs.duckdb_extension").resolve()
    for path in (args.duckdb, args.extension, args.httpfs):
        if not path.is_file():
            parser.error(f"Not found: {path}")
    tests = [
        test_paginated_account_listing,
        test_region_and_named_secret,
        test_empty_and_optional_metadata,
        test_repeated_execution,
        test_errors_are_not_empty_results,
        test_no_network_during_binding_or_when_disabled,
    ]
    for args.via_duckdb in (False, True):
        for test in tests:
            test(args)
            print(f"PASS {test.__name__} (aws_network_calls_via_duckdb={args.via_duckdb})", flush=True)


if __name__ == "__main__":
    main()
