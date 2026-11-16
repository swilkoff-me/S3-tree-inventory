#-----------------------------------------------------------------------------
# python3 s3_inventory_tree.py --bucket <bucket_name> --profile <aws_profile>
#-----------------------------------------------------------------------------
import boto3
import time
import csv
import argparse
import math
import json
import os
import sys
import tempfile
from typing import Any
from urllib.parse import urlparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# -------------------------------
# CONFIG
# -------------------------------
DATABASE = None
WORKGROUP = None
ATHENA_OUTPUT = None
QUERY_TIMEOUT_SECONDS = 900
POLL_INTERVAL_SECONDS = 2
RUN_DEADLINE = None
MAX_SAMPLES = 5

# These will be set from command-line arguments
TARGET_BUCKET = None
TABLE = None
DATE_FILTER = None
OUTPUT_CSV = None
PROBE_DAYS = 30
PROBE_TIME_SUFFIX = "01-00"

# -------------------------------
# SESSION (initialized after args are parsed)
# -------------------------------
session = None
athena = None
s3 = None
TEMP_DIR = tempfile.gettempdir()  # Cross-platform temporary directory

def init_session(profile_name=None, region_name=None):
    """Initialize boto3 session. Uses default credential chain if profile_name is None."""
    global session, athena, s3
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    athena = session.client("athena")
    s3 = session.client("s3")

def start_athena_query(query, query_database):
    if RUN_DEADLINE is not None and time.monotonic() >= RUN_DEADLINE:
        raise TimeoutError("Overall query-run timeout expired before starting a query.")

    query_args: dict[str, Any] = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": query_database},
    }
    if WORKGROUP:
        query_args["WorkGroup"] = WORKGROUP
    if ATHENA_OUTPUT:
        query_args["ResultConfiguration"] = {"OutputLocation": ATHENA_OUTPUT}
    return athena.start_query_execution(**query_args)["QueryExecutionId"]


def wait_for_query(query_id):
    """Wait for an Athena query, cancelling it if a configured deadline expires."""
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    if RUN_DEADLINE is not None:
        deadline = min(deadline, RUN_DEADLINE)

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return result

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            try:
                athena.stop_query_execution(QueryExecutionId=query_id)
            except Exception as error:
                print(f"[WARNING] Could not cancel timed-out query {query_id}: {error}", file=sys.stderr)
            raise TimeoutError(f"Athena query {query_id} exceeded its timeout.")

        time.sleep(min(POLL_INTERVAL_SECONDS, remaining))


def print_aws_context():
    print("AWS configuration:")
    try:
        identity = session.client("sts").get_caller_identity()
        print(f"  Account: {identity['Account']}")
        print(f"  Identity: {identity['Arn']}")
    except Exception as error:
        print(f"[WARNING] Could not resolve AWS caller identity: {error}", file=sys.stderr)
    print(f"  Region: {session.region_name or 'AWS SDK configuration'}")
    print(f"  Database: {DATABASE}")
    print(f"  Workgroup: {WORKGROUP or 'Athena default'}")
    if ATHENA_OUTPUT:
        print(f"  Athena output: {ATHENA_OUTPUT}")


def validate_positive_float(value):
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def validate_inventory_date(value):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d-%H-%M")
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD-HH-MM with a valid date and time") from error
    if parsed.strftime("%Y-%m-%d-%H-%M") != value:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD-HH-MM with a valid date and time")
    return value


def validate_probe_time(value):
    try:
        parsed = datetime.strptime(value, "%H-%M")
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use a valid HH-MM time") from error
    if parsed.strftime("%H-%M") != value:
        raise argparse.ArgumentTypeError("must use a valid HH-MM time")
    return value


def validate_nonnegative_int(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def load_config(config_path, required=False):
    """Load optional JSON configuration and reject unknown settings."""
    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        if required:
            raise
        return {}

    if not isinstance(config, dict):
        raise ValueError("Config file must contain a JSON object.")

    allowed_keys = {
        "database", "region", "profile", "workgroup", "athena_output",
        "query_timeout", "poll_interval", "run_timeout", "probe_days",
        "probe_time", "max_samples",
    }
    unknown_keys = set(config) - allowed_keys
    if unknown_keys:
        raise ValueError(f"Unknown config setting(s): {', '.join(sorted(unknown_keys))}")
    return config


def escape_spreadsheet_formula(value):
    """Prefix spreadsheet-sensitive text so CSV imports treat it as literal text."""
    if not isinstance(value, str):
        return value

    stripped = value.lstrip(" \t\r\n")
    formula_prefixes = ("=", "+", "-", "@", "＝", "＋", "－", "＠")
    if value.startswith(("\t", "\r", "\n")) or stripped.startswith(formula_prefixes):
        return "'" + value
    return value


def write_csv_row(writer, row):
    writer.writerow([escape_spreadsheet_formula(value) for value in row])


def resolve_string_setting(parser, cli_value, config, name):
    value = cli_value if cli_value is not None else config.get(name)
    if value is not None and not isinstance(value, str):
        parser.error(f"Config setting '{name}' must be a string or null")
    return value


def resolve_positive_float(parser, cli_value, config, name, default):
    value = cli_value if cli_value is not None else config.get(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        parser.error(f"Config setting '{name}' must be a positive number")
    try:
        return validate_positive_float(str(value))
    except argparse.ArgumentTypeError as error:
        parser.error(f"Config setting '{name}' {error}")


def resolve_integer(parser, cli_value, config, name, default, minimum):
    value = cli_value if cli_value is not None else config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        parser.error(f"Config setting '{name}' must be an integer of at least {minimum}")
    return value


def resolve_probe_time(parser, cli_value, config):
    value = cli_value if cli_value is not None else config.get("probe_time", "01-00")
    if not isinstance(value, str):
        parser.error("Config setting 'probe_time' must be a string")
    try:
        return validate_probe_time(value)
    except argparse.ArgumentTypeError as error:
        parser.error(f"Config setting 'probe_time' {error}")

# -------------------------------
# QUERY FUNCTIONS
# -------------------------------
def run_athena_query(query, query_database=None):
    """Run an Athena query and wait for completion."""
    qdb = query_database or DATABASE

    query_id = start_athena_query(query, qdb)
    res = wait_for_query(query_id)
    status = res["QueryExecution"]["Status"]
    if status["State"] == "SUCCEEDED":
        output = res["QueryExecution"].get("ResultConfiguration", {}).get("OutputLocation")
        if not output:
            raise RuntimeError(
                "Athena query succeeded without an output location. Configure the workgroup output "
                "or provide --athena-output."
            )
        return {"ok": True, "output": output, "reason": ""}

    reason = status.get("StateChangeReason", "Unknown error")
    return {"ok": False, "output": None, "reason": reason}


def download_query_csv(output_location, target_file):
    parsed = urlparse(output_location)
    s3.download_file(parsed.netloc, parsed.path.lstrip("/"), target_file)
    return target_file


def get_bucket_partitions():
    """Read dt partitions for the selected bucket using metadata only."""
    query = f"SHOW PARTITIONS {TABLE}"
    result = run_athena_query(query)

    if not result["ok"]:
        raise Exception(f"Unable to read partitions from table '{TABLE}': {result['reason']}")

    partition_file = download_query_csv(result["output"], os.path.join(TEMP_DIR, "partitions_result.csv"))

    dt_values = set()
    with open(partition_file, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Athena returns a single partition column, ex: bucket=x/dt=2026-04-12-01-00
            partition_value = list(row.values())[0]
            if not partition_value:
                continue

            part_map = {}
            for token in partition_value.split("/"):
                if "=" not in token:
                    continue
                k, v = token.split("=", 1)
                part_map[k] = v

            if part_map.get("bucket") == TARGET_BUCKET and part_map.get("dt"):
                dt_values.add(part_map["dt"])

    return sorted(dt_values, reverse=True)


def get_probe_dates(days, time_suffix):
    """Build dt candidates like YYYY-MM-DD-HH-MM for recent days."""
    suffix = (time_suffix or "01-00").strip()
    if len(suffix) != 5 or suffix[2] != "-":
        suffix = "01-00"

    today = datetime.now(timezone.utc).date()
    candidates = []
    for i in range(days):
        day = today - timedelta(days=i)
        candidates.append(f"{day.strftime('%Y-%m-%d')}-{suffix}")
    return candidates

# QUERY is now built dynamically in run_query() function based on latest DATE_FILTER

# -------------------------------
# DEBUG FUNCTIONS
# -------------------------------
def list_available_tables():
    """List all tables in the database."""
    query = f"SHOW TABLES IN `{DATABASE}`"
    print(f"Querying tables in database: {DATABASE}")

    result = run_athena_query(query, DATABASE)
    if not result["ok"]:
        print(f"[ERROR] Query failed: {result['reason']}")
        return

    try:
        parsed = urlparse(result["output"])
        result_file = os.path.join(TEMP_DIR, "tables_result.csv")
        s3.download_file(parsed.netloc, parsed.path.lstrip("/"), result_file)

        print(f"\nAvailable tables in {DATABASE}:")
        with open(result_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, 1):
                table_name = list(row.values())[0]  # First column value
                print(f"  {idx}. {table_name}")
    except Exception as e:
        print(f"[ERROR] Error reading table list: {e}")

def list_available_buckets():
    """List all buckets that have data in the specified table."""
    query = f"""
    SELECT DISTINCT bucket
    FROM {TABLE}
    ORDER BY bucket
    """
    print(f"Querying buckets in table: {TABLE}")

    result = run_athena_query(query, DATABASE)
    if not result["ok"]:
        print(f"[ERROR] Query failed: {result['reason']}")
        return

    try:
        parsed = urlparse(result["output"])
        result_file = os.path.join(TEMP_DIR, "buckets_result.csv")
        s3.download_file(parsed.netloc, parsed.path.lstrip("/"), result_file)

        print(f"\nAvailable buckets in table '{TABLE}':")
        with open(result_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            bucket_list = []
            for row in reader:
                bucket_name = list(row.values())[0]
                bucket_list.append(bucket_name)

            if bucket_list:
                for idx, bucket in enumerate(bucket_list, 1):
                    print(f"  {idx}. {bucket}")
            else:
                print(f"  No buckets found in table '{TABLE}'")
                print(f"\n[WARNING] This means the S3 Inventory hasn't been generated for any bucket yet.")
                print(f"     Configure S3 Inventory in AWS Console for your bucket first.")
    except Exception as e:
        print(f"[ERROR] Error reading bucket list: {e}")
def run_query(date_filter):
    query = f"""
    SELECT key, size, last_modified_date
    FROM {TABLE}
    WHERE bucket = '{TARGET_BUCKET}'
    AND dt = '{date_filter}'
    """
    return run_athena_query(query)

def download_result(output):
    return download_query_csv(output, os.path.join(TEMP_DIR, "result.csv"))


def csv_has_data(csv_file):
    """Return True if CSV has at least one data row (not just headers)."""
    with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for _ in reader:
            return True
    return False

# -------------------------------
# HELPERS
# -------------------------------
def format_size(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{round(size, 2)} {unit}"
        size /= 1024
    return f"{round(size, 2)} PB"

def parse_date(date_str):
    try:
        return datetime.fromisoformat(date_str.replace("Z", ""))
    except:
        return None

# -------------------------------
# CORE PROCESSING LOGIC
# -------------------------------
def process_data(csv_file):
    prefix_map = defaultdict(lambda: {
        "size": 0,
        "count": 0,
        "min_date": None,
        "max_date": None,
        "samples": [],
        "samples_set": set(),
        "has_child": False,
    })

    max_depth = 0

    def add_sample(prefix, key, file_size):
        data = prefix_map[prefix]
        if len(data["samples"]) >= MAX_SAMPLES:
            return
        if key in data["samples_set"]:
            return
        data["samples"].append((key, file_size))
        data["samples_set"].add(key)

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            key = (row.get("key") or "").strip()
            if not key:
                continue

            size = int(row.get("size") or 0)
            date = parse_date(row.get("last_modified_date") or "")

            # Split into folder parts + filename.
            # Important: do NOT treat the filename as a folder level.
            parts = key.split("/")
            is_folder_marker = key.endswith("/")

            if is_folder_marker:
                folder_parts = [p for p in parts if p]
                filename = None
            else:
                folder_parts = parts[:-1]
                filename = parts[-1]

            max_depth = max(max_depth, len(folder_parts))

            # ---------------------------
            # AGGREGATE ALL LEVELS
            # ---------------------------
            for i in range(len(folder_parts) + 1):
                prefix = tuple(folder_parts[:i])
                data = prefix_map[prefix]

                data["size"] += size
                data["count"] += 1

                if date:
                    if data["min_date"] is None or date < data["min_date"]:
                        data["min_date"] = date
                    if data["max_date"] is None or date > data["max_date"]:
                        data["max_date"] = date

            # ---------------------------
            # SAMPLES: DIRECT CHILDREN ONLY
            # ---------------------------
            # Only sample objects that are directly in this folder (not in subfolders).
            # Example: for folder "a/b/", key "a/b/x.txt" is included,
            # but "a/b/c/y.txt" is NOT included.
            if filename:
                direct_folder_prefix = tuple(folder_parts)
                add_sample(direct_folder_prefix, key, size)

            # ---------------------------
            # MARK CHILD FOLDERS
            # ---------------------------
            for i in range(len(folder_parts)):
                parent = tuple(folder_parts[:i])
                prefix_map[parent]["has_child"] = True

    return prefix_map, max_depth

# -------------------------------
# CSV GENERATION
# -------------------------------
def generate_csv(prefix_map, max_depth):
    headers = ["Bucket"]
    for i in range(max_depth):
        headers.append(f"Level {i+1}")

    headers += [
        "Object Count",
        "Folder Size (recursive)",
        "Oldest Object",
        "Latest Object",
        f"Sample File (max {MAX_SAMPLES} rows)",
        "Sample File Size"
    ]

    base_name, extension = os.path.splitext(OUTPUT_CSV)
    if not extension:
        extension = ".csv"
    elif extension.lower() != ".csv":
        raise ValueError("Output filename must use the .csv extension.")

    output_file = f"{base_name}{extension}"
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "w", encoding="utf-8-sig", newline="") as output_stream:
        writer = csv.writer(output_stream)
        writer.writerow(headers)

        for prefix, data in prefix_map.items():
            base_row = [TARGET_BUCKET]

            for i in range(max_depth):
                base_row.append(prefix[i] if i < len(prefix) else "-")

            base_row.append(data["count"])
            base_row.append(format_size(data["size"]))
            base_row.append(data["min_date"].strftime("%Y-%m-%d") if data["min_date"] else "-")
            base_row.append(data["max_date"].strftime("%Y-%m-%d") if data["max_date"] else "-")

            samples = data.get("samples") or []
            if not samples:
                write_csv_row(writer, base_row + ["", ""])
                continue

            for idx, (sample_key, sample_size) in enumerate(samples):
                row = list(base_row)
                if idx > 0:
                    row = [""] * len(base_row)
                write_csv_row(writer, row + [sample_key, format_size(sample_size)])

    print(f"[OK] CSV generated: {output_file}")

# -------------------------------
# MAIN
# -------------------------------
def main():
    global DATE_FILTER
    selected_csv_file = None

    if DATE_FILTER:
        print(f"[INFO] Using user-provided date: {DATE_FILTER}")
        result = run_query(DATE_FILTER)
        if not result["ok"]:
            raise Exception(f"Query failed for dt={DATE_FILTER}: {result['reason']}")
        selected_csv_file = download_result(result["output"])
        if not csv_has_data(selected_csv_file):
            print(f"[ERROR] No objects found for bucket '{TARGET_BUCKET}' on dt '{DATE_FILTER}'.")
            print("[INFO] Try another --date or run without --date to auto-probe.")
            return
    else:
        print("[INFO] Fetching available partitions...")
        dt_candidates = get_bucket_partitions()
        if not dt_candidates:
            dt_candidates = get_probe_dates(PROBE_DAYS, PROBE_TIME_SUFFIX)

        last_error = None

        # Try newest partition first; skip broken inventory partitions automatically.
        for dt in dt_candidates:
            result = run_query(dt)
            if not result["ok"]:
                last_error = result["reason"]
                continue

            candidate_csv = download_result(result["output"])
            if not csv_has_data(candidate_csv):
                continue

            DATE_FILTER = dt
            selected_csv_file = candidate_csv
            print(f"[OK] Using date: {DATE_FILTER}")
            print(f"[INFO] Data reflects S3 Inventory snapshot as of {DATE_FILTER}.")
            print(f"   Objects deleted after this date may still appear in the report.")
            break

        if not selected_csv_file:
            raise Exception(
                "No usable partition found for this bucket. Last error: "
                + (last_error or "Unknown error")
            )

    print("[INFO] Processing data...")
    prefix_map, max_depth = process_data(selected_csv_file)

    print("[INFO] Generating CSV...")
    generate_csv(prefix_map, max_depth)

# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="S3 Inventory Analysis - pulls sample files from each folder"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="JSON config file (default: s3_inventory_config.json beside this script, if present)"
    )
    parser.add_argument(
        "--bucket",
        required=False,
        help="S3 bucket name"
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Athena table name (required unless using --list-tables)"
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Athena database name (overrides the config file)"
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region (default: use the AWS configuration chain)"
    )
    parser.add_argument(
        "--workgroup",
        default=None,
        help="Athena workgroup (default: Athena's default workgroup)"
    )
    parser.add_argument(
        "--athena-output",
        default=None,
        help="S3 URI for Athena query results (may be overridden by workgroup settings)"
    )
    parser.add_argument(
        "--query-timeout",
        type=validate_positive_float,
        default=None,
        help="Maximum seconds to wait for each Athena query (default: 900)"
    )
    parser.add_argument(
        "--poll-interval",
        type=validate_positive_float,
        default=None,
        help="Seconds between Athena status checks (default: 2)"
    )
    parser.add_argument(
        "--run-timeout",
        type=validate_positive_float,
        default=None,
        help="Optional total seconds allowed for Athena query work across the run"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV filename (default: s3_inventory_<bucket>.csv)"
    )
    parser.add_argument(
        "--date",
        type=validate_inventory_date,
        default=None,
        help="Specific dt to query (example: 2026-04-12-01-00). If provided, auto date discovery is skipped."
    )
    parser.add_argument(
        "--probe-days",
        type=int,
        default=None,
        help="When no partitions are found, probe this many recent days (default: 30)."
    )
    parser.add_argument(
        "--probe-time",
        type=validate_probe_time,
        default=None,
        help="Time suffix used while probing dates, format HH-MM (default: 01-00)."
    )
    parser.add_argument(
        "--max-samples",
        type=validate_nonnegative_int,
        default=None,
        help="Maximum sample files to include per folder (default: 5; use 0 to disable)."
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all available tables in the database and exit"
    )
    parser.add_argument(
        "--list-buckets",
        action="store_true",
        help="List all buckets that have data in the specified table and exit"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (default: use the AWS credential chain)"
    )

    args = parser.parse_args()

    config_path = args.config or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "s3_inventory_config.json"
    )
    try:
        config = load_config(config_path, required=bool(args.config))
    except (OSError, ValueError) as error:
        parser.error(f"Unable to load config '{config_path}': {error}")

    DATABASE = resolve_string_setting(parser, args.database, config, "database")
    WORKGROUP = resolve_string_setting(parser, args.workgroup, config, "workgroup")
    ATHENA_OUTPUT = resolve_string_setting(parser, args.athena_output, config, "athena_output")
    region = resolve_string_setting(parser, args.region, config, "region")
    profile = resolve_string_setting(parser, args.profile, config, "profile")
    QUERY_TIMEOUT_SECONDS = resolve_positive_float(
        parser, args.query_timeout, config, "query_timeout", 900
    )
    POLL_INTERVAL_SECONDS = resolve_positive_float(
        parser, args.poll_interval, config, "poll_interval", 2
    )
    run_timeout = resolve_positive_float(parser, args.run_timeout, config, "run_timeout", None)
    PROBE_DAYS = resolve_integer(parser, args.probe_days, config, "probe_days", 30, 1)
    PROBE_TIME_SUFFIX = resolve_probe_time(parser, args.probe_time, config)
    MAX_SAMPLES = resolve_integer(parser, args.max_samples, config, "max_samples", 5, 0)

    if not DATABASE:
        parser.error("--database is required unless set in the config file")
    if not args.list_tables and not args.table:
        parser.error("--table is required unless using --list-tables")
    if not args.list_tables and not args.list_buckets and not args.bucket:
        parser.error("--bucket is required unless using --list-tables or --list-buckets")

    # Initialize boto3 session (None = use default AWS credential chain)
    init_session(profile_name=profile, region_name=region)
    print_aws_context()
    RUN_DEADLINE = time.monotonic() + run_timeout if run_timeout else None

    # Handle --list-tables
    if args.list_tables:
        list_available_tables()
        exit(0)

    # Handle --list-buckets
    if args.list_buckets:
        TABLE = args.table
        list_available_buckets()
        exit(0)

    # Set global variables from arguments
    TARGET_BUCKET = args.bucket
    TABLE = args.table

    if args.output:
        OUTPUT_CSV = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        OUTPUT_CSV = os.path.join("exports", f"s3_inventory_{TARGET_BUCKET}_{timestamp}.csv")

    DATE_FILTER = args.date
    print(f"Configuration:")
    print(f"  Bucket: {TARGET_BUCKET}")
    print(f"  Table: {TABLE}")
    print()

    main()
 