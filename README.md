# S3 Tree Inventory

A Python utility that reads S3 Inventory data from an Athena table and builds a folder/tree-style CSV summary of storage usage, object counts, and sample files per prefix.

## What it does

- Queries Athena for inventory data for a target bucket
- Finds the latest usable `dt` partition or allows a specific date override
- Aggregates size and object counts recursively by folder prefix
- Exports a CSV that shows:
  - bucket
  - folder levels
  - object count
  - recursive folder size
  - oldest and newest object dates
  - sample file names and sizes

## Features

- Athena query execution with timeout/poll controls
- Automatic fallback to recent probe dates when no inventory partitions are found
- Optional bucket and table discovery helpers
- CSV output safe for spreadsheet import
- Config-file support for AWS/Athena defaults

## Requirements

- Python 3.9+
- AWS credentials configured via AWS CLI, environment variables, or a named profile
- Access to an Athena database containing S3 Inventory table metadata
- An S3 location for Athena result output

Install dependencies:

```bash
pip install boto3
```

## Example configuration

Create a local config file from the example and keep it out of version control:

```bash
copy config.example.json config.json
```

Example contents:

```json
{
  "database": "your_athena_database",
  "region": "us-east-1",
  "profile": null,
  "workgroup": null,
  "athena_output": "s3://your-athena-bucket/results/",
  "query_timeout": 900,
  "poll_interval": 2,
  "run_timeout": null,
  "probe_days": 30,
  "probe_time": "01-00",
  "max_samples": 5
}
```

> Keep real AWS/account values in your local `config.json` and do not commit it to GitHub.

## Usage

List tables:

```bash
python tree_structure.py --config config.json --database your_athena_database --list-tables
```

List buckets in a table:

```bash
python tree_structure.py --config config.json --database your_athena_database --table s3_inventory_table --list-buckets
```

Generate a folder inventory report:

```bash
python tree_structure.py \
  --config config.json \
  --database your_athena_database \
  --table s3_inventory_table \
  --bucket my-bucket \
  --output s3_inventory_my_bucket.csv
```

Query a specific inventory snapshot:

```bash
python tree_structure.py \
  --config config.json \
  --database your_athena_database \
  --table s3_inventory_table \
  --bucket my-bucket \
  --date 2026-04-12-01-00 \
  --output s3_inventory_my_bucket.csv
```

## Example output

A small synthetic example file is included in the repository at [example_output.csv](example_output.csv). It shows the general format the script produces:

```csv
Bucket,Level 1,Level 2,Level 3,Object Count,Folder Size (recursive),Oldest Object,Latest Object,Sample File (max 5 rows),Sample File Size
example-bucket,images,2026,01,3,"1.50 KB","2026-01-10","2026-01-15","images/2026/01/photo-1.jpg","512 B"
example-bucket,images,2026,01,,"1.50 KB","2026-01-10","2026-01-15","images/2026/01/photo-2.jpg","512 B"
example-bucket,images,2026,01,,"1.50 KB","2026-01-10","2026-01-15","images/2026/01/photo-3.jpg","512 B"
example-bucket,documents,2026,02,2,"800 B","2026-02-01","2026-02-05","documents/2026/02/notes.txt","400 B"
example-bucket,documents,2026,02,,"800 B","2026-02-01","2026-02-05","documents/2026/02/report.pdf","400 B"
example-bucket,logs,2026,03,1,"256 B","2026-03-02","2026-03-02","logs/2026/03/app.log","256 B"
```

This example is meant to demonstrate the structure of the generated report without exposing real AWS data.

## Notes

- The script expects a table created from AWS S3 Inventory data, with partition columns like `dt` and `bucket`.
- `--date` skips partition discovery and uses a single inventory snapshot.
- If no partitions are found, the script probes recent dates using `probe_days` and `probe_time`.
- Output files are CSVs and may be large depending on the bucket size.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
