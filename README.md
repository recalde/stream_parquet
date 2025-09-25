# DDB → Parquet Lambda (with Local Tester)

## Files
- `lambda_handler.py`: Lambda entrypoint; triggered by DynamoDB Streams.
- `parquet_helpers.py`: Utilities for typing, timezone, S3 keys, and stream parsing.
- `schema_cache.py`: Loads and caches `schema.csv` packaged with the Lambda.
- `schema.csv`: Put your real schema here (headers: `db_table_name,db_field_name,df_type`).
- `test.py`: Local tester that scans real DynamoDB data and invokes the handler.

## Environment
- **Lambda**: `BUCKET_NAME`, `BASE_PREFIX` (required), `TIMEZONE` (default `America/New_York`),
  `DECIMAL_PRECISION`, `DECIMAL_SCALE` (for `decimal`).
- **Test**: `TABLE_PREFIX` (defaults to `dev_tables`). Also ensure the Lambda env vars are set locally.

## Run the local tester
```bash
export BUCKET_NAME=your-bucket
export BASE_PREFIX=your/base/prefix
export TABLE_PREFIX=dev_tables
python3 test.py
```

## Deploy
Zip the folder and upload as Lambda code (handler: `lambda_handler.lambda_handler`). Attach the
AWS SDK for pandas (awswrangler) layer compatible with your Python runtime.
