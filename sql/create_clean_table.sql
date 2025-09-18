CREATE EXTERNAL TABLE IF NOT EXISTS clean_logs (
  timestamp_utc timestamp,
  host string,
  method string,
  path string,
  status int,
  bytes int,
  ua string,
  latency_ms int,
  hr string
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/logs/clean/';
