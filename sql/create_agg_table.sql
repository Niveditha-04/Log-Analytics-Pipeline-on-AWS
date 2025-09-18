CREATE EXTERNAL TABLE IF NOT EXISTS agg_logs (
  bucket_ts string,
  total bigint,
  error_count bigint,
  error_rate double,
  mean_err double,
  std_err double,
  zscore double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/logs/agg/';
