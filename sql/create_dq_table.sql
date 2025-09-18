CREATE EXTERNAL TABLE IF NOT EXISTS dq_parsing_accuracy (
  hr string,
  total bigint,
  parsed bigint,
  accuracy double,
  job_run_ts_utc string
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://__BUCKET__/dq/parsing_accuracy/';
