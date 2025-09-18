import sys
import re
import json
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

# ----------------------------
# Job args (passed as --KEY VALUE)
# ----------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET",
    "RAW_PREFIX",
    "CLEAN_PREFIX",
    "REJECT_PREFIX",
    "DQ_PREFIX",
])

BUCKET = args["BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].rstrip("/") + "/"
CLEAN_PREFIX = args["CLEAN_PREFIX"].rstrip("/") + "/"
REJECT_PREFIX = args["REJECT_PREFIX"].rstrip("/") + "/"
DQ_PREFIX = args["DQ_PREFIX"].rstrip("/") + "/"

RAW_PATH = f"s3://{BUCKET}/{RAW_PREFIX}"

spark = (SparkSession.builder
         .appName(args["JOB_NAME"])
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# 1) Read raw lines with file path
# ----------------------------
df_raw = spark.read.text(RAW_PATH).withColumn("input_path", F.input_file_name())
# Expect partitioned path .../source=.../dt=YYYY-MM-DD/hr=HH/filename
dt_re = r".*[\/]dt=([0-9]{4}-[0-9]{2}-[0-9]{2})[\/]"
hr_re = r".*[\/]hr=([0-9]{2})[\/]"

df_raw = (df_raw
          .withColumn("dt", F.regexp_extract("input_path", dt_re, 1))
          .withColumn("hr", F.regexp_extract("input_path", hr_re, 1))
          .withColumn("line", F.col("value"))
          .drop("value"))

# ----------------------------
# 2) Detect JSON vs CLF (combined apache) per line
#    We'll try JSON first; if that fails, fallback to CLF regex.
# ----------------------------
json_schema = T.StructType([
    T.StructField("ts", T.StringType(), True),
    T.StructField("host", T.StringType(), True),
    T.StructField("method", T.StringType(), True),
    T.StructField("path", T.StringType(), True),
    T.StructField("status", T.IntegerType(), True),
    T.StructField("bytes", T.IntegerType(), True),
    T.StructField("ua", T.StringType(), True),
    T.StructField("latency_ms", T.IntegerType(), True),
    T.StructField("request_id", T.StringType(), True),
])

@F.udf(returnType=json_schema)
def try_json(line):
    try:
        if line and line.strip().startswith("{"):
            return json.loads(line)
    except Exception:
        return None
    return None

# Combined Apache (we generated this shape)
# host - user [dd/MMM/yyyy:HH:mm:ss +0000] "METHOD PATH HTTP/1.1" status bytes "-" "UA" latency_ms
clf_pat = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+(?P<user>\S+)\s+\[(?P<time>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<path>\S+)\s+\S+"\s+'
    r'(?P<status>\d{3})\s+(?P<bytes>\d+)\s+"[^"]*"\s+"(?P<ua>[^"]+)"\s+(?P<latency_ms>\d+)$'
)

clf_schema = T.StructType([
    T.StructField("ts", T.StringType(), True),
    T.StructField("host", T.StringType(), True),
    T.StructField("method", T.StringType(), True),
    T.StructField("path", T.StringType(), True),
    T.StructField("status", T.IntegerType(), True),
    T.StructField("bytes", T.IntegerType(), True),
    T.StructField("ua", T.StringType(), True),
    T.StructField("latency_ms", T.IntegerType(), True),
])

def parse_clf_line(line):
    m = clf_pat.match(line or "")
    if not m:
        return None
    # Convert CLF time like 17/Sep/2025:22:01:59 +0000 to ISO string
    t_raw = m.group("time")
    # Keep original; timestamp conversion happens later
    return {
        "ts": t_raw,
        "host": m.group("host"),
        "method": m.group("method"),
        "path": m.group("path"),
        "status": int(m.group("status")),
        "bytes": int(m.group("bytes")),
        "ua": m.group("ua"),
        "latency_ms": int(m.group("latency_ms")),
    }

@F.udf(returnType=clf_schema)
def try_clf(line):
    try:
        return parse_clf_line(line)
    except Exception:
        return None

df_parsed = (df_raw
    .withColumn("json_obj", try_json(F.col("line")))
    .withColumn("clf_obj", try_clf(F.col("line")))
    .withColumn("which", F.when(F.col("json_obj").isNotNull(), F.lit("json"))
                       .when(F.col("clf_obj").isNotNull(), F.lit("clf"))
                       .otherwise(F.lit("unknown")))
)

# Flatten to columns from whichever parsed
def pick(colname):
    return F.when(F.col("which")=="json", F.col(f"json_obj.{colname}")) \
            .when(F.col("which")=="clf",  F.col(f"clf_obj.{colname}")) \
            .otherwise(F.lit(None).cast("string"))

df_flat = (df_parsed
    .withColumn("ts_raw", pick("ts"))
    .withColumn("host", pick("host"))
    .withColumn("method", pick("method"))
    .withColumn("path", pick("path"))
    .withColumn("status", pick("status").cast("int"))
    .withColumn("bytes", pick("bytes").cast("int"))
    .withColumn("ua", pick("ua"))
    .withColumn("latency_ms", pick("latency_ms").cast("int"))
)

# Convert ts_raw to a proper timestamp in UTC
# JSON: ISO strings; CLF: dd/MMM/yyyy:HH:mm:ss Z
df_flat = df_flat.withColumn(
    "timestamp_utc",
    F.when(
        (F.col("which")=="json") & F.col("ts_raw").isNotNull(),
        F.to_timestamp("ts_raw")
    ).otherwise(
        F.to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z")
    )
)

# ----------------------------
# 3) Split good vs rejected & compute accuracy
# ----------------------------
required_ok = (
    F.col("timestamp_utc").isNotNull() &
    F.col("status").isNotNull() &
    F.col("method").isNotNull() &
    F.col("path").isNotNull()
)

df_good = (df_flat
    .where(required_ok)
    .select(
        "timestamp_utc","host","method","path","status","bytes","ua","latency_ms",
        "dt","hr"
    )
)

df_bad = (df_flat
    .where(~required_ok)
    .select("line","dt","hr","which","ts_raw","host","method","path","status","bytes","ua","latency_ms")
    .withColumn("reject_reason", F.lit("required_field_missing_or_ts_parse_failed"))
)

# counts & accuracy
agg = (df_flat.groupBy("dt","hr")
       .agg(F.count("*").alias("total"),
            F.sum(F.when(required_ok,1).otherwise(0)).alias("parsed"))
      ) \
      .withColumn("accuracy", F.when(F.col("total")>0, F.col("parsed")/F.col("total")).otherwise(F.lit(0.0))) \
      .withColumn("job_run_ts_utc", F.lit(datetime.now(timezone.utc).isoformat()))

# ----------------------------
# 4) Write outputs
# ----------------------------
# Clean parquet (partitioned)
(df_good.write
 .mode("overwrite")            # dynamic overwrite per partition
 .partitionBy("dt","hr")
 .parquet(f"s3://{BUCKET}/{CLEAN_PREFIX}")
)

# Rejected (JSON) for debugging
(df_bad
 .write
 .mode("overwrite")
 .partitionBy("dt","hr")
 .json(f"s3://{BUCKET}/{REJECT_PREFIX}")
)

# DQ / accuracy
(agg
 .write
 .mode("append")
 .partitionBy("dt")            # keep hr inside payload but partition by dt for easy scans
 .parquet(f"s3://{BUCKET}/{DQ_PREFIX}")
)

print("ETL finished OK")
