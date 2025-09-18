import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F, Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET",
    "CLEAN_PREFIX",
    "AGG_PREFIX"
])

BUCKET = args["BUCKET"]
CLEAN_PREFIX = args["CLEAN_PREFIX"].rstrip("/") + "/"
AGG_PREFIX = args["AGG_PREFIX"].rstrip("/") + "/"

spark = (SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1) Read clean parquet
df = spark.read.parquet(f"s3://{BUCKET}/{CLEAN_PREFIX}")

# 2) 5-minute buckets
df = df.withColumn("minute", F.date_trunc("minute", F.col("timestamp_utc")))
df = df.withColumn("bucket5", (F.floor(F.minute("minute")/5)*5).cast("int"))
df = df.withColumn(
    "bucket_ts",
    F.concat_ws(":",
        F.date_format("minute","yyyy-MM-dd HH"),
        F.lpad(F.col("bucket5"),2,"0"),
        F.lit("00")
    )
)

# 3) Aggregate
agg = (df.groupBy("dt","bucket_ts")
         .agg(
             F.count("*").alias("total"),
             F.sum(F.when(F.col("status") >= 500,1).otherwise(0)).alias("error_count")
         )
         .withColumn("error_rate", F.col("error_count")/F.col("total"))
)

# 4) Simple z-score window on the same day
w = Window.partitionBy("dt").orderBy("bucket_ts").rowsBetween(-20,0)
agg = (agg
       .withColumn("mean_err", F.avg("error_rate").over(w))
       .withColumn("std_err", F.stddev("error_rate").over(w))
       .withColumn("zscore",
           F.when(F.col("std_err")>0,
                  (F.col("error_rate")-F.col("mean_err"))/F.col("std_err"))
            .otherwise(F.lit(0.0))
       ))

# 5) Write parquet partitioned by dt
(agg.write.mode("overwrite")
    .partitionBy("dt")
    .parquet(f"s3://{BUCKET}/{AGG_PREFIX}"))
