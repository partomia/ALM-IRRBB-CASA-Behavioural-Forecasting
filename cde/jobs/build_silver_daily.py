"""
Stage 3 - Silver (daily segment balances)

Cleans the CBS extract and aggregates it to one row per segment per day:
  * standardise product / cust_type codes (trim, upper case)
  * keep the latest record per account and day (drops re-sent duplicates)
  * drop null and negative balances (the bronze gate caps these at 0.1%)
  * map to segment and IRRBB category, then sum balances and count accounts

Reads:
  <prefix>_bronze.cbs_daily_balance
  <prefix>_ref.casa_segment_map
Writes:
  <prefix>_silver.casa_daily_balance   (drop + recreate every run)

Usage:
  spark-submit build_silver_daily.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_casa_alb"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def build(spark: SparkSession, db_prefix: str):
    bronze = spark.table(f"{db_prefix}_bronze.cbs_daily_balance")
    seg_map = spark.table(f"{db_prefix}_ref.casa_segment_map").select(
        "product", "cust_type", "segment_id", "irrbb_category")

    latest = Window.partitionBy("account_id", "bal_date").orderBy(F.col("ingested_at").desc())
    clean = (bronze
             .withColumn("product", F.upper(F.trim("product")))
             .withColumn("cust_type", F.upper(F.trim("cust_type")))
             .withColumn("_rn", F.row_number().over(latest))
             .where((F.col("_rn") == 1) & F.col("balance").isNotNull() & (F.col("balance") >= 0))
             .drop("_rn"))

    return (clean.join(seg_map, ["product", "cust_type"])
            .groupBy("segment_id", "irrbb_category", "bal_date")
            .agg(F.sum("balance").alias("balance_inr"),
                 F.countDistinct("account_id").alias("account_count"))
            .withColumn("processed_at", F.current_timestamp()))


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("casa-build-silver-daily").getOrCreate()
    try:
        silver_db = f"{args.db_prefix}_silver"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
        (build(spark, args.db_prefix)
         .writeTo(f"{silver_db}.casa_daily_balance").using("iceberg")
         .tableProperty("format-version", "2")
         .partitionedBy(F.years("bal_date"))
         .createOrReplace())
        out = spark.table(f"{silver_db}.casa_daily_balance")
        logger.info("Wrote %d segment-day rows to %s.casa_daily_balance", out.count(), silver_db)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
