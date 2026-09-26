"""
Stage 4 - Gold (weekly segment balances)

Rolls daily segment balances up to one row per segment per Saturday-Friday
week: average daily balance in INR crore and the peak account count. Only
complete 7-day weeks are kept, so a mid-week as-of date never produces a
partial week.

Writes via MERGE INTO once the table exists. Only rows whose values changed
are updated, so each run's Iceberg snapshot records exactly what a monthly
load added or restated. That is what the time-travel demo relies on
(FOR SYSTEM_VERSION AS OF in Impala). WHEN NOT MATCHED BY SOURCE DELETE keeps
the table equal to the source, e.g. when re-running an earlier as-of date.

Reads:
  <prefix>_silver.casa_daily_balance
Writes:
  <prefix>_gold.casa_weekly_balance

Usage:
  spark-submit build_gold_weekly.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_casa_alb"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def weekly(daily):
    return (daily
            .withColumn("week_end_date", F.next_day(F.date_sub("bal_date", 1), "Fri"))
            .groupBy("segment_id", "irrbb_category", "week_end_date")
            .agg((F.avg("balance_inr") / 1e7).alias("balance_inr_cr"),
                 F.max("account_count").cast("bigint").alias("account_count"),
                 F.count("*").alias("days"))
            .where(F.col("days") == 7)
            .drop("days")
            .withColumn("balance_inr_cr", F.round("balance_inr_cr", 4))
            .withColumn("loaded_at", F.current_timestamp()))


def bootstrap_or_merge(spark: SparkSession, target: str, source_view: str) -> None:
    if spark.catalog.tableExists(target):
        spark.sql(f"""
            MERGE INTO {target} t
            USING {source_view} s
            ON t.segment_id = s.segment_id AND t.week_end_date = s.week_end_date
            WHEN MATCHED AND (t.balance_inr_cr <> s.balance_inr_cr
                              OR t.account_count <> s.account_count
                              OR t.irrbb_category <> s.irrbb_category) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
        logger.info("Merged into %s (new snapshot)", target)
    else:
        spark.sql(f"""
            CREATE TABLE {target}
            USING iceberg
            PARTITIONED BY (years(week_end_date))
            TBLPROPERTIES ('format-version'='2')
            AS SELECT * FROM {source_view}
        """)
        logger.info("Bootstrapped %s (first run)", target)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("casa-build-gold-weekly").getOrCreate()
    try:
        gold_db = f"{args.db_prefix}_gold"
        target = f"{gold_db}.casa_weekly_balance"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")
        weekly(spark.table(f"{args.db_prefix}_silver.casa_daily_balance")).createOrReplaceTempView("casa_weekly_src")
        bootstrap_or_merge(spark, target, "casa_weekly_src")

        summary = (spark.table(target).groupBy("segment_id", "irrbb_category")
                   .agg(F.count("*").alias("weeks"), F.min("week_end_date").alias("first_week"),
                        F.max("week_end_date").alias("last_week"),
                        F.round(F.max_by("balance_inr_cr", "week_end_date"), 1).alias("latest_inr_cr"))
                   .orderBy("segment_id"))
        summary.show(50, truncate=False)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
