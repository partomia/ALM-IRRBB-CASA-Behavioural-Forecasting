"""
Stage 2 - Validate (bronze gate)

Data-quality gate on the CBS extract. Exits non-zero on a hard failure so the
Airflow DAG stops before anything reaches silver/gold. Duplicate re-sent
records are expected and only logged: silver removes them.

Hard checks:
  * table is not empty, no null keys
  * every (product, cust_type) maps to a segment in the reference map
  * null and negative balances below 0.1% of rows
  * no missing calendar days between the first and last balance date

Reads:
  <prefix>_bronze.cbs_daily_balance
  <prefix>_ref.casa_segment_map

Usage:
  spark-submit validate_bronze.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_casa_alb"
MAX_BAD_BALANCE_RATE = 0.001


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def validate(spark: SparkSession, db_prefix: str) -> list[str]:
    bronze = spark.table(f"{db_prefix}_bronze.cbs_daily_balance")
    seg_map = spark.table(f"{db_prefix}_ref.casa_segment_map")
    errors = []

    stats = bronze.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("account_id").isNull() | F.col("bal_date").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.sum(F.when(F.col("balance").isNull(), 1).otherwise(0)).alias("null_bal"),
        F.sum(F.when(F.col("balance") < 0, 1).otherwise(0)).alias("neg_bal"),
        F.min("bal_date").alias("first"),
        F.max("bal_date").alias("last"),
        F.countDistinct("bal_date").alias("days"),
    ).first()
    rows = stats["rows"]
    logger.info("bronze rows=%s dates %s -> %s (%s distinct days)", rows, stats["first"], stats["last"], stats["days"])

    if not rows:
        return ["bronze table is empty"]
    if stats["null_keys"]:
        errors.append(f"{stats['null_keys']} rows with null account_id or bal_date")
    for col, label in (("null_bal", "null"), ("neg_bal", "negative")):
        rate = stats[col] / rows
        if rate > MAX_BAD_BALANCE_RATE:
            errors.append(f"{label} balances in {rate:.3%} of rows (limit {MAX_BAD_BALANCE_RATE:.1%})")
    expected_days = (stats["last"] - stats["first"]).days + 1
    if stats["days"] != expected_days:
        errors.append(f"missing balance dates: {expected_days - stats['days']} of {expected_days} calendar days absent")

    keys = bronze.select(F.upper(F.trim("product")).alias("product"),
                         F.upper(F.trim("cust_type")).alias("cust_type")).distinct()
    unmapped = keys.join(seg_map.select("product", "cust_type"), ["product", "cust_type"], "left_anti").collect()
    if unmapped:
        errors.append("unmapped product/cust_type: " + ", ".join(f"{r.product}/{r.cust_type}" for r in unmapped))

    dupes = bronze.groupBy("account_id", "bal_date").count().where("count > 1").count()
    logger.info("duplicate account/day records: %d (removed in silver)", dupes)
    return errors


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("casa-validate-bronze").getOrCreate()
    try:
        errors = validate(spark, args.db_prefix)
    finally:
        if own:
            spark.stop()
    if errors:
        for e in errors:
            logger.error("VALIDATION FAILED: %s", e)
        sys.exit(1)
    logger.info("Bronze validation passed")


if __name__ == "__main__":
    main()
