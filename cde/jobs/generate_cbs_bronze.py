"""
Stage 1 - Generate (bronze)

Synthesises a core banking system (CBS) extract of end-of-day CASA balances,
one row per account per calendar day, and lands it raw in bronze together
with the product/customer-type -> segment reference map.

In production this job would read the CBS extract (S3 drop / JDBC) instead.

History is prefix-stable: the "world" is generated from a fixed seed starting
at HISTORY_START, and --as-of only truncates it. Running with a later as-of
adds new days without changing any earlier balance, so month-end runs behave
like real monthly loads (see cde/scripts/backfill_drill.sh).

Segment behaviour built in (per segment parameters in SEGMENTS):
  * trend growth
  * salary cycle: balances peak after salary credit, drain towards month end
  * quarter-end and March year-end spikes (current accounts window dressing)
  * festival season spend (Oct-Nov)
  * 2022-23 RBI rate-hike cycle: rate-sensitive balances migrate to term
    deposits, partially return after the 2025 cuts
  * mean-reverting random shocks

Writes:
  <prefix>_bronze.cbs_daily_balance   (drop + recreate every run)
  <prefix>_ref.casa_segment_map       (drop + recreate every run)

Usage:
  spark-submit generate_cbs_bronze.py [--as-of YYYY-MM-DD] [--db-prefix P]
                                      [--accounts-per-segment N] [--seed S]
"""

from __future__ import annotations

import argparse
import logging
import math
import random
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_casa_alb"
HISTORY_START = date(2021, 4, 3)  # a Saturday, so the first Sat-Fri week is complete
WORLD_END = date(2030, 12, 31)    # account open/close dates span this, independent of --as-of
DEFAULT_SEED = 20210403
DEFAULT_ACCOUNTS = 120
DUPLICATE_RATE = 0.0002  # re-sent CBS records, removed in silver

# base_cr: balance at HISTORY_START in INR crore; growth: annual log growth;
# salary: month-cycle amplitude; qe/ye: quarter-end / March spike;
# festival: Oct-Nov effect; rate_beta: share lost at the peak of the hike cycle;
# vol: weekly shock volatility.
SEGMENTS = [
    dict(segment_id="SA_RETAIL_URBAN", product="SA", cust_type="RETAIL_URBAN",
         irrbb_category="retail_transactional", segment_name="Savings - retail urban",
         base_cr=18000, growth=0.070, salary=0.030, qe=0.000, ye=0.000, festival=-0.020, rate_beta=0.060, vol=0.006),
    dict(segment_id="SA_RETAIL_SEMIURBAN", product="SA", cust_type="RETAIL_SEMIURBAN",
         irrbb_category="retail_transactional", segment_name="Savings - retail semi-urban",
         base_cr=11000, growth=0.080, salary=0.020, qe=0.000, ye=0.000, festival=-0.015, rate_beta=0.040, vol=0.006),
    dict(segment_id="SA_RETAIL_RURAL", product="SA", cust_type="RETAIL_RURAL",
         irrbb_category="retail_transactional", segment_name="Savings - retail rural",
         base_cr=9000, growth=0.090, salary=0.010, qe=0.000, ye=0.000, festival=0.030, rate_beta=0.020, vol=0.007),
    dict(segment_id="SA_SALARY", product="SA", cust_type="SALARIED",
         irrbb_category="retail_transactional", segment_name="Savings - salary accounts",
         base_cr=6500, growth=0.060, salary=0.100, qe=0.000, ye=0.000, festival=-0.025, rate_beta=0.050, vol=0.006),
    dict(segment_id="SA_FINANCIAL_INCLUSION", product="SA", cust_type="BASIC_SAVINGS",
         irrbb_category="retail_transactional", segment_name="Savings - basic / financial inclusion",
         base_cr=4200, growth=0.120, salary=0.010, qe=0.000, ye=0.000, festival=0.000, rate_beta=0.000, vol=0.003),
    dict(segment_id="SA_SENIOR_CITIZEN", product="SA", cust_type="SENIOR_CITIZEN",
         irrbb_category="retail_non_transactional", segment_name="Savings - senior citizens",
         base_cr=7800, growth=0.050, salary=0.010, qe=0.000, ye=0.000, festival=0.000, rate_beta=0.100, vol=0.005),
    dict(segment_id="SA_HNI", product="SA", cust_type="HNI",
         irrbb_category="retail_non_transactional", segment_name="Savings - high net worth",
         base_cr=5200, growth=0.050, salary=0.000, qe=0.010, ye=0.000, festival=0.000, rate_beta=0.180, vol=0.012),
    dict(segment_id="SA_NRI", product="SA", cust_type="NRI",
         irrbb_category="retail_non_transactional", segment_name="Savings - NRE / NRO",
         base_cr=3600, growth=0.060, salary=0.000, qe=0.000, ye=0.000, festival=0.020, rate_beta=0.080, vol=0.010),
    dict(segment_id="CA_MICRO_BUSINESS", product="CA", cust_type="MICRO_BUSINESS",
         irrbb_category="retail_transactional", segment_name="Current - micro businesses",
         base_cr=3000, growth=0.060, salary=-0.040, qe=0.030, ye=0.030, festival=0.030, rate_beta=0.030, vol=0.010),
    dict(segment_id="CA_SME", product="CA", cust_type="SME",
         irrbb_category="wholesale", segment_name="Current - SME",
         base_cr=5500, growth=0.050, salary=-0.060, qe=0.050, ye=0.050, festival=0.020, rate_beta=0.070, vol=0.014),
    dict(segment_id="CA_CORPORATE", product="CA", cust_type="CORPORATE",
         irrbb_category="wholesale", segment_name="Current - large corporates",
         base_cr=9500, growth=0.040, salary=0.000, qe=0.120, ye=0.150, festival=0.000, rate_beta=0.100, vol=0.020),
    dict(segment_id="CA_GOVT_PSU", product="CA", cust_type="GOVT_PSU",
         irrbb_category="wholesale", segment_name="Current - government and PSU",
         base_cr=6000, growth=0.030, salary=0.000, qe=0.040, ye=0.200, festival=0.000, rate_beta=0.000, vol=0.008),
    dict(segment_id="CA_TRUST_INSTITUTION", product="CA", cust_type="TRUST_INSTITUTION",
         irrbb_category="wholesale", segment_name="Current - trusts and institutions",
         base_cr=2000, growth=0.040, salary=0.000, qe=0.020, ye=0.040, festival=0.000, rate_beta=0.050, vol=0.010),
    dict(segment_id="CA_BANKS_FI", product="CA", cust_type="BANK_FI",
         irrbb_category="wholesale", segment_name="Current - banks and financial institutions",
         base_cr=1500, growth=0.020, salary=0.000, qe=0.080, ye=0.080, festival=0.000, rate_beta=0.120, vol=0.030),
]

# Share of rate-sensitive balance that has left, by date (piecewise linear).
# Repo 4.0% -> 6.5% between May 2022 and Feb 2023; cuts from Feb 2025.
RATE_CYCLE = [
    (date(2022, 5, 1), 0.0),
    (date(2023, 3, 31), 1.0),
    (date(2024, 12, 31), 0.7),
    (date(2025, 9, 30), 0.4),
]

BRONZE_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("bal_date", DateType(), False),
    StructField("balance", DoubleType(), True),
    StructField("product", StringType(), True),
    StructField("cust_type", StringType(), True),
])


def rate_cycle_outflow(d: date) -> float:
    if d <= RATE_CYCLE[0][0]:
        return 0.0
    for (d0, v0), (d1, v1) in zip(RATE_CYCLE, RATE_CYCLE[1:]):
        if d <= d1:
            return v0 + (v1 - v0) * (d - d0).days / (d1 - d0).days
    return RATE_CYCLE[-1][1]


def _days_in_month(d: date) -> int:
    nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return (nxt - timedelta(days=1)).day


def salary_shape(d: date) -> float:
    """+1 right after month start (salary credited), -1 at month end."""
    dim = _days_in_month(d)
    return 1.0 - 2.0 * (d.day - 1) / (dim - 1)


def quarter_end_shape(d: date) -> float:
    return 1.0 if d.month in (3, 6, 9, 12) and _days_in_month(d) - d.day < 5 else 0.0


def year_end_shape(d: date) -> float:
    return 1.0 if d.month == 3 and d.day >= 22 else 0.0


def festival_shape(d: date) -> float:
    """Smooth bump over roughly 10 Oct - 20 Nov, peaking around Diwali."""
    start = date(d.year, 10, 10)
    x = (d - start).days / 41.0
    return math.sin(math.pi * x) if 0.0 <= x <= 1.0 else 0.0


def simulate_segment(seg: dict, end: date, seed: int) -> list[tuple[date, float]]:
    """Daily segment total in INR crore from HISTORY_START to `end` inclusive.

    Draws happen in date order from a per-segment RNG, so the path up to any
    date is identical whatever `end` is.
    """
    rng = random.Random(f"{seed}:{seg['segment_id']}")
    daily_sd = seg["vol"] / math.sqrt(7)
    shock = 0.0
    out = []
    d = HISTORY_START
    while d <= end:
        t = (d - HISTORY_START).days / 365.25
        shock = 0.995 * shock + rng.gauss(0.0, daily_sd)
        level = seg["base_cr"] * math.exp(seg["growth"] * t + shock)
        level *= 1.0 - seg["rate_beta"] * rate_cycle_outflow(d)
        level *= 1.0 + seg["salary"] * salary_shape(d)
        level *= 1.0 + seg["qe"] * quarter_end_shape(d) + seg["ye"] * year_end_shape(d)
        level *= 1.0 + seg["festival"] * festival_shape(d)
        level *= 1.0 + rng.gauss(0.0, 0.002)
        out.append((d, level))
        d += timedelta(days=1)
    return out


def generate_accounts(seg: dict, n: int, seed: int) -> list[dict]:
    """Synthetic accounts for one segment. Each represents a pool of real
    accounts; weights are heavy-tailed, more so for current accounts. Some
    accounts open during the history and a few close, so account counts move."""
    rng = random.Random(f"{seed}:{seg['segment_id']}:accounts")
    span = (WORLD_END - HISTORY_START).days
    rows = []
    for i in range(n):
        if seg["product"] == "CA":
            weight = rng.paretovariate(1.6)
        else:
            weight = rng.lognormvariate(0.0, 0.8)
        opened = HISTORY_START if rng.random() < 0.7 else HISTORY_START + timedelta(days=rng.randint(0, span))
        closed = None
        if rng.random() < 0.08:
            closed = opened + timedelta(days=rng.randint(90, max(91, span)))
        rows.append(dict(
            account_id=f"{seg['product']}-{seg['cust_type']}-{i:05d}",
            segment_id=seg["segment_id"],
            weight=float(weight),
            open_date=opened,
            close_date=closed,
        ))
    return rows


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--as-of", default=None, help="last balance date, YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--accounts-per-segment", type=int, default=DEFAULT_ACCOUNTS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args, _ = p.parse_known_args(argv)
    return args


def resolve_as_of(value: str | None) -> date:
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    as_of = resolve_as_of(args.as_of)
    bronze_db, ref_db = f"{args.db_prefix}_bronze", f"{args.db_prefix}_ref"
    logger.info("Generating CBS balances %s -> %s, %d accounts/segment, seed %d",
                HISTORY_START, as_of, args.accounts_per_segment, args.seed)

    seg_daily, accounts = [], []
    for seg in SEGMENTS:
        seg_daily += [(seg["segment_id"], d, v * 1e7) for d, v in simulate_segment(seg, as_of, args.seed)]
        accounts += generate_accounts(seg, args.accounts_per_segment, args.seed)

    seg_df = spark.createDataFrame(seg_daily, "segment_id string, bal_date date, seg_total_inr double")
    acc_df = spark.createDataFrame(
        accounts, "account_id string, segment_id string, weight double, open_date date, close_date date")
    map_df = spark.createDataFrame(
        [(s["product"], s["cust_type"], s["segment_id"], s["irrbb_category"], s["segment_name"]) for s in SEGMENTS],
        "product string, cust_type string, segment_id string, irrbb_category string, segment_name string")

    active = (seg_df.join(acc_df, "segment_id")
              .where((F.col("bal_date") >= F.col("open_date"))
                     & (F.col("close_date").isNull() | (F.col("bal_date") < F.col("close_date")))))
    w = Window.partitionBy("segment_id", "bal_date")
    # Deterministic per-row noise (F.rand depends on partitioning, so avoid it).
    noise = (F.abs(F.xxhash64("account_id", "bal_date", F.lit(args.seed))) % 10000) / 10000.0 - 0.5
    bronze = (active
              .withColumn("share", F.col("weight") / F.sum("weight").over(w))
              .withColumn("balance", F.round(F.col("seg_total_inr") * F.col("share") * (1 + 0.04 * noise), 2))
              .join(map_df.select("segment_id", "product", "cust_type"), "segment_id")
              .select(*[f.name for f in BRONZE_SCHEMA.fields]))

    dupes = bronze.where((F.abs(F.xxhash64("account_id", "bal_date")) % 1_000_000) < DUPLICATE_RATE * 1_000_000)
    bronze = (bronze.unionByName(dupes)
              .withColumn("currency", F.lit("INR"))
              .withColumn("source_system", F.lit("CBS"))
              .withColumn("batch_as_of", F.lit(as_of).cast("date"))
              .withColumn("ingested_at", F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp")))

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ref_db}")

    (map_df.withColumn("ingested_at", F.current_timestamp())
     .writeTo(f"{ref_db}.casa_segment_map").using("iceberg")
     .tableProperty("format-version", "2").createOrReplace())

    (bronze.writeTo(f"{bronze_db}.cbs_daily_balance").using("iceberg")
     .tableProperty("format-version", "2")
     .partitionedBy(F.years("bal_date"))
     .createOrReplace())

    n = spark.table(f"{bronze_db}.cbs_daily_balance").count()
    logger.info("Wrote %d rows to %s.cbs_daily_balance (%d segments, as of %s)", n, bronze_db, len(SEGMENTS), as_of)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("casa-generate-cbs-bronze").getOrCreate()
    try:
        run(spark, args)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
