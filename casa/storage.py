"""Read gold tables and write monthly outputs, on CDW Impala or local parquet.

  impala   CDW Impala virtual warehouse over HTTPS (impyla). Outputs are Iceberg
           v2 tables; a rerun for an as_of_date does DELETE + INSERT for that
           date, each committing an Iceberg snapshot.
  parquet  one file per table under storage.parquet_dir, for a laptop, Docker
           or an offline demo. Same replace-by-as_of_date semantics.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import pandas as pd

from casa.config import ROOT, settings, table
from casa.schema import OUTPUT_TABLES

logger = logging.getLogger(__name__)


def get_storage(backend: str | None = None):
    backend = backend or settings()["storage"]["backend"]
    if backend == "impala":
        return ImpalaStorage(settings()["impala"])
    if backend == "parquet":
        return ParquetStorage(Path(settings()["storage"]["parquet_dir"]))
    raise ValueError(f"unknown storage backend {backend!r} (impala | parquet)")


def _cast(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df.copy()
    for col, typ in columns:
        if col not in df.columns:
            continue
        if typ == "DATE":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif typ == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col])
    return df


class ParquetStorage:
    name = "parquet"

    def __init__(self, directory: Path):
        self.dir = directory if directory.is_absolute() else ROOT / directory

    def _path(self, full_name: str) -> Path:
        return self.dir / f"{full_name.split('.')[-1]}.parquet"

    def exists(self, key: str) -> bool:
        return self._path(table(key)).exists()

    def read(self, key: str, where_as_of: date | None = None) -> pd.DataFrame:
        path = self._path(table(key))
        if not path.exists():
            raise FileNotFoundError(f"{path} not found - run scripts/run_cde_local.py all, or copy an export")
        df = pd.read_parquet(path)
        if where_as_of is not None and "as_of_date" in df.columns:
            df = df[pd.to_datetime(df["as_of_date"]).dt.date == where_as_of]
        return df

    def replace_as_of(self, key: str, df: pd.DataFrame, as_of: date) -> None:
        cols = OUTPUT_TABLES[key]
        df = _cast(df[[c for c, _ in cols]], cols)
        path = self._path(table(key))
        if path.exists():
            old = pd.read_parquet(path)
            old = old[pd.to_datetime(old["as_of_date"]).dt.date != as_of]
            df = pd.concat([_cast(old, cols), df], ignore_index=True)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info("wrote %d rows for %s to %s", (df["as_of_date"] == as_of).sum(), as_of, path)

    def snapshot_id(self, key: str) -> str | None:
        return None


def _sql_literal(value, typ: str) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return "NULL"
    if typ in ("DOUBLE", "INT", "BIGINT"):
        return repr(float(value)) if typ == "DOUBLE" else str(int(value))
    if typ == "BOOLEAN":
        return "true" if bool(value) else "false"
    if typ == "DATE":
        return f"DATE '{pd.Timestamp(value).date().isoformat()}'"
    if typ == "TIMESTAMP":
        return f"CAST('{pd.Timestamp(value).strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ImpalaStorage:
    name = "impala"
    INSERT_CHUNK = 500

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._conn = None

    def _connect(self):
        if self._conn is None:
            from impala.dbapi import connect

            c = self.cfg
            if not c["host"]:
                raise RuntimeError("CASA_IMPALA_HOST is not set (CDW Impala virtual warehouse host)")
            kwargs = dict(host=c["host"], port=int(c["port"]), use_ssl=c["use_ssl"],
                          use_http_transport=c["use_http_transport"], http_path=c["http_path"],
                          auth_mechanism=c["auth_mechanism"])
            if c["auth_mechanism"].upper() == "GSSAPI":
                kwargs["kerberos_service_name"] = c["kerberos_service_name"]
            if c.get("user"):
                kwargs["user"] = c["user"]
            if c.get("password"):
                kwargs["password"] = c["password"]
            self._conn = connect(**kwargs)
        return self._conn

    def query(self, sql: str) -> pd.DataFrame:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
            if cur.description is None:
                return pd.DataFrame()
            cols = [d[0].split(".")[-1] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            cur.close()

    def execute(self, sql: str) -> None:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def exists(self, key: str) -> bool:
        db, name = table(key).split(".")
        return not self.query(f"SHOW TABLES IN {db} LIKE '{name}'").empty

    def read(self, key: str, where_as_of: date | None = None) -> pd.DataFrame:
        sql = f"SELECT * FROM {table(key)}"
        if where_as_of is not None:
            sql += f" WHERE as_of_date = DATE '{where_as_of.isoformat()}'"
        return self.query(sql)

    def ensure_table(self, key: str) -> None:
        full = table(key)
        cols = ", ".join(f"{c} {t}" for c, t in OUTPUT_TABLES[key])
        self.execute(f"CREATE DATABASE IF NOT EXISTS {full.split('.')[0]}")
        self.execute(f"CREATE TABLE IF NOT EXISTS {full} ({cols}) PARTITIONED BY SPEC (as_of_date) "
                     f"STORED AS ICEBERG TBLPROPERTIES ('format-version'='2')")

    def replace_as_of(self, key: str, df: pd.DataFrame, as_of: date) -> None:
        cols = OUTPUT_TABLES[key]
        self.ensure_table(key)
        full = table(key)
        self.execute(f"DELETE FROM {full} WHERE as_of_date = DATE '{as_of.isoformat()}'")
        records = df[[c for c, _ in cols]].to_dict("records")
        for i in range(0, len(records), self.INSERT_CHUNK):
            values = ",\n".join(
                "(" + ", ".join(_sql_literal(r[c], t) for c, t in cols) + ")"
                for r in records[i:i + self.INSERT_CHUNK])
            self.execute(f"INSERT INTO {full} ({', '.join(c for c, _ in cols)}) VALUES {values}")
        logger.info("wrote %d rows for %s to %s", len(records), as_of, full)

    def snapshot_id(self, key: str) -> str | None:
        """Latest Iceberg snapshot of a table, recorded for lineage."""
        try:
            hist = self.query(f"DESCRIBE HISTORY {table(key)}")
            return str(hist.iloc[-1]["snapshot_id"]) if not hist.empty else None
        except Exception as e:  # lineage is best-effort, never fail the run on it
            logger.warning("could not read snapshot history for %s: %s", key, e)
            return None
