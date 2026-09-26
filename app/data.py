"""Cached reads of the gold tables for the Streamlit app."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from casa.storage import get_storage

DATE_COLS = ("as_of_date", "week_end_date", "cutoff_week_end", "last_week_end")


@st.cache_resource
def storage():
    return get_storage()


@st.cache_data(ttl=300, show_spinner="Reading gold tables...")
def read(key: str) -> pd.DataFrame:
    df = storage().read(key)
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date
    return df


def for_run(key: str, as_of) -> pd.DataFrame:
    df = read(key)
    return df[df["as_of_date"] == as_of].copy()


def history(segment_id: str, as_of, weeks: int | None = None) -> pd.DataFrame:
    df = read("casa_weekly_balance")
    df = df[(df["segment_id"] == segment_id) & (df["week_end_date"] <= as_of)].sort_values("week_end_date")
    return df.tail(weeks) if weeks else df
