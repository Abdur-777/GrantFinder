# refresh.py — nightly job: scrape VIC sources, merge, expire past-deadline rows
from datetime import date
import pandas as pd

from sources_vic import VIC_SOURCES
from app import scrape_sources, merge_into_csv, load_df, save_df, normalize_date_str

def expire_old_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "deadline" not in df.columns:
        return df
    df = df.copy()
    df["deadline_iso"] = df["deadline"].apply(normalize_date_str)
    df["_deadline_dt"] = pd.to_datetime(df["deadline_iso"], errors="coerce")
    today = pd.to_datetime(date.today())
    keep_mask = df["_deadline_dt"].isna() | (df["_deadline_dt"] >= today)
    return df[keep_mask].drop(columns=["_deadline_dt"], errors="ignore")

if __name__ == "__main__":
    rows = scrape_sources(VIC_SOURCES)
    added = merge_into_csv(rows)
    print(f"Scraped & merged. Added {added} new rows.")
    df = load_df()
    before = len(df)
    df2 = expire_old_rows(df)
    if len(df2) != before:
        save_df(df2)
        print(f"Expired {before - len(df2)} rows (past deadline).")
    print(f"Done. Total rows: {len(df2)}")
