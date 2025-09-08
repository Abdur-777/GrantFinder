# app.py
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from supabase import create_client, Client
import yaml

# ---------- Page ----------
st.set_page_config(page_title="GrantFinder (VIC Councils)", page_icon="🗂️", layout="wide")

# ---------- Secrets / Config ----------
def require_secret(key: str, hint: str = "") -> Optional[str]:
    """Return secret if present; otherwise None (we'll show a friendly UI)."""
    return os.getenv(key) or (st.secrets.get(key) if hasattr(st, "secrets") else None)

SUPABASE_URL = require_secret(
    "SUPABASE_URL", "Supabase → Settings → API → Project URL (https://xxxx.supabase.co)"
)
SUPABASE_KEY = require_secret(
    "SUPABASE_SERVICE_KEY", "Supabase → Settings → API → Service role key (NOT anon key)"
)

TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")
INCLUDE_STATEWIDE_IN_EACH = os.getenv("INCLUDE_STATEWIDE_IN_EACH", "1") == "1"
STATEWIDE_SLUG = os.getenv("STATEWIDE_SLUG", "vic")
AUTO_REFRESH_MAX_AGE_MIN = int(os.getenv("AUTO_REFRESH_MAX_AGE_MIN", "60"))

def supabase_client() -> Optional[Client]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Failed to create Supabase client: {e}")
        return None

sb = supabase_client()

# ---------- Tenants ----------
def load_tenants() -> Dict[str, dict]:
    try:
        with open(TENANTS_FILE, "r") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict) and data:
                return data
    except Exception as e:
        st.warning(f"Could not read {TENANTS_FILE}: {e}")
    # Safe fallback (statewide only)
    return {
        STATEWIDE_SLUG: {
            "name": "Victoria — Statewide",
            "sources": [
                "https://www.vic.gov.au/grants",
                "https://business.vic.gov.au/grants-and-programs",
                "https://www.grants.gov.au/GO/list",
            ],
        }
    }

TENANTS = load_tenants()
ALL_SLUGS = list(TENANTS.keys())

def council_name(slug: str) -> str:
    return (TENANTS.get(slug) or {}).get("name") or slug.title()

# ---------- Helpers ----------
def parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Support "Z" and "+00:00"
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_last_run(sb_present: bool) -> Optional[datetime]:
    if not sb_present or not sb:
        return None
    try:
        res = sb.table("meta").select("value").eq("key", "last_run_utc").limit(1).execute()
        rows = getattr(res, "data", []) or []
        if rows:
            return parse_iso_utc(rows[0].get("value"))
    except Exception:
        pass
    return None

def _fetch_one(slug: str) -> pd.DataFrame:
    if not sb:
        return pd.DataFrame()
    try:
        q = (
            sb.table("grants")
            .select("id,title,description,amount,deadline,deadline_iso,link,source,summary,created_at,council_slug")
            .eq("council_slug", slug)
            .order("created_at", desc=True)
            .limit(2000)
            .execute()
        )
        data = getattr(q, "data", []) or []
        return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Error fetching grants for {slug}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_grants_for(slug: str, include_statewide: bool) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    df1 = _fetch_one(slug)
    if not df1.empty:
        frames.append(df1)
    if include_statewide and slug != STATEWIDE_SLUG:
        df2 = _fetch_one(STATEWIDE_SLUG)
        if not df2.empty:
            frames.append(df2)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Normalize datetimes (tz-aware UTC to avoid comparison errors)
    df["_created_dt"] = pd.to_datetime(df.get("created_at"), utc=True, errors="coerce")
    df["_deadline_dt"] = pd.to_datetime(df.get("deadline_iso"), utc=True, errors="coerce")

    now_utc = pd.Timestamp.now(tz="UTC")
    df["Status"] = df["_deadline_dt"].apply(lambda d: "Open" if (pd.isna(d) or d >= now_utc) else "Closed")
    df["New (24h)"] = df["_created_dt"].apply(lambda d: bool(pd.notna(d) and d >= (now_utc - pd.Timedelta(days=1))))

    # Keep only the columns we want, in a fixed order (prevents duplicate-name issues)
    cols = [
        "title", "description", "amount",
        "deadline", "deadline_iso",
        "Status", "New (24h)",
        "link", "source",
        "summary",
        "created_at", "council_slug",
        "_created_dt",  # for sorting, dropped after
    ]
    existing = [c for c in cols if c in df.columns]
    df = df[existing].copy()

    # Friendly names
    rename = {
        "title": "Title",
        "description": "Description",
        "amount": "Amount",
        "deadline": "Deadline (text)",
        "deadline_iso": "Deadline (ISO)",
        "link": "Link",
        "source": "Source",
        "summary": "AI Summary",
        "created_at": "Discovered At (UTC)",
        "council_slug": "Council",
    }
    df.rename(columns=rename, inplace=True)

    # Sort: Open first, then newest discovered
    df["_sort_open"] = (df["Status"] != "Open").astype(int)
    df.sort_values(by=["_sort_open", "_created_dt"], ascending=[True, False], inplace=True)
    df.drop(columns=["_sort_open", "_created_dt"], inplace=True, errors="ignore")

    # Guarantee unique column names (pyarrow/Streamlit quirk)
    seen = {}
    final_cols = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 1
            final_cols.append(c)
        else:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
    df.columns = final_cols

    return df

def do_refresh_now() -> Optional[int]:
    """Run refresh_supabase.py logic if available; return added count or None."""
    try:
        import refresh_supabase as rs  # your scraper/uploader
        total = 0
        for slug, cfg in (rs.TENANTS or {}).items():
            srcs = (cfg or {}).get("sources", []) or []
            rows = rs.scrape_sources(srcs)
            total += rs.upsert_grants(rows, slug)
        rs.set_last_run()
        # clear caches
        get_last_run.clear()
        fetch_grants_for.clear()
        return total
    except ModuleNotFoundError:
        st.info("`refresh_supabase.py` not found in this repo. Skipping local refresh.")
    except Exception as e:
        st.error(f"Refresh failed: {e}")
    return None

# ---------- UI ----------
st.title("GrantFinder — Victoria (Councils)")
st.caption("Aggregated grants from council & state sources (Supabase-backed).")

with st.sidebar:
    st.subheader("Filters")

    if not ALL_SLUGS:
        st.error("No tenants configured. Check your `tenants.yaml` or TENANTS_FILE env var.")
        st.stop()

    # Keep default stable across reloads
    default_slug = "wyndham" if "wyndham" in ALL_SLUGS else ALL_SLUGS[0]
    slug = st.selectbox("Council", options=ALL_SLUGS, index=ALL_SLUGS.index(default_slug),
                        format_func=lambda s: council_name(s))

    include_statewide = st.toggle(
        "Include statewide (VIC)", value=INCLUDE_STATEWIDE_IN_EACH,
        help="Also show statewide grants that may apply across councils."
    )

    query = st.text_input("Search", placeholder="e.g. youth sport, community safety")
    show_status = st.multiselect("Status", ["Open", "Closed"], default=["Open"])
    only_new = st.toggle("Only new in last 24h", value=False)

    st.divider()
    if not sb:
        st.error("Supabase credentials missing.\n\n"
                 "Add ENV vars on Render:\n"
                 "- SUPABASE_URL\n- SUPABASE_SERVICE_KEY")
    else:
        last_run = get_last_run(True)
        st.caption("Data freshness")
        st.write("Last refresh (UTC):", last_run.isoformat() if last_run else "—")
        if st.button("🔄 Refresh now"):
            added = do_refresh_now()
            if added is not None:
                st.success(f"Refreshed. Added/updated about {added} items.")
            else:
                st.warning("No refresh performed (see info above).")

# If Supabase not configured, stop after showing helpful message
if not sb:
    st.stop()

# ---------- Data ----------
with st.spinner("Loading grants…"):
    df = fetch_grants_for(slug, include_statewide)

if df.empty:
    st.warning("No grants found yet. Try **Refresh now** or check your tenant sources.")
    st.stop()

# Apply simple filters
work = df.copy()

if query:
    q = query.strip().lower()
    # Optional columns may be missing — guard with get()
    t = work.get("Title", pd.Series([""] * len(work))).fillna("").str.lower()
    d = work.get("Description", pd.Series([""] * len(work))).fillna("").str.lower()
    s = work.get("AI Summary", pd.Series([""] * len(work))).fillna("").str.lower()
    work = work[t.str.contains(q, na=False) | d.str.contains(q, na=False) | s.str.contains(q, na=False)]

if show_status and "Status" in work.columns:
    work = work[work["Status"].isin(show_status)]

if only_new and "New (24h)" in work.columns:
    work = work[work["New (24h)"] == True]

# KPIs
total = len(work)
open_ct = int((work.get("Status", pd.Series([], dtype=object)) == "Open").sum()) if total else 0
new24 = int((work.get("New (24h)", pd.Series([], dtype=bool)) == True).sum()) if total else 0

k1, k2, k3 = st.columns(3)
k1.metric("Total shown", total)
k2.metric("Open", open_ct)
k3.metric("New in last 24h", new24)

# Export
csv_buf = StringIO()
work.to_csv(csv_buf, index=False)
st.download_button("⬇️ Export CSV", data=csv_buf.getvalue(), file_name=f"grants_{slug}.csv", mime="text/csv")

# Table
col_cfg = {}
if "Link" in work.columns:
    col_cfg["Link"] = st.column_config.LinkColumn("Link")
if "Source" in work.columns:
    col_cfg["Source"] = st.column_config.LinkColumn("Source")
if "New (24h)" in work.columns:
    col_cfg["New (24h)"] = st.column_config.CheckboxColumn("New (24h)")

st.dataframe(work, use_container_width=True, hide_index=True, column_config=col_cfg)

with st.expander("Why this is useful to councils"):
    st.markdown(
        "- Single view across council + statewide sources\n"
        "- Quick search and CSV export\n"
        "- Open/Closed status and “new in last 24h” to triage\n"
        "- Per-council scoping with optional statewide adds"
    )
