# app.py
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from supabase import create_client
import yaml

# ----------------- Secrets / Config -----------------
def require_secret(key: str, hint: str = "") -> str:
    val = os.getenv(key) or (st.secrets.get(key) if hasattr(st, "secrets") else None)
    if not val:
        st.error(
            f"Missing required secret `{key}`.\n\n"
            f"Set it in Render → your Web Service → Environment → Add: `{key}`.\n{hint}"
        )
        st.stop()
    return val

SUPABASE_URL = require_secret(
    "SUPABASE_URL",
    "Supabase → Settings → API → Project URL (looks like https://xxxx.supabase.co).",
)
SUPABASE_KEY = require_secret(
    "SUPABASE_SERVICE_KEY",
    "Supabase → Settings → API → Service role key (NOT the anon key).",
)
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")
INCLUDE_STATEWIDE_IN_EACH = os.getenv("INCLUDE_STATEWIDE_IN_EACH", "1") == "1"
STATEWIDE_SLUG = os.getenv("STATEWIDE_SLUG", "vic")
AUTO_REFRESH_MAX_AGE_MIN = int(os.getenv("AUTO_REFRESH_MAX_AGE_MIN", "60"))
CRON_TOKEN = os.getenv("CRON_TOKEN", "")
APP_BASE_URL = os.getenv("APP_BASE_URL", "") or (st.secrets.get("app_base_url", "") if hasattr(st, "secrets") else "")
SUMMARIZE = os.getenv("SUMMARIZE", "0") == "1"  # read-only here; scraper populates summaries

# ----------------- Helpers -----------------
def load_tenants() -> Dict[str, dict]:
    try:
        with open(TENANTS_FILE, "r") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict) and data:
                return data
    except Exception as e:
        st.warning(f"Could not read {TENANTS_FILE}: {e}")
    # Fallback
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

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def parse_utc(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
    except Exception:
        return None

def get_qp(key: str) -> Optional[str]:
    try:
        qp = st.query_params  # Streamlit >= 1.34
        v = qp.get(key)
        if isinstance(v, list):
            return v[0] if v else None
        return v
    except Exception:
        return st.experimental_get_query_params().get(key, [None])[0]

def share_url_for(slug: str) -> str:
    if APP_BASE_URL:
        sep = "&" if "?" in APP_BASE_URL else "?"
        return f"{APP_BASE_URL}{sep}c={slug}"
    return f"/?c={slug}"

def council_options() -> List[str]:
    return list(TENANTS.keys())

def council_name(slug: str) -> str:
    return (TENANTS.get(slug) or {}).get("name") or slug.title()

@st.cache_data(ttl=60)
def get_last_run() -> Optional[datetime]:
    try:
        res = sb.table("meta").select("value").eq("key", "last_run_utc").limit(1).execute()
        rows = getattr(res, "data", []) or []
        if rows:
            return parse_utc(rows[0].get("value"))
    except Exception:
        pass
    return None

@st.cache_data(ttl=60)
def fetch_grants_for(slug: str, include_statewide: bool) -> pd.DataFrame:
    def _fetch(sl: str) -> pd.DataFrame:
        try:
            q = (
                sb.table("grants")
                .select("id,title,description,amount,deadline,deadline_iso,link,source,summary,created_at,council_slug")
                .eq("council_slug", sl)
                .order("created_at", desc=True)
                .limit(2000)
                .execute()
            )
            data = getattr(q, "data", []) or []
            return pd.DataFrame(data)
        except Exception:
            return pd.DataFrame()

    frames = [_fetch(slug)]
    if include_statewide and slug != STATEWIDE_SLUG:
        frames.append(_fetch(STATEWIDE_SLUG))

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Normalize datetimes (tz-aware UTC)
    df["_created_dt"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["_deadline_dt"] = pd.to_datetime(df["deadline_iso"], utc=True, errors="coerce")

    now_utc = pd.Timestamp.now(tz="UTC")
    df["Status"] = df["_deadline_dt"].apply(lambda d: "Open" if (pd.isna(d) or d >= now_utc) else "Closed")
    df["🆕 24h"] = df["_created_dt"].apply(lambda d: bool(pd.notna(d) and d >= (now_utc - pd.Timedelta(days=1))))

    # Clean display set
    cols = [
        "title", "description", "amount",
        "deadline", "deadline_iso",
        "Status", "🆕 24h",
        "link", "source",
        "summary",
        "created_at", "council_slug",
        "_created_dt",
    ]
    existing = [c for c in cols if c in df.columns]
    df = df[existing].copy()

    # Friendly names
    rename_map = {
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
    df.rename(columns=rename_map, inplace=True)

    # Sort: Open first, then newest discovered
    df["_sort_open"] = (df["Status"] != "Open").astype(int)
    df.sort_values(by=["_sort_open", "_created_dt"], ascending=[True, False], inplace=True)
    df.drop(columns=["_sort_open", "_created_dt"], inplace=True, errors="ignore")

    # Ensure unique column names (Streamlit/pyarrow quirk)
    seen = {}
    new_cols = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
    df.columns = new_cols

    return df

def refresh_all_tenants_once() -> int:
    """
    If refresh_supabase.py exists, call its logic to scrape+upsert, then update last_run.
    Returns count (best-effort). If module missing or error, returns -1.
    """
    try:
        import refresh_supabase as rs
        total = 0
        for slug, cfg in (rs.TENANTS or {}).items():
            srcs = (cfg or {}).get("sources", []) or []
            rows = rs.scrape_sources(srcs)
            total += rs.upsert_grants(rows, slug)
        rs.set_last_run()
        # bust caches
        get_last_run.clear()
        fetch_grants_for.clear()
        return total
    except Exception as e:
        print("refresh_all_tenants_once failed:", e)
        return -1

def maybe_handle_cron():
    if not CRON_TOKEN:
        return
    if get_qp("cron") == CRON_TOKEN:
        with st.spinner("Cron refresh in progress…"):
            added = refresh_all_tenants_once()
        st.write(f"OK • refreshed all tenants • added ~{added} items")
        st.stop()

# ----------------- UI -----------------
st.set_page_config(page_title="GrantFinder (VIC Councils)", page_icon="🗂️", layout="wide")
maybe_handle_cron()

st.title("GrantFinder — Victoria (Councils)")
st.caption("Aggregated grants from council & state sources. Supabase-backed. Searchable. Shareable.")

# Sidebar
with st.sidebar:
    st.header("Filters")
    slugs = council_options()
    slug_from_qp = get_qp("c")
    default_slug = slug_from_qp if slug_from_qp in slugs else ("wyndham" if "wyndham" in slugs else slugs[0])
    slug = st.selectbox("Council", options=slugs, index=slugs.index(default_slug))
    st.write(f"Viewing: **{council_name(slug)}**")
    st.write("Share this view:")
    st.code(share_url_for(slug), language="text")

    include_statewide = INCLUDE_STATEWIDE_IN_EACH and st.toggle(
        "Include statewide grants", value=True, help="Also include state-wide VIC grants."
    )

    query = st.text_input("Search (title/description/summary)", placeholder="e.g. youth sport, community safety")
    status_sel = st.multiselect("Status", ["Open", "Closed"], default=["Open"])
    only_new = st.toggle("Only ‘new in last 24h’", value=False)

# Refresh controls
last_run = get_last_run()
needs_refresh = (last_run is None) or ((utcnow() - last_run) > timedelta(minutes=AUTO_REFRESH_MAX_AGE_MIN))

c1, c2, c3, c4 = st.columns(4)
c1.metric("Auto-refresh window (min)", AUTO_REFRESH_MAX_AGE_MIN)
c2.metric("Last refresh (UTC)", last_run.isoformat() if last_run else "—")

with c3:
    if st.button("🔄 Refresh now"):
        with st.spinner("Refreshing all councils…"):
            added = refresh_all_tenants_once()
        if added >= 0:
            st.success(f"Done. Added/upserted ~{added} items.")
        else:
            st.warning("Refresh script not available or failed (see logs).")

with c4:
    if needs_refresh:
        st.info("Data looks stale. Use **Refresh now** or set up a free cron ping (see FAQ below).")

# Data
with st.spinner("Loading grants…"):
    df = fetch_grants_for(slug, include_statewide)

if df.empty:
    st.warning("No grants found yet. Try **Refresh now** or check your tenant sources.")
else:
    # Filters
    work = df.copy()

    if query:
        q = query.strip().lower()
        t = work.get("Title", pd.Series([""] * len(work))).fillna("").str.lower()
        d = work.get("Description", pd.Series([""] * len(work))).fillna("").str.lower()
        s = work.get("AI Summary", pd.Series([""] * len(work))).fillna("").str.lower()
        mask = t.str.contains(q) | d.str.contains(q) | s.str.contains(q)
        work = work[mask]

    if status_sel and "Status" in work.columns:
        work = work[work["Status"].isin(status_sel)]

    if only_new and "🆕 24h" in work.columns:
        work = work[work["🆕 24h"] == True]

    # KPIs
    total = len(work)
    open_ct = int((work.get("Status", pd.Series([])) == "Open").sum()) if not work.empty else 0
    new24 = int((work.get("🆕 24h", pd.Series([])) == True).sum()) if not work.empty else 0

    k1, k2, k3 = st.columns(3)
    k1.metric("Total shown", total)
    k2.metric("Open", open_ct)
    k3.metric("New in last 24h", new24)

    # CSV export
    csv_buf = StringIO()
    work.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Export CSV",
        data=csv_buf.getvalue(),
        file_name=f"grants_{slug}.csv",
        mime="text/csv",
    )

    # Data table
    col_cfg = {}
    if "Link" in work.columns:
        col_cfg["Link"] = st.column_config.LinkColumn("Link")
    if "Source" in work.columns:
        col_cfg["Source"] = st.column_config.LinkColumn("Source")
    if "🆕 24h" in work.columns:
        col_cfg["🆕 24h"] = st.column_config.CheckboxColumn("🆕 24h")

    st.dataframe(work, use_container_width=True, hide_index=True, column_config=col_cfg)

# FAQ / Help
with st.expander("How do I keep data fresh without paying for a worker?"):
    st.markdown("""
- Set `AUTO_REFRESH_MAX_AGE_MIN` (e.g., 60). First visitor after that triggers a refresh.
- Or set `CRON_TOKEN` and ping `/?cron=TOKEN` hourly with UptimeRobot (free).
- Or add a free **GitHub Actions** workflow to run `refresh_supabase.py` hourly.
""")

with st.expander("What makes this useful to councils?"):
    st.markdown("""
- **One place** for grants across council + state sources (deduped links).
- **Fast search** across title/description/summary.
- **Open/Closed + new in 24h** flags to triage quickly.
- **Shareable deep-links** like `/?c=wyndham` for each council.
- Optional **AI summaries** when your scraper follows details (`SUMMARIZE=1` + `OPENAI_API_KEY`).
""")
