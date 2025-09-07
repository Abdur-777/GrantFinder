# app.py
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

# ---------- Config & Clients ----------
from supabase import create_client
import yaml

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")
INCLUDE_STATEWIDE_IN_EACH = os.getenv("INCLUDE_STATEWIDE_IN_EACH", "1") == "1"
STATEWIDE_SLUG = os.getenv("STATEWIDE_SLUG", "vic")
AUTO_REFRESH_MAX_AGE_MIN = int(os.getenv("AUTO_REFRESH_MAX_AGE_MIN", "60"))
CRON_TOKEN = os.getenv("CRON_TOKEN", "")
SUMMARIZE = os.getenv("SUMMARIZE", "0") == "1"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ---------- Utils ----------
def load_tenants() -> Dict[str, dict]:
    try:
        with open(TENANTS_FILE, "r") as f:
            data = yaml.safe_load(f) or {}
            return data
    except Exception:
        # Minimal fallback if tenants file missing
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

def utcnow():
    return datetime.now(timezone.utc)

def parse_utc(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # supports "2025-09-03T01:23:45Z" and similar
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_last_run() -> Optional[datetime]:
    try:
        res = sb.table("meta").select("value").eq("key", "last_run_utc").limit(1).execute()
        rows = getattr(res, "data", []) or []
        if rows and rows[0].get("value"):
            return parse_utc(rows[0]["value"])
    except Exception:
        pass
    return None

@st.cache_data(ttl=60)
def fetch_grants_for(slug: str, include_statewide: bool) -> pd.DataFrame:
    """Fetch grants for a single council; optionally union statewide rows."""
    chunks: List[pd.DataFrame] = []

    def _fetch(sl: str) -> Optional[pd.DataFrame]:
        try:
            res = (
                sb.table("grants")
                .select(
                    "id,title,description,amount,deadline,deadline_iso,link,source,summary,created_at,council_slug"
                )
                .eq("council_slug", sl)
                .order("created_at", desc=True)
                .limit(2000)
                .execute()
            )
            data = getattr(res, "data", []) or []
            if not data:
                return None
            df = pd.DataFrame(data)
            return df
        except Exception:
            return None

    df_slug = _fetch(slug)
    if df_slug is not None:
        chunks.append(df_slug)

    if include_statewide and slug != STATEWIDE_SLUG:
        df_state = _fetch(STATEWIDE_SLUG)
        if df_state is not None:
            chunks.append(df_state)

    if not chunks:
        return pd.DataFrame(columns=["title", "amount", "deadline", "deadline_iso", "link", "source", "summary", "created_at", "council_slug"])

    df = pd.concat(chunks, ignore_index=True)

    # Normalize times & compute flags
    df["_created_dt"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["_deadline_dt"] = pd.to_datetime(df["deadline_iso"], utc=True, errors="coerce")

    now_utc = pd.Timestamp.utcnow()
    df["status"] = df["_deadline_dt"].apply(
        lambda d: "Open" if (pd.isna(d) or d >= now_utc) else "Closed"
    )
    df["new_24h"] = df["_created_dt"].apply(
        lambda d: bool(pd.notna(d) and d >= (now_utc - pd.Timedelta(days=1)))
    )

    # Ensure consistent, unique columns
    keep_cols = [
        "title", "amount", "deadline", "deadline_iso",
        "status", "new_24h", "link", "source", "summary",
        "created_at", "council_slug"
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()

    return df

def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Streamlit/pyarrow errors if duplicate column names exist."""
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

def council_options() -> List[str]:
    return list(TENANTS.keys())

def council_name(slug: str) -> str:
    cfg = TENANTS.get(slug) or {}
    return cfg.get("name") or slug.title()

def share_url_for(slug: str) -> str:
    base = st.secrets.get("app_base_url", "") or os.getenv("APP_BASE_URL", "")
    if not base:
        # Fall back to building from current page
        try:
            # st.query_params works in modern Streamlit, fallback to experimental
            qp = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
            # we can't get current base reliably server-side; just show relative
            return f"/?c={slug}"
        except Exception:
            return f"/?c={slug}"
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}c={slug}"

# ---------- Refresh integration ----------
def refresh_all_tenants_once() -> int:
    """
    Calls your refresh_supabase.py logic (scrape → upsert → meta).
    Returns number of rows upserted (best-effort).
    """
    try:
        import refresh_supabase as rs

        total = 0
        for slug, cfg in (rs.TENANTS or {}).items():
            srcs = (cfg or {}).get("sources", []) or []
            rows = rs.scrape_sources(srcs)
            total += rs.upsert_grants(rows, slug)
        rs.set_last_run()
        return total
    except Exception as e:
        # If the module isn't present or anything fails, don't crash the app.
        print("refresh_all_tenants_once() failed:", e)
        return -1

# ---------- Optional Cron endpoint ----------
def _get_qp(key: str) -> Optional[str]:
    try:
        qp = st.query_params  # Streamlit >= 1.34
        v = qp.get(key)
        return v[0] if isinstance(v, list) else v
    except Exception:
        v = st.experimental_get_query_params().get(key, [None])[0]
        return v

def maybe_handle_cron():
    if not CRON_TOKEN:
        return
    cron = _get_qp("cron")
    if cron and cron == CRON_TOKEN:
        with st.spinner("Cron refresh in progress…"):
            added = refresh_all_tenants_once()
        st.write(f"OK • refreshed all tenants • added {added} items")
        st.stop()

# ================== UI START ==================
st.set_page_config(page_title="GrantFinder (VIC)", page_icon="🗂️", layout="wide")
maybe_handle_cron()

st.title("GrantFinder — Victoria (Councils)")
st.caption("Auto-aggregated grants from council & state sources. Supabase-backed. 🐨")

# Sidebar: council selection
with st.sidebar:
    st.header("Filters")
    slugs = council_options()
    # Prefer a query param ?c=slug
    slug_from_qp = _get_qp("c")
    default_slug = slug_from_qp if slug_from_qp in slugs else ( "wyndham" if "wyndham" in slugs else slugs[0] )
    slug = st.selectbox("Council", options=slugs, index=slugs.index(default_slug))
    st.write(f"Viewing: **{council_name(slug)}**")
    st.write("Share this council:")
    st.code(share_url_for(slug), language="text")

    include_statewide = INCLUDE_STATEWIDE_IN_EACH and st.toggle(
        "Include statewide grants", value=True, help="Also include state-wide VIC grants alongside this council."
    )

    search = st.text_input("Search (title/description)", placeholder="e.g. youth sport equipment")
    status_filter = st.multiselect("Status", ["Open", "Closed"], default=["Open"])
    show_new_only = st.toggle("Only ‘new in last 24h’", value=False)

# Auto-refresh gate (free, no worker)
last_run = get_last_run()
needs_refresh = (
    last_run is None or (utcnow() - last_run) > timedelta(minutes=AUTO_REFRESH_MAX_AGE_MIN)
)

colA, colB, colC, colD = st.columns([1,1,1,1])
with colA:
    st.metric("Auto-refresh window (min)", AUTO_REFRESH_MAX_AGE_MIN)
with colB:
    st.metric("Last refresh (UTC)", last_run.isoformat() if last_run else "—")
with colC:
    st.write("")
    if st.button("🔄 Refresh now"):
        with st.spinner("Refreshing all councils…"):
            added = refresh_all_tenants_once()
        st.success(f"Done. Added/upserted ~{added} rows (best-effort).")
        # Invalidate caches so latest shows
        get_last_run.clear()
        fetch_grants_for.clear()
with colD:
    st.write("")
    if st.button("♻️ Force auto-refresh if stale"):
        if needs_refresh:
            with st.spinner("Auto-refresh kick…"):
                added = refresh_all_tenants_once()
            st.success(f"Auto-refresh complete (added ~{added}).")
            get_last_run.clear()
            fetch_grants_for.clear()
        else:
            st.info("Data is fresh enough; no refresh needed.")

if needs_refresh:
    st.info("Data is older than the refresh window. Click **Refresh now** above, or enable a cron ping (see README).")

# Fetch data
with st.spinner("Loading grants…"):
    df = fetch_grants_for(slug, include_statewide)
    if not df.empty:
        if search:
            s = search.strip().lower()
            mask = (
                df["title"].fillna("").str.lower().str.contains(s)
                | df["description"].fillna("").str.lower().str.contains(s)
                | df.get("summary", pd.Series([""]*len(df))).fillna("").str.lower().str.contains(s)
            )
            df = df[mask]

        if status_filter:
            df = df[df["status"].isin(status_filter)]

        if show_new_only:
            df = df[df["new_24h"] == True]

        # Sort: open first, then by created_at desc (fallback)
        created_dt = pd.to_datetime(df.get("created_at"), utc=True, errors="coerce")
        df = df.assign(_sort_open=(df["status"] != "Open").astype(int), _created_dt=created_dt)
        df = df.sort_values(by=["_sort_open", "_created_dt"], ascending=[True, False])

        # Humanized columns
        df_show = df.copy()
        df_show.rename(
            columns={
                "title": "Title",
                "amount": "Amount",
                "deadline": "Deadline (text)",
                "deadline_iso": "Deadline (ISO)",
                "status": "Status",
                "new_24h": "🆕 24h",
                "link": "Link",
                "source": "Source",
                "summary": "AI Summary",
                "created_at": "Discovered At (UTC)",
                "council_slug": "Council",
            },
            inplace=True,
        )

        # Remove helper cols if they slipped in
        for c in ["_sort_open", "_created_dt"]:
            if c in df_show.columns:
                df_show.drop(columns=[c], inplace=True, errors="ignore")

        # Ensure unique names (Streamlit/pyarrow quirk protection)
        df_show = ensure_unique_columns(df_show)

        # KPIs
        total = len(df_show)
        new24 = int((df["new_24h"] == True).sum())
        open_ct = int((df["status"] == "Open").sum())

        m1, m2, m3 = st.columns(3)
        m1.metric("Total shown", total)
        m2.metric("Open", open_ct)
        m3.metric("New in last 24h", new24)

        # Export
        csv_buf = StringIO()
        df_show.to_csv(csv_buf, index=False)
        st.download_button("⬇️ Export CSV", data=csv_buf.getvalue(), file_name=f"grants_{slug}.csv", mime="text/csv")

        # Data table with link columns
        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Link": st.column_config.LinkColumn("Link"),
                "Source": st.column_config.LinkColumn("Source"),
                "🆕 24h": st.column_config.CheckboxColumn("🆕 24h"),
            },
        )
    else:
        st.warning("No grants found yet for this council. Try **Refresh now** or check your tenant sources.")

# Help / FAQ
with st.expander("What makes this useful to councils?"):
    st.markdown("""
- **One place** for all grant opportunities (council sites + state pages).
- **De-duplicated** links and **fast search** across titles/descriptions.
- **Auto-refresh** (no manual copying) + optional **hourly cron**.
- Optional **AI summaries** (set `SUMMARIZE=1` + `OPENAI_API_KEY`) to give who/why in 2 sentences.
- Shareable deep links like `/?c=wyndham` for each council.
""")

with st.expander("How do I keep it fresh without paying for a worker?"):
    st.markdown("""
- Set `AUTO_REFRESH_MAX_AGE_MIN` (e.g., 60) to let the app refresh on first visit.
- Or set `CRON_TOKEN` and ping `/?cron=TOKEN` hourly with UptimeRobot (free).
- Or use the free GitHub Actions workflow to run `refresh_supabase.py` hourly.
""")
