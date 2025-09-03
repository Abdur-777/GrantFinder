# app.py — GrantFinder VIC (Multi-tenant, fixed table columns)
# - Tenants from tenants.yaml (e.g., wyndham, melton, brimbank, hobsonsbay, melbourne)
# - Per-tenant storage at DATA_DIR/<slug>/grants.csv
# - UTC-safe “new in last 24h” badge
# - No duplicate column names in st.dataframe (uses a display_df)

import os, time, hashlib, tempfile
from datetime import datetime, date
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
import streamlit as st
import yaml

APP_NAME = "GrantFinder — Victoria"

# ---------- Tenant loading ----------
DATA_ROOT = os.getenv("DATA_DIR", "data")
TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")

# Fallback tenants if file missing (keeps app bootable)
DEFAULT_TENANTS = {
    "vic": {
        "name": "Victoria – All Councils",
        "color": "#333333",
        "sources": [
            "https://business.vic.gov.au/grants-and-programs",
            "https://www.vic.gov.au/grants",
            "https://www.grants.gov.au/GO/list",
        ],
    }
}
try:
    with open(TENANTS_FILE, "r") as f:
        TENANTS = yaml.safe_load(f) or DEFAULT_TENANTS
except Exception:
    TENANTS = DEFAULT_TENANTS

# Robust query param read across Streamlit versions
try:
    qp = st.query_params
    slug = qp.get("c")
    if isinstance(slug, list):
        slug = slug[0]
except Exception:
    slug = st.experimental_get_query_params().get("c", [None])[0]

if not slug or slug not in TENANTS:
    slug = next(iter(TENANTS.keys()))

tenant = TENANTS[slug]
TENANT_NAME = tenant.get("name", slug.capitalize())
THEME_COLOR = tenant.get("color", "#222")
SOURCES = tenant.get("sources", [])

TENANT_DIR = os.path.join(DATA_ROOT, slug)
os.makedirs(TENANT_DIR, exist_ok=True)

CSV_PATH = os.path.join(TENANT_DIR, "grants.csv")  # per-tenant CSV
BASE_COLUMNS = ["id", "title", "description", "amount", "deadline", "link", "source", "created_at", "summary"]

# ---------- CSV helpers (atomic writes) ----------
def save_df_atomic(df: pd.DataFrame, path: str):
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="grants_", suffix=".csv", dir=os.path.dirname(path))
    os.close(tmp_fd)
    try:
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)  # atomic on POSIX
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

def ensure_csv():
    if not os.path.exists(CSV_PATH):
        save_df_atomic(pd.DataFrame(columns=BASE_COLUMNS), CSV_PATH)

def load_df(retries: int = 3, delay: float = 0.2) -> pd.DataFrame:
    ensure_csv()
    for i in range(retries):
        try:
            df = pd.read_csv(CSV_PATH)
            break
        except Exception:
            if i == retries - 1:
                df = pd.DataFrame(columns=BASE_COLUMNS)
            else:
                time.sleep(delay)
    for c in BASE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df

def save_df(df: pd.DataFrame):
    for c in BASE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    save_df_atomic(df[BASE_COLUMNS], CSV_PATH)

# ---------- Utilities ----------
def sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def row_id(r: Dict) -> str:
    return sha16(f"{r.get('title','')}|{r.get('link','')}")

def normalize_date_str(s: Optional[str]) -> Optional[str]:
    if not s or str(s).strip() == "":
        return None
    try:
        dt = dateparse.parse(str(s), dayfirst=True, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None

def safe_get(url: str, timeout=20) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "GrantFinderBot/1.0"})
        if r.ok:
            return r.text
    except Exception:
        pass
    return None

def near_text(el: BeautifulSoup, selector: str) -> str:
    n = el.find_next(selector)
    return (n.get_text(" ", strip=True) if n else "")[:600]

# ---------- Scraper ----------
KEYWORDS = ("grant", "fund", "funding", "program", "round", "apply")

def parse_generic(html: str, base_url: str) -> List[Dict]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select("a"):
        title = (a.get_text() or "").strip()
        href = (a.get("href") or "").strip()
        if not title or not href:
            continue
        if not any(k in title.lower() for k in KEYWORDS):
            continue
        link = href if href.startswith("http") else requests.compat.urljoin(base_url, href)
        desc = near_text(a, "p") or near_text(a, "li")
        out.append({
            "title": title,
            "description": desc,
            "amount": "",
            "deadline": "",
            "link": link,
            "source": base_url,
            "summary": "",
        })
    # dedupe by link
    seen, keep = set(), []
    for r in out:
        L = r.get("link", "")
        if L and L not in seen:
            seen.add(L)
            keep.append(r)
    return keep

def scrape_sources(sources: List[str]) -> List[Dict]:
    results = []
    for src in sources:
        html = safe_get(src)
        results.extend(parse_generic(html, src))
        time.sleep(0.3)
    # dedupe across pages
    seen, keep = set(), []
    for r in results:
        L = r.get("link", "")
        if L and L not in seen:
            seen.add(L)
            keep.append(r)
    return keep

# ---------- Merge ----------
def merge_into_csv(new_rows: List[Dict]) -> int:
    if not new_rows:
        return 0
    df = load_df()
    existing_links = set(df["link"].fillna("").astype(str).tolist()) if not df.empty else set()
    to_add = []
    for r in new_rows:
        link = r.get("link", "")
        if not link or link in existing_links:
            continue
        item = r.copy()
        item["id"] = row_id(item)
        item["created_at"] = datetime.utcnow().isoformat() + "Z"  # UTC marker
        iso = normalize_date_str(item.get("deadline"))
        item["deadline"] = iso or (item.get("deadline") or "")
        to_add.append(item)
    if not to_add:
        return 0
    df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True) if not df.empty else pd.DataFrame(to_add)
    save_df(df2)
    return len(to_add)

# ---------- UI ----------
st.set_page_config(page_title=f"{APP_NAME} — {TENANT_NAME}", page_icon="🏛️", layout="wide")
st.markdown(f"<style>:root {{ --brand: {THEME_COLOR}; }}</style>", unsafe_allow_html=True)
st.title(f"🏛️ GrantFinder — {TENANT_NAME}")
st.caption(f"🎯 Victoria (AU) • Tenant: **{slug}** • Data path: `{TENANT_DIR}`")

if st.button("🔄 Refresh grants"):
    with st.spinner("Scraping tenant sources…"):
        added = merge_into_csv(scrape_sources(SOURCES))
    st.success(f"✅ Added {added} new grants.")

ensure_csv()
df = load_df()
if df.empty:
    st.info("No grants yet. Click **Refresh grants** above to load data.")
else:
    last = df["created_at"].max()
    # UTC-safe "new in last 24h"
    df["_created_dt"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    now_utc = pd.Timestamp.now(tz="UTC")
    yesterday = now_utc - pd.Timedelta(days=1)
    new_24h = df[df["_created_dt"] >= yesterday]
    st.caption(f"🆕 {len(new_24h)} new in last 24h • Total {len(df)} • Last refresh: {last}")

# -------- Filters --------
st.subheader("🔎 Browse & filter")
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    q = st.text_input("Keyword", placeholder="e.g., youth, environment, arts…").strip().lower()
with c2:
    min_amount_text = st.text_input("Min amount (text search)", placeholder="$10,000")
with c3:
    deadline_before = st.date_input("Deadline before (optional)", value=None)

filtered = df.copy()
if not filtered.empty:
    # Parse deadlines for sorting/filter
    filtered["deadline_iso"] = filtered["deadline"].apply(normalize_date_str)
    filtered["_deadline_dt"] = pd.to_datetime(filtered["deadline_iso"], errors="coerce")

    if q:
        mask = (
            filtered["title"].fillna("").str.lower().str.contains(q, na=False) |
            filtered["description"].fillna("").str.lower().str.contains(q, na=False) |
            filtered["summary"].fillna("").str.lower().str.contains(q, na=False)
        )
        filtered = filtered[mask]

    if min_amount_text:
        mask_amt = (
            filtered["amount"].fillna("").astype(str).str.contains(min_amount_text, case=False, na=False) |
            filtered["description"].fillna("").astype(str).str.contains(min_amount_text, case=False, na=False)
        )
        filtered = filtered[mask_amt]

    if isinstance(deadline_before, date):
        filtered = filtered[(filtered["_deadline_dt"].notna()) & (filtered["_deadline_dt"].dt.date <= deadline_before)]

    # Sort: nearest deadline first, then newest created_at; unknown deadlines last
    filtered = filtered.sort_values(by=["_deadline_dt", "created_at"], ascending=[True, False], na_position="last").copy()

    # -------- Build a separate display DataFrame (avoid duplicate column names) --------
    def tidy_desc(s: str, n: int = 160) -> str:
        s = (s or "").strip().replace("\n", " ")
        return (s[:n] + "…") if len(s) > n else s

    filtered["description_preview"] = filtered["description"].fillna("").apply(tidy_desc)
    filtered["title_link"] = filtered["link"].fillna("")

    display_df = filtered[[
        "title",
        "title_link",       # will be shown as "open"
        "amount",
        "deadline_iso",     # will be shown as "deadline"
        "source",
        "description_preview"  # will be shown as "description"
    ]].copy()

    st.write(f"Showing **{len(display_df)}** grants")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": st.column_config.TextColumn("title", width="medium"),
            "title_link": st.column_config.LinkColumn("open", display_text="Open"),
            "amount": st.column_config.TextColumn("amount", width="small"),
            "deadline_iso": st.column_config.TextColumn("deadline", width="small"),
            "source": st.column_config.LinkColumn("source", display_text="Source"),
            "description_preview": st.column_config.TextColumn("description", width="large"),
        }
    )

    # Export current view (write a clean "deadline" column)
    export_cols = ["title", "amount", "deadline", "source", "link", "description", "summary"]
    filtered["deadline"] = filtered["deadline_iso"]
    csv_bytes = filtered[export_cols].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Export CSV", data=csv_bytes, file_name=f"grants_{slug}.csv", mime="text/csv")
else:
    st.write("Showing **0** grants")
    st.download_button("⬇️ Export CSV",
                       data=b"title,amount,deadline,source,link,description,summary\n",
                       file_name=f"grants_{slug}.csv",
                       mime="text/csv")

st.divider()
st.caption("ⓘ MVP demo. Best-effort extraction from public pages; verify details on source sites before applying.")
