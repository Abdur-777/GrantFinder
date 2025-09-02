# app.py — GrantFinder VIC (Open Demo)
# - Click "Refresh" to scrape curated VIC sources
# - Browse, filter, export CSV
# - Sorts by nearest deadline (unknowns last)
import os, time, hashlib
from datetime import datetime, date
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
import streamlit as st

from sources_vic import VIC_SOURCES

APP_NAME = "GrantFinder – Victoria (Open Demo)"
DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "grants.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# --------- Storage helpers ---------
def ensure_csv(path: str = CSV_PATH):
    if not os.path.exists(path):
        cols = ["id","title","description","amount","deadline","link","source","created_at","summary"]
        pd.DataFrame(columns=cols).to_csv(path, index=False)

def load_df() -> pd.DataFrame:
    ensure_csv()
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception:
        df = pd.DataFrame(columns=["id","title","description","amount","deadline","link","source","created_at","summary"])
    for c in ["id","title","description","amount","deadline","link","source","created_at","summary"]:
        if c not in df.columns:
            df[c] = ""
    return df

def save_df(df: pd.DataFrame):
    df.to_csv(CSV_PATH, index=False)

# --------- Utilities ---------
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

# --------- Scraper (generic heuristics) ---------
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
        # MVP: leave amount/deadline as empty text; councils can check details on source pages
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
        L = r.get("link","")
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
    # dedupe again (across pages)
    seen, keep = set(), []
    for r in results:
        L = r.get("link","")
        if L and L not in seen:
            seen.add(L)
            keep.append(r)
    return keep

# --------- Merge logic ---------
def merge_into_csv(new_rows: List[Dict]) -> int:
    if not new_rows:
        return 0
    df = load_df()
    existing_links = set(df["link"].fillna("").astype(str).tolist()) if not df.empty else set()
    to_add = []
    for r in new_rows:
        link = r.get("link","")
        if not link or link in existing_links:
            continue
        item = r.copy()
        item["id"] = row_id(item)
        item["created_at"] = datetime.utcnow().isoformat()
        iso = normalize_date_str(item.get("deadline"))
        item["deadline"] = iso or (item.get("deadline") or "")
        to_add.append(item)
    if not to_add:
        return 0
    df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True) if not df.empty else pd.DataFrame(to_add)
    save_df(df2)
    return len(to_add)

# --------- UI ---------
st.set_page_config(page_title=APP_NAME, page_icon="🏛️", layout="wide")
st.title("🏛️ GrantFinder — Victoria")
st.caption("🎯 Focus: Victoria (Australia) grant sources")

# Refresh button (no auth)
if st.button("🔄 Refresh grants"):
    with st.spinner("Scraping curated VIC sources…"):
        added = merge_into_csv(scrape_sources(VIC_SOURCES))
    st.success(f"✅ Added {added} new grants.")

# Load & status
ensure_csv()
df = load_df()
if df.empty:
    st.info("No grants yet. Click **Refresh grants** above to load data.")
else:
    last = df["created_at"].max()
    # New in last 24h
    if "created_at" in df.columns:
        df["_created_dt"] = pd.to_datetime(df["created_at"], errors="coerce")
        new_24h = df[df["_created_dt"] >= (pd.Timestamp.utcnow() - pd.Timedelta(days=1))]
        st.caption(f"🆕 {len(new_24h)} new in last 24h • Total {len(df)} • Last refresh: {last}")

# Filters
st.subheader("🔎 Browse & filter")
c1, c2, c3 = st.columns([2,1,1])
with c1:
    q = st.text_input("Keyword", placeholder="e.g., youth, environment, arts, small business…").strip().lower()
with c2:
    min_amount_text = st.text_input("Min amount (text search)", placeholder="$10,000")
with c3:
    deadline_before = st.date_input("Deadline before (optional)", value=None)

filtered = df.copy()
if not filtered.empty:
    # Parse deadlines for sorting / filter
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

    # Sort: nearest deadline first; unknowns last, tie-break by created_at (newer first)
    filtered = filtered.sort_values(by=["_deadline_dt", "created_at"], ascending=[True, False], na_position="last").copy()

    # Tidy presentation
    def tidy_desc(s: str, n: int = 160) -> str:
        s = (s or "").strip().replace("\n"," ")
        return (s[:n] + "…") if len(s) > n else s
    filtered["description_preview"] = filtered["description"].fillna("").apply(tidy_desc)
    filtered["title_link"] = filtered["link"].fillna("")

    st.write(f"Showing **{len(filtered)}** grants")
    st.dataframe(
        filtered.rename(columns={
            "title": "title",
            "title_link": "open",
            "amount": "amount",
            "deadline_iso": "deadline",
            "source": "source",
            "description_preview": "description"
        })[["title","open","amount","deadline","source","description"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": st.column_config.TextColumn("title", width="medium"),
            "open": st.column_config.LinkColumn("open", display_text="Open"),
            "amount": st.column_config.TextColumn("amount", width="small"),
            "deadline": st.column_config.TextColumn("deadline", width="small"),
            "source": st.column_config.LinkColumn("source", display_text="Source"),
            "description": st.column_config.TextColumn("description", width="large"),
        }
    )

    export_cols = ["title","amount","deadline","source","link","description","summary"]
    filtered["deadline"] = filtered["deadline_iso"]
    csv_bytes = filtered[export_cols].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Export CSV", data=csv_bytes, file_name="grants_vic_export.csv", mime="text/csv")
else:
    st.write("Showing **0** grants")
    st.download_button("⬇️ Export CSV", data=b"title,amount,deadline,source,link,description,summary\n",
                       file_name="grants_vic_export.csv", mime="text/csv")

st.divider()
st.caption("ⓘ MVP demo. Data is best-effort from public pages; verify details on source sites before applying.")
