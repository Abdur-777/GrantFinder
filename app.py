# app.py — GrantFinder (Lite, No PIN/Stripe) — instant refresh, open access
import os, time, hashlib
from datetime import datetime, date
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
import streamlit as st

# =========================
# Config
# =========================
APP_NAME = "GrantFinder (Lite – Open Demo)"
DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "grants.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# Grant sources
SOURCES = [
    "https://www.grants.gov.au/GO/list",
    "https://business.vic.gov.au/grants-and-programs",
    "https://www.vic.gov.au/grants",
    "https://www.wyndham.vic.gov.au/about-council/grants-funding",
    "https://www.hobsonsbay.vic.gov.au/Community/Grants",
    "https://www.brimbank.vic.gov.au/community/community-grants",
    "https://www.melbourne.vic.gov.au/community/funding/Pages/grants.aspx",
    "https://www.casey.vic.gov.au/community-grants",
]

# =========================
# Helpers
# =========================
def ensure_csv(path: str = CSV_PATH):
    if not os.path.exists(path):
        pd.DataFrame(columns=["id","title","description","amount","deadline","link","source","created_at"]).to_csv(path, index=False)

def load_df() -> pd.DataFrame:
    ensure_csv()
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception:
        df = pd.DataFrame(columns=["id","title","description","amount","deadline","link","source","created_at"])
    for c in ["id","title","description","amount","deadline","link","source","created_at"]:
        if c not in df.columns:
            df[c] = ""
    return df

def save_df(df: pd.DataFrame):
    df.to_csv(CSV_PATH, index=False)

def sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def row_id(r: Dict) -> str:
    return sha16(f"{r.get('title','')}|{r.get('link','')}")

def normalize_date_str(s: Optional[str]) -> Optional[str]:
    if not s:
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

# =========================
# Scraper
# =========================
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
        })
    return dedupe_by_link(out)

def dedupe_by_link(rows: List[Dict]) -> List[Dict]:
    seen, keep = set(), []
    for r in rows:
        link = r.get("link") or ""
        if link and link not in seen:
            seen.add(link)
            keep.append(r)
    return keep

def scrape_sources(sources: List[str]) -> List[Dict]:
    results = []
    for src in sources:
        html = safe_get(src)
        results.extend(parse_generic(html, src))
        time.sleep(0.3)
    return dedupe_by_link(results)

def merge_into_csv(new_rows: List[Dict]) -> int:
    if not new_rows:
        return 0
    df = load_df()
    existing_links = set(df["link"].fillna("").tolist())
    to_add = []
    for r in new_rows:
        if r["link"] in existing_links:
            continue
        r["id"] = row_id(r)
        r["created_at"] = datetime.utcnow().isoformat()
        iso = normalize_date_str(r.get("deadline"))
        r["deadline"] = iso or r["deadline"]
        to_add.append(r)
    if not to_add:
        return 0
    df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True) if not df.empty else pd.DataFrame(to_add)
    save_df(df2)
    return len(to_add)

# =========================
# UI (No Auth / Open Access)
# =========================
st.set_page_config(page_title=APP_NAME, page_icon="🏛️", layout="wide")
st.title("🏛️ GrantFinder (Open Demo)")

if st.button("🔄 Refresh grants"):
    with st.spinner("Scraping & updating…"):
        added = merge_into_csv(scrape_sources(SOURCES))
    st.success(f"✅ Added {added} new grants.")

# Load data
ensure_csv()
df = load_df()

if df.empty:
    st.info("No grants yet. Click **Refresh grants** above to load data.")
else:
    st.caption(f"Showing **{len(df)}** total grants • Last refresh: {df['created_at'].max()}")

# -------- Filters --------
st.subheader("🔎 Browse & filter")
c1, c2, c3 = st.columns([2,1,1])
with c1:
    q = st.text_input("Keyword", placeholder="e.g., youth, environment, arts…").strip().lower()
with c2:
    min_amount = st.text_input("Min amount (text search)", placeholder="$10,000")
with c3:
    deadline_before = st.date_input("Deadline before", value=None)

filtered = df.copy()

if not filtered.empty:
    filtered["deadline_iso"] = filtered["deadline"].apply(lambda v: normalize_date_str(v))
    filtered["_deadline_dt"] = pd.to_datetime(filtered["deadline_iso"], errors="coerce")

    if q:
        mask = (
            filtered["title"].fillna("").str.lower().str.contains(q, na=False) |
            filtered["description"].fillna("").str.lower().str.contains(q, na=False)
        )
        filtered = filtered[mask]

    if min_amount:
        mask_amt = filtered["description"].fillna("").str.contains(min_amount, case=False)
        filtered = filtered[mask_amt]

    if isinstance(deadline_before, date):
        filtered = filtered[(filtered["_deadline_dt"].notna()) & (filtered["_deadline_dt"].dt.date <= deadline_before)]

    filtered = filtered.sort_values(by=["_deadline_dt", "created_at"], ascending=[True, False], na_position="last").copy()
    filtered["description"] = filtered["description"].fillna("").apply(lambda s: (s[:160] + "…") if len(s) > 160 else s)

    st.write(f"Showing **{len(filtered)}** grants")
    st.dataframe(filtered[["title","amount","deadline","source","link","description"]], use_container_width=True)

    csv_bytes = filtered[["title","amount","deadline","source","link","description"]].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Export CSV", data=csv_bytes, file_name="grants_export.csv", mime="text/csv")
else:
    st.write("Showing **0** grants")

st.divider()
st.caption("ⓘ MVP demo. Data is best-effort from public pages; verify details on source sites before applying.")
