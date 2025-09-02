import os, io, csv, time, hashlib
from datetime import datetime
from dateutil import parser as dateparse
from typing import List, Dict
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- Config ----------
APP_NAME = "GrantFinder – Councils & NGOs"
ADMIN_PIN = os.getenv("ADMIN_PIN", "4242")  # change in Render
STRIPE_CHECKOUT_URL = os.getenv("STRIPE_CHECKOUT_URL", "https://buy.stripe.com/test_XXXX")  # replace
DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "grants.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# Seed sources (keep small & reliable; add more later)
SOURCES = [
    # Australian federal (example pages; swap to your region if needed)
    "https://www.grants.gov.au/",
    # Victoria sample (you can add more state/local sources):
    "https://www.vic.gov.au/grants",
    # Add your council(s) grant pages here:
    "https://www.wyndham.vic.gov.au/about-council/grants-funding",
]

# ---------- Utilities ----------
def hash_row(d: Dict) -> str:
    base = f"{d.get('title','')}-{d.get('link','')}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def normalize_date(s: str):
    if not s:
        return None
    try:
        dt = dateparse.parse(s, dayfirst=True, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None

def ensure_csv(path=CSV_PATH):
    if not os.path.exists(path):
        pd.DataFrame(columns=["id","title","description","amount","deadline","link","source","created_at"]).to_csv(path, index=False)

def load_df() -> pd.DataFrame:
    ensure_csv()
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception:
        df = pd.DataFrame(columns=["id","title","description","amount","deadline","link","source","created_at"])
    # Coerce types
    for c in ["title","description","amount","deadline","link","source","created_at","id"]:
        if c not in df.columns: df[c] = ""
    return df

def save_df(df: pd.DataFrame):
    df.to_csv(CSV_PATH, index=False)

def safe_get(url: str, timeout=20):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"GrantFinderBot/1.0"})
        if r.ok:
            return r.text
    except Exception:
        return None
    return None

# ---------- Scrapers (simple, resilient fallbacks) ----------
def parse_generic_list_page(html: str, base_url: str) -> List[Dict]:
    """Very simple parser: grabs <a> with text, nearby description, looks for dates/amounts heuristically."""
    out = []
    if not html: return out
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select("a"):
        title = (a.get_text() or "").strip()
        href = (a.get("href") or "").strip()
        if not title or not href:
            continue
        # Basic filters to reduce noise
        t_low = title.lower()
        if not any(k in t_low for k in ["grant", "fund", "funding", "program", "round"]):
            continue

        link = href if href.startswith("http") else requests.compat.urljoin(base_url, href)
        # Try to find a nearby paragraph
        desc = ""
        p = a.find_next("p")
        if p: desc = p.get_text(" ", strip=True)[:500]

        # Heuristic amount/deadline extraction
        amount = ""
        deadline = ""
        chunk = " ".join([title, desc])[:800].lower()

        # Amount heuristic
        for token in ["$", "aud", "amount", "up to", "from"]:
            if token in chunk:
                amount = token

        # Deadline heuristic
        for lbl in ["close", "closing", "deadline", "apply by", "applications close"]:
            if lbl in chunk:
                # Try to grab the next numbers
                deadline = None  # leave empty if unreliable; admin can edit in CSV
                break

        out.append({
            "title": title,
            "description": desc,
            "amount": amount,
            "deadline": deadline,
            "link": link,
            "source": base_url,
        })
    return out

def scrape_sources(sources=SOURCES) -> List[Dict]:
    results = []
    for src in sources:
        html = safe_get(src)
        rows = parse_generic_list_page(html, src)
        results.extend(rows)
        time.sleep(0.5)
    return dedupe_by_link(results)

def dedupe_by_link(rows: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for r in rows:
        link = r.get("link")
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(r)
    return out

def merge_into_csv(new_rows: List[Dict]) -> int:
    if not new_rows: return 0
    df = load_df()
    if df.empty:
        enriched = []
        for r in new_rows:
            item = r.copy()
            item["created_at"] = datetime.utcnow().isoformat()
            item["id"] = hash_row(item)
            enriched.append(item)
        save_df(pd.DataFrame(enriched))
        return len(enriched)

    # Deduplicate by link
    existing_links = set(df["link"].fillna("").tolist())
    to_add = []
    for r in new_rows:
        if r.get("link","") not in existing_links:
            item = r.copy()
            item["created_at"] = datetime.utcnow().isoformat()
            item["id"] = hash_row(item)
            to_add.append(item)

    if to_add:
        df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True)
        save_df(df2)
    return len(to_add)

# ---------- UI ----------
st.set_page_config(page_title=APP_NAME, page_icon="💰", layout="wide")
st.title("🏛️ GrantFinder (Lite)")

# Sidebar: Auth + Actions
with st.sidebar:
    st.header("Admin")
    pin_ok = st.text_input("Enter admin PIN", type="password")
    is_admin = (pin_ok == ADMIN_PIN)

    if not is_admin:
        st.caption("🔒 Enter admin PIN to refresh data.")
        st.link_button("Subscribe / Start Trial", STRIPE_CHECKOUT_URL, help="Start your subscription")

    if is_admin:
        if st.button("🔄 Refresh grants (1 click)"):
            with st.spinner("Scraping sources…"):
                rows = scrape_sources()
                added = merge_into_csv(rows)
            st.success(f"Done. Added {added} new grants.")
        st.caption("Tip: Add/remove sources in code (SOURCES list). Keep it lean & high-signal.")

# Main: Filters & Table
ensure_csv()
df = load_df()

# Cleaning
def coerce_deadline(v):
    v = str(v or "").strip()
    # Accept ISO (YYYY-MM-DD) or attempt parse
    iso = normalize_date(v)
    return iso or v

if not df.empty:
    df["deadline"] = df["deadline"].apply(coerce_deadline)

st.subheader("🔎 Browse & filter")
col1, col2, col3 = st.columns([2,1,1])
with col1:
    q = st.text_input("Keyword", placeholder="e.g., youth, environment, arts, small business…").strip().lower()
with col2:
    min_amount = st.text_input("Min amount (text search)", placeholder="e.g., $10,000")
with col3:
    deadline_before = st.date_input("Deadline before (optional)", value=None)

filtered = df.copy()
if q:
    mask = (
        filtered["title"].str.lower().str.contains(q, na=False) |
        filtered["description"].str.lower().str.contains(q, na=False) |
        filtered["source"].str.lower().str.contains(q, na=False)
    )
    filtered = filtered[mask]

if min_amount:
    # simple text search (keeps MVP simple)
    mask_amt = (
        filtered["amount"].fillna("").astype(str).str.contains(min_amount, case=False) |
        filtered["description"].fillna("").astype(str).str.contains(min_amount, case=False)
    )
    filtered = filtered[mask_amt]

if deadline_before:
    def _le(d):
        try:
            return dateparse.parse(str(d)).date() <= deadline_before
        except Exception:
            return False
    filtered = filtered[filtered["deadline"].apply(_le)]

st.write(f"Showing **{len(filtered)}** grants")
# Nice columns order
show_cols = ["title","amount","deadline","source","link","description"]
for c in show_cols:
    if c not in filtered.columns:
        filtered[c] = ""

st.dataframe(filtered[show_cols], use_container_width=True)

# Export
csv_bytes = filtered[show_cols].to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Export CSV", data=csv_bytes, file_name="grants_export.csv", mime="text/csv")

st.divider()
st.caption("ⓘ MVP demo. Data is best-effort from public pages; verify details on source sites before applying.")
