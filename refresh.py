# refresh.py — Hourly refresher for all tenants (5 councils)
# Reads tenants.yaml, refreshes each tenant's sources, writes to DATA_DIR/<slug>/grants.csv

import os, time, hashlib, tempfile
from datetime import date, datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
import yaml
from pathlib import Path

DATA_ROOT = os.getenv("DATA_DIR", "data")
TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")

with open(TENANTS_FILE, "r") as f:
    TENANTS = yaml.safe_load(f) or {}

BASE_COLUMNS = ["id","title","description","amount","deadline","link","source","created_at","summary"]

# ---- shared helpers (same logic as app.py) ----
def save_df_atomic(df: pd.DataFrame, path: str):
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="grants_", suffix=".csv", dir=os.path.dirname(path))
    os.close(tmp_fd)
    try:
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except Exception: pass

def ensure_csv(path: str):
    if not os.path.exists(path):
        save_df_atomic(pd.DataFrame(columns=BASE_COLUMNS), path)

def load_df(path: str, retries: int = 3, delay: float = 0.2) -> pd.DataFrame:
    ensure_csv(path)
    for i in range(retries):
        try:
            df = pd.read_csv(path)
            break
        except Exception:
            if i == retries - 1:
                df = pd.DataFrame(columns=BASE_COLUMNS)
            else:
                time.sleep(delay)
    for c in BASE_COLUMNS:
        if c not in df.columns: df[c] = ""
    return df

def save_df(path: str, df: pd.DataFrame):
    for c in BASE_COLUMNS:
        if c not in df.columns: df[c] = ""
    save_df_atomic(df[BASE_COLUMNS], path)

def sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def row_id(r: Dict) -> str:
    return sha16(f"{r.get('title','')}|{r.get('link','')}")

def normalize_date_str(s: Optional[str]) -> Optional[str]:
    if not s or str(s).strip() == "": return None
    try:
        dt = dateparse.parse(str(s), dayfirst=True, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None

def safe_get(url: str, timeout=20) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "GrantFinderBot/1.0"})
        if r.ok: return r.text
    except Exception:
        pass
    return None

def near_text(el: BeautifulSoup, selector: str) -> str:
    n = el.find_next(selector)
    return (n.get_text(" ", strip=True) if n else "")[:600]

KEYWORDS = ("grant", "fund", "funding", "program", "round", "apply")

def parse_generic(html: str, base_url: str) -> List[Dict]:
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select("a"):
        title = (a.get_text() or "").strip()
        href = (a.get("href") or "").strip()
        if not title or not href: continue
        if not any(k in title.lower() for k in KEYWORDS): continue
        link = href if href.startswith("http") else requests.compat.urljoin(base_url, href)
        desc = near_text(a, "p") or near_text(a, "li")
        out.append({"title": title, "description": desc, "amount": "", "deadline": "",
                    "link": link, "source": base_url, "summary": ""})
    seen, keep = set(), []
    for r in out:
        L = r.get("link","")
        if L and L not in seen:
            seen.add(L); keep.append(r)
    return keep

def scrape_sources(sources: List[str]) -> List[Dict]:
    results = []
    for src in sources:
        html = safe_get(src)
        results.extend(parse_generic(html, src))
        time.sleep(0.3)
    seen, keep = set(), []
    for r in results:
        L = r.get("link","")
        if L and L not in seen:
            seen.add(L); keep.append(r)
    return keep

def merge_into_csv(path: str, new_rows: List[Dict]) -> int:
    if not new_rows: return 0
    df = load_df(path)
    existing_links = set(df["link"].fillna("").astype(str).tolist()) if not df.empty else set()
    to_add = []
    for r in new_rows:
        link = r.get("link","")
        if not link or link in existing_links: continue
        item = r.copy()
        item["id"] = row_id(item)
        item["created_at"] = datetime.utcnow().isoformat() + "Z"
        iso = normalize_date_str(item.get("deadline"))
        item["deadline"] = iso or (item.get("deadline") or "")
        to_add.append(item)
    if not to_add: return 0
    df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True) if not df.empty else pd.DataFrame(to_add)
    save_df(path, df2)
    return len(to_add)

def expire_old_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "deadline" not in df.columns: return df
    df = df.copy()
    df["deadline_iso"] = df["deadline"].apply(normalize_date_str)
    df["_deadline_dt"] = pd.to_datetime(df["deadline_iso"], errors="coerce")
    today = pd.to_datetime(date.today())
    keep = df["_deadline_dt"].isna() | (df["_deadline_dt"] >= today)
    return df[keep].drop(columns=["_deadline_dt"], errors="ignore")

# ---- process all tenants ----
if __name__ == "__main__":
    start = datetime.utcnow().isoformat() + "Z"
    print(f"[{start}] Refresh start • tenants={list(TENANTS.keys())} • data_root={DATA_ROOT}")
    for slug, cfg in TENANTS.items():
        sources = cfg.get("sources", [])
        tdir = Path(DATA_ROOT) / slug
        tdir.mkdir(parents=True, exist_ok=True)
        csv_path = str(tdir / "grants.csv")

        try:
            rows = scrape_sources(sources)
            added = merge_into_csv(csv_path, rows)
            print(f"[{slug}] added {added}")
            df = load_df(csv_path)
            before = len(df)
            df2 = expire_old_rows(df)
            if len(df2) != before:
                save_df(csv_path, df2)
                print(f"[{slug}] expired {before - len(df2)}")
            print(f"[{slug}] total {len(df2)}")
        except Exception as e:
            print(f"[{slug}] ERROR:", repr(e))
