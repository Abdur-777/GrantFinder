# refresh.py — Master crawl + per-council split (15 councils)
# ENV:
#   DATA_DIR=/var/data/grants          (shared disk)
#   TENANTS_FILE=tenants.yaml
#   INCLUDE_STATEWIDE_IN_EACH=1        (include 'vic' rows in every council feed)
#   REFRESH_INTERVAL_SECONDS=3600      (used only by your worker loop)

import os, time, hashlib, tempfile
from datetime import date, datetime
from typing import List, Dict, Optional
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
import yaml
from urllib.parse import urljoin

DATA_ROOT = os.getenv("DATA_DIR", "data")
TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")
INCLUDE_STATEWIDE_IN_EACH = os.getenv("INCLUDE_STATEWIDE_IN_EACH", "1") == "1"

MASTER_DIR = Path(DATA_ROOT) / "master"
MASTER_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = str(MASTER_DIR / "all_vic.csv")

BASE_COLUMNS = ["id","title","description","amount","deadline","link","source","created_at","summary"]
MASTER_COLUMNS = BASE_COLUMNS + ["council_slug"]

# ----------------- CSV helpers -----------------
def save_df_atomic(df: pd.DataFrame, path: str, columns: List[str]):
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="grants_", suffix=".csv", dir=os.path.dirname(path))
    os.close(tmp_fd)
    try:
        for c in columns:
            if c not in df.columns:
                df[c] = ""
        df[columns].to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except Exception: pass

def ensure_df(path: str, columns: List[str]) -> pd.DataFrame:
    if not os.path.exists(path):
        save_df_atomic(pd.DataFrame(columns=columns), path, columns)
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.DataFrame(columns=columns)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df

def save_master(df: pd.DataFrame):
    save_df_atomic(df, MASTER_CSV, MASTER_COLUMNS)

def save_tenant_csv(slug: str, df: pd.DataFrame):
    tdir = Path(DATA_ROOT) / slug
    tdir.mkdir(parents=True, exist_ok=True)
    out = str(tdir / "grants.csv")
    save_df_atomic(df, out, BASE_COLUMNS)

# ----------------- Utilities & scraping -----------------
def sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

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
        low = title.lower()
        if not any(k in low for k in KEYWORDS) and ("grant" not in href.lower() and "fund" not in href.lower()):
            continue
        link = href if href.startswith("http") else urljoin(base_url, href)
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
    # de-dup by link
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
    # de-dup across sources
    seen, keep = set(), []
    for r in results:
        L = r.get("link","")
        if L and L not in seen:
            seen.add(L); keep.append(r)
    return keep

# ----------------- Merge logic -----------------
def merge_into_master(new_rows: List[Dict]) -> int:
    """Merge rows into master CSV (dedupe by link)."""
    if not new_rows:
        return 0
    df = ensure_df(MASTER_CSV, MASTER_COLUMNS)
    existing_links = set(df["link"].fillna("").astype(str).tolist()) if not df.empty else set()
    to_add = []
    for r in new_rows:
        link = r.get("link","")
        if not link or link in existing_links:
            continue
        item = r.copy()
        item["id"] = sha16(f"{item.get('title','')}|{link}")
        item["created_at"] = datetime.utcnow().isoformat() + "Z"
        iso = normalize_date_str(item.get("deadline"))
        item["deadline"] = iso or (item.get("deadline") or "")
        # make sure council_slug exists
        if "council_slug" not in item:
            item["council_slug"] = "vic"
        to_add.append(item)
    if not to_add:
        return 0
    df2 = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True) if not df.empty else pd.DataFrame(to_add)
    save_master(df2)
    return len(to_add)

def expire_old_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows with unknown deadline or deadline >= today."""
    if df.empty or "deadline" not in df.columns:
        return df
    df = df.copy()
    df["deadline_iso"] = df["deadline"].apply(normalize_date_str)
    df["_deadline_dt"] = pd.to_datetime(df["deadline_iso"], errors="coerce")
    today = pd.to_datetime(date.today())
    keep_mask = df["_deadline_dt"].isna() | (df["_deadline_dt"] >= today)
    return df[keep_mask].drop(columns=["_deadline_dt"], errors="ignore")

# ----------------- Main: master crawl → split -----------------
if __name__ == "__main__":
    start = datetime.utcnow().isoformat() + "Z"
    print(f"[{start}] Refresh start • data_root={DATA_ROOT} • tenants_file={TENANTS_FILE}")
    with open(TENANTS_FILE, "r") as f:
        TENANTS = yaml.safe_load(f) or {}
    slugs = list(TENANTS.keys())
    print("Tenants:", slugs)

    # 1) Statewide once
    total_added = 0
    if "vic" in TENANTS:
        sw_sources = TENANTS["vic"].get("sources", [])
        rows = scrape_sources(sw_sources)
        for r in rows:
            r["council_slug"] = "vic"
        added = merge_into_master(rows)
        total_added += added
        print(f"[vic] added {added}")
    else:
        print("[vic] no statewide tenant found; skipping.")

    # 2) Each council once
    for slug, cfg in TENANTS.items():
        if slug == "vic":
            continue
        sources = cfg.get("sources", [])
        try:
            rows = scrape_sources(sources)
            for r in rows:
                r["council_slug"] = slug
            added = merge_into_master(rows)
            total_added += added
            print(f"[{slug}] added {added}")
        except Exception as e:
            print(f"[{slug}] ERROR:", repr(e))

    print(f"Master merge complete. Total added this run: {total_added}")

    # 3) Split master to per-council CSVs
    master = ensure_df(MASTER_CSV, MASTER_COLUMNS)

    for slug in TENANTS.keys():
        try:
            if slug == "vic":
                council_df = master[master["council_slug"] == "vic"].copy()
            else:
                council_df = master[master["council_slug"] == slug].copy()
                if INCLUDE_STATEWIDE_IN_EACH:
                    council_df = pd.concat([council_df, master[master["council_slug"] == "vic"]], ignore_index=True)
                # dedupe by link
                council_df = council_df.drop_duplicates(subset=["link"], keep="last")

            before = len(council_df)
            council_df = expire_old_rows(council_df)

            save_tenant_csv(slug, council_df)
            print(f"[{slug}] wrote {len(council_df)} rows (expired {before - len(council_df)})")
        except Exception as e:
            print(f"[{slug}] SPLIT ERROR:", repr(e))

    print("Done.")
