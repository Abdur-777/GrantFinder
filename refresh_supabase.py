import os, time, re, hashlib
from datetime import datetime
from typing import List, Dict, Optional

import requests, pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from urllib.parse import urljoin
import yaml
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

FOLLOW_DETAILS = int(os.getenv("FOLLOW_DETAILS", "25"))
SUMMARIZE = os.getenv("SUMMARIZE", "0") == "1"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TENANTS_FILE = os.getenv("TENANTS_FILE", "tenants.yaml")

with open(TENANTS_FILE, "r") as f:
    TENANTS = yaml.safe_load(f)

def sha16(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode()).hexdigest()[:16]

def normalize_date_str(s: Optional[str]) -> Optional[str]:
    if not s or not str(s).strip(): return None
    try:
        return dateparse.parse(str(s), dayfirst=True, fuzzy=True).date().isoformat()
    except Exception:
        return None

def safe_get(url: str, timeout=20) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "GrantFinderBot/1.0"})
        if r.ok: return r.text
    except Exception: pass
    return None

def first_text(soup: BeautifulSoup, sels: list[str]) -> str:
    for sel in sels:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(" ", strip=True)
            if t: return t
    return ""

def meta_content(soup: BeautifulSoup) -> str:
    for name in ["description", "og:description"]:
        el = soup.select_one(f'meta[name="{name}"], meta[property="{name}"]')
        if el and el.get("content"): return el["content"].strip()
    return ""

AMOUNT_RX = re.compile(r"\$\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\$\s?\d+[KkMm]?", re.I)
DEADLINE_RX = re.compile(r"(deadline|close[s]?|closing date|apply by)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", re.I)

def extract_amount(text: str) -> str:
    m = AMOUNT_RX.search(text or ""); return m.group(0) if m else ""

def extract_deadline(text: str) -> str:
    m = DEADLINE_RX.search(text or ""); return m.group(0) if m else ""

def fetch_detail(link: str) -> dict:
    html = safe_get(link)
    if not html: return {}
    s = BeautifulSoup(html, "html.parser")
    title = first_text(s, ["h1","h2"]) or ""
    meta = meta_content(s)
    desc = meta or first_text(s, ["article p","main p",".content p",".rich-text p","p"])
    gist = f"{title} {desc}"
    return {
        "title": title.strip(),
        "description": (desc or "").strip(),
        "amount": extract_amount(gist),
        "deadline": extract_deadline(gist),
    }

def ai_summarize(title: str, description: str) -> str:
    if not (OPENAI_API_KEY and SUMMARIZE): return ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":
                "Summarize in 2 short sentences (who it's for + key benefit). "
                f"Title: {title}\nDescription: {description[:1500]}"}],
            temperature=0.2, max_tokens=140,
        )
        return (r.choices[0].message.content or "").strip()
    except Exception:
        return ""

KEYWORDS = ("grant","fund","funding","program","round","apply")

def near_text(a: BeautifulSoup, selector: str) -> str:
    n = a.find_next(selector)
    return (n.get_text(" ", strip=True) if n else "")[:600]

def parse_generic(html: str, base_url: str) -> List[Dict]:
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items, seen = [], set()
    for a in soup.select("a"):
        title = (a.get_text() or "").strip()
        href = (a.get("href") or "").strip()
        if not title or not href: continue
        low = title.lower()
        if not any(k in low for k in KEYWORDS) and ("grant" not in href.lower() and "fund" not in href.lower()):
            continue
        link = href if href.startswith("http") else urljoin(base_url, href)
        if link in seen: continue
        seen.add(link)
        items.append({"title": title, "description": near_text(a,"p") or near_text(a,"li"),
                      "amount":"", "deadline":"", "link": link, "source": base_url, "summary": ""})
    for r in items[:max(0, FOLLOW_DETAILS)]:
        try:
            d = fetch_detail(r["link"])
            if d.get("title"): r["title"] = d["title"]
            if d.get("description"): r["description"] = d["description"]
            if d.get("amount"): r["amount"] = d["amount"]
            if not r["deadline"] and d.get("deadline"): r["deadline"] = d["deadline"]
            s = ai_summarize(r["title"], r["description"])
            if s: r["summary"] = s
            time.sleep(0.15)
        except Exception:
            pass
    return items

def scrape_sources(sources: List[str]) -> List[Dict]:
    rows, seen = [], set()
    for src in sources:
        html = safe_get(src)
        for r in parse_generic(html, src):
            if r["link"] not in seen:
                seen.add(r["link"]); rows.append(r)
        time.sleep(0.25)
    return rows

def upsert_grants(rows: List[Dict], council_slug: str) -> int:
    if not rows: return 0
    batch = []
    for r in rows:
        link = (r.get("link") or "").strip()
        if not link: continue
        item = {
            "id": sha16(f"{r.get('title','')}|{link}"),
            "title": r.get("title",""),
            "description": r.get("description",""),
            "amount": r.get("amount",""),
            "deadline": r.get("deadline",""),
            "deadline_iso": normalize_date_str(r.get("deadline")),
            "link": link,
            "source": r.get("source",""),
            "created_at": datetime.utcnow().isoformat()+"Z",
            "summary": r.get("summary",""),
            "council_slug": council_slug,
        }
        batch.append(item)
    if batch:
        sb.table("grants").upsert(batch, on_conflict="id").execute()
    return len(batch)

def set_last_run():
    sb.table("meta").upsert([{"key":"last_run_utc","value": datetime.utcnow().isoformat()+"Z"}], on_conflict="key").execute()

if __name__ == "__main__":
    print("Refresh (Supabase) starting…")
    total = 0
    for slug, cfg in TENANTS.items():
        srcs = cfg.get("sources", [])
        rows = scrape_sources(srcs)
        added = upsert_grants(rows, slug)
        total += added
        print(f"[{slug}] upserted {added}")
    set_last_run()
    print("Done. Total:", total)
