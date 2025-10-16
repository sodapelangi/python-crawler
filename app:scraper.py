# app/scraper.py
import os, re, time, hashlib, json
from datetime import datetime
from urllib.parse import urljoin, urlencode
import requests
from bs4 import BeautifulSoup

BASE = "https://peraturan.bpk.go.id"
HEADERS = {"User-Agent": "RegwatchService/0.1 (polite scraper; contact: noviana.banafitrah@gmail.com)"}

# ---------- small utils ----------
def _clean(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "")).strip()

def sleep_rate(rate_per_sec: float):
    time.sleep(max(0.001, 1.0 / max(rate_per_sec, 0.1)))

def fetch(url: str, rate: float = 1.5) -> requests.Response:
    sleep_rate(rate)
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

MONTHS_ID = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "nopember": 11, "november": 11, "desember": 12
}
def parse_date_id(s: str):
    s = _clean(s)
    if not s: return None
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", s, flags=re.I)
    if m:
        d, month_name, y = m.groups()
        mm = MONTHS_ID.get(month_name.lower())
        if mm: return f"{int(y):04d}-{mm:02d}-{int(d):02d}"
    m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        y, mm, d = m.groups()
        return f"{int(y):04d}-{int(mm):02d}-{int(d):02d}"
    m = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", s)
    if m:
        d, mm, y = m.groups()
        return f"{int(y):04d}-{int(mm):02d}-{int(d):02d}"
    return None

def normalize_status(s: str) -> str | None:
    if not s: return None
    t = _clean(s).lower()
    if "tidak berlaku sebagian" in t or "dicabut sebagian" in t: return "TIDAK_BERLAKU_SEBAGIAN"
    if "dicabut" in t or "tidak berlaku" in t: return "DICABUT"
    if "diubah" in t or "perubahan" in t: return "DIUBAH"
    if "berlaku" in t or "masih berlaku" in t: return "BERLAKU"
    return None

LN_RE  = re.compile(r"LN\s*(\d{4})\s*\(([^)]+)\)", re.I)
TLN_RE = re.compile(r"TLN\s*\(([^)]+)\)", re.I)

def local_pdf_path(outdir: str, jenis: str, tahun: int, nomor: str) -> str:
    nomor_safe = re.sub(r"[^0-9A-Za-z_-]+", "-", str(nomor)).strip("-").lower()
    return os.path.normpath(os.path.join(outdir, jenis.upper(), str(tahun), f"{nomor_safe}.pdf"))

def download_pdf_to_local(pdf_url: str, dest_path: str, rate: float = 1.5) -> dict:
    if not pdf_url: return {"ok": False, "error": "No PDF URL"}
    sleep_rate(rate)
    r = requests.get(pdf_url, headers=HEADERS, timeout=90)
    if r.status_code != 200: return {"ok": False, "error": f"HTTP {r.status_code}"}
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f: f.write(r.content)
    return {"ok": True, "sha256": sha256_bytes(r.content), "bytes": len(r.content), "path": dest_path}

# ---------- parser helpers ----------
def find_card_by_heading_text(soup: BeautifulSoup, words: list[str]) -> BeautifulSoup | None:
    for body in soup.select(".card .card-body"):
        hdr = body.find(["h3","h4"])
        if not hdr: continue
        txt = _clean(hdr.get_text(" ", strip=True)).lower()
        if all(w.lower() in txt for w in words):
            return body
    return None

# ---------- detail page parser ----------
def parse_detail_page(html: str, url: str, debug: bool = False) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "url_detail": url,
        "jenis": None, "nomor": None, "tahun": None,
        "judul": None, "tentang": "",
        "status_raw": None, "status": None,
        "issuer": None,
        "penetapan_date": None, "pengundangan_date": None, "berlaku_date": None,
        "lokasi": None, "bidang": None,
        "ln": "", "tln": "",
        "relations": {"MENCABUT": [], "DICABUT_OLEH": [], "MENGUBAH": [], "DIUBAH_OLEH": []},
        "pdf_url": None,
        "_debug_rows": [],
        "_debug_found_cards": []
    }

    # Title + tentang
    h = soup.find(["h1","h2"])
    if h: data["judul"] = _clean(h.get_text(" ", strip=True))
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and not data["tentang"]: data["tentang"] = meta_desc.get("content") or ""

    # PDF
    a_pdf = soup.select_one('a.download-file[href*=".pdf"]') or soup.find("a", href=re.compile(r"\.pdf($|\?)", re.I))
    if a_pdf and a_pdf.get("href"): data["pdf_url"] = urljoin(url, a_pdf["href"])

    # METADATA
    meta_card = find_card_by_heading_text(soup, ["metadata", "peraturan"])
    if meta_card:
        data["_debug_found_cards"].append("metadata")
        for row in meta_card.select(".row"):
            label_el = row.select_one(".col-lg-3.fw-bold")
            value_el = row.select_one(".col-lg-9")
            if not label_el or not value_el: continue
            label = _clean(label_el.get_text(" ", strip=True)).lower()
            value = _clean(value_el.get_text(" ", strip=True))
            if not label or not value: continue
            if debug: data["_debug_rows"].append({"label": label, "value": value})

            if label == "judul":
                if not data["judul"]: data["judul"] = value
                m = re.search(r"(uu|pp|perpres|permen|perda)[^\d]*([0-9a-zA-Z]+)\s*tahun\s*(\d{4})", value, re.I)
                if m:
                    data["jenis"] = data["jenis"] or m.group(1).upper()
                    data["nomor"] = data["nomor"] or m.group(2)
                    try: data["tahun"] = data["tahun"] or int(m.group(3))
                    except: pass
                continue
            if label in ("t.e.u.", "teu"):
                data["issuer"] = data["issuer"] or value; continue
            if label == "nomor":
                data["nomor"] = data["nomor"] or value; continue
            if label == "bentuk singkat":
                if not data["jenis"]: data["jenis"] = value.upper(); continue
            if label == "tahun":
                try: data["tahun"] = data["tahun"] or int(value)
                except: pass
                continue
            if label == "tempat penetapan":
                data["lokasi"] = data["lokasi"] or value; continue
            if label == "tanggal penetapan":
                data["penetapan_date"] = parse_date_id(value) or data["penetapan_date"]; continue
            if label == "tanggal pengundangan":
                data["pengundangan_date"] = parse_date_id(value) or data["pengundangan_date"]; continue
            if label == "tanggal berlaku":
                data["berlaku_date"] = parse_date_id(value) or data["berlaku_date"]; continue
            if label in ("subjek", "bidang"):
                data["bidang"] = data["bidang"] or value; continue
            if label == "status":
                data["status_raw"] = value; continue
            if label == "sumber":
                m_ln = LN_RE.search(value)
                if m_ln:
                    yr, num = m_ln.groups()
                    data["ln"] = f"LN {yr} ({num})"
                m_tln = TLN_RE.search(value)
                if m_tln:
                    data["tln"] = f"TLN ({m_tln.group(1)})"
                if not data["ln"] and "ln" in value.lower(): data["ln"] = value
                if not data["tln"] and "tln" in value.lower(): data["tln"] = value
                continue
            if label == "lokasi":
                data["lokasi"] = data["lokasi"] or value; continue

    # STATUS PERATURAN (relations)
    status_card = find_card_by_heading_text(soup, ["status", "peraturan"])
    if status_card:
        data["_debug_found_cards"].append("status")
        container = status_card.select_one(".container.fs-6") or status_card
        current_section = None
        for row in container.select(":scope > .row"):
            sec = row.select_one(".fw-semibold")
            if sec:
                head = _clean(sec.get_text(" ", strip=True)).lower()
                if "mengubah" in head: current_section = "MENGUBAH"
                elif "dicabut oleh" in head: current_section = "DICABUT_OLEH"
                elif "diubah oleh" in head: current_section = "DIUBAH_OLEH"
                elif "mencabut" in head: current_section = "MENCABUT"
                else: current_section = None
                continue
            for li in row.select("ol li"):
                a = li.find("a")
                text = _clean(a.get_text(" ", strip=True) if a else li.get_text(" ", strip=True))
                href = urljoin(url, a["href"]) if (a and a.get("href")) else None
                rtype = current_section or "MENGUBAH"
                data["relations"][rtype].append({"text": text, "url": href})

    # fallback jenis/nomor/tahun from URL
    if not (data.get("jenis") and data.get("nomor") and data.get("tahun")):
        m2 = re.search(r"/details/\d+/(uu|pp|perpres|permen|perda)-no-([0-9a-zA-Z]+)-tahun-(\d{4})", url, re.I)
        if m2:
            data["jenis"] = data["jenis"] or m2.group(1).upper()
            data["nomor"] = data["nomor"] or m2.group(2)
            data["tahun"] = data["tahun"] or int(m2.group(3))

    data["status"] = normalize_status(data.get("status_raw"))
    return data

# ---------- high-level ops ----------
def run_once(url: str, rate: float = 1.5, download_pdf: bool = False, outdir: str = "./downloads", debug: bool = False) -> dict:
    resp = fetch(url, rate=rate)
    data = parse_detail_page(resp.text, url, debug=debug)
    if download_pdf:
        if data.get("jenis") and data.get("tahun") and data.get("nomor"):
            p = local_pdf_path(outdir, data["jenis"], int(data["tahun"]), str(data["nomor"]))
        else:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            p = os.path.join(outdir, "misc", f"doc_{stamp}.pdf")
        data["pdf_local"] = download_pdf_to_local(data.get("pdf_url"), p, rate=rate)
    # strip debug keys for API cleanliness
    data.pop("_debug_rows", None); data.pop("_debug_found_cards", None)
    return data

# ---- search crawling (with years & jenis) ----
DEFAULT_YEARS = [2025, 2024, 2023]
DEFAULT_JENIS_IDS = [8, 10, 11, 19]
DEFAULT_SEARCH_URL = f"{BASE}/Search"

def build_search_url(base: str, jenis_ids: list[int], years: list[int], page: int = 1) -> str:
    qs = [("keywords",""), ("tentang",""), ("nomor","")]
    for y in years: qs.append(("tahun", str(y)))
    for j in jenis_ids: qs.append(("jenis", str(j)))
    if page > 1: qs.append(("page", str(page)))
    return f"{base}?{urlencode(qs)}"

def extract_detail_links_from_search(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"^/Details/\d+/", href, re.I):
            links.append(urljoin(base_url, href))
    # de-dup keep order
    seen, out = set(), []
    for u in links:
        if u in seen: continue
        seen.add(u); out.append(u)
    return out

def crawl_collect(max_items: int = 10, jenis_ids: list[int] | None = None, years: list[int] | None = None, rate: float = 1.5, download_pdf: bool = False) -> list[dict]:
    jenis_ids = jenis_ids or DEFAULT_JENIS_IDS
    years = years or DEFAULT_YEARS
    page, total = 1, 0
    results = []
    while total < max_items:
        url = build_search_url(DEFAULT_SEARCH_URL, jenis_ids, years, page=page)
        try:
            resp = fetch(url, rate=rate)
        except Exception:
            break
        detail_links = extract_detail_links_from_search(resp.text, BASE)
        if not detail_links:
            break
        for link in detail_links:
            results.append(run_once(link, rate=rate, download_pdf=download_pdf, outdir="./downloads", debug=False))
            total += 1
            if total >= max_items:
                break
        page += 1
    return results
