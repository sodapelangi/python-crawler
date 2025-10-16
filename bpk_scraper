#!/usr/bin/env python3
"""
BPK Regulation Scraper with Supabase Integration
Crawls regulations from peraturan.bpk.go.id, converts PDFs to text, and stores in Supabase
"""

import os
import re
import time
import hashlib
import io
from urllib.parse import urljoin, urlencode
from datetime import datetime
from typing import Optional, Dict, List, Any

import requests
import pdfplumber
from bs4 import BeautifulSoup

BASE = "https://peraturan.bpk.go.id"
HEADERS = {"User-Agent": "RegwatchCrawler/1.0 (compliance monitoring; contact: admin@regwatch.id)"}

# --------------------- Utility Functions ---------------------

def _clean(s: str) -> str:
    """Clean whitespace from string"""
    return re.sub(r"\s+", " ", (s or "")).strip()

def sleep_rate(rate_per_sec: float):
    """Rate limiting sleep"""
    time.sleep(max(0.001, 1.0 / max(rate_per_sec, 0.1)))

def fetch(url: str, rate: float = 1.5) -> requests.Response:
    """Fetch URL with rate limiting"""
    sleep_rate(rate)
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r

def sha256_bytes(b: bytes) -> str:
    """Calculate SHA256 hash of bytes"""
    return hashlib.sha256(b).hexdigest()

# --------------------- Date Parsing ---------------------

MONTHS_ID = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "nopember": 11,
    "november": 11, "desember": 12
}

def parse_date_id(s: str) -> Optional[str]:
    """Parse Indonesian date format to ISO format (YYYY-MM-DD)"""
    s = _clean(s)
    if not s:
        return None

    # Format: "1 Januari 2025"
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", s, flags=re.I)
    if m:
        d, month_name, y = m.groups()
        mm = MONTHS_ID.get(month_name.lower())
        if mm:
            return f"{int(y):04d}-{mm:02d}-{int(d):02d}"

    # Format: "2025-01-01"
    m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        y, mm, d = m.groups()
        return f"{int(y):04d}-{int(mm):02d}-{int(d):02d}"

    # Format: "01/01/2025"
    m = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", s)
    if m:
        d, mm, y = m.groups()
        return f"{int(y):04d}-{int(mm):02d}-{int(d):02d}"

    return None

# --------------------- Status Normalization ---------------------

def normalize_status(s: str) -> Optional[str]:
    """Normalize status text to enum values"""
    if not s:
        return None
    t = _clean(s).lower()
    if "tidak berlaku sebagian" in t or "dicabut sebagian" in t:
        return "TIDAK_BERLAKU_SEBAGIAN"
    if "dicabut" in t or "tidak berlaku" in t:
        return "DICABUT"
    if "diubah" in t or "perubahan" in t:
        return "DIUBAH"
    if "berlaku" in t or "masih berlaku" in t:
        return "BERLAKU"
    return None

# --------------------- LN/TLN Parsing ---------------------

LN_RE = re.compile(r"LN\s*(\d{4})\s*\(([^)]+)\)", re.I)
TLN_RE = re.compile(r"TLN\s*\(([^)]+)\)", re.I)

# --------------------- PDF Conversion ---------------------

def convert_pdf_to_markdown(pdf_bytes: bytes) -> str:
    """Convert PDF bytes to markdown format using pdfplumber"""
    try:
        markdown_content = []
        markdown_content.append("# Dokumen Peraturan\n\n")

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                if text:
                    markdown_content.append(f"## Halaman {page_num}\n\n")
                    markdown_content.append(text)
                    markdown_content.append("\n\n---\n\n")

        return "".join(markdown_content)
    except Exception as e:
        raise Exception(f"Failed to convert PDF to markdown: {str(e)}")

# --------------------- Parser Helpers ---------------------

def find_card_by_heading_text(soup: BeautifulSoup, words: List[str]) -> Optional[BeautifulSoup]:
    """Find card element containing specific heading words"""
    for body in soup.select(".card .card-body"):
        hdr = body.find(["h3", "h4"])
        if not hdr:
            continue
        txt = _clean(hdr.get_text(" ", strip=True)).lower()
        if all(w.lower() in txt for w in words):
            return body
    return None

# --------------------- Detail Page Parser ---------------------

def parse_detail_page(html: str, url: str) -> Dict[str, Any]:
    """Parse BPK regulation detail page and extract metadata"""
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "url_detail": url,
        "jenis": None,
        "nomor": None,
        "tahun": None,
        "judul": None,
        "tentang": "",
        "status_raw": None,
        "status": None,
        "issuer": None,
        "penetapan_date": None,
        "pengundangan_date": None,
        "berlaku_date": None,
        "lokasi": None,
        "bidang": None,
        "ln": "",
        "tln": "",
        "relations": {
            "MENCABUT": [],
            "DICABUT_OLEH": [],
            "MENGUBAH": [],
            "DIUBAH_OLEH": []
        },
        "pdf_url": None
    }

    # Extract title
    h = soup.find(["h1", "h2"])
    if h:
        data["judul"] = _clean(h.get_text(" ", strip=True))

    # Extract tentang from meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and not data["tentang"]:
        data["tentang"] = meta_desc.get("content") or ""

    # Extract PDF URL
    a_pdf = soup.select_one('a.download-file[href*=".pdf"]') or soup.find("a", href=re.compile(r"\.pdf($|\?)", re.I))
    if a_pdf and a_pdf.get("href"):
        data["pdf_url"] = urljoin(url, a_pdf["href"])

    # Parse METADATA card
    meta_card = find_card_by_heading_text(soup, ["metadata", "peraturan"])
    if meta_card:
        for row in meta_card.select(".row"):
            label_el = row.select_one(".col-lg-3.fw-bold")
            value_el = row.select_one(".col-lg-9")
            if not label_el or not value_el:
                continue

            label = _clean(label_el.get_text(" ", strip=True)).lower()
            value = _clean(value_el.get_text(" ", strip=True))
            if not label or not value:
                continue

            if label == "judul":
                if not data["judul"]:
                    data["judul"] = value
                # Extract jenis, nomor, tahun from title
                m = re.search(r"(uu|pp|perpres|permen|perda)[^\d]*([0-9a-zA-Z]+)\s*tahun\s*(\d{4})", value, re.I)
                if m:
                    data["jenis"] = data["jenis"] or m.group(1).upper()
                    data["nomor"] = data["nomor"] or m.group(2)
                    try:
                        data["tahun"] = data["tahun"] or int(m.group(3))
                    except:
                        pass
            elif label in ("t.e.u.", "teu"):
                data["issuer"] = data["issuer"] or value
            elif label == "nomor":
                data["nomor"] = data["nomor"] or value
            elif label == "bentuk singkat":
                if not data["jenis"]:
                    data["jenis"] = value.upper()
            elif label == "tahun":
                try:
                    data["tahun"] = data["tahun"] or int(value)
                except:
                    pass
            elif label == "tempat penetapan":
                data["lokasi"] = data["lokasi"] or value
            elif label == "tanggal penetapan":
                data["penetapan_date"] = parse_date_id(value) or data["penetapan_date"]
            elif label == "tanggal pengundangan":
                data["pengundangan_date"] = parse_date_id(value) or data["pengundangan_date"]
            elif label == "tanggal berlaku":
                data["berlaku_date"] = parse_date_id(value) or data["berlaku_date"]
            elif label in ("subjek", "bidang"):
                data["bidang"] = data["bidang"] or value
            elif label == "status":
                data["status_raw"] = value
            elif label == "sumber":
                m_ln = LN_RE.search(value)
                if m_ln:
                    yr, num = m_ln.groups()
                    data["ln"] = f"LN {yr} ({num})"
                m_tln = TLN_RE.search(value)
                if m_tln:
                    data["tln"] = f"TLN ({m_tln.group(1)})"
                if not data["ln"] and "ln" in value.lower():
                    data["ln"] = value
                if not data["tln"] and "tln" in value.lower():
                    data["tln"] = value
            elif label == "lokasi":
                data["lokasi"] = data["lokasi"] or value

    # Parse STATUS PERATURAN card (relations)
    status_card = find_card_by_heading_text(soup, ["status", "peraturan"])
    if status_card:
        container = status_card.select_one(".container.fs-6") or status_card
        current_section = None
        for row in container.select(":scope > .row"):
            sec = row.select_one(".fw-semibold")
            if sec:
                head = _clean(sec.get_text(" ", strip=True)).lower()
                if "mengubah" in head:
                    current_section = "MENGUBAH"
                elif "dicabut oleh" in head:
                    current_section = "DICABUT_OLEH"
                elif "diubah oleh" in head:
                    current_section = "DIUBAH_OLEH"
                elif "mencabut" in head:
                    current_section = "MENCABUT"
                else:
                    current_section = None
                continue

            for li in row.select("ol li"):
                a = li.find("a")
                text = _clean(a.get_text(" ", strip=True) if a else li.get_text(" ", strip=True))
                href = urljoin(url, a["href"]) if (a and a.get("href")) else None
                rtype = current_section or "MENGUBAH"
                data["relations"][rtype].append({"text": text, "url": href})

    # Fallback: extract jenis/nomor/tahun from URL
    if not (data.get("jenis") and data.get("nomor") and data.get("tahun")):
        m2 = re.search(r"/details/\d+/(uu|pp|perpres|permen|perda)-no-([0-9a-zA-Z]+)-tahun-(\d{4})", url, re.I)
        if m2:
            data["jenis"] = data["jenis"] or m2.group(1).upper()
            data["nomor"] = data["nomor"] or m2.group(2)
            data["tahun"] = data["tahun"] or int(m2.group(3))

    data["status"] = normalize_status(data.get("status_raw"))
    return data

# --------------------- Search Crawler ---------------------

DEFAULT_YEARS = [2025, 2024, 2023]
DEFAULT_JENIS_IDS = [8, 10, 11, 19]  # UU, PP, Perpres, Permen
DEFAULT_SEARCH_URL = f"{BASE}/Search"

def build_search_url(base: str, jenis_ids: List[int], years: List[int], page: int = 1) -> str:
    """Build search URL with multiple years and jenis filters"""
    params = [("keywords", ""), ("tentang", ""), ("nomor", "")]
    for y in years:
        params.append(("tahun", str(y)))
    for j in jenis_ids:
        params.append(("jenis", str(j)))
    if page > 1:
        params.append(("page", str(page)))
    return f"{base}?{urlencode(params)}"

def extract_detail_links_from_search(html: str, base_url: str) -> List[str]:
    """Extract regulation detail page URLs from search results"""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"^/Details/\d+/", href, re.I):
            links.append(urljoin(base_url, href))

    # De-duplicate while preserving order
    seen, uniq = set(), []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq

def crawl_search_results(max_items: int, jenis_ids: List[int], years: List[int], rate: float = 1.5):
    """Generator that yields detail page URLs from search results"""
    page = 1
    total = 0
    while total < max_items:
        url = build_search_url(DEFAULT_SEARCH_URL, jenis_ids, years, page=page)
        try:
            resp = fetch(url, rate=rate)
        except Exception as e:
            print(f"Warning: Failed to fetch search page {page}: {e}")
            break

        detail_links = extract_detail_links_from_search(resp.text, BASE)
        if not detail_links:
            break

        for link in detail_links:
            yield link
            total += 1
            if total >= max_items:
                break

        page += 1

# --------------------- Public API ---------------------

def scrape_regulation(url: str, rate: float = 1.5) -> Dict[str, Any]:
    """Scrape a single regulation detail page"""
    resp = fetch(url, rate=rate)
    return parse_detail_page(resp.text, url)

def download_pdf(pdf_url: str, rate: float = 1.5) -> bytes:
    """Download PDF file and return bytes"""
    if not pdf_url:
        raise ValueError("No PDF URL provided")

    sleep_rate(rate)
    r = requests.get(pdf_url, headers=HEADERS, timeout=90)
    r.raise_for_status()
    return r.content

def convert_pdf_bytes_to_text(pdf_bytes: bytes) -> str:
    """Convert PDF bytes to markdown text"""
    return convert_pdf_to_markdown(pdf_bytes)
