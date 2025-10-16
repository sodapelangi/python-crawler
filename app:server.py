# app/server.py
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app.scraper import run_once, crawl_collect

app = FastAPI(title="Regwatch Scraper API", version="0.1")

# to call the API from the browser.
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ingest")
def ingest(url: str = Query(..., description="Detail URL from peraturan.bpk.go.id")):
    return run_once(url=url, rate=1.5, download_pdf=False, outdir="./downloads", debug=False)

class CrawlBody(BaseModel):
    max_items: int = 10
    years: list[int] | None = None        # e.g., [2025,2024,2023]
    jenis_ids: list[int] | None = None    # e.g., [8,10,11,19]
    download_pdf: bool = False

@app.post("/crawl")
def crawl(body: CrawlBody):
    # guardrails for cost/safety
    max_items = min(max(1, body.max_items), 50)
    items = crawl_collect(
        max_items=max_items,
        years=body.years,
        jenis_ids=body.jenis_ids,
        rate=1.0,
        download_pdf=body.download_pdf
    )
    return {"items": items}
