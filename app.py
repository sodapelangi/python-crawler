#!/usr/bin/env python3
"""
Flask API for BPK Regulation Crawler
Provides REST endpoints for crawling regulations and storing in Supabase
"""

import os
import uuid
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client
from dotenv import load_dotenv

import bpk_scraper

load_dotenv()

app = Flask(__name__)
CORS(app)

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

STORAGE_BUCKET = "regulations"

# --------------------- Helper Functions ---------------------

def check_regulation_exists(jenis: str, nomor: str, tahun: int) -> bool:
    """Check if regulation already exists in database"""
    try:
        result = supabase.table("regulations_metadata") \
            .select("id") \
            .eq("jenis", jenis) \
            .eq("nomor", nomor) \
            .eq("tahun", tahun) \
            .maybeSingle() \
            .execute()
        return result.data is not None
    except Exception as e:
        print(f"Error checking regulation existence: {e}")
        return False

def upload_to_storage(file_bytes: bytes, file_path: str, content_type: str) -> str:
    """Upload file to Supabase storage and return public URL"""
    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(
            file_path,
            file_bytes,
            file_options={"content-type": content_type, "upsert": "true"}
        )

        # Get public URL
        public_url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(file_path)
        return public_url
    except Exception as e:
        print(f"Error uploading to storage: {e}")
        raise

def insert_regulation_metadata(data: Dict[str, Any], pdf_storage_path: str, txt_storage_path: str) -> str:
    """Insert regulation metadata into database and return regulation_id"""
    try:
        metadata = {
            "bpk_detail_url": data["url_detail"],
            "jenis": data.get("jenis"),
            "nomor": data.get("nomor"),
            "tahun": data.get("tahun"),
            "judul": data.get("judul"),
            "tentang": data.get("tentang", ""),
            "status_raw": data.get("status_raw"),
            "status": data.get("status"),
            "issuer": data.get("issuer"),
            "penetapan_date": data.get("penetapan_date"),
            "pengundangan_date": data.get("pengundangan_date"),
            "berlaku_date": data.get("berlaku_date"),
            "lokasi": data.get("lokasi"),
            "bidang": data.get("bidang"),
            "ln": data.get("ln", ""),
            "tln": data.get("tln", ""),
            "pdf_url": data.get("pdf_url"),
            "pdf_storage_path": pdf_storage_path,
            "txt_storage_path": txt_storage_path,
            "updated_at": datetime.utcnow().isoformat()
        }

        result = supabase.table("regulations_metadata").insert(metadata).execute()
        regulation_id = result.data[0]["id"]

        # Insert relations
        for relation_type, relations in data.get("relations", {}).items():
            for rel in relations:
                supabase.table("regulation_relations").insert({
                    "regulation_id": regulation_id,
                    "relation_type": relation_type,
                    "related_regulation_text": rel.get("text", ""),
                    "related_regulation_url": rel.get("url")
                }).execute()

        return regulation_id
    except Exception as e:
        print(f"Error inserting regulation metadata: {e}")
        raise

def update_crawl_job(job_id: str, updates: Dict[str, Any]):
    """Update crawl job status and progress"""
    try:
        supabase.table("crawl_jobs").update(updates).eq("id", job_id).execute()
    except Exception as e:
        print(f"Error updating crawl job: {e}")

def add_error_to_job(job_id: str, error_msg: str, detail_url: Optional[str] = None):
    """Add error to job error_log"""
    try:
        result = supabase.table("crawl_jobs").select("error_log").eq("id", job_id).single().execute()
        error_log = result.data.get("error_log", [])
        error_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "message": error_msg,
            "url": detail_url
        })
        supabase.table("crawl_jobs").update({"error_log": error_log}).eq("id", job_id).execute()
    except Exception as e:
        print(f"Error adding error to job log: {e}")

def process_single_regulation(detail_url: str, job_id: str, rate: float = 1.5) -> Dict[str, Any]:
    """Process a single regulation: scrape, convert PDF, upload, store metadata"""
    try:
        # Scrape regulation details
        data = bpk_scraper.scrape_regulation(detail_url, rate=rate)

        # Check if already exists
        jenis = data.get("jenis")
        nomor = data.get("nomor")
        tahun = data.get("tahun")

        if not (jenis and nomor and tahun):
            return {
                "status": "error",
                "message": "Missing required fields (jenis, nomor, or tahun)",
                "url": detail_url
            }

        if check_regulation_exists(jenis, nomor, tahun):
            return {
                "status": "skipped",
                "message": "Regulation already exists",
                "url": detail_url,
                "jenis": jenis,
                "nomor": nomor,
                "tahun": tahun
            }

        # Download and convert PDF
        pdf_url = data.get("pdf_url")
        if not pdf_url:
            return {
                "status": "error",
                "message": "No PDF URL found",
                "url": detail_url
            }

        pdf_bytes = bpk_scraper.download_pdf(pdf_url, rate=rate)
        txt_content = bpk_scraper.convert_pdf_bytes_to_text(pdf_bytes)

        # Upload files to storage
        pdf_path = f"{jenis}/{tahun}/{nomor}.pdf"
        txt_path = f"{jenis}/{tahun}/{nomor}.md"

        upload_to_storage(pdf_bytes, pdf_path, "application/pdf")
        upload_to_storage(txt_content.encode("utf-8"), txt_path, "text/markdown")

        # Insert metadata into database
        regulation_id = insert_regulation_metadata(data, pdf_path, txt_path)

        return {
            "status": "success",
            "message": "Regulation processed successfully",
            "url": detail_url,
            "regulation_id": regulation_id,
            "jenis": jenis,
            "nomor": nomor,
            "tahun": tahun
        }

    except Exception as e:
        error_msg = f"Error processing regulation: {str(e)}"
        print(f"{error_msg}\n{traceback.format_exc()}")
        add_error_to_job(job_id, error_msg, detail_url)
        return {
            "status": "error",
            "message": error_msg,
            "url": detail_url
        }

# --------------------- API Endpoints ---------------------

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

@app.route("/api/crawl", methods=["POST"])
def start_crawl():
    """
    Start a new crawl job
    Body: {
        "max_items": 50,
        "years": [2025, 2024, 2023],
        "jenis_ids": [8, 10, 11, 19],
        "created_by": "user_uuid"
    }
    """
    try:
        data = request.get_json()
        max_items = data.get("max_items", 50)
        years = data.get("years", [2025, 2024, 2023])
        jenis_ids = data.get("jenis_ids", [8, 10, 11, 19])
        created_by = data.get("created_by")
        rate = data.get("rate", 1.5)

        if not created_by:
            return jsonify({"error": "created_by is required"}), 400

        # Create crawl job
        job_data = {
            "status": "pending",
            "parameters": {
                "max_items": max_items,
                "years": years,
                "jenis_ids": jenis_ids
            },
            "total_items": max_items,
            "items_crawled": 0,
            "items_skipped": 0,
            "created_by": created_by,
            "started_at": datetime.utcnow().isoformat()
        }

        result = supabase.table("crawl_jobs").insert(job_data).execute()
        job_id = result.data[0]["id"]

        # Update status to running
        update_crawl_job(job_id, {"status": "running"})

        # Start crawling
        items_crawled = 0
        items_skipped = 0

        for detail_url in bpk_scraper.crawl_search_results(max_items, jenis_ids, years, rate=rate):
            result = process_single_regulation(detail_url, job_id, rate=rate)

            if result["status"] == "success":
                items_crawled += 1
            elif result["status"] == "skipped":
                items_skipped += 1

            # Update progress
            update_crawl_job(job_id, {
                "items_crawled": items_crawled,
                "items_skipped": items_skipped
            })

        # Mark job as completed
        update_crawl_job(job_id, {
            "status": "completed",
            "completed_at": datetime.utcnow().isoformat(),
            "items_crawled": items_crawled,
            "items_skipped": items_skipped
        })

        return jsonify({
            "job_id": job_id,
            "status": "completed",
            "items_crawled": items_crawled,
            "items_skipped": items_skipped
        })

    except Exception as e:
        error_msg = f"Crawl failed: {str(e)}"
        print(f"{error_msg}\n{traceback.format_exc()}")

        if 'job_id' in locals():
            update_crawl_job(job_id, {
                "status": "failed",
                "completed_at": datetime.utcnow().isoformat()
            })
            add_error_to_job(job_id, error_msg)

        return jsonify({"error": error_msg}), 500

@app.route("/api/crawl/jobs/<job_id>", methods=["GET"])
def get_crawl_job_status(job_id: str):
    """Get status of a specific crawl job"""
    try:
        result = supabase.table("crawl_jobs").select("*").eq("id", job_id).single().execute()
        return jsonify(result.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

@app.route("/api/crawl/jobs", methods=["GET"])
def list_crawl_jobs():
    """List all crawl jobs with pagination"""
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 20))
        offset = (page - 1) * limit

        result = supabase.table("crawl_jobs") \
            .select("*") \
            .order("created_at", desc=True) \
            .range(offset, offset + limit - 1) \
            .execute()

        return jsonify({
            "jobs": result.data,
            "page": page,
            "limit": limit
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
