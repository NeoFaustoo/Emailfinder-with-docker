#!/usr/bin/env python3
"""
Email Scraper API Application
Simplified FastAPI-based REST API for email discovery
"""

import os
import logging
import sys
import asyncio
import time
import json
import csv
import uuid
import zipfile
from typing import List, Optional, Dict, Any
from pathlib import Path
import shutil
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn
from starlette.middleware.gzip import GZipMiddleware

# Import our email scraper
from enhanced_email_scraper import (
    scrape_companies_batch,
    scrape_single_company, 
    process_file_and_update,
    process_large_dataset,
    EmailScraper
)

# Configure logging for production
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
is_production = os.getenv('ENVIRONMENT', 'development') == 'production'

# Production-ready logging configuration
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/app.log') if Path('logs').exists() else logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set specific log levels for production
if is_production:
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('fastapi').setLevel(logging.WARNING)

# Initialize FastAPI app with production settings
app = FastAPI(
    title="Email Scraper API",
    description="Production-ready REST API for email discovery with detailed worker logging",
    version="2.0.0",
    docs_url="/docs" if not is_production else None,  # Disable docs in production
    redoc_url="/redoc" if not is_production else None,  # Disable redoc in production
)

# Enable gzip compression
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create required directories
UPLOAD_DIR = Path("uploads")
RESULTS_DIR = Path("results")

for directory in [UPLOAD_DIR, RESULTS_DIR]:
    directory.mkdir(exist_ok=True)

# Global job storage
active_jobs: Dict[str, Dict] = {}
job_logs: Dict[str, List[Dict]] = {}

# Pydantic models
class JobRequest(BaseModel):
    file_path: str = Field(..., description="Direct path to the folder to process")
    workers: int = Field(default=150, ge=1, le=500, description="Number of worker threads")
    batch_size: int = Field(default=100, ge=10, le=2000, description="Batch size for processing")
    verbose: bool = Field(default=False, description="Enable verbose logging")
    row_limit: Optional[int] = Field(default=None, ge=1, description="Maximum number of rows to process")

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    total_files: int

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    total_processed: int
    total_emails: int
    start_time: float
    end_time: Optional[float] = None
    errors: List[str] = []
    files_processed: List[str] = []
    job_type: Optional[str] = None
    folder_name: Optional[str] = None
    total_files: Optional[int] = None

class ProcessingStats(BaseModel):
    total_jobs: int
    active_jobs: int
    completed_jobs: int
    failed_jobs: int
    total_emails_found: int

class LogEntry(BaseModel):
    timestamp: float
    level: str
    message: str
    details: Optional[Dict[str, Any]] = None

class JobLogs(BaseModel):
    job_id: str
    logs: List[LogEntry]
    total_count: int

# Utility functions
def validate_file_extension(filename: str) -> bool:
    """Validate file extension."""
    allowed_extensions = {'.csv', '.xlsx', '.xls', '.ndjson', '.json', '.zip'}
    return Path(filename).suffix.lower() in allowed_extensions

def is_processable_file(filename: str) -> bool:
    """Check if file can be processed directly (not ZIP)."""
    processable_extensions = {'.csv', '.xlsx', '.xls', '.ndjson', '.json'}
    return Path(filename).suffix.lower() in processable_extensions

async def save_uploaded_file(upload_file: UploadFile, job_id: str) -> Path:
    """Save uploaded file and return the path."""
    safe_filename = f"{job_id}_{upload_file.filename}"
    file_path = UPLOAD_DIR / safe_filename
    
    with open(file_path, "wb") as buffer:
        content = await upload_file.read()
        buffer.write(content)
    
    return file_path

def create_job_id() -> str:
    """Create a unique job ID."""
    return f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"

def extract_zip_file(zip_path: Path, extract_to: Path) -> List[Path]:
    """Extract zip file and return list of extracted file paths."""
    extracted_files = []
    try:
        logger.info(f"Extracting ZIP file: {zip_path} to {extract_to}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of files in ZIP
            file_list = zip_ref.namelist()
            logger.info(f"ZIP contains {len(file_list)} files")
            
            # Extract all files
            zip_ref.extractall(extract_to)
            logger.info(f"Extracted all files to {extract_to}")
            
            # Find processable files
            all_extracted = list(extract_to.rglob('*'))
            logger.info(f"Found {len(all_extracted)} total extracted items")
            
            for file_path in all_extracted:
                if file_path.is_file():
                    logger.info(f"Checking file: {file_path} (extension: {file_path.suffix.lower()})")
                    if is_processable_file(str(file_path)):
                        extracted_files.append(file_path)
                        logger.info(f"Added processable file: {file_path}")
                    else:
                        logger.warning(f"Skipping non-processable file: {file_path}")
                        
        logger.info(f"Total processable files found: {len(extracted_files)}")
        return extracted_files
        
    except Exception as e:
        logger.error(f"Error extracting zip file {zip_path}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def add_job_log(job_id: str, level: str, message: str, details: Optional[Dict[str, Any]] = None):
    """Add a log entry for a specific job."""
    if job_id not in job_logs:
        job_logs[job_id] = []
    
    log_entry = {
        "timestamp": time.time(),
        "level": level,
        "message": message,
        "details": details or {}
    }
    
    job_logs[job_id].append(log_entry)
    
    # Keep only last 100 log entries per job
    if len(job_logs[job_id]) > 100:
        job_logs[job_id] = job_logs[job_id][-100:]

def load_companies_from_file(file_path: str) -> List[Dict]:
    """Load companies from various file formats."""
    companies = []
    file_ext = file_path.lower().split('.')[-1]
    
    try:
        if file_ext == 'json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    companies = data
                else:
                    companies = [data]
        
        elif file_ext == 'ndjson':
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        companies.append(json.loads(line.strip()))
        
        elif file_ext == 'csv':
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                companies = list(reader)
        
        elif file_ext in ['xlsx', 'xls']:
            try:
                import pandas as pd
                df = pd.read_excel(file_path)
                companies = df.to_dict('records')
            except ImportError:
                raise HTTPException(status_code=400, detail="pandas is required for Excel files")
        elif file_ext == 'zip':
            # For ZIP files, extract and load from first valid file
            extract_dir = Path(file_path).parent / f"{Path(file_path).stem}_temp_extract"
            extract_dir.mkdir(exist_ok=True)
            extracted_files = extract_zip_file(Path(file_path), extract_dir)
            if not extracted_files:
                raise HTTPException(status_code=400, detail="No valid files found in ZIP archive")
            companies = load_companies_from_file(str(extracted_files[0]))
    
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        raise HTTPException(status_code=400, detail=f"Error loading file: {str(e)}")
    
    return companies

async def process_file_with_scraper(job_id: str, file_path: str, workers: int, batch_size: int, row_limit: Optional[int] = None):
    """Process file using the email scraper."""
    add_job_log(job_id, "INFO", f"Starting processing for {file_path}")
    
    try:
        # Load companies from file
        companies = load_companies_from_file(file_path)
        
        if row_limit and len(companies) > row_limit:
            companies = companies[:row_limit]
            add_job_log(job_id, "INFO", f"Limited to {row_limit} companies")
        
        add_job_log(job_id, "INFO", f"Processing {len(companies)} companies with {workers} workers")
        
        # Process in batches
        all_results = []
        total_processed = 0
        total_emails = 0
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(companies) - 1) // batch_size + 1
            
            add_job_log(job_id, "INFO", f"Processing batch {batch_num}/{total_batches}")
            
            # Update progress
            progress = (i / len(companies))  # Store as decimal (0.0 to 1.0)
            active_jobs[job_id].update({
                "progress": progress,
                "total_processed": total_processed,
                "total_emails": total_emails
            })
            
            # Process batch
            batch_results, batch_stats = await scrape_companies_batch(batch, workers)
            all_results.extend(batch_results)
            
            # Update stats
            total_processed += len(batch_results)
            total_emails += sum(len(r['emails']) for r in batch_results if r['success'])
            
            add_job_log(job_id, "INFO", f"Batch {batch_num} completed: {len(batch_results)} processed")
        
        # Update file with results (in-place)
        from enhanced_email_scraper import update_input_file_with_emails
        update_success = update_input_file_with_emails(file_path, all_results)
        
        # Complete the job
        active_jobs[job_id].update({
            "status": "completed",
            "end_time": time.time(),
            "progress": 1.0,  # Store as decimal (100%)
            "total_processed": total_processed,
            "total_emails": total_emails
        })
        
        add_job_log(job_id, "INFO", f"Processing completed: {total_processed} companies, {total_emails} emails found")
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        active_jobs[job_id].update({
            "status": "failed",
            "end_time": time.time(),
            "errors": [str(e)]
        })
        add_job_log(job_id, "ERROR", f"Processing failed: {str(e)}")

async def process_folder_with_scraper(job_id: str, folder_path: str, workers: int, batch_size: int, row_limit: Optional[int] = None):
    """Process all files in a folder, including extracting ZIP files."""
    try:
        folder = Path(folder_path)
        if not folder.exists():
            raise Exception(f"Folder not found: {folder_path}")
        
        # Get all valid files, including ZIP files
        files_to_process = []
        zip_files_found = []
        
        for file_path in folder.rglob('*'):
            if file_path.is_file():
                if file_path.suffix.lower() == '.zip':
                    zip_files_found.append(file_path)
                elif is_processable_file(str(file_path)):
                    files_to_process.append(file_path)
        
        # Extract ZIP files and add their contents
        for zip_file in zip_files_found:
            add_job_log(job_id, "INFO", f"Extracting ZIP file: {zip_file.name}")
            try:
                extract_dir = zip_file.parent / f"{zip_file.stem}_extracted"
                extract_dir.mkdir(exist_ok=True)
                extracted_files = extract_zip_file(zip_file, extract_dir)
                if extracted_files:
                    files_to_process.extend(extracted_files)
                    add_job_log(job_id, "INFO", f"Extracted {len(extracted_files)} files from {zip_file.name}")
                else:
                    add_job_log(job_id, "WARNING", f"No valid files found in ZIP: {zip_file.name}")
            except Exception as e:
                add_job_log(job_id, "ERROR", f"Failed to extract ZIP {zip_file.name}: {str(e)}")
        
        if not files_to_process:
            raise Exception("No valid files found in folder (including ZIP contents)")
        
        add_job_log(job_id, "INFO", f"Found {len(files_to_process)} files to process")
        
        total_processed = 0
        total_emails = 0
        
        # OPTIMIZATION: Load ALL companies from ALL files first
        all_companies = []
        file_company_mapping = {}  # Track which companies belong to which files
        total_companies = 0
        
        for file_path in files_to_process:
            companies = load_companies_from_file(str(file_path))
            start_idx = len(all_companies)
            all_companies.extend(companies)
            end_idx = len(all_companies)
            file_company_mapping[str(file_path)] = (start_idx, end_idx, companies)
            total_companies += len(companies)
        
        add_job_log(job_id, "INFO", f"Loaded {total_companies} companies from {len(files_to_process)} files")
        
        if row_limit and total_companies > row_limit:
            all_companies = all_companies[:row_limit]
            total_companies = row_limit
            add_job_log(job_id, "INFO", f"Limited to {row_limit} companies")
        
        # OPTIMIZATION: Process ALL companies with ALL workers simultaneously
        all_results = []
        
        # Process in batches across ALL companies from ALL files
        for i in range(0, len(all_companies), batch_size):
            batch = all_companies[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(all_companies) - 1) // batch_size + 1
            
            add_job_log(job_id, "INFO", f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")
            
            # Update progress based on COMPANIES processed, not files
            progress = (i / total_companies)  # Store as decimal (0.0 to 1.0)
            active_jobs[job_id].update({
                "progress": progress,
                "total_processed": total_processed,
                "total_emails": total_emails
            })
            
            # Process batch with ALL workers
            add_job_log(job_id, "INFO", f"Starting batch {batch_num} with {workers} workers processing {len(batch)} companies")
            batch_start_time = time.time()
            
            batch_results, batch_stats = await scrape_companies_batch(batch, workers)
            all_results.extend(batch_results)
            
            # Update totals
            batch_processed = len(batch_results)
            batch_emails = sum(len(r['emails']) for r in batch_results if r['success'])
            total_processed += batch_processed
            total_emails += batch_emails
            
            batch_time = time.time() - batch_start_time
            rate_per_min = (batch_processed / batch_time) * 60 if batch_time > 0 else 0
            
            add_job_log(job_id, "INFO", f"Batch {batch_num} completed: {batch_processed} processed, {batch_emails} emails found in {batch_time:.1f}s ({rate_per_min:.1f} companies/min)")
            
            # Log worker efficiency
            if batch_stats:
                success_rate = (batch_stats.get('successful', 0) / batch_processed) * 100 if batch_processed > 0 else 0
                add_job_log(job_id, "DEBUG", f"Batch {batch_num} stats: {success_rate:.1f}% success rate, {workers} workers utilized")
            
            # Log real-time email discoveries for UI
            emails_found_in_batch = []
            for result in batch_results:
                if isinstance(result, dict) and result.get('success') and result.get('emails'):
                    emails_found_in_batch.append({
                        'company': result.get('company_name', 'Unknown'),
                        'domain': result.get('domain', ''),
                        'emails': result['emails'],
                        'timestamp': time.time()
                    })
            
            if emails_found_in_batch:
                # Store recent emails for real-time display
                if 'recent_emails' not in active_jobs[job_id]:
                    active_jobs[job_id]['recent_emails'] = []
                
                active_jobs[job_id]['recent_emails'].extend(emails_found_in_batch)
                # Keep only last 50 recent email discoveries
                active_jobs[job_id]['recent_emails'] = active_jobs[job_id]['recent_emails'][-50:]
                
                total_new_emails = sum(len(discovery['emails']) for discovery in emails_found_in_batch)
                add_job_log(job_id, "EMAIL_FOUND", f"Found {total_new_emails} new emails from {len(emails_found_in_batch)} companies in batch {batch_num}")
        
        # Update all files with their respective results
        add_job_log(job_id, "INFO", "Updating files with results...")
        from enhanced_email_scraper import update_input_file_with_emails
        for file_path, (start_idx, end_idx, original_companies) in file_company_mapping.items():
            file_results = all_results[start_idx:end_idx]
            if len(file_results) > 0:
                update_input_file_with_emails(file_path, file_results)
                add_job_log(job_id, "INFO", f"Updated {Path(file_path).name} with {len(file_results)} results")
        
        # Complete the job
        active_jobs[job_id].update({
            "status": "completed",
            "end_time": time.time(),
            "progress": 1.0,  # Store as decimal (100%)
            "total_processed": total_processed,
            "total_emails": total_emails
        })
        
        add_job_log(job_id, "INFO", f"Folder processing completed: {len(files_to_process)} files, {total_processed} companies, {total_emails} emails")
        
    except Exception as e:
        logger.error(f"Error processing folder {folder_path}: {e}")
        active_jobs[job_id].update({
            "status": "failed",
            "end_time": time.time(),
            "errors": [str(e)]
        })
        add_job_log(job_id, "ERROR", f"Folder processing failed: {str(e)}")

# API Endpoints

@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "message": "Email Scraper API v2.0 (Simplified)",
        "status": "running",
        "description": "Direct email scraper integration with high performance",
        "endpoints": {
            "process_file": "POST /api/process-file",
            "process_files_zip": "POST /api/process-files-zip", 
            "process_files_folder": "POST /api/process-files-folder",
            "jobs": "GET /api/jobs",
            "job_status": "GET /api/jobs/{job_id}",
            "job_logs": "GET /api/jobs/{job_id}/logs",
            "download": "GET /api/download/{job_id}",
            "health": "GET /api/health",
            "stats": "GET /api/stats"
        }
    }

@app.post("/api/process-file", response_model=JobResponse)
async def process_file(
    file: UploadFile = File(..., description="File to upload and process"),
    workers: int = Form(150, description="Number of worker threads"),
    batch_size: int = Form(100, description="Batch size for processing"),
    verbose: bool = Form(False, description="Enable verbose logging"),
    row_limit: Optional[int] = Form(None, description="Maximum number of rows to process")
):
    """Upload and process a single file."""
    try:
        # Validate file extension
        if not validate_file_extension(file.filename):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file.filename}")
        
        # Validate parameters
        if workers < 1 or workers > 500:
            raise HTTPException(status_code=400, detail="Workers must be between 1 and 500")
        if batch_size < 10 or batch_size > 2000:
            raise HTTPException(status_code=400, detail="Batch size must be between 10 and 2000")
        
        # Create job ID
        job_id = create_job_id()
        
        # Save uploaded file
        file_path = await save_uploaded_file(file, job_id)
        
        # Store job info
        active_jobs[job_id] = {
            "status": "running",
            "file_path": str(file_path),
            "original_filename": file.filename,
            "start_time": time.time(),
            "end_time": None,
            "config": {
                "workers": workers,
                "batch_size": batch_size,
                "verbose": verbose,
                "row_limit": row_limit
            },
            "progress": 0.0,
            "total_processed": 0,
            "total_emails": 0,
            "recent_emails": [],  # Store last 50 email discoveries
            "worker_logs": [],    # Store detailed worker activity logs
            "errors": [],
            "files_processed": [str(file_path)],
            "job_type": "file"
        }
        
        # Add initial log
        add_job_log(job_id, "INFO", f"File uploaded: {file.filename}", 
                   {"file_path": str(file_path), "workers": workers, "batch_size": batch_size})
        
        # Start processing in background
        asyncio.create_task(process_file_with_scraper(job_id, str(file_path), workers, batch_size, row_limit))
        
        return JobResponse(
            job_id=job_id,
            status="running",
            message=f"File uploaded and processing started: {file.filename}",
            total_files=1
        )
        
    except Exception as e:
        logger.error(f"Error in process_file: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/api/process-files-zip", response_model=JobResponse)
async def process_files_zip(
    folder: UploadFile = File(..., description="Zip file containing files to process"),
    workers: int = Form(150, description="Number of worker threads"),
    batch_size: int = Form(100, description="Batch size for processing"),
    verbose: bool = Form(False, description="Enable verbose logging"),
    row_limit: Optional[int] = Form(None, description="Maximum number of rows to process")
):
    """Upload a zip file and process all files in it."""
    try:
        # Validate file extension
        if not folder.filename.lower().endswith('.zip'):
            raise HTTPException(status_code=400, detail="Only ZIP files are supported")
        
        # Create job ID
        job_id = create_job_id()
        
        # Create job directory
        job_dir = UPLOAD_DIR / job_id
        job_dir.mkdir(exist_ok=True)
        
        # Save uploaded zip file
        zip_path = job_dir / folder.filename
        with open(zip_path, "wb") as buffer:
            shutil.copyfileobj(folder.file, buffer)
        
        # Extract zip file
        extract_dir = job_dir / "extracted"
        extract_dir.mkdir(exist_ok=True)
        
        extracted_files = extract_zip_file(zip_path, extract_dir)
        if not extracted_files:
            raise HTTPException(status_code=400, detail="No valid files found in ZIP archive")
        
        # Store job info
        active_jobs[job_id] = {
            "status": "running",
            "file_path": str(extract_dir),
            "start_time": time.time(),
            "end_time": None,
            "config": {
                "workers": workers,
                "batch_size": batch_size,
                "verbose": verbose,
                "row_limit": row_limit
            },
            "progress": 0.0,
            "total_processed": 0,
            "total_emails": 0,
            "recent_emails": [],  # Store last 50 email discoveries
            "worker_logs": [],    # Store detailed worker activity logs
            "errors": [],
            "files_processed": [str(f) for f in extracted_files],
            "job_type": "folder",
            "folder_name": folder.filename,
            "total_files": len(extracted_files)
        }
        
        # Add initial log
        add_job_log(job_id, "INFO", f"ZIP uploaded and extracted: {folder.filename}", 
                   {"files_count": len(extracted_files), "workers": workers})
        
        # Start processing in background
        asyncio.create_task(process_folder_with_scraper(job_id, str(extract_dir), workers, batch_size, row_limit))
        
        return JobResponse(
            job_id=job_id,
            status="running",
            message=f"Started processing {len(extracted_files)} files from {folder.filename}",
            total_files=len(extracted_files)
        )
        
    except Exception as e:
        logger.error(f"Error in process_files_zip: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing ZIP file: {str(e)}")

@app.post("/api/process-files-folder", response_model=JobResponse)
async def process_files_folder(request: JobRequest):
    """Process all files in a folder directly."""
    try:
        # Validate folder exists
        if not os.path.exists(request.file_path):
            raise HTTPException(status_code=404, detail=f"Folder not found: {request.file_path}")
        
        if not os.path.isdir(request.file_path):
            raise HTTPException(status_code=400, detail=f"Path is not a directory: {request.file_path}")
        
        # Create job ID
        job_id = create_job_id()
        
        # Initialize job in active_jobs first (required for logging)
        active_jobs[job_id] = {
            "status": "initializing",
            "file_path": request.file_path,
            "start_time": time.time(),
            "end_time": None,
            "config": {
                "workers": request.workers,
                "batch_size": request.batch_size,
                "verbose": request.verbose,
                "row_limit": request.row_limit
            },
            "recent_emails": [],  # Store last 50 email discoveries
            "worker_logs": [],    # Store detailed worker activity logs
            "progress": 0.0,
            "total_processed": 0,
            "total_emails": 0,
            "errors": [],
            "files_processed": [],
            "job_type": "folder",
            "folder_name": Path(request.file_path).name,
            "total_files": 0
        }
        
        # Get all valid files in the folder, including ZIP extraction
        folder = Path(request.file_path)
        files_to_process = []
        zip_files_found = []
        
        # First pass: collect regular files and ZIP files
        for file_path in folder.rglob('*'):
            if file_path.is_file():
                if file_path.suffix.lower() == '.zip':
                    zip_files_found.append(file_path)
                elif is_processable_file(str(file_path)):
                    files_to_process.append(file_path)
        
        # Extract ZIP files and add their contents
        for zip_file in zip_files_found:
            logger.info(f"Extracting ZIP file: {zip_file}")
            try:
                extract_dir = zip_file.parent / f"{zip_file.stem}_extracted"
                extract_dir.mkdir(exist_ok=True)
                extracted_files = extract_zip_file(zip_file, extract_dir)
                if extracted_files:
                    files_to_process.extend(extracted_files)
                    logger.info(f"Extracted {len(extracted_files)} files from {zip_file.name}")
            except Exception as e:
                logger.error(f"Failed to extract ZIP {zip_file.name}: {str(e)}")
        
        if not files_to_process:
            raise HTTPException(status_code=400, detail="No valid files found in folder (including ZIP contents)")
        
        # Update job info with final file list
        active_jobs[job_id].update({
            "status": "running",
            "files_processed": [str(f) for f in files_to_process],
            "total_files": len(files_to_process)
        })
        
        # Add initial log
        add_job_log(job_id, "INFO", f"Starting folder processing: {request.file_path}", 
                   {"files_count": len(files_to_process), "workers": request.workers})
        
        # Start processing in background
        asyncio.create_task(process_folder_with_scraper(job_id, request.file_path, request.workers, request.batch_size, request.row_limit))
        
        return JobResponse(
            job_id=job_id,
            status="running",
            message=f"Started processing {len(files_to_process)} files from folder: {request.file_path}",
            total_files=len(files_to_process)
        )
        
    except Exception as e:
        logger.error(f"Error in process_files_folder: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating folder processing job: {str(e)}")

@app.get("/api/jobs", response_model=List[JobStatus])
async def get_all_jobs():
    """Get status of all jobs."""
    jobs = []
    for job_id, job_data in active_jobs.items():
        jobs.append(JobStatus(
            job_id=job_id,
            status=job_data["status"],
            progress=job_data["progress"],
            total_processed=job_data["total_processed"],
            total_emails=job_data["total_emails"],
            start_time=job_data["start_time"],
            end_time=job_data.get("end_time"),
            errors=job_data.get("errors", []),
            files_processed=job_data.get("files_processed", []),
            job_type=job_data.get("job_type"),
            folder_name=job_data.get("folder_name"),
            total_files=job_data.get("total_files")
        ))
    return jobs

@app.get("/api/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get status of a specific job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    job_data = active_jobs[job_id]
    return JobStatus(
        job_id=job_id,
        status=job_data["status"],
        progress=job_data["progress"],
        total_processed=job_data["total_processed"],
        total_emails=job_data["total_emails"],
        start_time=job_data["start_time"],
        end_time=job_data.get("end_time"),
        errors=job_data.get("errors", []),
        files_processed=job_data.get("files_processed", []),
        job_type=job_data.get("job_type"),
        folder_name=job_data.get("folder_name"),
        total_files=job_data.get("total_files")
    )

@app.get("/api/jobs/{job_id}/logs", response_model=JobLogs)
async def get_job_logs(job_id: str, limit: int = 100, offset: int = 0):
    """Get logs for a specific job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    logs = job_logs.get(job_id, [])
    total_count = len(logs)
    
    # Apply pagination
    start_idx = offset
    end_idx = min(offset + limit, total_count)
    paginated_logs = logs[start_idx:end_idx]
    
    # Convert to LogEntry format
    log_entries = [
        LogEntry(
            timestamp=log["timestamp"],
            level=log["level"],
            message=log["message"],
            details=log.get("details", {})
        )
        for log in paginated_logs
    ]
    
    return JobLogs(
        job_id=job_id,
        logs=log_entries,
        total_count=total_count
    )

@app.get("/api/stats", response_model=ProcessingStats)
async def get_processing_stats():
    """Get overall processing statistics."""
    total_jobs = len(active_jobs)
    active_jobs_count = sum(1 for job in active_jobs.values() if job["status"] == "running")
    completed_jobs = sum(1 for job in active_jobs.values() if job["status"] == "completed")
    failed_jobs = sum(1 for job in active_jobs.values() if job["status"] == "failed")
    total_emails = sum(job["total_emails"] for job in active_jobs.values())
    
    return ProcessingStats(
        total_jobs=total_jobs,
        active_jobs=active_jobs_count,
        completed_jobs=completed_jobs,
        failed_jobs=failed_jobs,
        total_emails_found=total_emails
    )

@app.get("/api/workers/status")
async def get_worker_status():
    """Get real-time worker status for running jobs."""
    worker_status = {}
    
    for job_id, job_info in active_jobs.items():
        if job_info['status'] == 'running':
            # Get recent logs to show worker activity
            recent_logs = job_logs.get(job_id, [])[-5:]  # Last 5 logs
            
            # Calculate processing rate
            duration = time.time() - job_info.get('start_time', time.time())
            rate = job_info.get('total_processed', 0) / (duration / 60) if duration > 0 else 0
            
            worker_status[job_id] = {
                'job_id': job_id,
                'status': job_info['status'],
                'progress': job_info.get('progress', 0) * 100,
                'companies_processed': job_info.get('total_processed', 0),
                'emails_found': job_info.get('total_emails', 0),
                'processing_rate_per_min': round(rate, 1),
                'duration_seconds': round(duration),
                'recent_activity': [
                    {
                        'timestamp': log['timestamp'],
                        'level': log['level'],
                        'message': log['message']
                    }
                    for log in recent_logs
                ]
            }
    
    return {
        'active_workers': len(worker_status),
        'worker_details': worker_status,
        'timestamp': time.time()
    }

@app.get("/api/jobs/{job_id}/emails/recent")
async def get_recent_emails(job_id: str):
    """Get recent email discoveries for a job in real-time"""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_info = active_jobs[job_id]
    recent_emails = job_info.get('recent_emails', [])
    
    return {
        'job_id': job_id,
        'recent_emails': recent_emails,
        'total_emails': job_info.get('total_emails', 0),
        'total_processed': job_info.get('total_processed', 0),
        'status': job_info.get('status', 'unknown'),
        'timestamp': time.time()
    }

@app.get("/api/jobs/{job_id}/worker-logs")
async def get_worker_logs(job_id: str, limit: int = 50):
    """Get detailed worker activity logs for a job"""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_info = active_jobs[job_id]
    worker_logs = job_info.get('worker_logs', [])
    
    # Return most recent logs
    recent_logs = worker_logs[-limit:] if len(worker_logs) > limit else worker_logs
    
    return {
        'job_id': job_id,
        'worker_logs': recent_logs,
        'total_log_entries': len(worker_logs),
        'status': job_info.get('status', 'unknown'),
        'timestamp': time.time()
    }

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    active_running = len([j for j in active_jobs.values() if j["status"] == "running"])
    
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "active_jobs": active_running,
        "email_scraper_ready": True,
        "version": "2.0.0"
    }

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and clean up associated files."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    # Clean up job directory
    job_dir = UPLOAD_DIR / job_id
    if job_dir.exists():
        try:
            shutil.rmtree(job_dir)
            logger.info(f"Deleted job directory: {job_dir}")
        except Exception as e:
            logger.warning(f"Could not delete job directory {job_dir}: {e}")
    
    # Remove job from active jobs and logs
    del active_jobs[job_id]
    if job_id in job_logs:
        del job_logs[job_id]
    
    return {"message": f"Job {job_id} deleted successfully"}

@app.get("/api/download/{job_id}")
async def download_results(job_id: str):
    """Download processing results for a completed job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")
    
    # Return information about processed files
    result_files = []
    for file_path in job_data["files_processed"]:
        if os.path.exists(file_path):
            result_files.append({
                "filename": os.path.basename(file_path),
                "path": file_path,
                "size": os.path.getsize(file_path)
            })
    
    return {
        "job_id": job_id,
        "status": "completed",
        "files": result_files,
        "total_emails": job_data["total_emails"],
        "total_processed": job_data["total_processed"]
    }

@app.get("/api/download/{job_id}/file")
async def download_result_file(job_id: str):
    """Download the first processed file for a completed job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")
    
    if not job_data["files_processed"]:
        raise HTTPException(status_code=404, detail="No processed files found")
    
    file_path = Path(job_data["files_processed"][0])
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Processed file not found")
    
    # Create download filename
    original_name = job_data.get("original_filename", file_path.name)
    name_parts = original_name.rsplit('.', 1)
    if len(name_parts) == 2:
        download_name = f"{name_parts[0]}_with_emails.{name_parts[1]}"
    else:
        download_name = f"{original_name}_with_emails"
    
    return FileResponse(
        path=file_path,
        filename=download_name,
        media_type='application/octet-stream'
    )

if __name__ == "__main__":
    # Production-ready startup configuration
    port = int(os.getenv('PORT', 8000))
    host = os.getenv('HOST', '0.0.0.0')
    workers = int(os.getenv('UVICORN_WORKERS', 1))
    
    print("Starting Email Scraper API v2.0 (Production Ready)")
    print(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    print(f"API will be available at: http://{host}:{port}")
    if not is_production:
        print(f"API documentation at: http://{host}:{port}/docs")
    print(f"Log level: {log_level}")
    print(f"Workers: {workers}")
    
    # Ensure required directories exist
    for dir_name in ['logs', 'uploads', 'results', 'data']:
        Path(dir_name).mkdir(exist_ok=True)
    
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        workers=workers if is_production else 1,
        reload=not is_production,  # Disable reload in production
        log_level=log_level.lower(),
        access_log=not is_production,  # Reduce access logs in production
        server_header=not is_production,  # Remove server header in production
        date_header=not is_production     # Remove date header in production
    )