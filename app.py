#!/usr/bin/env python3
"""
Email Scraper API Application
FastAPI-based REST API for the enhanced email discovery script with streaming support
"""

import os
import json
import asyncio
import time
import uuid
import aiofiles
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
import shutil

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
import uvicorn

# Import Kafka producer
from kafka_producer import get_kafka_producer
from kafka_config import JOB_STATUS

# Initialize FastAPI app
app = FastAPI(
    title="Email Scraper API",
    description="REST API for enhanced email discovery with streaming support",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
active_jobs: Dict[str, Dict[str, Any]] = {}

# Pydantic models
class JobRequest(BaseModel):
    workers: int = Field(default=150, ge=1, le=500, description="Number of worker threads")
    batch_size: int = Field(default=500, ge=100, le=2000, description="Batch size for processing")
    verbose: bool = Field(default=False, description="Enable verbose logging")
    limit: Optional[int] = Field(default=None, ge=1, description="Limit number of companies to process")
    max_hours: Optional[float] = Field(default=None, ge=0.1, description="Maximum processing time in hours")

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

class ProcessingStats(BaseModel):
    total_jobs: int
    active_jobs: int
    completed_jobs: int
    failed_jobs: int
    total_emails_found: int

# Utility functions
def get_upload_dir() -> Path:
    """Get or create uploads directory."""
    upload_dir = Path("uploads")
    upload_dir.mkdir(exist_ok=True)
    return upload_dir

def validate_file_extension(filename: str) -> bool:
    """Validate file extension."""
    allowed_extensions = {'.csv', '.xlsx', '.xls', '.ndjson'}
    return Path(filename).suffix.lower() in allowed_extensions

def create_job_id() -> str:
    """Create a unique job ID."""
    return f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"

# Background task for processing
async def process_files_background(
    job_id: str, 
    file_paths: List[str], 
    config: JobRequest
):
    """Background task for processing files with Kafka workers."""
    global active_jobs
    
    try:
        # Update job status
        active_jobs[job_id]["status"] = "submitted"
        active_jobs[job_id]["start_time"] = time.time()
        
        print(f"🚀 Job {job_id} submitted to Kafka for processing")
        print(f"📁 Files: {len(file_paths)}")
        print(f"⚙️  Config: {config.dict()}")
        
        # Job is now handled by Kafka workers
        # Status updates will come through Kafka topics
        
    except Exception as e:
        error_msg = f"Job submission error: {str(e)}"
        active_jobs[job_id]["status"] = "failed"
        active_jobs[job_id]["errors"].append(error_msg)
        active_jobs[job_id]["end_time"] = time.time()
        print(f"❌ Job {job_id} submission failed with error: {e}")

# API Endpoints

@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "message": "Email Scraper API v2.0 (Kafka-enabled)",
        "status": "running",
        "architecture": "async-kafka",
        "endpoints": {
            "upload": "POST /api/upload",
            "jobs": "GET /api/jobs",
            "job_status": "GET /api/jobs/{job_id}",
            "stream_results": "GET /api/stream-results/{job_id}",
            "websocket": "WS /ws/{job_id}",
            "health": "GET /api/health"
        }
    }

@app.post("/api/upload", response_model=JobResponse)
async def upload_and_process(
    files: List[UploadFile] = File(...),
    workers: int = 150,
    batch_size: int = 500,
    verbose: bool = False,
    limit: Optional[int] = None,
    max_hours: Optional[float] = None
):
    """Upload files and submit job to Kafka for processing."""
    
    if not files:
        raise HTTPException(
            status_code=400, 
            detail="No files provided"
        )
    
    # Validate files
    valid_files = []
    upload_dir = get_upload_dir()
    
    for file in files:
        if not validate_file_extension(file.filename):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type: {file.filename}. Supported: .csv, .xlsx, .xls, .ndjson"
            )
        
        # Save file asynchronously
        file_path = upload_dir / f"{uuid.uuid4().hex}_{file.filename}"
        try:
            async with aiofiles.open(file_path, "wb") as buffer:
                content = await file.read()
                await buffer.write(content)
            valid_files.append(str(file_path))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error saving file {file.filename}: {str(e)}"
            )
    
    # Create job
    job_id = create_job_id()
    config = JobRequest(
        workers=workers,
        batch_size=batch_size,
        verbose=verbose,
        limit=limit,
        max_hours=max_hours
    )
    
    # Prepare job data for Kafka
    job_data = {
        "job_id": job_id,
        "status": JOB_STATUS["PENDING"],
        "progress": 0.0,
        "total_processed": 0,
        "total_emails": 0,
        "start_time": time.time(),
        "end_time": None,
        "errors": [],
        "files_processed": valid_files,
        "config": config.dict()
    }
    
    # Store job in memory (will be replaced by Kafka/Redis later)
    active_jobs[job_id] = job_data
    
    # Send job to Kafka
    try:
        producer = await get_kafka_producer()
        success = await producer.send_job(job_data)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to submit job to processing queue"
            )
        
        # Send initial status update
        await producer.send_job_status(job_id, JOB_STATUS["PENDING"])
        
    except Exception as e:
        # Clean up on failure
        for file_path in valid_files:
            try:
                os.remove(file_path)
            except:
                pass
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit job: {str(e)}"
        )
    
    return JobResponse(
        job_id=job_id,
        status=JOB_STATUS["PENDING"],
        message=f"Job submitted successfully for {len(valid_files)} files",
        total_files=len(valid_files)
    )

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
            end_time=job_data["end_time"],
            errors=job_data["errors"],
            files_processed=job_data["files_processed"]
        ))
    return jobs

@app.get("/api/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get status of a specific job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    return JobStatus(
        job_id=job_id,
        status=job_data["status"],
        progress=job_data["progress"],
        total_processed=job_data["total_processed"],
        total_emails=job_data["total_emails"],
        start_time=job_data["start_time"],
        end_time=job_data["end_time"],
        errors=job_data["errors"],
        files_processed=job_data["files_processed"]
    )

@app.get("/api/stream-results/{job_id}")
async def stream_job_results(job_id: str):
    """Stream real-time results for a job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    async def generate():
        """Generate streaming response."""
        while True:
            job_data = active_jobs.get(job_id)
            if not job_data:
                yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                break
            
            # Get current status
            status_data = {
                "job_id": job_id,
                "status": job_data["status"],
                "progress": job_data["progress"],
                "total_processed": job_data["total_processed"],
                "total_emails": job_data["total_emails"],
                "errors": job_data["errors"],
                "timestamp": time.time()
            }
            
            yield f"data: {json.dumps(status_data)}\n\n"
            
            # If job is completed or failed, stop streaming
            if job_data["status"] in ["completed", "failed"]:
                break
            
            # Wait before next update
            await asyncio.sleep(2)
    
    return StreamingResponse(
        generate(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream"
        }
    )

@app.websocket("/ws/{job_id}")
async def websocket_job_status(websocket: WebSocket, job_id: str):
    """WebSocket endpoint for real-time job status."""
    await websocket.accept()
    
    try:
        while True:
            if job_id not in active_jobs:
                await websocket.send_text(json.dumps({"error": "Job not found"}))
                break
            
            job_data = active_jobs[job_id]
            status_data = {
                "job_id": job_id,
                "status": job_data["status"],
                "progress": job_data["progress"],
                "total_processed": job_data["total_processed"],
                "total_emails": job_data["total_emails"],
                "errors": job_data["errors"],
                "timestamp": time.time()
            }
            
            await websocket.send_text(json.dumps(status_data))
            
            # If job is completed or failed, close connection
            if job_data["status"] in ["completed", "failed"]:
                break
            
            # Wait before next update
            await asyncio.sleep(2)
            
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for job {job_id}")

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

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "kafka_enabled": True,
        "active_jobs": len([j for j in active_jobs.values() if j["status"] in ["running", "submitted"]])
    }

@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its associated files."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    # Clean up files
    for file_path in job_data["files_processed"]:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Warning: Could not delete file {file_path}: {e}")
    
    # Remove job from active jobs
    del active_jobs[job_id]
    
    return {"message": f"Job {job_id} deleted successfully"}

@app.get("/api/download/{job_id}")
async def download_results(job_id: str):
    """Download processing results for a completed job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(
            status_code=400, 
            detail="Job not completed yet"
        )
    
    # Return the processed files (they were updated in place)
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

if __name__ == "__main__":
    print("🚀 Starting Email Scraper API v2.0 (Kafka-enabled)")
    print("📦 Kafka-based async processing enabled")
    print("🌐 API will be available at: http://localhost:8000")
    print("📚 API documentation at: http://localhost:8000/docs")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )