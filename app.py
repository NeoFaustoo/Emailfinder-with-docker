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

from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
import shutil

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from pydantic import BaseModel, Field
import uvicorn

# Import Kafka producer and consumer
from kafka_producer import get_kafka_producer
from kafka_config import JOB_STATUS, get_consumer_config, get_topic_name
from aiokafka import AIOKafkaConsumer

# Initialize FastAPI app
app = FastAPI(
    title="Email Scraper API",
    description="REST API for enhanced email discovery with streaming support",
    version="2.0.0"
)

# Create upload directories
UPLOAD_DIR = Path("uploads")
RESULTS_DIR = Path("results") 
UPLOAD_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

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
job_logs: Dict[str, List[Dict[str, Any]]] = {}  # Store logs for each job
status_consumer_task: Optional[asyncio.Task] = None

# Pydantic models
class JobRequest(BaseModel):
    file_path: str = Field(..., description="Direct path to the file to process")
    workers: int = Field(default=150, ge=1, le=500, description="Number of worker threads")
    batch_size: int = Field(default=100, ge=10, le=2000, description="Batch size for processing")
    verbose: bool = Field(default=False, description="Enable verbose logging")

class UploadJobRequest(BaseModel):
    workers: int = Field(default=150, ge=1, le=500, description="Number of worker threads")
    batch_size: int = Field(default=100, ge=10, le=2000, description="Batch size for processing")
    verbose: bool = Field(default=False, description="Enable verbose logging")

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
    allowed_extensions = {'.csv', '.xlsx', '.xls', '.ndjson', '.json'}
    return Path(filename).suffix.lower() in allowed_extensions

async def save_uploaded_file(upload_file: UploadFile, job_id: str) -> Path:
    """Save uploaded file and return the path."""
    # Create a safe filename
    safe_filename = f"{job_id}_{upload_file.filename}"
    file_path = UPLOAD_DIR / safe_filename
    
    # Save the file
    with open(file_path, "wb") as buffer:
        content = await upload_file.read()
        buffer.write(content)
    
    return file_path

def create_job_id() -> str:
    """Create a unique job ID."""
    return f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"

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
    
    # Keep only last 1000 log entries per job to prevent memory issues
    if len(job_logs[job_id]) > 1000:
        job_logs[job_id] = job_logs[job_id][-1000:]

async def start_status_consumer():
    """Start Kafka consumer for status updates."""
    global status_consumer_task
    if status_consumer_task is None or status_consumer_task.done():
        status_consumer_task = asyncio.create_task(consume_status_updates())

async def consume_status_updates():
    """Consume status updates from Kafka and update active_jobs with retry mechanism."""
    consumer = None
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            config = get_consumer_config("email-scraper-api-status")
            consumer = AIOKafkaConsumer(
                get_topic_name("JOB_STATUS"),
                get_topic_name("JOB_PROGRESS"),
                **config
            )
            await consumer.start()
            
            print("🔄 Status consumer started, listening for job updates...")
            
            async for message in consumer:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    job_id = data.get("job_id")
                    
                    if job_id and job_id in active_jobs:
                        # Update job status
                        if message.topic == get_topic_name("JOB_STATUS"):
                            status = data.get("status")
                            if status:
                                active_jobs[job_id]["status"] = status
                                if status in ["completed", "failed"]:
                                    active_jobs[job_id]["end_time"] = time.time()
                                
                                # Add status change log
                                add_job_log(job_id, "INFO", f"Job status changed to: {status}")
                                
                                # Add any error information
                                error_data = data.get("data", {})
                                if error_data.get("error"):
                                    active_jobs[job_id]["errors"].append(error_data["error"])
                                    add_job_log(job_id, "ERROR", f"Job error: {error_data['error']}")
                        
                        # Update job progress
                        elif message.topic == get_topic_name("JOB_PROGRESS"):
                            progress_data = data.get("data", {})
                            if progress_data:
                                active_jobs[job_id].update({
                                    "total_processed": progress_data.get("total_processed", 0),
                                    "total_emails": progress_data.get("total_emails", 0),
                                    "progress": min(100.0, progress_data.get("total_processed", 0) / 100.0)  # Rough progress estimate
                                })
                                
                                # Add progress log
                                add_job_log(job_id, "INFO", 
                                           f"Progress update: {progress_data.get('total_processed', 0)} processed, {progress_data.get('total_emails', 0)} emails found",
                                           {"processed": progress_data.get("total_processed", 0), 
                                            "emails": progress_data.get("total_emails", 0)})
                    
                    await consumer.commit()
                    
                except Exception as e:
                    print(f"Error processing status message: {e}")
                    continue
                    
        except Exception as e:
            print(f"Status consumer error (attempt {attempt + 1}/{max_retries}): {e}")
            if consumer:
                try:
                    await consumer.stop()
                except:
                    pass
                consumer = None
            
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("Max retries reached, status consumer failed to start")
                return
        finally:
            if consumer:
                await consumer.stop()



# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Start background tasks on app startup."""
    print("🚀 Starting Email Scraper API...")
    await start_status_consumer()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on app shutdown."""
    print("🛑 Shutting down Email Scraper API...")
    global status_consumer_task
    if status_consumer_task and not status_consumer_task.done():
        status_consumer_task.cancel()
        try:
            await status_consumer_task
        except asyncio.CancelledError:
            pass

# API Endpoints

@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "message": "Email Scraper API v2.0 (Direct Processing)",
        "status": "running",
        "architecture": "async-kafka-direct",
        "description": "Direct file processing without uploads",
        "endpoints": {
            "upload": "POST /api/upload",
            "process": "POST /api/process",
            "jobs": "GET /api/jobs",
            "job_status": "GET /api/jobs/{job_id}",
            "job_logs": "GET /api/jobs/{job_id}/logs",
            "download": "GET /api/download/{job_id}",
            "download_file": "GET /api/download/{job_id}/file",
            "stream_results": "GET /api/stream-results/{job_id}",
            "websocket": "WS /ws/{job_id}",
            "health": "GET /api/health",
            "stats": "GET /api/stats"
        }
    }

@app.post("/api/process", response_model=JobResponse)
async def process_file(request: JobRequest):
    """Process a file directly without uploading it."""
    try:
        # Validate file exists
        if not os.path.exists(request.file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")
        
        # Validate file extension
        if not validate_file_extension(request.file_path):
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Create job ID
        job_id = create_job_id()
        
        # Create job data
        job_data = {
            "job_id": job_id,
            "file_path": request.file_path,
            "config": {
                "workers": request.workers,
                "batch_size": request.batch_size,
                "verbose": request.verbose
            }
        }
        
        # Send job to Kafka
        producer = get_kafka_producer()
        success = await producer.send_job(job_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to submit job to processing queue")
        
        # Store job info with all required fields
        active_jobs[job_id] = {
            "status": "queued",
            "file_path": request.file_path,
            "start_time": time.time(),
            "end_time": None,
            "config": job_data["config"],
            "progress": 0.0,
            "total_processed": 0,
            "total_emails": 0,
            "errors": [],
            "files_processed": [request.file_path]
        }
        
        # Add initial log entry
        add_job_log(job_id, "INFO", f"Job created for file: {request.file_path}", 
                   {"file_path": request.file_path, "workers": request.workers, "batch_size": request.batch_size})
        
        return JobResponse(
            job_id=job_id,
            status="queued",
            message=f"Job queued for processing: {request.file_path}",
            total_files=1
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating job: {str(e)}")


@app.post("/api/upload", response_model=JobResponse)
async def upload_and_process_file(
    file: UploadFile = File(..., description="File to upload and process"),
    workers: int = Form(150, description="Number of worker threads"),
    batch_size: int = Form(100, description="Batch size for processing"),
    verbose: bool = Form(False, description="Enable verbose logging")
):
    """Upload and process a file."""
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
        
        # Create job data
        job_data = {
            "job_id": job_id,
            "file_path": str(file_path),
            "original_filename": file.filename,
            "config": {
                "workers": workers,
                "batch_size": batch_size,
                "verbose": verbose
            }
        }
        
        # Send job to Kafka
        producer = get_kafka_producer()
        success = await producer.send_job(job_data)
        
        if not success:
            # Clean up uploaded file if job submission fails
            if file_path.exists():
                file_path.unlink()
            raise HTTPException(status_code=500, detail="Failed to submit job to processing queue")
        
        # Store job info with all required fields
        active_jobs[job_id] = {
            "status": "queued",
            "file_path": str(file_path),
            "original_filename": file.filename,
            "start_time": time.time(),
            "end_time": None,
            "config": job_data["config"],
            "progress": 0.0,
            "total_processed": 0,
            "total_emails": 0,
            "errors": [],
            "files_processed": [str(file_path)]
        }
        
        # Add initial log entry
        add_job_log(job_id, "INFO", f"File uploaded and job created: {file.filename}", 
                   {"original_filename": file.filename, "file_path": str(file_path), "workers": workers, "batch_size": batch_size})
        
        return JobResponse(
            job_id=job_id,
            status="queued",
            message=f"File uploaded and job queued for processing: {file.filename}",
            total_files=1
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading and processing file: {str(e)}")


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

@app.get("/api/jobs/{job_id}/logs", response_model=JobLogs)
async def get_job_logs(job_id: str, limit: int = 100, offset: int = 0):
    """Get logs for a specific job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # First try to get logs from memory
    memory_logs = job_logs.get(job_id, [])
    
    # Also try to read from log file
    file_logs = []
    log_file_path = f"logs/job_{job_id}.log"
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            # Parse log line format: "timestamp - level - message"
                            parts = line.split(' - ', 2)
                            if len(parts) >= 3:
                                timestamp_str = parts[0]
                                level = parts[1]
                                message = parts[2]
                                
                                # Parse timestamp
                                from datetime import datetime
                                timestamp = datetime.fromisoformat(timestamp_str.replace(',', '.')).timestamp()
                                
                                file_logs.append({
                                    "timestamp": timestamp,
                                    "level": level,
                                    "message": message,
                                    "details": {}
                                })
                        except (ValueError, IndexError):
                            # If parsing fails, add as a generic info log
                            file_logs.append({
                                "timestamp": time.time(),
                                "level": "INFO",
                                "message": line,
                                "details": {}
                            })
        except Exception as e:
            print(f"Error reading log file {log_file_path}: {e}")
    
    # Combine memory logs and file logs
    all_logs = memory_logs + file_logs
    
    # Sort by timestamp (newest first)
    all_logs.sort(key=lambda x: x["timestamp"], reverse=True)
    
    # Remove duplicates based on timestamp and message
    seen = set()
    unique_logs = []
    for log in all_logs:
        key = (log["timestamp"], log["message"])
        if key not in seen:
            seen.add(key)
            unique_logs.append(log)
    
    total_count = len(unique_logs)
    
    # Apply pagination
    start_idx = offset
    end_idx = min(offset + limit, total_count)
    
    paginated_logs = unique_logs[start_idx:end_idx]
    
    # Convert to LogEntry format
    log_entries = [
        LogEntry(
            timestamp=log["timestamp"],
            level=log["level"],
            message=log["message"],
            details=log.get("details")
        )
        for log in paginated_logs
    ]
    
    return JobLogs(
        job_id=job_id,
        logs=log_entries,
        total_count=total_count
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
    
    # Remove job from active jobs and clean up logs
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


@app.get("/api/download/{job_id}/file")
async def download_result_file(job_id: str):
    """Download the actual processed file for a completed job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = active_jobs[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(
            status_code=400, 
            detail="Job not completed yet"
        )
    
    # Get the processed file path
    if not job_data["files_processed"]:
        raise HTTPException(status_code=404, detail="No processed files found")
    
    file_path = Path(job_data["files_processed"][0])
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Processed file not found")
    
    # Create a download filename with results suffix
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