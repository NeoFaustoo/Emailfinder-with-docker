# 🔍 Enhanced Email Scraper with Streaming Support

A high-performance email discovery script with streaming processing, direct file updates, and REST API support.

## ✨ Features

### 🚀 **Core Capabilities**
- **📤 File Upload**: Support for CSV, Excel (.xlsx, .xls), and NDJSON files
- **⚙️ Configurable Processing**: Adjust workers, limits, timeouts, and monitoring options
- **📊 Real-time Progress**: Live job status updates and progress tracking
- **📥 Result Downloads**: Download results as ZIP files with CSV and TXT formats
- **🔄 Job Management**: View all jobs and their status
- **🐳 Containerized**: Fully containerized with Docker and Docker Compose
- **🔌 REST API**: FastAPI-based REST API for integration with any frontend

### 🌊 **NEW: Streaming Features**
- **🌊 Streaming Processing**: Memory-efficient batch processing with 100+ workers
- **💾 Direct File Updates**: Results written directly back to input files
- **🔄 Automatic Backups**: Original files backed up before modification
- **📊 Real-time Streaming**: Live progress updates via async processing
- **🧠 Memory Management**: Intelligent memory monitoring and garbage collection

## 🚀 Quick Start

### Option 1: API Server (Recommended)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the API server (using startup script)
python start_api.py

# Or start directly
python app.py

# 3. Access the API
# API Documentation: http://localhost:8000/docs
# Health Check: http://localhost:8000/api/health
# Root Endpoint: http://localhost:8000/

# Development mode with auto-reload
python start_api.py --reload

# Check dependencies only
python start_api.py --check-only
```

### Option 2: Command Line Usage

```bash
# Standard processing (saves to results directory)
python enhanced_email_scraper.py companies.ndjson --workers 150 --verbose

# Streaming processing (updates input files directly)
python enhanced_email_scraper.py companies.ndjson --streaming --workers 150 --batch-size 500

# High-performance streaming
python enhanced_email_scraper.py large_dataset.xlsx --streaming --workers 200 --batch-size 1000

# Process multiple files with streaming
python enhanced_email_scraper.py *.csv --streaming --workers 100 --batch-size 250
```

### Option 3: Docker

```bash
# Build and run with Docker Compose (Recommended)
docker-compose up --build

# Or build and run manually
docker build -t email-scraper-api .
docker run -p 8000:8000 \
  -v $(pwd)/uploads:/app/uploads \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/backups:/app/backups \
  email-scraper-api

# Access the API
# API Documentation: http://localhost:8000/docs
# Health Check: http://localhost:8000/api/health
```

## 🌊 Streaming Processing

### Command Line Usage

```bash
# Standard processing (saves to results directory)
python enhanced_email_scraper.py companies.ndjson --workers 150 --verbose

# Streaming processing (updates input files directly)
python enhanced_email_scraper.py companies.ndjson --streaming --workers 150 --batch-size 500

# High-performance streaming
python enhanced_email_scraper.py large_dataset.xlsx --streaming --workers 200 --batch-size 1000

# Process multiple files with streaming
python enhanced_email_scraper.py *.csv --streaming --workers 100 --batch-size 250
```

### Streaming Features

- **Direct File Updates**: Results are written back to the original input files
- **Automatic Backups**: Original files are backed up in `backups/` directory before modification
- **Memory Efficient**: Processes files in configurable batches to prevent memory overflow
- **Real-time Progress**: Live progress updates with processing rate and success statistics
- **High Performance**: Supports 100+ workers with optimized connection pooling

### Output Format

When using streaming mode, your input files will be updated with new columns:

```json
{
  "name": "Company Name",
  "website": "https://example.com",
  "emails_found": ["contact@example.com", "info@example.com"],
  "email_count": 2,
  "discovery_method": "web_contact",
  "success": true,
  "pages_accessed": ["https://example.com/contact"],
  "processing_time": 2.5,
  "processed_at": "2024-01-15T10:30:00"
}
```

## 🔌 API Endpoints

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information and available endpoints |
| `GET` | `/api/health` | Health check and system status |
| `GET` | `/api/stats` | Overall processing statistics |

### Job Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/upload` | Upload files and start processing |
| `GET` | `/api/jobs` | Get all jobs status |
| `GET` | `/api/jobs/{job_id}` | Get specific job status |
| `DELETE` | `/api/jobs/{job_id}` | Delete job and clean up files |
| `GET` | `/api/download/{job_id}` | Download processing results |

### Real-time Streaming

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/stream-results/{job_id}` | Server-Sent Events (SSE) for real-time updates |
| `WS` | `/ws/{job_id}` | WebSocket connection for real-time updates |

### Example API Usage

#### Upload and Process Files

```bash
curl -X POST "http://localhost:8000/api/upload" \
  -F "files=@companies.csv" \
  -F "workers=150" \
  -F "batch_size=500" \
  -F "verbose=true"
```

#### Monitor Job Progress

```bash
# Get job status
curl "http://localhost:8000/api/jobs/job_1234567890_abc12345"

# Stream real-time updates
curl "http://localhost:8000/api/stream-results/job_1234567890_abc12345"
```

#### Get Processing Statistics

```bash
curl "http://localhost:8000/api/stats"
```

### Testing the API

Run the included test script to verify API functionality:

```bash
python test_api.py
```

## 🧪 Testing

The project includes a comprehensive test script (`test_api.py`) that validates:

- ✅ Health check and API availability
- ✅ File upload and processing
- ✅ Job status monitoring
- ✅ Real-time streaming updates
- ✅ Processing statistics
- ✅ Error handling

Run tests with:

```bash
python test_api.py
```

## 🐳 Docker Deployment

### Quick Start with Docker Compose

```bash
# 1. Build and start the API
docker-compose up --build

# 2. Access the API
# API Documentation: http://localhost:8000/docs
# Health Check: http://localhost:8000/api/health

# 3. Stop the service
docker-compose down
```

### Using the Deployment Script

```bash
# Make script executable (Linux/macOS)
chmod +x deploy.sh

# Build and run the API
./deploy.sh run

# View logs
./deploy.sh logs

# Test API health
./deploy.sh test

# Stop the container
./deploy.sh stop

# Check status
./deploy.sh status
```

### Manual Docker Build

```bash
# 1. Build the image
docker build -t email-scraper-api .

# 2. Run the container
docker run -d \
  --name email-scraper-api \
  -p 8000:8000 \
  -v $(pwd)/uploads:/app/uploads \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/backups:/app/backups \
  email-scraper-api

# 3. Check logs
docker logs -f email-scraper-api

# 4. Stop and remove
docker stop email-scraper-api
docker rm email-scraper-api
```

### Docker Configuration

The Docker setup includes:

- **Port**: 8000 (FastAPI default)
- **Volumes**: 
  - `./uploads` - File uploads
  - `./data` - Persistent data and job information
  - `./backups` - Automatic file backups
- **Health Check**: Monitors `/api/health` endpoint
- **Environment**: Production-ready configuration
- **Restart Policy**: Automatic restart on failure

### Testing with Docker

```bash
# Test the API from outside the container
curl http://localhost:8000/api/health

# Run the test script against the containerized API
python test_api.py
```