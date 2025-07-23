# 🔍 Email Scraper Web Application

A containerized web interface for the enhanced email discovery script with real-time progress tracking and result downloads.

## ✨ Features

- **📤 File Upload**: Support for CSV, Excel (.xlsx, .xls), and NDJSON files
- **⚙️ Configurable Processing**: Adjust workers, limits, timeouts, and monitoring options
- **📊 Real-time Progress**: Live job status updates and progress tracking
- **📥 Result Downloads**: Download results as ZIP files with CSV and TXT formats
- **🔄 Job Management**: View all jobs and their status
- **🐳 Containerized**: Fully containerized with Docker and Docker Compose

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# 1. Create project directory
mkdir email-scraper-web && cd email-scraper-web

# 2. Copy all files to the project directory:
#    - app.py (the Flask web app)
#    - enhanced_email_scraper.py (your enhanced scraper)
#    - Dockerfile
#    - docker-compose.yml
#    - requirements.txt

# 3. Deploy with Docker Compose
chmod +x deployment.sh
./deployment.sh compose

# 4. Access the web app
open http://localhost:5000
```

### Option 2: Development Mode

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run development server
./deployment.sh dev

# 3. Access at http://localhost:5000
```

### Option 3: Docker Build Only

```bash
# Build Docker image
./deployment.sh docker

# Run container
docker run -p 5000:5000 \
  -v $(pwd)/