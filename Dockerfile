# Email Scraper API Container - Simplified Architecture
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py enhanced_email_scraper.py ./

# Create necessary directories with proper permissions
RUN mkdir -p uploads data results logs && \
    chmod 755 uploads data results logs

# Set permissions for Python files
RUN chmod +x *.py

# Expose port (FastAPI default)
EXPOSE 8000

# Health check with longer start period for initialization
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Set environment variables for optimal performance
ENV PYTHONPATH=/app
ENV HOST=0.0.0.0
ENV PORT=8000
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Run the application with optimized uvicorn settings
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--access-log", "--workers", "1"]