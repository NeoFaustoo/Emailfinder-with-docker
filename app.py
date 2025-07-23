#!/usr/bin/env python3
"""
Email Scraper Web Application
Flask-based web interface for the enhanced email discovery script
"""

import os
import json
import csv
import io
import zipfile
import tempfile
import shutil
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
from werkzeug.utils import secure_filename
import threading
import time
from pathlib import Path
import subprocess
import sys
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None
# Import the enhanced email scraper
try:
    # Try to import from the same directory
    from enhanced_email_scraper import process_files, convert_file_to_ndjson
    SCRAPER_AVAILABLE = True
except ImportError:
    print("Warning: Enhanced email scraper not available as module")
    SCRAPER_AVAILABLE = False

app = Flask(__name__)
app.secret_key = 'email-scraper-secret-key-2024'
app.config['MAX_CONTENT_LENGTH'] = 300 * 1024 * 1024  # 50MB max file size

# Global variables for job tracking
active_jobs = {}
job_counter = 0

class JobManager:
    """Manages scraping jobs with status tracking."""
    
    def __init__(self):
        self.jobs = {}
        self.counter = 0
    
    def create_job(self, filename, total_companies, config):
        """Create a new scraping job."""
        self.counter += 1
        job_id = f"job_{self.counter}_{int(time.time())}"
        
        self.jobs[job_id] = {
            'id': job_id,
            'filename': filename,
            'total_companies': total_companies,
            'processed': 0,
            'emails_found': 0,
            'status': 'pending',
            'progress': 0,
            'config': config,
            'start_time': time.time(),
            'output_dir': f"results_{job_id}",
            'errors': [],
            'results_ready': False,
            'result_files': []
        }
        
        return job_id
    
    def update_job(self, job_id, **kwargs):
        """Update job status."""
        if job_id in self.jobs:
            self.jobs[job_id].update(kwargs)
    
    def get_job(self, job_id):
        """Get job information."""
        return self.jobs.get(job_id)
    
    def get_all_jobs(self):
        """Get all jobs."""
        return list(self.jobs.values())

job_manager = JobManager()

def run_scraper_process(job_id, input_file, config):
    """Run the email scraper in a separate process."""
    try:
        job = job_manager.get_job(job_id)
        if not job:
            return
        
        job_manager.update_job(job_id, status='running')
        
        # Create output directory
        output_dir = job['output_dir']
        os.makedirs(output_dir, exist_ok=True)
        
        # Prepare arguments
        workers = config.get('workers', 100)
        verbose = config.get('verbose', False)
        limit = config.get('limit')
        max_hours = config.get('max_hours')
        monitor = config.get('monitor', False)
        
        # Mock processing for demonstration (replace with actual scraper call)
        if SCRAPER_AVAILABLE:
            try:
                process_files(
                    input_files=[input_file],
                    output_dir=output_dir,
                    workers=workers,
                    verbose=verbose,
                    resume=False,
                    limit=limit,
                    max_hours=max_hours,
                    monitor=monitor
                )
                
                # Find result files
                result_files = []
                for file in os.listdir(output_dir):
                    if file.endswith('.csv') or file.endswith('.txt') or file.endswith('.json'):
                        result_files.append(os.path.join(output_dir, file))
                
                job_manager.update_job(
                    job_id,
                    status='completed',
                    progress=100,
                    results_ready=True,
                    result_files=result_files
                )
                
            except Exception as e:
                job_manager.update_job(
                    job_id,
                    status='failed',
                    errors=[str(e)]
                )
        else:
            # Mock processing for demo
            total_steps = 10
            for i in range(total_steps + 1):
                time.sleep(2)  # Simulate processing
                progress = int((i / total_steps) * 100)
                emails_found = i * 5  # Mock emails found
                
                job_manager.update_job(
                    job_id,
                    progress=progress,
                    processed=i * 10,
                    emails_found=emails_found
                )
                
                if progress == 100:
                    # Create mock result files
                    csv_file = os.path.join(output_dir, f"results_{job_id}.csv")
                    emails_file = os.path.join(output_dir, f"emails_{job_id}.txt")
                    
                    with open(csv_file, 'w') as f:
                        f.write("name,domain,emails_found\n")
                        f.write("Mock Company,example.com,contact@example.com\n")
                    
                    with open(emails_file, 'w') as f:
                        f.write("contact@example.com\ninfo@example.com\n")
                    
                    job_manager.update_job(
                        job_id,
                        status='completed',
                        results_ready=True,
                        result_files=[csv_file, emails_file]
                    )
        
    except Exception as e:
        job_manager.update_job(
            job_id,
            status='failed',
            errors=[str(e)]
        )

@app.route('/')
def index():
    """Main page with upload form."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and start processing."""
    if 'file' not in request.files:
        flash('No file selected')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        # Save uploaded file
        filename = secure_filename(file.filename)
        upload_dir = 'uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        
        # Get configuration from form
        config = {
            'workers': int(request.form.get('workers', 100)),
            'verbose': 'verbose' in request.form,
            'monitor': 'monitor' in request.form,
            'limit': int(request.form.get('limit')) if request.form.get('limit') else None,
            'max_hours': float(request.form.get('max_hours')) if request.form.get('max_hours') else None
        }
        
        # Count companies from actual file
        total_companies = count_companies_in_file(filepath)
        print(f"Detected {total_companies} companies in {filename}")
        
        # Create job
        job_id = job_manager.create_job(filename, total_companies, config)
        
        # Start processing in background thread
        thread = threading.Thread(
            target=run_scraper_process,
            args=(job_id, filepath, config)
        )
        thread.daemon = True
        thread.start()
        
        flash(f'File uploaded successfully! Job ID: {job_id}')
        return redirect(url_for('job_status', job_id=job_id))
    
    flash('Invalid file type. Please upload .csv, .xlsx, .xls, or .ndjson files.')
    return redirect(request.url)

@app.route('/job/<job_id>')
def job_status(job_id):
    """Show job status page."""
    job = job_manager.get_job(job_id)
    if not job:
        flash('Job not found')
        return redirect(url_for('index'))
    
    return render_template('job_status.html', job=job)

@app.route('/api/job/<job_id>')
def api_job_status(job_id):
    """API endpoint for job status."""
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify(job)

@app.route('/jobs')
def all_jobs():
    """Show all jobs."""
    jobs = job_manager.get_all_jobs()
    return render_template('all_jobs.html', jobs=jobs)

@app.route('/download/<job_id>')
def download_results(job_id):
    """Download job results as zip file."""
    job = job_manager.get_job(job_id)
    if not job or not job['results_ready']:
        flash('Results not available')
        return redirect(url_for('job_status', job_id=job_id))
    
    # Create zip file
    memory_file = io.BytesIO()
    
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in job['result_files']:
            if os.path.exists(file_path):
                zf.write(file_path, os.path.basename(file_path))
    
    memory_file.seek(0)
    
    return send_file(
        memory_file,
        as_attachment=True,
        download_name=f'email_scraper_results_{job_id}.zip',
        mimetype='application/zip'
    )

def allowed_file(filename):
    """Check if file extension is allowed."""
    allowed_extensions = {'csv', 'xlsx', 'xls', 'ndjson'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions
def count_companies_in_file(filepath):
    """Count companies in uploaded file."""
    try:
        file_ext = os.path.splitext(filepath)[1].lower()
        
        if file_ext == '.ndjson':
            count = 0
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            data = json.loads(line)
                            if data.get('name') or data.get('company_name') or data.get('raw_name'):
                                count += 1
                        except:
                            continue
            return count
        
        elif file_ext in ['.xlsx', '.xls'] and PANDAS_AVAILABLE:
            import pandas as pd
            df = pd.read_excel(filepath)
            return len(df)
        
        elif file_ext == '.csv' and PANDAS_AVAILABLE:
            import pandas as pd
            # Try different encodings
            for encoding in ['utf-8', 'latin1', 'iso-8859-1']:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    return len(df)
                except:
                    continue
        
        return 100  # Default fallback
        
    except Exception as e:
        print(f"Error counting companies: {e}")
        return 100  # Default fallback
# Template files (inline for simplicity)

def create_templates():
    """Create template files."""
    templates_dir = 'templates'
    os.makedirs(templates_dir, exist_ok=True)
    
    # Base template
    base_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Email Scraper{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">🔍 Email Scraper</a>
            <div class="navbar-nav">
                <a class="nav-link" href="/">Upload</a>
                <a class="nav-link" href="/jobs">Jobs</a>
            </div>
        </div>
    </nav>
    
    <div class="container mt-4">
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="alert alert-info alert-dismissible fade show" role="alert">
                        {{ message }}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    </div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        
        {% block content %}{% endblock %}
    </div>
</body>
</html>
"""
    
    # Index template
    index_template = """
{% extends "base.html" %}

{% block content %}
<div class="row justify-content-center">
    <div class="col-md-8">
        <div class="card">
            <div class="card-header">
                <h3>📧 Enhanced Email Scraper</h3>
                <p class="mb-0">Upload your company data file to extract emails</p>
            </div>
            <div class="card-body">
                <form method="POST" action="/upload" enctype="multipart/form-data">
                    <div class="mb-3">
                        <label for="file" class="form-label">Choose File</label>
                        <input type="file" class="form-control" id="file" name="file" 
                               accept=".csv,.xlsx,.xls,.ndjson" required>
                        <div class="form-text">Supported formats: CSV, Excel (.xlsx, .xls), NDJSON</div>
                    </div>
                    
                    <div class="row">
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label for="workers" class="form-label">Workers</label>
                                <input type="number" class="form-control" id="workers" name="workers" 
                                       value="100" min="1" max="200">
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label for="limit" class="form-label">Limit (Testing)</label>
                                <input type="number" class="form-control" id="limit" name="limit" 
                                       placeholder="Optional">
                            </div>
                        </div>
                    </div>
                    
                    <div class="row">
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label for="max_hours" class="form-label">Max Hours</label>
                                <input type="number" class="form-control" id="max_hours" name="max_hours" 
                                       step="0.5" placeholder="Optional">
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="mb-3">
                                <div class="form-check">
                                    <input class="form-check-input" type="checkbox" id="verbose" name="verbose">
                                    <label class="form-check-label" for="verbose">Verbose Output</label>
                                </div>
                                <div class="form-check">
                                    <input class="form-check-input" type="checkbox" id="monitor" name="monitor">
                                    <label class="form-check-label" for="monitor">Enable Monitoring</label>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <button type="submit" class="btn btn-primary btn-lg">
                        🚀 Start Email Discovery
                    </button>
                </form>
            </div>
        </div>
        
        <div class="card mt-4">
            <div class="card-body">
                <h5>📋 Features</h5>
                <ul class="list-unstyled">
                    <li>✅ Advanced email obfuscation detection</li>
                    <li>✅ French business email patterns</li>
                    <li>✅ Parallel processing for speed</li>
                    <li>✅ Comprehensive monitoring</li>
                    <li>✅ Multiple file format support</li>
                </ul>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""
    
    # Job status template
    job_status_template = """
{% extends "base.html" %}

{% block content %}
<div class="row justify-content-center">
    <div class="col-md-10">
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <h4>📊 Job Status: {{ job.id }}</h4>
                <span class="badge bg-{% if job.status == 'completed' %}success{% elif job.status == 'failed' %}danger{% elif job.status == 'running' %}warning{% else %}secondary{% endif %}">
                    {{ job.status.upper() }}
                </span>
            </div>
            <div class="card-body">
                <div class="row mb-3">
                    <div class="col-md-6">
                        <strong>File:</strong> {{ job.filename }}<br>
                        <strong>Started:</strong> {{ (job.start_time|int) | timestamp }}<br>
                        <strong>Companies:</strong> {{ job.total_companies }}
                    </div>
                    <div class="col-md-6">
                        <strong>Processed:</strong> {{ job.processed }}<br>
                        <strong>Emails Found:</strong> {{ job.emails_found }}<br>
                        <strong>Workers:</strong> {{ job.config.workers }}
                    </div>
                </div>
                
                <div class="progress mb-3">
                    <div class="progress-bar" role="progressbar" style="width: {{ job.progress }}%">
                        {{ job.progress }}%
                    </div>
                </div>
                
                {% if job.status == 'completed' and job.results_ready %}
                    <div class="alert alert-success">
                        <h5>✅ Job Completed Successfully!</h5>
                        <a href="/download/{{ job.id }}" class="btn btn-success">
                            📥 Download Results
                        </a>
                    </div>
                {% elif job.status == 'failed' %}
                    <div class="alert alert-danger">
                        <h5>❌ Job Failed</h5>
                        {% for error in job.errors %}
                            <p>{{ error }}</p>
                        {% endfor %}
                    </div>
                {% elif job.status == 'running' %}
                    <div class="alert alert-info">
                        <h5>🔄 Job Running...</h5>
                        <p>The email discovery process is currently running. This page will auto-refresh.</p>
                    </div>
                    <script>
                        setTimeout(() => location.reload(), 5000);
                    </script>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""
    
    # All jobs template
    all_jobs_template = """
{% extends "base.html" %}

{% block content %}
<div class="card">
    <div class="card-header">
        <h4>📋 All Jobs</h4>
    </div>
    <div class="card-body">
        {% if jobs %}
            <div class="table-responsive">
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>Job ID</th>
                            <th>File</th>
                            <th>Status</th>
                            <th>Progress</th>
                            <th>Emails</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for job in jobs %}
                        <tr>
                            <td><code>{{ job.id }}</code></td>
                            <td>{{ job.filename }}</td>
                            <td>
                                <span class="badge bg-{% if job.status == 'completed' %}success{% elif job.status == 'failed' %}danger{% elif job.status == 'running' %}warning{% else %}secondary{% endif %}">
                                    {{ job.status }}
                                </span>
                            </td>
                            <td>{{ job.progress }}%</td>
                            <td>{{ job.emails_found }}</td>
                            <td>
                                <a href="/job/{{ job.id }}" class="btn btn-sm btn-primary">View</a>
                                {% if job.results_ready %}
                                    <a href="/download/{{ job.id }}" class="btn btn-sm btn-success">Download</a>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        {% else %}
            <p class="text-muted">No jobs found. <a href="/">Upload a file</a> to get started.</p>
        {% endif %}
    </div>
</div>
{% endblock %}
"""
    
    # Write template files
    with open(os.path.join(templates_dir, 'base.html'), 'w') as f:
        f.write(base_template)
    
    with open(os.path.join(templates_dir, 'index.html'), 'w') as f:
        f.write(index_template)
    
    with open(os.path.join(templates_dir, 'job_status.html'), 'w') as f:
        f.write(job_status_template)
    
    with open(os.path.join(templates_dir, 'all_jobs.html'), 'w') as f:
        f.write(all_jobs_template)

# Custom filter for timestamp
@app.template_filter('timestamp')
def timestamp_filter(timestamp):
    """Convert timestamp to readable format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    # Create templates on startup instead of using deprecated decorator
    create_templates()
    app.run(host='0.0.0.0', port=5000, debug=True)
# Custom filter for timestamp
@app.template_filter('timestamp')
def timestamp_filter(timestamp):
    """Convert timestamp to readable format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    create_templates()
    app.run(host='0.0.0.0', port=5000, debug=True)