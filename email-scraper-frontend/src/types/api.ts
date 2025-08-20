// API Types for Simplified Email Scraper

export interface ProcessFileRequest {
  file: File;
  workers: number;
  batch_size: number;
  verbose: boolean;
}

export interface ProcessFolderRequest {
  file_path: string;
  workers: number;
  batch_size: number;
  verbose: boolean;
}

export interface JobResponse {
  job_id: string;
  status: string;
  message: string;
  total_files?: number;
}

export interface JobStatus {
  job_id: string;
  status: string;
  progress: number;
  total_processed: number;
  total_emails: number;
  start_time: number;
  end_time?: number;
  errors: string[];
  files_processed: string[];
  job_type?: string;
  folder_name?: string;
  total_files?: number;
}

export interface ProcessingStats {
  total_jobs: number;
  active_jobs: number;
  completed_jobs: number;
  failed_jobs: number;
  total_emails_found: number;
}

export interface PerformanceOptimizations {
  enabled: boolean;
  max_workers: number;
  semaphore_limits: {
    domain_discovery: number;
    batch_processing: number;
  };
  connection_pool: {
    http_sessions: number;
    aiohttp_connections: number;
    per_host_limit: number;
  };
  memory_monitoring: boolean;
  circuit_breaker: boolean;
  dns_caching: boolean;
}

export interface HealthCheck {
  status: string;
  timestamp: number;
  active_jobs: number;
  email_scraper_ready: boolean;
  version: string;
}

// Removed JobResult - no files are saved in in-memory processing

export interface ApiError {
  detail: string;
}

export interface LogEntry {
  timestamp: number;
  level: string;
  message: string;
  details?: Record<string, any>;
}

export interface JobLogs {
  job_id: string;
  logs: LogEntry[];
  total_count: number;
}

export interface JobResult {
  status: string;
  total_processed: number;
  total_emails: number;
  files: Array<{
    filename: string;
    size: number;
  }>;
} 