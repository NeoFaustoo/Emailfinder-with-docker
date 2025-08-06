export interface JobRequest {
  file_path: string;
  workers: number;
  batch_size: number;
  verbose: boolean;
}

export interface UploadJobRequest {
  file: File;
  workers: number;
  batch_size: number;
  verbose: boolean;
}

export interface JobResponse {
  job_id: string;
  status: string;
  message: string;
  total_files: number;
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
}

export interface ProcessingStats {
  total_jobs: number;
  active_jobs: number;
  completed_jobs: number;
  failed_jobs: number;
  total_emails_found: number;
}

export interface HealthCheck {
  status: string;
  timestamp: number;
  kafka_enabled: boolean;
  active_jobs: number;
}

export interface JobResult {
  job_id: string;
  status: string;
  files: Array<{
    filename: string;
    path: string;
    size: number;
  }>;
  total_emails: number;
  total_processed: number;
}

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