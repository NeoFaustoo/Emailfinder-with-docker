import axios from 'axios';
import {
  ProcessFileRequest,
  ProcessFolderRequest,
  JobResponse,
  JobStatus,
  ProcessingStats,
  HealthCheck,
  JobLogs
} from '../types/api';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 8000, // 8 seconds default timeout
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    if (process.env.NODE_ENV !== 'production') {
      console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    // More detailed error logging with suppression of common errors
    if (error.code === 'ECONNABORTED') {
      console.warn('API Timeout (suppressed):', error.config?.url);
    } else if (error.code === 'ERR_NETWORK') {
      console.warn('Network Error (suppressed):', error.config?.url);
    } else if (error.response?.status === 404) {
      console.warn('Resource not found (suppressed):', error.config?.url);
    } else if (error.response) {
      console.error('API Error:', error.response.status, error.response.data);
    } else {
      console.error('API Error:', error.message);
    }
    
    // Create a more user-friendly error for timeouts
    if (error.code === 'ECONNABORTED' || error.code === 'ERR_NETWORK') {
      const friendlyError = new Error('Connection timeout - please try again');
      friendlyError.name = 'TimeoutError';
      return Promise.reject(friendlyError);
    }
    
    return Promise.reject(error);
  }
);

export const apiService = {
  // Health check
  async getHealth(): Promise<HealthCheck> {
    const response = await api.get('/api/health', {
      timeout: 10000 // 10 seconds for health check
    });
    return response.data;
  },

  // Get statistics
  async getStats(): Promise<ProcessingStats> {
    const response = await api.get('/api/stats', {
      timeout: 5000 // 5 seconds for stats
    });
    return response.data;
  },

  // Upload and process a file
  async processFile(processRequest: ProcessFileRequest): Promise<JobResponse> {
    const formData = new FormData();
    formData.append('file', processRequest.file);
    formData.append('workers', processRequest.workers.toString());
    formData.append('batch_size', processRequest.batch_size.toString());
    formData.append('verbose', processRequest.verbose.toString());

    const response = await api.post('/api/process-file', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 60000, // 1 minute for file upload
      maxContentLength: Infinity as unknown as number,
      maxBodyLength: Infinity as unknown as number,
    });
    return response.data;
  },

  // Process ZIP file upload
  async processZipFile(processRequest: ProcessFileRequest): Promise<JobResponse> {
    const formData = new FormData();
    formData.append('folder', processRequest.file); // Note: backend expects 'folder' for ZIP endpoint
    formData.append('workers', processRequest.workers.toString());
    formData.append('batch_size', processRequest.batch_size.toString());
    formData.append('verbose', processRequest.verbose.toString());

    const response = await api.post('/api/process-files-zip', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 60000, // 1 minute for ZIP upload
      maxContentLength: Infinity as unknown as number,
      maxBodyLength: Infinity as unknown as number,
    });
    return response.data;
  },

  // Process folder (file path)
  async processFolder(processRequest: ProcessFolderRequest): Promise<JobResponse> {
    const response = await api.post('/api/process-files-folder', processRequest, {
      timeout: 30000, // 30 seconds for folder processing
    });
    return response.data;
  },

  // Get job status
  async getJobStatus(jobId: string): Promise<JobStatus> {
    const response = await api.get(`/api/jobs/${jobId}`, {
      timeout: 5000 // 5 seconds for job status
    });
    return response.data;
  },

  // Get job logs
  async getJobLogs(jobId: string, limit: number = 50): Promise<JobLogs> {
    const response = await api.get(`/api/jobs/${jobId}/logs?limit=${limit}`, {
      timeout: 10000 // 10 seconds for logs
    });
    return response.data;
  },

  // Download results summary
  async downloadResults(jobId: string): Promise<any> {
    const response = await api.get(`/api/download/${jobId}`, {
      timeout: 15000 // 15 seconds for download info
    });
    return response.data;
  },

  // Download processed file
  async downloadFile(jobId: string): Promise<Blob> {
    const response = await api.get(`/api/download/${jobId}/file`, {
      responseType: 'blob',
      timeout: 120000, // 2 minutes for file download
    });
    return response.data;
  },

  // Get all jobs
  async getAllJobs(): Promise<JobStatus[]> {
    const response = await api.get('/api/jobs', {
      timeout: 5000 // 5 seconds for all jobs
    });
    return response.data;
  },

  // Delete a job
  async deleteJob(jobId: string): Promise<{ message: string }> {
    const response = await api.delete(`/api/jobs/${jobId}`, {
      timeout: 10000 // 10 seconds for job deletion
    });
    return response.data;
  },

  // Get recent email discoveries for real-time display
  async getRecentEmails(jobId: string): Promise<{
    job_id: string;
    recent_emails: Array<{
      company: string;
      domain: string;
      emails: string[];
      timestamp: number;
    }>;
    total_emails: number;
    total_processed: number;
    status: string;
    timestamp: number;
  }> {
    const response = await api.get(`/api/jobs/${jobId}/emails/recent`, {
      timeout: 5000 // 5 seconds for recent emails
    });
    return response.data;
  },
}; 