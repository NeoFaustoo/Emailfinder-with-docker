import axios from 'axios';
import {
  JobRequest,
  UploadJobRequest,
  JobResponse,
  JobStatus,
  ProcessingStats,
  HealthCheck,
  JobResult,
  ApiError,
  JobLogs
} from '../types/api';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000, // Increased to 2 minutes for long-running operations
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
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
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

export const apiService = {
  // Health check
  async getHealth(): Promise<HealthCheck> {
    const response = await api.get('/health');
    return response.data;
  },

  // Submit a new job (direct file path)
  async submitJob(jobRequest: JobRequest): Promise<JobResponse> {
    const response = await api.post('/process', jobRequest);
    return response.data;
  },

  // Upload and process a file
  async uploadAndProcessFile(uploadRequest: UploadJobRequest): Promise<JobResponse> {
    const formData = new FormData();
    formData.append('file', uploadRequest.file);
    formData.append('workers', uploadRequest.workers.toString());
    formData.append('batch_size', uploadRequest.batch_size.toString());
    formData.append('verbose', uploadRequest.verbose.toString());

    const response = await api.post('/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },

  // Get all jobs
  async getAllJobs(): Promise<JobStatus[]> {
    const response = await api.get('/jobs');
    return response.data;
  },

  // Get specific job status
  async getJobStatus(jobId: string): Promise<JobStatus> {
    const response = await api.get(`/jobs/${jobId}`);
    return response.data;
  },

  // Get job logs
  async getJobLogs(jobId: string, limit: number = 100, offset: number = 0): Promise<JobLogs> {
    const response = await api.get(`/jobs/${jobId}/logs`, {
      params: { limit, offset }
    });
    return response.data;
  },

  // Get processing statistics
  async getStats(): Promise<ProcessingStats> {
    const response = await api.get('/stats');
    return response.data;
  },

  // Download job results (metadata)
  async downloadResults(jobId: string): Promise<JobResult> {
    const response = await api.get(`/download/${jobId}`);
    return response.data;
  },

  // Download processed file
  async downloadResultFile(jobId: string): Promise<Blob> {
    const response = await api.get(`/download/${jobId}/file`, {
      responseType: 'blob',
    });
    return response.data;
  },

  // Delete a job
  async deleteJob(jobId: string): Promise<{ message: string }> {
    const response = await api.delete(`/jobs/${jobId}`);
    return response.data;
  },

  // Stream job results (SSE)
  streamJobResults(jobId: string): EventSource {
    return new EventSource(`${API_BASE_URL}/stream-results/${jobId}`);
  },

  // WebSocket connection for real-time updates
  createWebSocket(jobId: string): WebSocket {
    const wsUrl = window.location.origin.replace('http', 'ws');
    return new WebSocket(`${wsUrl}/ws/${jobId}`);
  }
};

export default apiService; 