import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { 
  ArrowLeft,
  Clock,
  CheckCircle,
  XCircle,
  Activity,
  Mail,
  Calendar,
  FileText,
  Download,
  Trash2,
  RefreshCw,
  AlertCircle,
  Info,
  Play,
  Pause
} from 'lucide-react';
import { JobStatus, JobResult, JobLogs, LogEntry } from '../types/api';
import apiService from '../services/api';
import { useJobs } from '../contexts/JobContext';
import toast from 'react-hot-toast';
import { formatDistanceToNow, format } from 'date-fns';

const JobDetails: React.FC = () => {
  const { jobId } = useParams<{ jobId: string }>();
  const { state, refreshJob, deleteJob } = useJobs();
  const [job, setJob] = useState<JobStatus | null>(null);
  const [result, setResult] = useState<JobResult | null>(null);
  const [logs, setLogs] = useState<JobLogs | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [showLogs, setShowLogs] = useState(false);

  useEffect(() => {
    if (jobId) {
      loadJobDetails();
    }
  }, [jobId]);

  const loadJobDetails = async () => {
    if (!jobId) return;
    
    setIsLoading(true);
    try {
      const jobData = await apiService.getJobStatus(jobId);
      setJob(jobData);
      
      if (jobData.status === 'completed') {
        try {
          const resultData = await apiService.downloadResults(jobId);
          setResult(resultData);
        } catch (error) {
          console.error('Failed to load job results:', error);
        }
      }
    } catch (error) {
      toast.error('Failed to load job details');
    } finally {
      setIsLoading(false);
    }
  };

  const handleRefresh = async () => {
    if (!jobId) return;
    
    setIsRefreshing(true);
    try {
      await refreshJob(jobId);
      await loadJobDetails();
      toast.success('Job details refreshed');
    } catch (error) {
      toast.error('Failed to refresh job details');
    } finally {
      setIsRefreshing(false);
    }
  };

  const handleDelete = async () => {
    if (!jobId) return;
    
    if (window.confirm('Are you sure you want to delete this job?')) {
      await deleteJob(jobId);
      toast.success('Job deleted successfully');
    }
  };

  const handleDownload = async () => {
    if (!jobId || !job) return;
    
    if (job.status !== 'completed') {
      toast.error('Job must be completed before downloading');
      return;
    }

    try {
      const blob = await apiService.downloadResultFile(jobId);
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Get original filename and add suffix
      const originalName = job.files_processed?.[0]?.split('/').pop() || 'results';
      const nameParts = originalName.split('.');
      if (nameParts.length > 1) {
        const extension = nameParts.pop();
        const baseName = nameParts.join('.');
        link.download = `${baseName}_with_emails.${extension}`;
      } else {
        link.download = `${originalName}_with_emails`;
      }
      
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      toast.success('File downloaded successfully');
    } catch (error) {
      toast.error('Failed to download file');
    }
  };

  const loadJobLogs = async () => {
    if (!jobId) return;
    
    setIsLoadingLogs(true);
    try {
      const logsData = await apiService.getJobLogs(jobId);
      setLogs(logsData);
    } catch (error) {
      toast.error('Failed to load job logs');
    } finally {
      setIsLoadingLogs(false);
    }
  };

  const handleToggleLogs = async () => {
    if (!showLogs && !logs) {
      await loadJobLogs();
    }
    setShowLogs(!showLogs);
  };

  const getLogLevelColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'error':
        return 'text-red-600 bg-red-50 border-red-200';
      case 'warning':
        return 'text-yellow-600 bg-yellow-50 border-yellow-200';
      case 'info':
        return 'text-blue-600 bg-blue-50 border-blue-200';
      case 'debug':
        return 'text-gray-600 bg-gray-50 border-gray-200';
      default:
        return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'text-success-600 bg-success-50 border-success-200';
      case 'running':
        return 'text-primary-600 bg-primary-50 border-primary-200';
      case 'failed':
        return 'text-error-600 bg-error-50 border-error-200';
      case 'queued':
        return 'text-warning-600 bg-warning-50 border-warning-200';
      default:
        return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-5 w-5" />;
      case 'running':
        return <Activity className="h-5 w-5 animate-pulse" />;
      case 'failed':
        return <XCircle className="h-5 w-5" />;
      case 'queued':
        return <Clock className="h-5 w-5" />;
      default:
        return <Clock className="h-5 w-5" />;
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-center py-12">
        <AlertCircle className="h-12 w-12 mx-auto text-gray-400 mb-4" />
        <h2 className="text-xl font-semibold text-gray-900 mb-2">Job Not Found</h2>
        <p className="text-gray-600 mb-4">The job you're looking for doesn't exist or has been deleted.</p>
        <Link
          to="/jobs"
          className="inline-flex items-center px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700"
        >
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back to Jobs
        </Link>
      </div>
    );
  }

  const duration = job.end_time 
    ? job.end_time - job.start_time 
    : Date.now() / 1000 - job.start_time;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link
            to="/jobs"
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h1 className="text-3xl font-bold text-gray-900">
              Job {job.job_id.slice(-8)}
            </h1>
            <p className="text-gray-600">
              Email discovery job details and progress
            </p>
          </div>
        </div>
        
        <div className="flex items-center space-x-2">
          <button
            onClick={handleToggleLogs}
            disabled={isLoadingLogs}
            className="p-2 text-gray-400 hover:text-purple-600 hover:bg-purple-50 rounded-md transition-colors disabled:opacity-50"
            title="View Logs"
          >
            <FileText className={`h-5 w-5 ${isLoadingLogs ? 'animate-pulse' : ''}`} />
          </button>
          <button
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="p-2 text-gray-400 hover:text-primary-600 hover:bg-primary-50 rounded-md transition-colors disabled:opacity-50"
            title="Refresh"
          >
            <RefreshCw className={`h-5 w-5 ${isRefreshing ? 'animate-spin' : ''}`} />
          </button>
          {job?.status === 'completed' && (
            <button
              onClick={handleDownload}
              className="p-2 text-gray-400 hover:text-green-600 hover:bg-green-50 rounded-md transition-colors"
              title="Download Results"
            >
              <Download className="h-5 w-5" />
            </button>
          )}
          <button
            onClick={handleDelete}
            className="p-2 text-gray-400 hover:text-error-600 hover:bg-error-50 rounded-md transition-colors"
            title="Delete Job"
          >
            <Trash2 className="h-5 w-5" />
          </button>
        </div>
      </div>

      {/* Status Card */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Job Status</h2>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="flex items-center space-x-3">
              {getStatusIcon(job.status)}
              <div>
                <p className="text-sm font-medium text-gray-600">Status</p>
                <p className="text-lg font-semibold text-gray-900">{job.status}</p>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <Mail className="h-5 w-5 text-primary-600" />
              <div>
                <p className="text-sm font-medium text-gray-600">Emails Found</p>
                <p className="text-lg font-semibold text-gray-900">{job.total_emails}</p>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <FileText className="h-5 w-5 text-success-600" />
              <div>
                <p className="text-sm font-medium text-gray-600">Processed</p>
                <p className="text-lg font-semibold text-gray-900">{job.total_processed}</p>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <Clock className="h-5 w-5 text-warning-600" />
              <div>
                <p className="text-sm font-medium text-gray-600">Duration</p>
                <p className="text-lg font-semibold text-gray-900">
                  {Math.round(duration)}s
                </p>
              </div>
            </div>
          </div>

          {/* Progress Bar */}
          {job.progress > 0 && (
            <div className="mt-6">
              <div className="flex justify-between text-sm text-gray-600 mb-2">
                <span>Progress</span>
                <span>{Math.round(job.progress)}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-primary-600 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${Math.min(job.progress, 100)}%` }}
                ></div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Job Information */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Job Details */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Job Information</h2>
          </div>
          <div className="p-6 space-y-4">
            <div>
              <p className="text-sm font-medium text-gray-600">Job ID</p>
              <p className="text-sm text-gray-900 font-mono">{job.job_id}</p>
            </div>
            
            <div>
              <p className="text-sm font-medium text-gray-600">Started</p>
              <p className="text-sm text-gray-900">
                {format(job.start_time * 1000, 'PPP p')}
              </p>
            </div>
            
            {job.end_time && (
              <div>
                <p className="text-sm font-medium text-gray-600">Completed</p>
                <p className="text-sm text-gray-900">
                  {format(job.end_time * 1000, 'PPP p')}
                </p>
              </div>
            )}
            
            <div>
              <p className="text-sm font-medium text-gray-600">Files Processed</p>
              <div className="mt-1">
                {job.files_processed.map((file, index) => (
                  <p key={index} className="text-sm text-gray-900 font-mono">
                    {file.split('/').pop()}
                  </p>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Results */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Results</h2>
          </div>
          <div className="p-6">
            {job.status === 'completed' && result ? (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-600">Total Emails</span>
                  <span className="text-lg font-semibold text-gray-900">{result.total_emails}</span>
                </div>
                
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-600">Total Processed</span>
                  <span className="text-lg font-semibold text-gray-900">{result.total_processed}</span>
                </div>
                
                {result.files.length > 0 && (
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-2">Result Files</p>
                    <div className="space-y-2">
                      {result.files.map((file, index) => (
                        <div key={index} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                          <span className="text-sm text-gray-900">{file.filename}</span>
                          <span className="text-xs text-gray-500">
                            {(file.size / 1024).toFixed(1)} KB
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : job.status === 'running' ? (
              <div className="text-center py-8">
                <Activity className="h-8 w-8 mx-auto text-primary-600 animate-pulse mb-2" />
                <p className="text-sm text-gray-600">Processing in progress...</p>
              </div>
            ) : job.status === 'failed' ? (
              <div className="text-center py-8">
                <XCircle className="h-8 w-8 mx-auto text-error-600 mb-2" />
                <p className="text-sm text-gray-600">Job failed</p>
              </div>
            ) : (
              <div className="text-center py-8">
                <Clock className="h-8 w-8 mx-auto text-warning-600 mb-2" />
                <p className="text-sm text-gray-600">Job queued</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Errors */}
      {job.errors.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <AlertCircle className="h-5 w-5 mr-2 text-error-600" />
              Errors
            </h2>
          </div>
          <div className="p-6">
            <div className="space-y-2">
              {job.errors.map((error, index) => (
                <div key={index} className="p-3 bg-error-50 border border-error-200 rounded-md">
                  <p className="text-sm text-error-800">{error}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Job Logs */}
      {showLogs && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                <FileText className="h-5 w-5 mr-2 text-purple-600" />
                Job Logs
              </h2>
              <div className="flex items-center space-x-2">
                <button
                  onClick={loadJobLogs}
                  disabled={isLoadingLogs}
                  className="text-sm text-gray-500 hover:text-primary-600 disabled:opacity-50"
                >
                  {isLoadingLogs ? 'Loading...' : 'Refresh Logs'}
                </button>
                <button
                  onClick={() => setShowLogs(false)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <XCircle className="h-5 w-5" />
                </button>
              </div>
            </div>
          </div>
          <div className="p-6">
            {isLoadingLogs ? (
              <div className="flex items-center justify-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
              </div>
            ) : logs && logs.logs.length > 0 ? (
              <div className="space-y-3">
                <div className="text-sm text-gray-600 mb-4">
                  Showing {logs.logs.length} of {logs.total_count} log entries
                </div>
                <div className="max-h-96 overflow-y-auto space-y-2">
                  {logs.logs.map((log, index) => (
                    <div
                      key={index}
                      className={`p-3 rounded-md border ${getLogLevelColor(log.level)}`}
                    >
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="flex items-center space-x-2 mb-1">
                            <span className="text-xs font-medium px-2 py-1 rounded bg-white bg-opacity-50">
                              {log.level.toUpperCase()}
                            </span>
                            <span className="text-xs text-gray-500">
                              {format(log.timestamp * 1000, 'HH:mm:ss')}
                            </span>
                          </div>
                          <p className="text-sm font-medium">{log.message}</p>
                          {log.details && Object.keys(log.details).length > 0 && (
                            <div className="mt-2 text-xs">
                              <details className="cursor-pointer">
                                <summary className="text-gray-600 hover:text-gray-800">
                                  View Details
                                </summary>
                                <pre className="mt-1 p-2 bg-white bg-opacity-50 rounded text-xs overflow-x-auto">
                                  {JSON.stringify(log.details, null, 2)}
                                </pre>
                              </details>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="text-center py-8">
                <FileText className="h-8 w-8 mx-auto text-gray-400 mb-2" />
                <p className="text-sm text-gray-600">No logs available for this job</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Real-time Updates Info */}
      {job.status === 'running' && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex">
            <Info className="h-5 w-5 text-blue-400 mt-0.5 mr-3" />
            <div>
              <h3 className="text-sm font-medium text-blue-800">Real-time Updates</h3>
              <p className="text-sm text-blue-700 mt-1">
                This page automatically refreshes every 5 seconds to show the latest progress.
                You can also manually refresh using the refresh button.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default JobDetails; 