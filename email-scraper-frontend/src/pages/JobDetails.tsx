import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { 
  ArrowLeft,
  Clock,
  CheckCircle,
  XCircle,
  Activity,
  Mail,
  FileText,
  Download,
  Trash2,
  RefreshCw,
  AlertCircle
} from 'lucide-react';
import { JobStatus, JobLogs, JobResult } from '../types/api';
import { apiService } from '../services/api';
import { useJobs } from '../contexts/JobContext';
import toast from 'react-hot-toast';
import { format } from 'date-fns';

const JobDetails: React.FC = () => {
  const { jobId } = useParams<{ jobId: string }>();
  const { refreshJob, deleteJob } = useJobs();
  const [job, setJob] = useState<JobStatus | null>(null);
  const [result, setResult] = useState<JobResult | null>(null);
  const [logs, setLogs] = useState<JobLogs | null>(null);
  const [recentEmails, setRecentEmails] = useState<Array<{
    company: string;
    domain: string;
    emails: string[];
    timestamp: number;
  }>>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [isBackgroundRefreshing, setIsBackgroundRefreshing] = useState(false);

  useEffect(() => {
    if (jobId) {
      loadJobDetails();
      
      // Auto-refresh for running jobs every 5 seconds for better real-time updates
      const interval = setInterval(() => {
        if (job?.status === 'running') {
          loadJobDetails(true); // Background refresh
        }
      }, 5000);

      return () => clearInterval(interval);
    }
  }, [jobId, job?.status]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadJobDetails = async (isBackground = false) => {
    if (!jobId) return;
    
    if (isBackground) {
      setIsBackgroundRefreshing(true);
    } else {
      setIsLoading(true);
    }
    
    try {
      const jobData = await apiService.getJobStatus(jobId);
      setJob(jobData);
      
      // Load recent emails for running jobs
      if (jobData.status === 'running') {
        try {
          const emailData = await apiService.getRecentEmails(jobId);
          setRecentEmails(emailData.recent_emails || []);
        } catch (error) {
          console.error('Failed to load recent emails:', error);
        }
      }
      
      if (jobData.status === 'completed') {
        try {
          const resultData = await apiService.downloadResults(jobId);
          setResult(resultData);
        } catch (error) {
          console.error('Failed to load job results:', error);
        }
      }
    } catch (error) {
      if (!isBackground) {
        toast.error('Failed to load job details');
      }
    } finally {
      if (isBackground) {
        setIsBackgroundRefreshing(false);
      } else {
        setIsLoading(false);
      }
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
              const blob = await apiService.downloadFile(jobId);
      
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

  const SmoothCounter = ({ value }: { value: number }) => {
    const [displayValue, setDisplayValue] = useState(0);
    
    useEffect(() => {
      const timer = setTimeout(() => {
        setDisplayValue(value);
      }, 100);
      
      return () => clearTimeout(timer);
    }, [value]);
    
    return (
      <span className="transition-all duration-1000 ease-out">
        {Math.round(displayValue)}
      </span>
    );
  };

  const ProgressBar = ({ progress, status }: { progress: number; status: string }) => {

    const getProgressGradient = () => {
      switch (status) {
        case 'completed':
          return 'bg-gradient-to-r from-green-500 to-green-600';
        case 'failed':
          return 'bg-gradient-to-r from-red-500 to-red-600';
        case 'running':
          return 'bg-gradient-to-r from-blue-500 to-blue-600';
        default:
          return 'bg-gradient-to-r from-gray-500 to-gray-600';
      }
    };

    return (
      <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden relative">
        {/* Background shimmer effect for running jobs */}
        {status === 'running' && (
          <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white to-transparent opacity-20 animate-pulse"></div>
        )}
        
        {/* Main progress bar with smooth animation */}
        <div
          className={`h-full transition-all duration-2000 ease-in-out ${getProgressGradient()} relative overflow-hidden`}
          style={{ 
            width: `${Math.min(100, Math.max(0, progress))}%`,
            transition: 'width 2s cubic-bezier(0.4, 0, 0.2, 1)'
          }}
        >
          {/* Wave effect for running jobs */}
          {status === 'running' && progress > 0 && (
            <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white to-transparent opacity-30 progress-wave"></div>
          )}
        </div>
        
        {/* Animated dots for running jobs */}
        {status === 'running' && progress > 0 && (
          <div className="absolute top-0 right-0 h-full w-2 bg-white opacity-30 animate-pulse"></div>
        )}
      </div>
    );
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading job details...</p>
        </div>
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
    <div className="space-y-6 animate-fade-in">
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
              <div className="flex items-center space-x-2">
                <h1 className="text-3xl font-bold text-gray-900">
                  Job {job.job_id.slice(-8)}
                </h1>
                {isBackgroundRefreshing && (
                  <div className="animate-pulse rounded-full h-2 w-2 bg-blue-600" title="Updating..."></div>
                )}
              </div>
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
                <p className="text-lg font-semibold text-gray-900">
                  <SmoothCounter value={job.total_emails} />
                </p>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <FileText className="h-5 w-5 text-success-600" />
              <div>
                <p className="text-sm font-medium text-gray-600">Processed</p>
                <p className="text-lg font-semibold text-gray-900">
                  <SmoothCounter value={job.total_processed} />
                </p>
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
          <div className="mt-6">
            <div className="flex justify-between text-sm text-gray-600 mb-2">
              <span>Progress</span>
              <span><SmoothCounter value={Math.round(job.progress * 100)} />%</span>
            </div>
            <ProgressBar progress={job.progress * 100} status={job.status} />
            {/* Debug info - remove after testing */}
            {process.env.NODE_ENV === 'development' && (
              <div className="text-xs text-gray-400 mt-1">
                Raw progress: {job.progress} | Converted: {job.progress * 100}
              </div>
            )}
            {job.status === 'running' && (
              <p className="text-xs text-gray-500 mt-2 flex items-center space-x-2">
                <span>Auto-refreshing every 5 seconds</span>
                {isBackgroundRefreshing && (
                  <div className="animate-pulse rounded-full h-1 w-1 bg-blue-600"></div>
                )}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Real-time Email Discoveries */}
      {job?.status === 'running' && recentEmails.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
                <Mail className="h-5 w-5 text-green-600" />
                <span>Recent Email Discoveries</span>
              </h2>
              <div className="text-sm text-gray-500">
                Last {recentEmails.length} discoveries
              </div>
            </div>
          </div>
          <div className="p-6">
            <div className="space-y-4 max-h-96 overflow-y-auto">
              {recentEmails.slice().reverse().map((discovery, index) => (
                <div key={index} className="border border-gray-200 rounded-lg p-4 bg-green-50">
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900">{discovery.company}</h3>
                      <p className="text-sm text-gray-600">{discovery.domain}</p>
                    </div>
                    <div className="text-xs text-gray-500">
                      {format(discovery.timestamp * 1000, 'HH:mm:ss')}
                    </div>
                  </div>
                  <div className="space-y-1">
                    {discovery.emails.map((email, emailIndex) => (
                      <div key={emailIndex} className="flex items-center space-x-2">
                        <Mail className="h-4 w-4 text-green-600" />
                        <span className="text-sm font-mono bg-white px-2 py-1 rounded border">
                          {email}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

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
                {job.job_type === 'folder' ? (
                  <div>
                    <p className="text-sm text-gray-900 font-mono">
                      📁 {job.folder_name || 'Folder Processing'}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      {job.total_files || job.files_processed.length} files in folder
                    </p>
                  </div>
                ) : job.files_processed.length === 1 ? (
                  <p className="text-sm text-gray-900 font-mono">
                    📄 {job.files_processed[0].split('/').pop()}
                  </p>
                ) : (
                  <div>
                    <p className="text-sm text-gray-900 font-mono">
                      📦 Multiple files ({job.files_processed.length})
                    </p>
                    <details className="mt-2">
                      <summary className="text-xs text-blue-600 cursor-pointer hover:text-blue-800">
                        Show all files
                      </summary>
                      <div className="mt-2 pl-4 border-l-2 border-gray-200">
                        {job.files_processed.map((file, index) => (
                          <p key={index} className="text-xs text-gray-700 font-mono">
                            {file.split('/').pop()}
                          </p>
                        ))}
                      </div>
                    </details>
                  </div>
                )}
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
        <div className="bg-green-50 border border-green-200 rounded-lg p-4">
          <div className="flex">
            <Activity className="h-5 w-5 text-green-600 mt-0.5 mr-3 animate-pulse" />
            <div>
              <h3 className="text-sm font-medium text-green-800">Live Processing</h3>
              <p className="text-sm text-green-700 mt-1">
                Your email scraper is running! This page updates every 5 seconds to show live progress 
                as companies are processed and emails are discovered.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default JobDetails; 