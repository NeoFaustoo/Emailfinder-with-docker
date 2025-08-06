import React from 'react';
import { 
  BarChart3, 
  TrendingUp, 
  Mail, 
  CheckCircle, 
  XCircle, 
  Activity,
  Clock,
  FileText,
  Users,
  Zap
} from 'lucide-react';
import { useJobs } from '../contexts/JobContext';
import { ProcessingStats } from '../types/api';

const Statistics: React.FC = () => {
  const { state } = useJobs();
  const stats = state.stats;

  const getStatusDistribution = () => {
    const jobs = state.jobs;
    const distribution = {
      completed: 0,
      running: 0,
      failed: 0,
      queued: 0
    };

    jobs.forEach(job => {
      if (distribution.hasOwnProperty(job.status)) {
        distribution[job.status as keyof typeof distribution]++;
      }
    });

    return distribution;
  };

  const getRecentActivity = () => {
    const now = Date.now() / 1000;
    const jobs = state.jobs
      .filter(job => now - job.start_time < 24 * 60 * 60) // Last 24 hours
      .sort((a, b) => b.start_time - a.start_time)
      .slice(0, 10);

    return jobs;
  };

  const getSuccessRate = () => {
    if (!stats || stats.total_jobs === 0) return 0;
    return (stats.completed_jobs / stats.total_jobs) * 100;
  };

  const getAverageEmailsPerJob = () => {
    if (!stats || stats.completed_jobs === 0) return 0;
    return stats.total_emails_found / stats.completed_jobs;
  };

  const statusDistribution = getStatusDistribution();
  const recentActivity = getRecentActivity();

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Email Scraper Analytics
        </h1>
        <p className="text-gray-600">
          Comprehensive statistics and performance metrics
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-primary-100 rounded-lg">
              <Mail className="h-6 w-6 text-primary-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Emails Found</p>
              <p className="text-2xl font-bold text-gray-900">
                {stats?.total_emails_found || 0}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-success-100 rounded-lg">
              <CheckCircle className="h-6 w-6 text-success-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Success Rate</p>
              <p className="text-2xl font-bold text-gray-900">
                {getSuccessRate().toFixed(1)}%
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-warning-100 rounded-lg">
              <TrendingUp className="h-6 w-6 text-warning-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Avg Emails/Job</p>
              <p className="text-2xl font-bold text-gray-900">
                {getAverageEmailsPerJob().toFixed(1)}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-error-100 rounded-lg">
              <XCircle className="h-6 w-6 text-error-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Failure Rate</p>
              <p className="text-2xl font-bold text-gray-900">
                {stats && stats.total_jobs > 0 
                  ? ((stats.failed_jobs / stats.total_jobs) * 100).toFixed(1)
                  : 0}%
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Detailed Statistics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Job Status Distribution */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Job Status Distribution</h2>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {Object.entries(statusDistribution).map(([status, count]) => (
                <div key={status} className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className={`w-3 h-3 rounded-full ${
                      status === 'completed' ? 'bg-success-500' :
                      status === 'running' ? 'bg-primary-500' :
                      status === 'failed' ? 'bg-error-500' :
                      'bg-warning-500'
                    }`}></div>
                    <span className="text-sm font-medium text-gray-900 capitalize">
                      {status}
                    </span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <span className="text-sm font-bold text-gray-900">{count}</span>
                    <span className="text-sm text-gray-500">
                      ({stats && stats.total_jobs > 0 
                        ? ((count / stats.total_jobs) * 100).toFixed(1)
                        : 0}%)
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Performance Metrics */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Performance Metrics</h2>
          </div>
          <div className="p-6 space-y-6">
            <div>
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>Total Jobs</span>
                <span>{stats?.total_jobs || 0}</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div className="bg-primary-600 h-2 rounded-full" style={{ width: '100%' }}></div>
              </div>
            </div>

            <div>
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>Completed Jobs</span>
                <span>{stats?.completed_jobs || 0}</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-success-600 h-2 rounded-full" 
                  style={{ width: `${getSuccessRate()}%` }}
                ></div>
              </div>
            </div>

            <div>
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>Active Jobs</span>
                <span>{stats?.active_jobs || 0}</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-warning-600 h-2 rounded-full" 
                  style={{ width: `${stats && stats.total_jobs > 0 ? (stats.active_jobs / stats.total_jobs) * 100 : 0}%` }}
                ></div>
              </div>
            </div>

            <div>
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>Failed Jobs</span>
                <span>{stats?.failed_jobs || 0}</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-error-600 h-2 rounded-full" 
                  style={{ width: `${stats && stats.total_jobs > 0 ? (stats.failed_jobs / stats.total_jobs) * 100 : 0}%` }}
                ></div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Recent Activity */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Recent Activity (Last 24 Hours)</h2>
        </div>
        <div className="divide-y divide-gray-200">
          {recentActivity.length === 0 ? (
            <div className="px-6 py-8 text-center text-gray-500">
              <Clock className="h-12 w-12 mx-auto mb-4 text-gray-300" />
              <p>No recent activity</p>
            </div>
          ) : (
            recentActivity.map((job) => (
              <div key={job.job_id} className="px-6 py-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className={`w-2 h-2 rounded-full ${
                      job.status === 'completed' ? 'bg-success-500' :
                      job.status === 'running' ? 'bg-primary-500' :
                      job.status === 'failed' ? 'bg-error-500' :
                      'bg-warning-500'
                    }`}></div>
                    <div>
                      <p className="text-sm font-medium text-gray-900">
                        Job {job.job_id.slice(-8)}
                      </p>
                      <p className="text-xs text-gray-500">
                        {job.files_processed[0]?.split('/').pop() || 'Unknown file'}
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-medium text-gray-900">
                      {job.total_emails} emails
                    </p>
                    <p className="text-xs text-gray-500 capitalize">
                      {job.status}
                    </p>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* System Information */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">System Information</h2>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Total Jobs Processed</span>
                <span className="text-sm font-medium text-gray-900">{stats?.total_jobs || 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Total Emails Discovered</span>
                <span className="text-sm font-medium text-gray-900">{stats?.total_emails_found || 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Success Rate</span>
                <span className="text-sm font-medium text-gray-900">{getSuccessRate().toFixed(1)}%</span>
              </div>
            </div>
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Average Emails per Job</span>
                <span className="text-sm font-medium text-gray-900">{getAverageEmailsPerJob().toFixed(1)}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Currently Active</span>
                <span className="text-sm font-medium text-gray-900">{stats?.active_jobs || 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Failed Jobs</span>
                <span className="text-sm font-medium text-gray-900">{stats?.failed_jobs || 0}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Statistics; 