import React, { useState, useEffect } from 'react';
import { 
  TrendingUp, 
  Mail, 
  CheckCircle, 
  Clock,
  Activity,
  BarChart3,
  RefreshCw
} from 'lucide-react';
import { useJobs } from '../contexts/JobContext';

const Statistics: React.FC = () => {
  const { state, fetchStats } = useJobs();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const stats = state.stats;

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      await fetchStats();
    } finally {
      setIsRefreshing(false);
    }
  };

  // Auto-refresh stats every 10 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      fetchStats();
    }, 10000);

    return () => clearInterval(interval);
  }, [fetchStats]);

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

  const getTotalProcessedCompanies = () => {
    return state.jobs.reduce((total, job) => total + job.total_processed, 0);
  };

  const getProcessingEfficiency = () => {
    const totalProcessed = getTotalProcessedCompanies();
    if (!stats || totalProcessed === 0) return 0;
    return (stats.total_emails_found / totalProcessed) * 100;
  };

  const statusDistribution = getStatusDistribution();
  const recentActivity = getRecentActivity();

  // Show loading state if no stats are available yet
  if (!stats && state.loading) {
    return (
      <div className="flex items-center justify-center min-h-96">
        <div className="text-center">
          <Activity className="h-12 w-12 mx-auto mb-4 text-gray-400 animate-pulse" />
          <p className="text-gray-600">Loading statistics...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            Email Scraper Analytics
          </h1>
          <p className="text-gray-600">
            Comprehensive statistics and performance metrics
          </p>
        </div>
        <div className="flex items-center space-x-4">
          <button
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          {stats && (
            <div className="flex items-center space-x-2 text-sm text-gray-500">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span>Live updates • Last: {new Date().toLocaleTimeString()}</span>
            </div>
          )}
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {/* Total Companies Processed */}
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow">
          <div className="flex items-center">
            <div className="p-2 bg-blue-100 rounded-lg">
              <BarChart3 className="h-6 w-6 text-blue-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Companies Processed</p>
              <p className="text-2xl font-bold text-gray-900">
                {getTotalProcessedCompanies().toLocaleString()}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                {getProcessingEfficiency().toFixed(2)}% email discovery rate
              </p>
            </div>
          </div>
        </div>
        {/* Total Emails Found */}
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow">
          <div className="flex items-center">
            <div className="p-2 bg-primary-100 rounded-lg">
              <Mail className="h-6 w-6 text-primary-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Emails Found</p>
              <p className="text-2xl font-bold text-gray-900">
                {(stats?.total_emails_found || 0).toLocaleString()}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                From {stats?.completed_jobs || 0} completed jobs
              </p>
            </div>
          </div>
        </div>

        {/* Success Rate */}
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow">
          <div className="flex items-center">
            <div className="p-2 bg-success-100 rounded-lg">
              <CheckCircle className="h-6 w-6 text-success-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Job Success Rate</p>
              <p className="text-2xl font-bold text-gray-900">
                {getSuccessRate().toFixed(1)}%
              </p>
              <p className="text-xs text-gray-500 mt-1">
                {stats?.completed_jobs || 0} of {stats?.total_jobs || 0} jobs
              </p>
            </div>
          </div>
        </div>

        {/* Average Emails per Job */}
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow">
          <div className="flex items-center">
            <div className="p-2 bg-warning-100 rounded-lg">
              <TrendingUp className="h-6 w-6 text-warning-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Avg Emails/Job</p>
              <p className="text-2xl font-bold text-gray-900">
                {getAverageEmailsPerJob().toFixed(1)}
              </p>
              <p className="text-xs text-gray-500 mt-1">
                Per successful job
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
              {Object.entries(statusDistribution).map(([status, count]) => {
                const percentage = stats && stats.total_jobs > 0 ? (count / stats.total_jobs) * 100 : 0;
                return (
                  <div key={status} className="space-y-2">
                    <div className="flex items-center justify-between">
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
                          ({percentage.toFixed(1)}%)
                        </span>
                      </div>
                    </div>
                    {/* Visual progress bar */}
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className={`h-2 rounded-full transition-all duration-500 ${
                          status === 'completed' ? 'bg-success-500' :
                          status === 'running' ? 'bg-primary-500' :
                          status === 'failed' ? 'bg-error-500' :
                          'bg-warning-500'
                        }`}
                        style={{ width: `${percentage}%` }}
                      ></div>
                    </div>
                  </div>
                );
              })}
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