import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { 
  Upload, 
  List, 
  BarChart3, 
  Activity, 
  Mail, 
  Clock, 
  CheckCircle, 
  XCircle,
  Zap,
  TrendingUp,
  Users,
  FileText
} from 'lucide-react';
import { useJobs } from '../contexts/JobContext';
import { ProcessingStats } from '../types/api';
import apiService from '../services/api';
import toast from 'react-hot-toast';

const Dashboard: React.FC = () => {
  const { state } = useJobs();
  const [health, setHealth] = useState<any>(null);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const healthData = await apiService.getHealth();
        setHealth(healthData);
      } catch (error) {
        console.error('Health check failed:', error);
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  const recentJobs = state.jobs.slice(0, 5);
  const stats = state.stats;

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
        return <CheckCircle className="h-4 w-4" />;
      case 'running':
        return <Activity className="h-4 w-4 animate-pulse" />;
      case 'failed':
        return <XCircle className="h-4 w-4" />;
      case 'queued':
        return <Clock className="h-4 w-4" />;
      default:
        return <Clock className="h-4 w-4" />;
    }
  };

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Email Scraper Dashboard
        </h1>
        <p className="text-gray-600">
          Advanced email discovery with real-time processing and analytics
        </p>
      </div>

      {/* Quick Stats */}
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
              <p className="text-sm font-medium text-gray-600">Completed Jobs</p>
              <p className="text-2xl font-bold text-gray-900">
                {stats?.completed_jobs || 0}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-warning-100 rounded-lg">
              <Activity className="h-6 w-6 text-warning-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Active Jobs</p>
              <p className="text-2xl font-bold text-gray-900">
                {stats?.active_jobs || 0}
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
              <p className="text-sm font-medium text-gray-600">Failed Jobs</p>
              <p className="text-2xl font-bold text-gray-900">
                {stats?.failed_jobs || 0}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Link
          to="/submit"
          className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow group"
        >
          <div className="flex items-center mb-4">
            <div className="p-2 bg-primary-100 rounded-lg group-hover:bg-primary-200 transition-colors">
              <Upload className="h-6 w-6 text-primary-600" />
            </div>
            <h3 className="ml-3 text-lg font-semibold text-gray-900">Submit New Job</h3>
          </div>
          <p className="text-gray-600">
            Upload a file and start email discovery with advanced processing options.
          </p>
        </Link>

        <Link
          to="/jobs"
          className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow group"
        >
          <div className="flex items-center mb-4">
            <div className="p-2 bg-success-100 rounded-lg group-hover:bg-success-200 transition-colors">
              <List className="h-6 w-6 text-success-600" />
            </div>
            <h3 className="ml-3 text-lg font-semibold text-gray-900">View All Jobs</h3>
          </div>
          <p className="text-gray-600">
            Monitor all your email discovery jobs with real-time status updates.
          </p>
        </Link>

        <Link
          to="/stats"
          className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow group"
        >
          <div className="flex items-center mb-4">
            <div className="p-2 bg-warning-100 rounded-lg group-hover:bg-warning-200 transition-colors">
              <BarChart3 className="h-6 w-6 text-warning-600" />
            </div>
            <h3 className="ml-3 text-lg font-semibold text-gray-900">Analytics</h3>
          </div>
          <p className="text-gray-600">
            View detailed statistics and performance metrics for your email discovery.
          </p>
        </Link>
      </div>

      {/* Recent Jobs */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Recent Jobs</h2>
        </div>
        <div className="divide-y divide-gray-200">
          {recentJobs.length === 0 ? (
            <div className="px-6 py-8 text-center text-gray-500">
              <FileText className="h-12 w-12 mx-auto mb-4 text-gray-300" />
              <p>No jobs found. Start by submitting a new job!</p>
            </div>
          ) : (
            recentJobs.map((job) => (
              <div key={job.job_id} className="px-6 py-4 hover:bg-gray-50">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    {getStatusIcon(job.status)}
                    <div>
                      <p className="text-sm font-medium text-gray-900">
                        Job {job.job_id.slice(-8)}
                      </p>
                      <p className="text-sm text-gray-500">
                        {job.files_processed[0]?.split('/').pop() || 'Unknown file'}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-4">
                    <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusColor(job.status)}`}>
                      {job.status}
                    </span>
                    <div className="text-right">
                      <p className="text-sm font-medium text-gray-900">
                        {job.total_emails} emails
                      </p>
                      <p className="text-xs text-gray-500">
                        {job.total_processed} processed
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
        {recentJobs.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t border-gray-200">
            <Link
              to="/jobs"
              className="text-sm text-primary-600 hover:text-primary-700 font-medium"
            >
              View all jobs →
            </Link>
          </div>
        )}
      </div>

      {/* System Status */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">System Status</h2>
        </div>
        <div className="px-6 py-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-600">API Status</span>
              <div className="flex items-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${health?.status === 'healthy' ? 'bg-success-500' : 'bg-error-500'}`}></div>
                <span className="text-sm font-medium text-gray-900">
                  {health?.status === 'healthy' ? 'Online' : 'Offline'}
                </span>
              </div>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-600">Kafka Status</span>
              <div className="flex items-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${health?.kafka_enabled ? 'bg-success-500' : 'bg-error-500'}`}></div>
                <span className="text-sm font-medium text-gray-900">
                  {health?.kafka_enabled ? 'Connected' : 'Disconnected'}
                </span>
              </div>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-600">Active Jobs</span>
              <span className="text-sm font-medium text-gray-900">
                {health?.active_jobs || 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-600">Last Updated</span>
              <span className="text-sm font-medium text-gray-900">
                {health?.timestamp ? new Date(health.timestamp * 1000).toLocaleTimeString() : 'Never'}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard; 