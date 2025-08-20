import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { 
  Search, 
  SortAsc, 
  SortDesc,
  Clock,
  CheckCircle,
  XCircle,
  Activity,
  Mail,
  Calendar,
  FileText,
  Trash2,
  Eye
} from 'lucide-react';
import { useJobs } from '../contexts/JobContext';
import { formatDistanceToNow } from 'date-fns';

const JobList: React.FC = () => {
  const { state, deleteJob } = useJobs();
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [sortBy, setSortBy] = useState<'start_time' | 'status' | 'total_emails'>('start_time');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');

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

  const filteredJobs = state.jobs
    .filter(job => {
      const matchesSearch = job.job_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
                           job.files_processed.some(file => file.toLowerCase().includes(searchTerm.toLowerCase()));
      const matchesStatus = statusFilter === 'all' || job.status === statusFilter;
      return matchesSearch && matchesStatus;
    })
    .sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case 'start_time':
          comparison = a.start_time - b.start_time;
          break;
        case 'status':
          comparison = a.status.localeCompare(b.status);
          break;
        case 'total_emails':
          comparison = a.total_emails - b.total_emails;
          break;
      }
      return sortOrder === 'asc' ? comparison : -comparison;
    });

  const handleSort = (field: 'start_time' | 'status' | 'total_emails') => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('desc');
    }
  };

  const handleDelete = async (jobId: string) => {
    if (window.confirm('Are you sure you want to delete this job?')) {
      await deleteJob(jobId);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">All Jobs</h1>
          <p className="text-gray-600">Monitor and manage your email discovery jobs</p>
        </div>
        <Link
          to="/submit"
          className="px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 transition-colors"
        >
          New Job
        </Link>
      </div>

      {/* Filters and Search */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search jobs..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 w-full border border-gray-300 rounded-md focus:ring-primary-500 focus:border-primary-500"
            />
          </div>

          {/* Status Filter */}
          <div>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-primary-500 focus:border-primary-500"
            >
              <option value="all">All Status</option>
              <option value="queued">Queued</option>
              <option value="running">Running</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
            </select>
          </div>

          {/* Sort */}
          <div className="flex space-x-2">
            <button
              onClick={() => handleSort('start_time')}
              className={`px-3 py-2 border rounded-md text-sm font-medium flex items-center space-x-1 ${
                sortBy === 'start_time'
                  ? 'border-primary-500 text-primary-600 bg-primary-50'
                  : 'border-gray-300 text-gray-700 hover:bg-gray-50'
              }`}
            >
              <Calendar className="h-4 w-4" />
              <span>Date</span>
              {sortBy === 'start_time' && (
                sortOrder === 'asc' ? <SortAsc className="h-4 w-4" /> : <SortDesc className="h-4 w-4" />
              )}
            </button>
            <button
              onClick={() => handleSort('total_emails')}
              className={`px-3 py-2 border rounded-md text-sm font-medium flex items-center space-x-1 ${
                sortBy === 'total_emails'
                  ? 'border-primary-500 text-primary-600 bg-primary-50'
                  : 'border-gray-300 text-gray-700 hover:bg-gray-50'
              }`}
            >
              <Mail className="h-4 w-4" />
              <span>Emails</span>
              {sortBy === 'total_emails' && (
                sortOrder === 'asc' ? <SortAsc className="h-4 w-4" /> : <SortDesc className="h-4 w-4" />
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Jobs List */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">
            Jobs ({filteredJobs.length})
          </h2>
        </div>
        
        {filteredJobs.length === 0 ? (
          <div className="px-6 py-12 text-center text-gray-500">
            <FileText className="h-12 w-12 mx-auto mb-4 text-gray-300" />
            <p>No jobs found matching your criteria.</p>
          </div>
        ) : (
          <div className="divide-y divide-gray-200">
            {filteredJobs.map((job) => (
              <div key={job.job_id} className="px-6 py-4 hover:bg-gray-50">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    {getStatusIcon(job.status)}
                    <div>
                      <div className="flex items-center space-x-2">
                        <p className="text-sm font-medium text-gray-900">
                          Job {job.job_id.slice(-8)}
                        </p>
                        <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusColor(job.status)}`}>
                          {job.status}
                        </span>
                      </div>
                      <p className="text-sm text-gray-500">
                        {job.job_type === 'folder' 
                          ? job.folder_name || 'Folder'
                          : job.files_processed[0]?.split('/').pop() || 'Unknown file'
                        }
                      </p>
                      <p className="text-xs text-gray-400">
                        Started {formatDistanceToNow(job.start_time * 1000, { addSuffix: true })}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center space-x-6">
                    <div className="text-right">
                      <p className="text-sm font-medium text-gray-900">
                        {job.total_emails} emails
                      </p>
                      <p className="text-xs text-gray-500">
                        {job.total_processed} processed
                      </p>
                      {job.progress > 0 && (
                        <div className="mt-1">
                          <div className="w-24 bg-gray-200 rounded-full h-1">
                            <div
                              className="bg-primary-600 h-1 rounded-full"
                              style={{ width: `${Math.min(job.progress * 100, 100)}%` }}
                            ></div>
                          </div>
                        </div>
                      )}
                    </div>

                    <div className="flex items-center space-x-2">
                      <Link
                        to={`/jobs/${job.job_id}`}
                        className="p-2 text-gray-400 hover:text-primary-600 hover:bg-primary-50 rounded-md transition-colors"
                        title="View Details"
                      >
                        <Eye className="h-4 w-4" />
                      </Link>
                      <button
                        onClick={() => handleDelete(job.job_id)}
                        className="p-2 text-gray-400 hover:text-error-600 hover:bg-error-50 rounded-md transition-colors"
                        title="Delete Job"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                </div>

                {job.errors.length > 0 && (
                  <div className="mt-3 p-3 bg-error-50 border border-error-200 rounded-md">
                    <p className="text-sm text-error-800">
                      <strong>Errors:</strong> {job.errors.join(', ')}
                    </p>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default JobList; 