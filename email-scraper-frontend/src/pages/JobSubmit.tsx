import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { 
  Upload, 
  FileText, 
  Settings, 
  Play, 
  AlertCircle,
  CheckCircle,
  XCircle,
  Info
} from 'lucide-react';
import { UploadJobRequest } from '../types/api';
import apiService from '../services/api';
import toast from 'react-hot-toast';

const JobSubmit: React.FC = () => {
  const navigate = useNavigate();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [config, setConfig] = useState({
    workers: 150,
    batch_size: 100,
    verbose: true
  });

  const { getRootProps, getInputProps, isDragActive, acceptedFiles } = useDropzone({
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
      'application/json': ['.ndjson']
    },
    maxFiles: 1,
    onDrop: (acceptedFiles) => {
      if (acceptedFiles.length > 0) {
        const file = acceptedFiles[0];
        setSelectedFile(file);
        toast.success(`File selected: ${file.name}`);
      }
    }
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!selectedFile) {
      toast.error('Please select a file first');
      return;
    }

    setIsSubmitting(true);
    
    try {
      const uploadRequest: UploadJobRequest = {
        file: selectedFile,
        workers: config.workers,
        batch_size: config.batch_size,
        verbose: config.verbose
      };

      const response = await apiService.uploadAndProcessFile(uploadRequest);
      
      toast.success(`Job submitted successfully! Job ID: ${response.job_id}`);
      navigate(`/jobs/${response.job_id}`);
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || error.message || 'Failed to submit job';
      toast.error(errorMessage);
    } finally {
      setIsSubmitting(false);
    }
  };

  const getFileIcon = (fileName: string) => {
    const extension = fileName.split('.').pop()?.toLowerCase();
    switch (extension) {
      case 'csv':
        return <FileText className="h-8 w-8 text-blue-500" />;
      case 'xlsx':
      case 'xls':
        return <FileText className="h-8 w-8 text-green-500" />;
      case 'ndjson':
        return <FileText className="h-8 w-8 text-purple-500" />;
      default:
        return <FileText className="h-8 w-8 text-gray-500" />;
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-8">
      {/* Header */}
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Submit New Email Discovery Job
        </h1>
        <p className="text-gray-600">
          Upload a file and configure processing options to start email discovery
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-8">
        {/* File Upload Section */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Upload className="h-5 w-5 mr-2" />
              File Upload
            </h2>
          </div>
          <div className="p-6">
            <div
              {...getRootProps()}
              className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors ${
                isDragActive
                  ? 'border-primary-400 bg-primary-50'
                  : 'border-gray-300 hover:border-primary-400 hover:bg-gray-50'
              }`}
            >
              <input {...getInputProps()} />
              <Upload className="h-12 w-12 mx-auto mb-4 text-gray-400" />
              {isDragActive ? (
                <p className="text-primary-600 font-medium">Drop the file here...</p>
              ) : (
                <div>
                  <p className="text-gray-600 mb-2">
                    Drag and drop a file here, or click to select
                  </p>
                  <p className="text-sm text-gray-500">
                    Supports CSV, Excel (.xlsx, .xls), and NDJSON files
                  </p>
                </div>
              )}
            </div>

            {/* Selected File */}
            {acceptedFiles.length > 0 && (
              <div className="mt-4 p-4 bg-success-50 border border-success-200 rounded-lg">
                <div className="flex items-center">
                  {getFileIcon(acceptedFiles[0].name)}
                  <div className="ml-3">
                    <p className="text-sm font-medium text-success-900">
                      {acceptedFiles[0].name}
                    </p>
                    <p className="text-sm text-success-700">
                      {(acceptedFiles[0].size / 1024 / 1024).toFixed(2)} MB
                    </p>
                  </div>
                  <CheckCircle className="h-5 w-5 text-success-500 ml-auto" />
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Configuration Section */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Settings className="h-5 w-5 mr-2" />
              Processing Configuration
            </h2>
          </div>
          <div className="p-6 space-y-6">
            {/* Workers */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Number of Workers
              </label>
              <div className="flex items-center space-x-4">
                <input
                  type="range"
                  min="1"
                  max="500"
                  value={config.workers}
                  onChange={(e) => setConfig({ ...config, workers: parseInt(e.target.value) })}
                  className="flex-1"
                />
                <span className="text-sm font-medium text-gray-900 w-16">
                  {config.workers}
                </span>
              </div>
              <p className="text-sm text-gray-500 mt-1">
                Number of concurrent worker threads (1-500)
              </p>
            </div>

            {/* Batch Size */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Batch Size
              </label>
              <div className="flex items-center space-x-4">
                <input
                  type="range"
                  min="10"
                  max="2000"
                  value={config.batch_size}
                  onChange={(e) => setConfig({ ...config, batch_size: parseInt(e.target.value) })}
                  className="flex-1"
                />
                <span className="text-sm font-medium text-gray-900 w-16">
                  {config.batch_size}
                </span>
              </div>
              <p className="text-sm text-gray-500 mt-1">
                Number of companies processed per batch (10-2000)
              </p>
            </div>

            {/* Verbose Logging */}
            <div className="flex items-center">
              <input
                type="checkbox"
                id="verbose"
                checked={config.verbose}
                onChange={(e) => setConfig({ ...config, verbose: e.target.checked })}
                className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
              />
              <label htmlFor="verbose" className="ml-2 block text-sm text-gray-900">
                Enable verbose logging
              </label>
            </div>
          </div>
        </div>

        {/* Information Section */}
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex">
            <Info className="h-5 w-5 text-blue-400 mt-0.5 mr-3" />
            <div>
              <h3 className="text-sm font-medium text-blue-800">Processing Information</h3>
              <div className="mt-2 text-sm text-blue-700">
                <ul className="list-disc list-inside space-y-1">
                  <li>Files are processed directly on the server</li>
                  <li>Results are written back to the original file with email data</li>
                  <li>Processing time depends on file size and number of companies</li>
                  <li>Real-time progress updates are available during processing</li>
                </ul>
              </div>
            </div>
          </div>
        </div>

        {/* Submit Button */}
        <div className="flex justify-end space-x-4">
          <button
            type="button"
            onClick={() => navigate('/')}
            className="px-6 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={isSubmitting || !selectedFile}
            className="px-6 py-2 bg-primary-600 border border-transparent rounded-md text-sm font-medium text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center"
          >
            {isSubmitting ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Submitting...
              </>
            ) : (
              <>
                <Play className="h-4 w-4 mr-2" />
                Start Processing
              </>
            )}
          </button>
        </div>
      </form>
    </div>
  );
};

export default JobSubmit; 