import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { 
  Upload, 
  FileText, 
  Settings, 
  Play, 
  AlertCircle,
  CheckCircle,
  Info,
  Activity,
  Folder
} from 'lucide-react';
import { ProcessFileRequest, ProcessFolderRequest } from '../types/api';
import { apiService } from '../services/api';
import toast from 'react-hot-toast';

const JobSubmit: React.FC = () => {
  const navigate = useNavigate();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [folderPath, setFolderPath] = useState<string>('');
  const [config, setConfig] = useState({
    workers: 200,
    batch_size: 200,
    verbose: true
  });
  const [healthStatus, setHealthStatus] = useState<any>(null);
  const [uploadMode, setUploadMode] = useState<'file' | 'zip' | 'folder-path'>('file');
  const [performanceInfo, setPerformanceInfo] = useState({
    optimized: false,
    maxWorkers: 300,
    semaphoreLimit: 300,
    connectionPool: 1000
  });
  const [folderConfig, setFolderConfig] = useState({
    delete_originals: false,
    output_format: 'zip',
    workers: 200,
    batch_size: 200,
    verbose: true
  });

  const { getRootProps, getInputProps, isDragActive, acceptedFiles } = useDropzone({
    accept: uploadMode === 'file' ? {
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
      'application/json': ['.ndjson', '.json']
    } : uploadMode === 'zip' ? {
      'application/zip': ['.zip']
    } : {
      'text/plain': ['.txt'],
      'text/csv': ['.csv']
    },
    maxFiles: 1,
    onDrop: (acceptedFiles) => {
      if (acceptedFiles.length > 0) {
        const file = acceptedFiles[0];
        setSelectedFile(file);
        const modeText = uploadMode === 'file' ? 'File' : uploadMode === 'zip' ? 'ZIP file' : 'Folder';
        toast.success(`${modeText} selected: ${file.name}`);
      }
    }
  });

  useEffect(() => {
    // Check health status on component mount
    const checkHealth = async () => {
      try {
        const health = await apiService.getHealth();
        setHealthStatus(health);
        
        // Update performance info based on health status
        setPerformanceInfo({
          optimized: health.email_scraper_ready,
          maxWorkers: 300, // Our new max workers
          semaphoreLimit: 200, // Optimized batch processing
          connectionPool: 1000 // High connection pool
        });
      } catch (error) {
        console.error('Failed to check health:', error);
      }
    };
    checkHealth();
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (uploadMode === 'folder-path' && !folderPath) {
      toast.error('Please enter a folder path');
      return;
    }
    
    if (uploadMode !== 'folder-path' && !selectedFile) {
      toast.error(`Please select a ${uploadMode === 'file' ? 'file' : 'ZIP file'} first`);
      return;
    }

    setIsSubmitting(true);
    
    try {
      let response;
      
      if (uploadMode === 'file') {
        const processRequest: ProcessFileRequest = {
          file: selectedFile!,
          workers: config.workers,
          batch_size: config.batch_size,
          verbose: config.verbose
        };
        response = await apiService.processFile(processRequest);
      } else if (uploadMode === 'zip') {
        // ZIP file upload mode
        const processRequest: ProcessFileRequest = {
          file: selectedFile!,
          workers: config.workers,
          batch_size: config.batch_size,
          verbose: config.verbose
        };
        response = await apiService.processZipFile(processRequest);
      } else {
        // Folder path mode - process folder path on server
        const processRequest: ProcessFolderRequest = {
          file_path: folderPath,
          workers: folderConfig.workers,
          batch_size: folderConfig.batch_size,
          verbose: folderConfig.verbose
        };
        response = await apiService.processFolder(processRequest);
      }
      
      toast.success(`Job submitted successfully! Job ID: ${response.job_id}`);
      navigate(`/jobs/${response.job_id}`);
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || error.message || 'Failed to submit job';
      toast.error(errorMessage);
    } finally {
      setIsSubmitting(false);
    }
  };

  // Removed createZipFromFiles - no longer supporting folder path processing

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

      {/* System Status */}
      {healthStatus && (
        <div className={`rounded-lg p-4 ${healthStatus.email_scraper_ready ? 'bg-green-50 border border-green-200' : 'bg-yellow-50 border border-yellow-200'}`}>
          <div className="flex items-center">
            {healthStatus.email_scraper_ready ? (
              <Activity className="h-5 w-5 text-green-600 mr-3" />
            ) : (
              <AlertCircle className="h-5 w-5 text-yellow-600 mr-3" />
            )}
            <div>
              <h3 className={`text-sm font-medium ${healthStatus.email_scraper_ready ? 'text-green-800' : 'text-yellow-800'}`}>
                {healthStatus.email_scraper_ready ? 'Email Scraper Ready' : 'Running in Mock Mode'}
              </h3>
              <p className={`text-sm ${healthStatus.email_scraper_ready ? 'text-green-700' : 'text-yellow-700'}`}>
                {healthStatus.email_scraper_ready 
                  ? 'Your enhanced email scraper is loaded and ready to process files.'
                  : 'The email scraper is not available. Jobs will run in demonstration mode with mock data.'}
              </p>
            </div>
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-8">
        {/* Upload Mode Toggle */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Settings className="h-5 w-5 mr-2" />
              Upload Mode
            </h2>
          </div>
          <div className="p-6">
            <div className="flex space-x-4">
              <button
                type="button"
                onClick={() => setUploadMode('file')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  uploadMode === 'file'
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Single File
              </button>
              <button
                type="button"
                onClick={() => setUploadMode('zip')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  uploadMode === 'zip'
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                ZIP Upload
              </button>
              <button
                type="button"
                onClick={() => setUploadMode('folder-path')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  uploadMode === 'folder-path'
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Server Folder
              </button>
            </div>
            <p className="text-sm text-gray-600 mt-2">
              {uploadMode === 'file' 
                ? 'Upload a single CSV, Excel, or NDJSON file for processing'
                : uploadMode === 'zip'
                ? 'Upload a ZIP file containing CSV, Excel, or NDJSON files'
                : 'Specify a folder path on the server for batch processing'
              }
            </p>
          </div>
        </div>

        {/* File Upload Section */}
        {(uploadMode === 'file' || uploadMode === 'zip') && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200">
            <div className="px-6 py-4 border-b border-gray-200">
                              <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                  <Upload className="h-5 w-5 mr-2" />
                  {uploadMode === 'file' ? 'File Upload' : 'ZIP File Upload'}
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
                      {uploadMode === 'file' 
                        ? 'Supports CSV, Excel (.xlsx, .xls), JSON, and NDJSON files'
                        : 'Upload a ZIP file containing CSV, Excel, JSON, or NDJSON files'
                      }
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
        )}

        {/* Server Folder Path Section */}
        {uploadMode === 'folder-path' && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200">
            <div className="px-6 py-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                <Folder className="h-5 w-5 mr-2" />
                Server Folder Path
              </h2>
            </div>
            <div className="p-6">
              <div className="space-y-4">
                <div>
                  <label htmlFor="folderPathInput" className="block text-sm font-medium text-gray-700 mb-2">
                    Enter Folder Path on Server
                  </label>
                  <input
                    type="text"
                    id="folderPathInput"
                    value={folderPath}
                    onChange={(e) => setFolderPath(e.target.value)}
                    placeholder="e.g., /path/to/your/data/folder"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
                  />
                  <p className="text-sm text-gray-500 mt-1">
                    Enter the absolute path to the folder containing CSV, Excel, JSON, NDJSON, or ZIP files
                  </p>
                </div>
                
                {folderPath && (
                  <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
                    <div className="flex items-center">
                      <Folder className="h-5 w-5 text-blue-500 mr-2" />
                      <div className="flex-1">
                        <p className="text-sm font-medium text-blue-900">
                          Folder Path Entered
                        </p>
                        <p className="text-sm text-blue-700 font-mono">
                          {folderPath}
                        </p>
                        <p className="text-xs text-blue-600 mt-1">
                          Server will process all valid files in this folder
                        </p>
                      </div>
                      <CheckCircle className="h-5 w-5 text-blue-500 ml-auto" />
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Configuration Section */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Settings className="h-5 w-5 mr-2" />
              Processing Configuration
            </h2>
          </div>
          <div className="p-6 space-y-6">
            {/* Folder/ZIP-specific options */}
            {(uploadMode === 'folder-path' || uploadMode === 'zip') && (
              <>
                <div className="border-t pt-6">
                  <h3 className="text-md font-medium text-gray-900 mb-4">
                    {uploadMode === 'folder-path' ? 'Folder Processing Options' : 'ZIP Processing Options'}
                  </h3>
                  
                  {/* Delete Originals - only for folder path mode */}
                  {uploadMode === 'folder-path' && (
                    <div className="flex items-center mb-4">
                      <input
                        type="checkbox"
                        id="delete_originals"
                        checked={folderConfig.delete_originals}
                        onChange={(e) => setFolderConfig({ ...folderConfig, delete_originals: e.target.checked })}
                        className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
                      />
                      <label htmlFor="delete_originals" className="ml-2 block text-sm text-gray-900">
                        Delete original files after processing
                      </label>
                    </div>
                  )}
                  
                  {/* Output Format */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Output Format
                    </label>
                    <select
                      value={folderConfig.output_format}
                      onChange={(e) => setFolderConfig({ ...folderConfig, output_format: e.target.value })}
                      className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
                    >
                      <option value="zip">ZIP Archive</option>
                      <option value="individual">Individual Files</option>
                    </select>
                    <p className="text-sm text-gray-500 mt-1">
                      Choose how processed files should be delivered
                    </p>
                  </div>
                </div>
              </>
            )}
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
                    value={uploadMode === 'file' ? config.workers : (uploadMode === 'zip' ? config.workers : folderConfig.workers)}
                    onChange={(e) => {
                      const value = parseInt(e.target.value);
                      if (uploadMode === 'file' || uploadMode === 'zip') {
                        setConfig({ ...config, workers: value });
                      } else {
                        setFolderConfig({ ...folderConfig, workers: value });
                      }
                    }}
                    className="flex-1"
                  />
                  <span className="text-sm font-medium text-gray-900 w-16">
                    {uploadMode === 'file' ? config.workers : (uploadMode === 'zip' ? config.workers : folderConfig.workers)}
                  </span>
                </div>
                <p className="text-sm text-gray-500 mt-1">
                  Number of concurrent worker threads (1-500) - Will be automatically optimized based on file size
                </p>
                
                {/* Performance Optimization Info */}
                <div className={`mt-3 p-3 border rounded-md ${
                  performanceInfo.optimized 
                    ? 'bg-green-50 border-green-200' 
                    : 'bg-yellow-50 border-yellow-200'
                }`}>
                  <div className="flex items-start">
                    <Activity className={`h-4 w-4 mt-0.5 mr-2 flex-shrink-0 ${
                      performanceInfo.optimized ? 'text-green-600' : 'text-yellow-600'
                    }`} />
                    <div className="text-sm">
                      <p className={`font-medium ${
                        performanceInfo.optimized ? 'text-green-800' : 'text-yellow-800'
                      }`}>
                        {performanceInfo.optimized ? '🚀 Performance Optimizations Active' : '⚠️ Performance Optimizations Loading...'}
                      </p>
                      {performanceInfo.optimized ? (
                        <>
                          <ul className="mt-1 text-green-700 space-y-1">
                            <li>• Max workers: {performanceInfo.maxWorkers}</li>
                            <li>• Semaphore limit: {performanceInfo.semaphoreLimit} concurrent operations</li>
                            <li>• Connection pool: {performanceInfo.connectionPool} connections</li>
                            <li>• Memory monitoring with auto garbage collection</li>
                            <li>• Circuit breaker for failed domains</li>
                            <li>• DNS caching and connection optimization</li>
                          </ul>
                          <p className="mt-2 text-xs text-green-600">
                            Expected performance: 2-4x faster than standard processing
                          </p>
                        </>
                      ) : (
                        <p className="mt-1 text-yellow-700">
                          Optimizations will be available once the scraper is fully initialized
                        </p>
                      )}
                    </div>
                  </div>
                </div>
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
                    value={uploadMode === 'file' ? config.batch_size : (uploadMode === 'zip' ? config.batch_size : folderConfig.batch_size)}
                    onChange={(e) => {
                      const value = parseInt(e.target.value);
                      if (uploadMode === 'file' || uploadMode === 'zip') {
                        setConfig({ ...config, batch_size: value });
                      } else {
                        setFolderConfig({ ...folderConfig, batch_size: value });
                      }
                    }}
                    className="flex-1"
                  />
                  <span className="text-sm font-medium text-gray-900 w-16">
                    {uploadMode === 'file' ? config.batch_size : (uploadMode === 'zip' ? config.batch_size : folderConfig.batch_size)}
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
                  checked={uploadMode === 'file' ? config.verbose : (uploadMode === 'zip' ? config.verbose : folderConfig.verbose)}
                  onChange={(e) => {
                    if (uploadMode === 'file' || uploadMode === 'zip') {
                      setConfig({ ...config, verbose: e.target.checked });
                    } else {
                      setFolderConfig({ ...folderConfig, verbose: e.target.checked });
                    }
                  }}
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
            disabled={isSubmitting || (uploadMode === 'folder-path' ? !folderPath : !selectedFile)}
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