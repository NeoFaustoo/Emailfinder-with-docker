import React, { createContext, useContext, useReducer, useEffect } from 'react';
import { JobStatus, ProcessingStats } from '../types/api';
import { apiService } from '../services/api';
import toast from 'react-hot-toast';

interface JobState {
  jobs: JobStatus[];
  stats: ProcessingStats | null;
  loading: boolean;
  error: string | null;
}

type JobAction =
  | { type: 'SET_LOADING'; payload: boolean }
  | { type: 'SET_ERROR'; payload: string | null }
  | { type: 'SET_JOBS'; payload: JobStatus[] }
  | { type: 'UPDATE_JOB'; payload: JobStatus }
  | { type: 'ADD_JOB'; payload: JobStatus }
  | { type: 'REMOVE_JOB'; payload: string }
  | { type: 'SET_STATS'; payload: ProcessingStats };

const initialState: JobState = {
  jobs: [],
  stats: null,
  loading: false,
  error: null,
};

function jobReducer(state: JobState, action: JobAction): JobState {
  switch (action.type) {
    case 'SET_LOADING':
      return { ...state, loading: action.payload };
    case 'SET_ERROR':
      return { ...state, error: action.payload };
    case 'SET_JOBS':
      return { ...state, jobs: action.payload };
    case 'UPDATE_JOB':
      return {
        ...state,
        jobs: state.jobs.map(job =>
          job.job_id === action.payload.job_id ? action.payload : job
        ),
      };
    case 'ADD_JOB':
      return {
        ...state,
        jobs: [action.payload, ...state.jobs],
      };
    case 'REMOVE_JOB':
      return {
        ...state,
        jobs: state.jobs.filter(job => job.job_id !== action.payload),
      };
    case 'SET_STATS':
      return { ...state, stats: action.payload };
    default:
      return state;
  }
}

interface JobContextType {
  state: JobState;
  dispatch: React.Dispatch<JobAction>;
  fetchJobs: () => Promise<void>;
  fetchStats: () => Promise<void>;
  refreshJob: (jobId: string) => Promise<void>;
  deleteJob: (jobId: string) => Promise<void>;
}

const JobContext = createContext<JobContextType | undefined>(undefined);

export function JobProvider({ children }: { children: React.ReactNode }) {
  const [state, dispatch] = useReducer(jobReducer, initialState);

  const fetchJobs = async () => {
    try {
      dispatch({ type: 'SET_LOADING', payload: true });
      const jobs = await apiService.getAllJobs();
      dispatch({ type: 'SET_JOBS', payload: jobs });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch jobs';
      dispatch({ type: 'SET_ERROR', payload: errorMessage });
      toast.error(errorMessage);
    } finally {
      dispatch({ type: 'SET_LOADING', payload: false });
    }
  };

  const fetchStats = async () => {
    try {
      const stats = await apiService.getStats();
      dispatch({ type: 'SET_STATS', payload: stats });
    } catch (error) {
      console.error('Failed to fetch stats:', error);
    }
  };

  const refreshJob = async (jobId: string) => {
    try {
      const job = await apiService.getJobStatus(jobId);
      dispatch({ type: 'UPDATE_JOB', payload: job });
    } catch (error) {
      console.error('Failed to refresh job:', error);
    }
  };

  const deleteJob = async (jobId: string) => {
    try {
      await apiService.deleteJob(jobId);
      dispatch({ type: 'REMOVE_JOB', payload: jobId });
      toast.success('Job deleted successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to delete job';
      toast.error(errorMessage);
    }
  };

  // Auto-refresh jobs every 5 seconds, pause when tab is hidden to reduce load
  useEffect(() => {
    let interval: number | undefined;

    const startPolling = () => {
      fetchJobs();
      fetchStats();
      interval = window.setInterval(() => {
        fetchJobs();
        fetchStats();
      }, 5000);
    };

    const stopPolling = () => {
      if (interval) {
        window.clearInterval(interval);
        interval = undefined;
      }
    };

    const handleVisibility = () => {
      if (document.hidden) {
        stopPolling();
      } else {
        startPolling();
      }
    };

    startPolling();
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      stopPolling();
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, []);

  const value: JobContextType = {
    state,
    dispatch,
    fetchJobs,
    fetchStats,
    refreshJob,
    deleteJob,
  };

  return <JobContext.Provider value={value}>{children}</JobContext.Provider>;
}

export function useJobs() {
  const context = useContext(JobContext);
  if (context === undefined) {
    throw new Error('useJobs must be used within a JobProvider');
  }
  return context;
} 