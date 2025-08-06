import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import Navbar from './components/Navbar';
import Dashboard from './pages/Dashboard';
import JobSubmit from './pages/JobSubmit';
import JobDetails from './pages/JobDetails';
import JobList from './pages/JobList';
import Statistics from './pages/Statistics';
import { JobProvider } from './contexts/JobContext';

function App() {
  return (
    <JobProvider>
      <Router>
        <div className="min-h-screen bg-gray-50">
          <Navbar />
          <main className="container mx-auto px-4 py-8">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/submit" element={<JobSubmit />} />
              <Route path="/jobs" element={<JobList />} />
              <Route path="/jobs/:jobId" element={<JobDetails />} />
              <Route path="/stats" element={<Statistics />} />
            </Routes>
          </main>
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#363636',
                color: '#fff',
              },
              success: {
                duration: 3000,
                iconTheme: {
                  primary: '#22c55e',
                  secondary: '#fff',
                },
              },
              error: {
                duration: 5000,
                iconTheme: {
                  primary: '#ef4444',
                  secondary: '#fff',
                },
              },
            }}
          />
        </div>
      </Router>
    </JobProvider>
  );
}

export default App; 