# Email Scraper Frontend

A modern React frontend for the Email Scraper API, providing a comprehensive interface for managing email discovery jobs with real-time updates and analytics.

## Features

### 🚀 Core Functionality
- **Job Submission**: Upload files and configure processing parameters
- **Real-time Monitoring**: Live progress updates with WebSocket and SSE support
- **Job Management**: View, filter, and manage all email discovery jobs
- **Detailed Analytics**: Comprehensive statistics and performance metrics
- **File Support**: CSV, Excel (.xlsx, .xls), and NDJSON formats

### 🎨 User Interface
- **Modern Design**: Clean, responsive interface built with Tailwind CSS
- **Real-time Updates**: Live progress bars and status indicators
- **Interactive Charts**: Visual representation of job statistics
- **Mobile Responsive**: Optimized for all device sizes
- **Dark Mode Ready**: Prepared for theme switching

### 📊 Analytics & Monitoring
- **Dashboard Overview**: Key metrics and recent activity
- **Job Statistics**: Success rates, email counts, and performance metrics
- **Real-time Progress**: Live updates during job processing
- **Error Tracking**: Detailed error reporting and logging
- **System Health**: API and Kafka connectivity monitoring

## Technology Stack

- **React 18** with TypeScript
- **React Router** for navigation
- **Tailwind CSS** for styling
- **Axios** for API communication
- **React Hot Toast** for notifications
- **Lucide React** for icons
- **Date-fns** for date formatting
- **React Dropzone** for file uploads

## Getting Started

### Prerequisites

- Node.js 16+ and npm
- Email Scraper API running on `http://localhost:8000`

### Installation

1. **Install dependencies**:
   ```bash
   cd email-scraper-frontend
   npm install
   ```

2. **Start the development server**:
   ```bash
   npm start
   ```

3. **Open your browser**:
   Navigate to `http://localhost:3000`

### Building for Production

```bash
npm run build
```

The build files will be created in the `build/` directory.

## API Integration

The frontend communicates with the Email Scraper API through the following endpoints:

### Job Management
- `POST /api/process` - Submit new job
- `GET /api/jobs` - Get all jobs
- `GET /api/jobs/{job_id}` - Get specific job status
- `DELETE /api/jobs/{job_id}` - Delete job

### Real-time Updates
- `GET /api/stream-results/{job_id}` - Server-Sent Events for progress
- `WS /ws/{job_id}` - WebSocket for real-time updates

### Analytics
- `GET /api/stats` - Get processing statistics
- `GET /api/health` - Health check

## Project Structure

```
src/
├── components/          # Reusable UI components
│   └── Navbar.tsx      # Navigation component
├── contexts/           # React contexts
│   └── JobContext.tsx  # Global job state management
├── pages/              # Page components
│   ├── Dashboard.tsx   # Main dashboard
│   ├── JobSubmit.tsx   # Job submission form
│   ├── JobList.tsx     # Job listing with filters
│   ├── JobDetails.tsx  # Detailed job view
│   └── Statistics.tsx  # Analytics and metrics
├── services/           # API services
│   └── api.ts         # API communication layer
├── types/              # TypeScript type definitions
│   └── api.ts         # API response types
├── App.tsx            # Main application component
└── index.tsx          # Application entry point
```

## Key Features Explained

### 1. Job Submission
- Drag-and-drop file upload
- Configurable processing parameters (workers, batch size, verbose logging)
- Real-time validation and feedback
- Support for multiple file formats

### 2. Real-time Monitoring
- Live progress updates every 5 seconds
- WebSocket connection for instant updates
- Progress bars and status indicators
- Automatic refresh for running jobs

### 3. Job Management
- Comprehensive job listing with search and filters
- Sort by date, status, or email count
- Bulk operations and individual job actions
- Detailed job information and error reporting

### 4. Analytics Dashboard
- Key performance indicators
- Job status distribution charts
- Success rate calculations
- Recent activity timeline
- System health monitoring

## Configuration

### Environment Variables

Create a `.env` file in the root directory:

```env
REACT_APP_API_URL=http://localhost:8000
```

### API Configuration

The frontend is configured to proxy requests to the API server. Update the `proxy` field in `package.json` if needed:

```json
{
  "proxy": "http://localhost:8000"
}
```

## Development

### Available Scripts

- `npm start` - Start development server
- `npm run build` - Build for production
- `npm test` - Run tests
- `npm run eject` - Eject from Create React App

### Code Style

The project uses:
- TypeScript for type safety
- ESLint for code linting
- Prettier for code formatting
- Tailwind CSS for styling

### Adding New Features

1. **New API Endpoints**: Add to `src/services/api.ts`
2. **New Types**: Add to `src/types/api.ts`
3. **New Pages**: Add to `src/pages/` and update routing in `App.tsx`
4. **New Components**: Add to `src/components/`

## Troubleshooting

### Common Issues

1. **API Connection Failed**
   - Ensure the Email Scraper API is running on port 8000
   - Check network connectivity
   - Verify CORS settings

2. **Real-time Updates Not Working**
   - Check WebSocket connection
   - Verify Kafka is running
   - Check browser console for errors

3. **File Upload Issues**
   - Ensure file format is supported (CSV, Excel, NDJSON)
   - Check file size limits
   - Verify file encoding (UTF-8)

### Debug Mode

Enable debug logging by setting the environment variable:

```env
REACT_APP_DEBUG=true
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Check the API documentation
- Review the troubleshooting section
- Open an issue on GitHub 