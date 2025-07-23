#!/bin/bash
# deployment.sh - Complete deployment script

echo "🚀 Email Scraper Web App Deployment"
echo "====================================="

# Create project structure
create_project_structure() {
    echo "📁 Creating project structure..."
    
    mkdir -p email-scraper-web/{uploads,results,templates,static,data}
    cd email-scraper-web
    
    # Copy your enhanced_email_scraper.py to the project directory
    # You'll need to copy the enhanced scraper script manually
    
    echo "✅ Project structure created"
}

# Build and run with Docker Compose
deploy_with_compose() {
    echo "🐳 Building and deploying with Docker Compose..."
    
    docker-compose down --remove-orphans
    docker-compose build --no-cache
    docker-compose up -d
    
    echo "⏳ Waiting for service to be ready..."
    sleep 10
    
    echo "🔍 Checking service health..."
    if curl -f http://localhost:5000/ > /dev/null 2>&1; then
        echo "✅ Service is running successfully!"
        echo "🌐 Access the web app at: http://localhost:5000"
    else
        echo "❌ Service health check failed"
        echo "📋 Checking logs..."
        docker-compose logs email-scraper-web
        exit 1
    fi
}

# Build standalone Docker image
build_docker_image() {
    echo "🐳 Building Docker image..."
    
    docker build -t email-scraper-web:latest .
    
    if [ $? -eq 0 ]; then
        echo "✅ Docker image built successfully!"
        echo "🚀 Run with: docker run -p 5000:5000 -v $(pwd)/uploads:/app/uploads -v $(pwd)/results:/app/results email-scraper-web:latest"
    else
        echo "❌ Docker build failed"
        exit 1
    fi
}

# Run development server
run_dev_server() {
    echo "🔧 Starting development server..."
    
    # Install dependencies
    pip install -r requirements.txt
    
    # Run Flask development server
    export FLASK_APP=app.py
    export FLASK_ENV=development
    python app.py
}

# Main deployment options
case "${1:-compose}" in
    "structure")
        create_project_structure
        ;;
    "compose")
        deploy_with_compose
        ;;
    "docker")
        build_docker_image
        ;;
    "dev")
        run_dev_server
        ;;
    "full")
        create_project_structure
        deploy_with_compose
        ;;
    *)
        echo "Usage: $0 {structure|compose|docker|dev|full}"
        echo ""
        echo "Commands:"
        echo "  structure  - Create project directories"
        echo "  compose    - Deploy with Docker Compose (recommended)"
        echo "  docker     - Build Docker image only"
        echo "  dev        - Run development server"
        echo "  full       - Complete setup and deployment"
        exit 1
        ;;
esac