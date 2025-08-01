#!/bin/bash
"""
Deployment script for Email Scraper API
Provides easy commands for building, running, and managing the Docker container
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="email-scraper-api"
CONTAINER_NAME="email-scraper-api"
PORT="8000"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Function to create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    mkdir -p uploads data backups
    print_success "Directories created"
}

# Function to build the Docker image
build_image() {
    print_status "Building Docker image..."
    docker build -t $IMAGE_NAME .
    print_success "Docker image built successfully"
}

# Function to run the container
run_container() {
    print_status "Starting container..."
    docker run -d \
        --name $CONTAINER_NAME \
        -p $PORT:8000 \
        -v $(pwd)/uploads:/app/uploads \
        -v $(pwd)/data:/app/data \
        -v $(pwd)/backups:/app/backups \
        $IMAGE_NAME
    
    print_success "Container started successfully"
    print_status "API will be available at: http://localhost:$PORT"
    print_status "API Documentation: http://localhost:$PORT/docs"
    print_status "Health Check: http://localhost:$PORT/api/health"
}

# Function to stop the container
stop_container() {
    print_status "Stopping container..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    print_success "Container stopped and removed"
}

# Function to show logs
show_logs() {
    print_status "Showing container logs..."
    docker logs -f $CONTAINER_NAME
}

# Function to check container status
check_status() {
    print_status "Checking container status..."
    if docker ps | grep -q $CONTAINER_NAME; then
        print_success "Container is running"
        docker ps | grep $CONTAINER_NAME
    else
        print_warning "Container is not running"
    fi
}

# Function to test the API
test_api() {
    print_status "Testing API health endpoint..."
    if curl -f http://localhost:$PORT/api/health > /dev/null 2>&1; then
        print_success "API is responding"
        curl -s http://localhost:$PORT/api/health | python -m json.tool
    else
        print_error "API is not responding"
    fi
}

# Function to clean up
cleanup() {
    print_status "Cleaning up..."
    stop_container
    docker rmi $IMAGE_NAME 2>/dev/null || true
    print_success "Cleanup completed"
}

# Function to show help
show_help() {
    echo "Email Scraper API Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build     - Build the Docker image"
    echo "  run       - Build and run the container"
    echo "  start     - Start the container (if already built)"
    echo "  stop      - Stop and remove the container"
    echo "  restart   - Restart the container"
    echo "  logs      - Show container logs"
    echo "  status    - Check container status"
    echo "  test      - Test the API health endpoint"
    echo "  cleanup   - Stop container and remove image"
    echo "  help      - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 run     # Build and start the API"
    echo "  $0 logs    # View API logs"
    echo "  $0 test    # Test API health"
}

# Main script logic
case "${1:-help}" in
    build)
        check_docker
        create_directories
        build_image
        ;;
    run)
        check_docker
        create_directories
        stop_container
        build_image
        run_container
        ;;
    start)
        check_docker
        run_container
        ;;
    stop)
        check_docker
        stop_container
        ;;
    restart)
        check_docker
        stop_container
        run_container
        ;;
    logs)
        show_logs
        ;;
    status)
        check_status
        ;;
    test)
        test_api
        ;;
    cleanup)
        check_docker
        cleanup
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac 