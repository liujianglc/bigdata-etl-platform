#!/bin/bash

echo "🔨 Building custom Airflow image..."

# Build the custom Airflow image
docker build -f Dockerfile.airflow -t custom-airflow:latest .

if [ $? -eq 0 ]; then
    echo "✅ Custom Airflow image built successfully!"
    echo "📋 Image details:"
    docker images | grep custom-airflow
else
    echo "❌ Failed to build custom Airflow image"
    exit 1
fi

echo ""
echo "🚀 You can now run docker-compose up to start the services"
echo "   The custom image includes all required Python packages"