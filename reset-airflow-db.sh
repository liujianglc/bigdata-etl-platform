#!/bin/bash

# Complete Airflow database reset
echo "🔄 Resetting Airflow database completely..."

# Stop all services
echo "⏹️  Stopping all services..."
docker-compose down

# Remove Airflow database
echo "🗑️  Removing Airflow database..."
docker-compose exec postgres psql -U airflow -c "DROP DATABASE IF EXISTS airflow;"
docker-compose exec postgres psql -U airflow -c "CREATE DATABASE airflow;"

# Start only required services for initialization
echo "🚀 Starting core services..."
docker-compose up -d postgres redis namenode datanode hive-metastore

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Run Airflow initialization
echo "🔧 Initializing Airflow..."
docker-compose up airflow-init

# Start all services
echo "🌟 Starting all services..."
docker-compose up -d

echo "✅ Airflow database reset completed!"
echo "🌐 Airflow UI will be available at: http://localhost:8082"
echo "👤 Login: admin / admin123"