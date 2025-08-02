#!/bin/bash

# Hive Metastore initialization script
set -e

echo "Initializing Hive Metastore..."

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until nc -z postgres 5432; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready"

# Initialize schema if needed
cd /opt/hive/bin
echo "Initializing Hive schema..."
./schematool -dbType postgres -initSchema || echo "Schema already exists or initialization failed"

echo "Starting Hive Metastore service..."
exec ./hive --service metastore