#!/bin/bash

# Create jars directory if it doesn't exist
mkdir -p jars

# Download PostgreSQL JDBC driver
echo "Downloading PostgreSQL JDBC driver..."
curl -L -o jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Download MySQL JDBC driver (if needed)
echo "Downloading MySQL JDBC driver..."
curl -L -o jars/mysql-connector-java-8.0.33.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar

echo "JAR files downloaded successfully!"
echo "Files in jars directory:"
ls -la jars/