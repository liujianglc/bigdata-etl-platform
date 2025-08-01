#!/bin/bash

set -e
set -u

function create_user_and_database() {
	local database=$1
	local user=$2
	local password=$3
	echo "  Creating user '$user' and database '$database'"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	    CREATE USER $user WITH PASSWORD '$password';
	    CREATE DATABASE $database;
	    GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
	echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
	create_user_and_database "hive_metastore" "hive" "hive"
	echo "Multiple databases created"
fi