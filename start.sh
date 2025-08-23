#!/usr/bin/bash
set -e

echo "Checking if postgres user exists..."
if ! id "postgres" >/dev/null 2>&1; then
    echo "Creating postgres user..."
    useradd postgres
fi

if [[ ! -d /data/pg/data ]]; then
    echo "Creating data directories in /data/pg..."
    sudo mkdir -p /data/pg/data
    sudo chown -R postgres:postgres /data/pg
fi

echo "Starting postgreSQL"
sudo su postgres -c 'pg_ctl start -D /data/pg/data -l /data/pg/postgres.log'

# Do all Python pre-setup
. .env/bin/activate

echo "Sleeping 5 seconds..."
sleep 5

echo "Launching RQL bot..."
# python bot.py --preload --pg
