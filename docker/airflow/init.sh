#!/usr/bin/env bash
set -e

echo "==> Running Airflow DB migration..."
airflow db migrate

echo "==> Creating admin user..."
python /opt/airflow/docker/airflow/create_admin.py

echo "==> Airflow init complete."
