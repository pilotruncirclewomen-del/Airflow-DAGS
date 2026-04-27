FROM apache/airflow:3.1.8-python3.12

COPY docker/airflow/requirements.txt /requirements-turn-platform.txt
COPY docker/airflow/init.sh /opt/airflow/docker/airflow/init.sh
COPY docker/airflow/create_admin.py /opt/airflow/docker/airflow/create_admin.py

RUN pip install --no-cache-dir -r /requirements-turn-platform.txt && \
    chmod +x /opt/airflow/docker/airflow/init.sh

