FROM apache/airflow:3.3.1

# Environnement Python séparé pour notre pipeline, isolé d'Airflow
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /home/airflow/pipeline-venv \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt