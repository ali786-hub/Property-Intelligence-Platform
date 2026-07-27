FROM apache/airflow:2.10.4

# Install any OS-level dependencies if needed
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Airflow and its database driver
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.4" psycopg2-binary

# Create an isolated virtual environment in /tmp to prevent Docker volume shadowing
RUN python -m venv /tmp/propintel_venv
RUN /tmp/propintel_venv/bin/pip install --default-timeout=1000 --no-cache-dir -r /requirements.txt
RUN /tmp/propintel_venv/bin/pip install --default-timeout=1000 --no-cache-dir adlfs azure-storage-blob
