ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# 1. Set the destination for the COPY command
# We copy it to the current working directory (/opt/airflow)
COPY requirements.txt .

# 2. Install your dependencies
# Note: We removed the re-install of apache-airflow (it's already there)
# We run this as the default 'airflow' user provided by the base image
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy your Python scripts into the DAGs folder
# Airflow looks for scripts in /opt/airflow/dags
COPY --chown=airflow:root . /opt/airflow/dags

# Example addition to your Dockerfile
RUN pip install soda-core soda-core-postgres