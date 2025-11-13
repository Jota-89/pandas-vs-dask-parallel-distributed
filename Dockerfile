FROM python:3.10-slim

WORKDIR /workspace

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Instalar Airflow primero con sus constraints oficiales
RUN pip install --no-cache-dir \
    apache-airflow==2.7.0 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.10.txt"

# Instalar dependencias adicionales VERSIONES COMPATIBLES
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas==1.5.3 \
    numpy==1.24.4 \
    dask[complete]==2023.4.1 \
    distributed==2023.4.1 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    pyarrow==12.0.1 \
    jupyter==1.0.0 \
    psutil==5.9.5

# Crear directorios
RUN mkdir -p data results dags logs

# Configurar Airflow  
ENV AIRFLOW_HOME=/workspace
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags

EXPOSE 8888 8787 8080

# Inicio con múltiples workers Dask + Airflow
CMD ["sh", "-c", "until pg_isready -h postgres -p 5432 -U airflow; do sleep 2; done && airflow db init && airflow users create --username demo --firstname Demo --lastname User --role Admin --email demo@example.com --password demo 2>/dev/null || true; rm -f /workspace/airflow-webserver.pid || true; dask-scheduler --host 0.0.0.0 --port 8786 --dashboard-address 0.0.0.0:8787 & sleep 5 && for i in {1..4}; do dask-worker tcp://localhost:8786 --nthreads 2 --memory-limit 2GB & sleep 1; done && airflow scheduler & sleep 5 && airflow webserver -p 8080"]