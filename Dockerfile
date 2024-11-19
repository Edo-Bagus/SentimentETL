FROM apache/airflow:2.7.2

# Switch to root to install dependencies
USER airflow

# Install the necessary dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user after installation
USER airflow
