FROM apache/airflow:2.9.1

# Switch to root to install system dependencies if needed (optional)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install python packages
USER airflow

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set Matplotlib backend to Agg (Non-interactive)
# This prevents your scripts from crashing when they try to pop up a plot window
ENV MPLBACKEND=Agg