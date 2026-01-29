FROM apache/airflow:3.1.6

# Switch to root to install system packages
USER root

# Install Java (OpenJDK 17 is standard for Spark 3.5)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so Spark knows where to look
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user
USER airflow

# Copy and install your Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt