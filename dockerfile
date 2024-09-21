# Use an official Apache Airflow image with Python 3.8
FROM apache/airflow:2.5.1-python3.8

# Switch to root user
USER root

# Update the package list and install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to the airflow user
USER airflow

# # Upgrade pip
# RUN pip install --upgrade pip

# # Install dependencies
# RUN pip install --no-cache-dir kafka-python==2.0.2 pyspark apache-airflow-providers-apache-spark apache-airflow-providers-celery

# Copy your DAGs and any other necessary files
# COPY ./dags /opt/airflow/dags
# COPY ./requirements.txt /opt/airflow/requirements.txt

# Install any additional requirements if necessary
# RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
