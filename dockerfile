FROM apache/airflow:2.5.1

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
