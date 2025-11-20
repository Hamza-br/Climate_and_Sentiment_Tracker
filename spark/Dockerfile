FROM openjdk:17-jdk-slim

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3.3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER root

# Install system dependencies
RUN apt-get update \
		&& apt-get install -y --no-install-recommends \
			 python3 python3-pip wget curl netcat-openbsd gnupg ca-certificates \
		&& rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install --no-cache-dir pyspark kafka-python requests

# Download and extract Spark prebuilt for Hadoop
RUN mkdir -p /opt && \
		wget -qO /tmp/spark.tgz \
			https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
		tar -xzf /tmp/spark.tgz -C /opt && \
		mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
		rm /tmp/spark.tgz

# Add Kafka and Cassandra connectors into Spark jars directory
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar ${SPARK_HOME}/jars/

# Create app directory
WORKDIR /app

# Copy the Spark streaming script and utils
COPY spark-streaming.py /app/
COPY utils /app/utils

# Ensure spark-submit is available and use it as entrypoint
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/app/spark-streaming.py"]
