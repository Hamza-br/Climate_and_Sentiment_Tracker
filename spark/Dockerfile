FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/spark/app

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

COPY processor.py /opt/spark/app/processor.py
COPY logging_utils.py /opt/spark/app/logging_utils.py
COPY entrypoint.sh /opt/spark/app/entrypoint.sh

RUN chmod +x /opt/spark/app/entrypoint.sh

ENTRYPOINT ["/opt/spark/app/entrypoint.sh"]
