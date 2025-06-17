FROM python:3.10-slim

ENV SPARK_VERSION=3.5.6 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

WORKDIR /app

CMD ["python", "main.py"]
