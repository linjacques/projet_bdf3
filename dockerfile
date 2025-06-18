FROM python:3.10-slim

ENV SPARK_VERSION=3.5.6 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    HIVE_HOME=/opt/hive \
    PATH=$PATH:/opt/spark/bin:/opt/hive/bin \
    CLASSPATH=$CLASSPATH:/opt/hive/lib/*

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    wget \
    procps \
    ca-certificates \
    libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

RUN wget https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz && \
    tar -xzf apache-hive-3.1.3-bin.tar.gz -C /opt && \
    mv /opt/apache-hive-3.1.3-bin /opt/hive && \
    rm apache-hive-3.1.3-bin.tar.gz

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P $SPARK_HOME/jars/

WORKDIR /app

CMD ["tail", "-f", "/dev/null"]
