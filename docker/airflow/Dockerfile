FROM apache/airflow:2.7.3

USER root

RUN apt-get update && apt-get install -y \
      default-jdk \
      gcc \
      python3-dev \
      libpq-dev \
      build-essential \
      curl \
    && apt-get clean

RUN ln -s /usr/lib/jvm/java-11-openjdk-arm64 /usr/lib/jvm/java-11-openjdk-amd64

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}

ENV SPARK_VERSION=3.3.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark

RUN curl -SL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH=${SPARK_HOME}/bin:${PATH} \
    HADOOP_CONF_DIR=/spark/conf

USER airflow

RUN pip install --no-cache-dir \
      apache-airflow-providers-amazon \
      apache-airflow-providers-apache-spark \
      apache-airflow-providers-docker \
      delta-spark==2.3.0 \
    && pip uninstall -y apache-airflow-providers-openlineage
