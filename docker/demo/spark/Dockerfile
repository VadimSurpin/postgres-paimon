FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl && \
    pip install --no-cache-dir pyspark==3.5.3 && \
    rm -rf /var/lib/apt/lists/*

ARG PAIMON_VERSION=1.4.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_BUNDLE_VERSION=1.12.262
ARG MVN=https://repo1.maven.org/maven2

RUN mkdir -p /opt/demo/jars && \
    curl -fsSL "${MVN}/org/apache/paimon/paimon-spark-3.5_2.12/${PAIMON_VERSION}/paimon-spark-3.5_2.12-${PAIMON_VERSION}.jar" \
         -o /opt/demo/jars/paimon-spark.jar && \
    curl -fsSL "${MVN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
         -o /opt/demo/jars/hadoop-aws.jar && \
    curl -fsSL "${MVN}/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" \
         -o /opt/demo/jars/aws-java-sdk-bundle.jar

ENV PYSPARK_SUBMIT_ARGS="--jars /opt/demo/jars/paimon-spark.jar,/opt/demo/jars/hadoop-aws.jar,/opt/demo/jars/aws-java-sdk-bundle.jar pyspark-shell"

COPY docker/demo/spark/read_paimon.py /opt/demo/read_paimon.py

CMD ["python", "/opt/demo/read_paimon.py"]
