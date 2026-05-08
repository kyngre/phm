FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Python 의존성 (KMeans 등이 numpy 필요, simulator 가 kafka-python 사용)
RUN pip install --no-cache-dir numpy kafka-python

# Ivy 캐시 디렉토리 사전 생성 (권한 이슈 회피)
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars /home/spark/.ivy2/local && \
    chown -R spark:spark /home/spark/.ivy2 && \
    chmod -R 777 /home/spark/.ivy2

# Iceberg + S3A + Kafka 의존 JAR을 직접 /opt/spark/jars 에 배치
# (spark-submit --packages 의 매 기동시 다운로드를 회피)
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V2_VERSION=2.24.6
# hadoop-aws 3.3.4 의 S3AFileSystem 은 AWS SDK v1 을 요구. Iceberg S3FileIO 는 v2 사용 —
# 둘은 패키지(`com.amazonaws.*` vs `software.amazon.awssdk.*`) 가 달라 동일 JVM 공존 가능.
# remove_orphan_files 가 Hadoop FileSystem API 로 warehouse 를 list 할 때 v1 필요.
ARG AWS_SDK_V1_VERSION=1.12.262
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.5.1
ARG COMMONS_POOL2_VERSION=2.11.1

WORKDIR /opt/spark/jars

RUN set -eux; \
    BASE="https://repo1.maven.org/maven2"; \
    curl -fSL -o iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        ${BASE}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fSL -o iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        ${BASE}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fSL -o hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        ${BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSL -o aws-sdk-bundle-${AWS_SDK_V2_VERSION}.jar \
        ${BASE}/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar; \
    curl -fSL -o aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar \
        ${BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar; \
    curl -fSL -o spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
        ${BASE}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar; \
    curl -fSL -o spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
        ${BASE}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar; \
    curl -fSL -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
        ${BASE}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar; \
    curl -fSL -o commons-pool2-${COMMONS_POOL2_VERSION}.jar \
        ${BASE}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar; \
    chown spark:spark *.jar

USER spark
WORKDIR /opt/spark
