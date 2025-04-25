# # docker/Dockerfile

# FROM bitnami/spark:3.5

# USER root

# RUN apt-get update && apt-get install -y curl && apt-get install -y nano

# RUN pip install delta-spark==3.1.0

# # Устанавливаем Delta Lake JAR (можно взять версию под Spark 3.5)
# ENV DELTA_VERSION=3.1.0

# RUN mkdir -p /opt/spark/jars \
#     && curl -L https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar \
#        -o /opt/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar \
#     && curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
#        -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar

FROM spark:3.5.1-scala2.12-java17-ubuntu
USER root
RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*
# Install PySpark
RUN pip3 install pyspark
USER root
# Add PySpark to PATH for the spark user
ENV PATH=$PATH:/home/spark/.local/bin

RUN export JAVA_HOME=/opt/java/openjdk
RUN export PYSPARK_PYTHON=$(which python3)
RUN export SPARK_VERSION=3.5

RUN pip3 install delta-spark==3.2.0

RUN mkdir -p /opt/spark/conf

COPY log4j.properties /opt/spark/conf/log4j.properties

ENV SPARK_CONF_DIR=/opt/spark/conf