FROM flink:1.20.5-scala_2.12-java17

COPY ./lib/iceberg-flink-runtime-1.20-1.8.1.jar /opt/flink/lib/

COPY ./lib/iceberg-aws-bundle-1.8.1.jar /opt/flink/lib/

COPY ./lib/hadoop-common-3.3.6.jar /opt/flink/lib/
COPY ./lib/hadoop-client-api-3.3.6.jar /opt/flink/lib/
COPY ./lib/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/
COPY ./lib/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
COPY ./lib/kafka-clients-3.4.0.jar /opt/flink/lib/

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar \
          /opt/flink/plugins/s3-fs-hadoop/
RUN wget -q \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.20.5/flink-connector-base-1.20.5.jar \
    -O /opt/flink/lib/flink-connector-base-1.20.5.jar