# ============================================================
# Stage 1 — Build canonical sensor streaming application
# ============================================================

FROM maven:3.9-eclipse-temurin-17 AS sensor-job-builder

WORKDIR /build

COPY ./sensor_stream_job/pom.xml ./pom.xml
COPY ./sensor_stream_job/src ./src

RUN mvn -B clean package -DskipTests


# ============================================================
# Stage 2 — Flink runtime
# ============================================================

FROM flink:1.20.5-scala_2.12-java17

COPY ./lib/iceberg-flink-runtime-1.20-1.8.1.jar /opt/flink/lib/
COPY ./lib/iceberg-aws-bundle-1.8.1.jar /opt/flink/lib/

COPY ./lib/hadoop-common-3.3.6.jar /opt/flink/lib/
COPY ./lib/hadoop-client-api-3.3.6.jar /opt/flink/lib/
COPY ./lib/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/

COPY ./lib/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
COPY ./lib/kafka-clients-3.4.0.jar /opt/flink/lib/

# Canonical streaming application JAR.
COPY --from=sensor-job-builder \
    /build/target/sensor-stream-job.jar \
    /opt/flink/sensor-stream-job.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar \
          /opt/flink/plugins/s3-fs-hadoop/

RUN wget -q \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.20.5/flink-connector-base-1.20.5.jar \
    -O /opt/flink/lib/flink-connector-base-1.20.5.jar