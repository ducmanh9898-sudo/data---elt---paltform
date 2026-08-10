ARG SPARK_BASE_IMAGE

FROM alpine:3.20 AS downloader

ARG HADOOP_AWS_VERSION
ARG AWS_SDK_V2_BUNDLE_VERSION
ARG ANALYTICS_ACCELERATOR_VERSION
ARG ICEBERG_VERSION

RUN apk add --no-cache ca-certificates wget

RUN mkdir -p /jars \
    && wget \
        --quiet \
        --output-document=/jars/hadoop-aws.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && wget \
        --quiet \
        --output-document=/jars/aws-sdk-v2-bundle.jar \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar" \
    && wget \
        --quiet \
        --output-document=/jars/analyticsaccelerator-s3.jar \
        "https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${ANALYTICS_ACCELERATOR_VERSION}.jar" \
    && wget \
        --quiet \
        --output-document=/jars/iceberg-spark-runtime.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    && wget \
        --quiet \
        --output-document=/jars/iceberg-aws-bundle.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"


FROM ${SPARK_BASE_IMAGE}
COPY --from=downloader \
    /jars/hadoop-aws.jar \
    /opt/spark/jars/hadoop-aws.jar

COPY --from=downloader \
    /jars/aws-sdk-v2-bundle.jar \
    /opt/spark/jars/aws-sdk-v2-bundle.jar

COPY --from=downloader \
    /jars/analyticsaccelerator-s3.jar \
    /opt/spark/jars/analyticsaccelerator-s3.jar

COPY --from=downloader \
    /jars/iceberg-spark-runtime.jar \
    /opt/spark/jars/iceberg-spark-runtime.jar

COPY --from=downloader \
    /jars/iceberg-aws-bundle.jar \
    /opt/spark/jars/iceberg-aws-bundle.jar