package com.environment.platform.streaming.sink;

import com.environment.platform.streaming.model.RawKafkaEvent;
import com.environment.platform.streaming.serialization.RawKafkaEventToRowData;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;

public final class BronzeIcebergSink {

    private BronzeIcebergSink() {
    }

    public static void attach(
        DataStream<RawKafkaEvent> rawEvents
    ) {

        Map<String, String> catalogProperties =
            new HashMap<>();

        catalogProperties.put(
            "uri",
            envOrDefault(
                "POLARIS_URI",
                "http://polaris:8181/api/catalog"
            )
        );

        catalogProperties.put(
            "warehouse",
            envOrDefault(
                "POLARIS_CATALOG_NAME",
                "environment_catalog"
            )
        );

        catalogProperties.put(
            "io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        );

        catalogProperties.put(
            "credential",
            requiredEnv("POLARIS_FLINK_CREDENTIAL")
        );

        catalogProperties.put(
            "scope",
            envOrDefault(
                "POLARIS_SCOPE",
                "PRINCIPAL_ROLE:ALL"
            )
        );

        catalogProperties.put(
            "s3.endpoint",
            envOrDefault(
                "MINIO_ENDPOINT_INTERNAL",
                "http://minio:9000"
            )
        );

        catalogProperties.put(
            "s3.path-style-access",
            "true"
        );

        catalogProperties.put(
            "s3.access-key-id",
            requiredEnv("MINIO_ACCESS_KEY")
        );

        catalogProperties.put(
            "s3.secret-access-key",
            requiredEnv("MINIO_SECRET_KEY")
        );

        Configuration hadoopConf =
            new Configuration();

        CatalogLoader catalogLoader =
            CatalogLoader.rest(
                "polaris",
                hadoopConf,
                catalogProperties
            );

        TableIdentifier tableIdentifier =
            TableIdentifier.of(
                "bronze",
                "sensor_events_raw"
            );

        TableLoader tableLoader =
            TableLoader.fromCatalog(
                catalogLoader,
                tableIdentifier
            );

        LogicalType[] fieldTypes =
            new LogicalType[] {
                new VarCharType(
                    VarCharType.MAX_LENGTH
                ),
                new VarCharType(
                    VarCharType.MAX_LENGTH
                ),
                new IntType(),
                new BigIntType(),
               new LocalZonedTimestampType(6),
                new LocalZonedTimestampType(6)
            };

        String[] fieldNames =
            new String[] {
                "raw_payload",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at_utc"
            };

        RowType rowType =
            RowType.of(
                fieldTypes,
                fieldNames
            );

        FlinkSink
            .builderFor(
                rawEvents,
                new RawKafkaEventToRowData(),
                FlinkCompatibilityUtil.toTypeInfo(
                    rowType
                )
            )
            .tableLoader(tableLoader)
            .append();
    }

    private static String requiredEnv(
        String name
    ) {

        String value =
            System.getenv(name);

        if (
            value == null ||
            value.isBlank()
        ) {
            throw new IllegalStateException(
                "Missing required environment variable: "
                    + name
            );
        }

        return value;
    }

    private static String envOrDefault(
        String name,
        String defaultValue
    ) {

        String value =
            System.getenv(name);

        if (
            value == null ||
            value.isBlank()
        ) {
            return defaultValue;
        }

        return value;
    }
}
