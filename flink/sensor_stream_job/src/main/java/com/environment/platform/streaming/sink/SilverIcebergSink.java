package com.environment.platform.streaming.sink;

import com.environment.platform.streaming.model.SensorReading;
import com.environment.platform.streaming.serialization.SensorReadingToSilverRowData;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public final class SilverIcebergSink {

    private SilverIcebergSink() {
    }

    public static void attach(
        DataStream<SensorReading> readings
    ) {

        Map<String, String> catalogProperties =
            new HashMap<>();

        catalogProperties.put(
            "uri",
            env(
                "POLARIS_URI",
                "http://polaris:8181/api/catalog"
            )
        );

        catalogProperties.put(
            "warehouse",
            env(
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
            requiredEnv(
                "POLARIS_FLINK_CREDENTIAL"
            )
        );

        catalogProperties.put(
            "scope",
            env(
                "POLARIS_SCOPE",
                "PRINCIPAL_ROLE:ALL"
            )
        );

        catalogProperties.put(
            "s3.endpoint",
            env(
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
            requiredEnv(
                "MINIO_ACCESS_KEY"
            )
        );

        catalogProperties.put(
            "s3.secret-access-key",
            requiredEnv(
                "MINIO_SECRET_KEY"
            )
        );

        CatalogLoader catalogLoader =
            CatalogLoader.rest(
                "polaris",
                new Configuration(),
                catalogProperties
            );

        TableIdentifier tableIdentifier =
            TableIdentifier.of(
                "silver",
                "sensor_readings_clean"
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
                ), // schema_version

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // event_type

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // event_id

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // source_system

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // dataset_name

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // device_id

                new BigIntType(), // sequence_number

                new IntType(), // city_id

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // city_name

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // country_code

                new LocalZonedTimestampType(
                    6
                ), // event_time_utc

                new LocalZonedTimestampType(
                    6
                ), // produced_at_utc

                new DoubleType(), // temperature_2m

                new DoubleType(), // relative_humidity_2m

                new DoubleType(), // pm2_5

                new DoubleType(), // pm10

                new DoubleType(), // carbon_monoxide

                new DoubleType(), // nitrogen_dioxide

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // quality_status

                new VarCharType(
                    VarCharType.MAX_LENGTH
                ), // quality_error

                new LocalZonedTimestampType(
                    6
                ) // processed_at
            };

        String[] fieldNames =
            new String[] {
                "schema_version",
                "event_type",
                "event_id",
                "source_system",
                "dataset_name",
                "device_id",
                "sequence_number",
                "city_id",
                "city_name",
                "country_code",
                "event_time_utc",
                "produced_at_utc",
                "temperature_2m",
                "relative_humidity_2m",
                "pm2_5",
                "pm10",
                "carbon_monoxide",
                "nitrogen_dioxide",
                "quality_status",
                "quality_error",
                "processed_at"
            };

        RowType rowType =
            RowType.of(
                fieldTypes,
                fieldNames
            );

        FlinkSink
            .builderFor(
                readings,
                new SensorReadingToSilverRowData(),
                FlinkCompatibilityUtil.toTypeInfo(
                    rowType
                )
            )
            .tableLoader(
                tableLoader
            )
            .append();
    }

    private static String env(
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
}