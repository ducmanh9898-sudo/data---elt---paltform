package com.environment.platform.streaming.process;

import com.environment.platform.streaming.model.SensorReading;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class DeduplicateSensorEventFunction
    extends KeyedProcessFunction<
        String,
        SensorReading,
        SensorReading
    > {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> seenState;

    @Override
    public void open(
        OpenContext openContext
    ) throws Exception {

        StateTtlConfig ttlConfig =
            StateTtlConfig
                .newBuilder(
                    Duration.ofHours(24)
                )
                .setUpdateType(
                    StateTtlConfig.UpdateType.OnCreateAndWrite
                )
                .setStateVisibility(
                    StateTtlConfig.StateVisibility.NeverReturnExpired
                )
                .build();

        ValueStateDescriptor<Boolean> descriptor =
            new ValueStateDescriptor<>(
                "seen-event-id",
                Boolean.class
            );

        descriptor.enableTimeToLive(
            ttlConfig
        );

        seenState =
            getRuntimeContext()
                .getState(
                    descriptor
                );
    }

    @Override
    public void processElement(
        SensorReading reading,
        Context context,
        Collector<SensorReading> out
    ) throws Exception {

        Boolean alreadySeen =
            seenState.value();

        if (
            Boolean.TRUE.equals(
                alreadySeen
            )
        ) {
            return;
        }

        seenState.update(
            true
        );

        out.collect(
            reading
        );
    }
}