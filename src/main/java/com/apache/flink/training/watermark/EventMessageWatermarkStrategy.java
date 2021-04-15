package com.apache.flink.training.watermark;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.Context;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class EventMessageWatermarkStrategy implements WatermarkStrategy<EventMessage> {

    @Override
    public WatermarkGenerator<EventMessage> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new EventMessageWatermarkGenerator();
    }

    @Override
    public TimestampAssigner<EventMessage> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampEventMessageAssigner();
    }
}
