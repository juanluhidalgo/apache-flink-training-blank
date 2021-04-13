package com.apache.flink.training.watermark;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class TimestampEventMessageAssigner implements TimestampAssigner<EventMessage> {

    @Override
    public long extractTimestamp(EventMessage eventMessage, long l) {
        return eventMessage.getTimestamp();
    }
}
