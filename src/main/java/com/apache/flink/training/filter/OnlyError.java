package com.apache.flink.training.filter;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.api.common.functions.FilterFunction;

public class OnlyError implements FilterFunction<EventMessage> {

    @Override
    public boolean filter(EventMessage eventMessage) throws Exception {
        return "ERROR".equals(eventMessage.getSeverity());
    }
}
