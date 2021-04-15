package com.apache.flink.training.pattern;

import com.apache.flink.training.model.EventMessage;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

public class FromEventMessageToAlertEventMessage extends PatternProcessFunction<EventMessage, EventMessage> {

    @Override
    public void processMatch(Map<String, List<EventMessage>> map, Context context, Collector<EventMessage> collector) throws Exception {

        for (Map.Entry<String, List<EventMessage>> entry : map.entrySet()) {
            for (EventMessage eventMessage : entry.getValue()) {
                collector.collect(eventMessage);
            }
        }
    }
}
