package com.apache.flink.training.pattern;

import com.apache.flink.training.model.EventMessage;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

public class FromEventMessageToAlert extends PatternProcessFunction<EventMessage, String> {

    @Override
    public void processMatch(Map<String, List<EventMessage>> map, Context context, Collector<String> collector) throws Exception {

        /*for (Map.Entry<String, List<EventMessage>> entry : map.entrySet()) {
            for (EventMessage eventMessage : entry.getValue()) {
                collector.collect(eventMessage.getMessage());
            }
        }*/

        if (map.containsKey("fatal")) {
            collector.collect("Major alert!!");
        } else {
            collector.collect("Minor alert!!");
        }

    }
}
