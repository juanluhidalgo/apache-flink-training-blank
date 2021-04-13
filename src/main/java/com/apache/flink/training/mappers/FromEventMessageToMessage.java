package com.apache.flink.training.mappers;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FromEventMessageToMessage extends ProcessFunction<EventMessage, String> {

    final OutputTag<String> errors = new OutputTag<String>("errors") {
    };

    private transient Counter failed;
    private transient Counter success;
    private transient LongGauge enqueueTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        failed = getRuntimeContext().getMetricGroup().addGroup("messages").counter("failed",
                                                                                   new NonNegativeCounter());
        success = getRuntimeContext().getMetricGroup().addGroup("messages").counter("success",
                                                                                    new NonNegativeCounter());

        enqueueTime = getRuntimeContext().getMetricGroup().addGroup("messages").gauge("enqueueTime",
                                                                                      new LongGauge());
    }

    @Override
    public void processElement(EventMessage eventMessage, Context context, Collector<String> collector) throws Exception {

        if (!"ERROR".equals(eventMessage.getSeverity())) {
            collector.collect(eventMessage.getMessage());
            success.inc();
            enqueueTime.setValue(System.currentTimeMillis() - eventMessage.getTimestamp());
        } else {
            context.output(errors,
                           eventMessage.getMessage());
            failed.inc();
        }

    }

    static class NonNegativeCounter extends SimpleCounter {


    }

    public static class LongGauge implements Gauge<Long> {

        private Long value;

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public Long getValue() {
            return value;
        }
    }
}
