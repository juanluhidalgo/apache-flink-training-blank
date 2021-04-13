package com.apache.flink.training.mappers;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

public class TestStopFunction extends ProcessWindowFunction<String, String, Integer, TimeWindow> {

    @Override
    public void process(Integer s, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
        for (String i : iterable) {
            collector.collect(i);
        }
    }
}
