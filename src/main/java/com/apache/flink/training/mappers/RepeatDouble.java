package com.apache.flink.training.mappers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class RepeatDouble implements FlatMapFunction<Integer, Integer> {

    // Dado un número generar dos veces su doble

    @Override
    public void flatMap(Integer number, Collector<Integer> collector) throws Exception {

        collector.collect(number * 2);
        collector.collect(number * 2);

    }
}
