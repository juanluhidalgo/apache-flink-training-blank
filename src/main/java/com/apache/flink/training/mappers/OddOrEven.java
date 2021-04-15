package com.apache.flink.training.mappers;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OddOrEven extends ProcessFunction<Integer, Integer> {

    final OutputTag<Integer> impares = new OutputTag<Integer>("impares") {
    };

    @Override
    public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {

        if (integer % 2 == 0) {
            collector.collect(integer);
        } else {
            context.output(impares,
                           integer);
        }

    }
}
