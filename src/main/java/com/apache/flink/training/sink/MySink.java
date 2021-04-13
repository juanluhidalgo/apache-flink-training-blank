package com.apache.flink.training.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MySink implements SinkFunction<Integer> {

    public void invoke(Integer record, Context context) {
        System.out.println("Sinking " + record);
    }


}
