package com.apache.flink.training.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SumNumbers implements ReduceFunction<Integer> {

    @Override
    public Integer reduce(Integer num1, Integer num2) throws Exception {
        return num1 + num2;
    }
}
