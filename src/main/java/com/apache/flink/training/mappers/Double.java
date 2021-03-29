package com.apache.flink.training.mappers;

import org.apache.flink.api.common.functions.MapFunction;

public class Double implements MapFunction<Integer, Integer> {

    // Dado un numero genera su doble

    @Override
    public Integer map(Integer number) throws Exception {
        return number;
    }
}
