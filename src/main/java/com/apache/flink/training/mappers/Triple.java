package com.apache.flink.training.mappers;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

public class Triple implements MapPartitionFunction<Integer, Integer> {

    // Dado un número generar su triple

    @Override
    public void mapPartition(Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {

    }
}
