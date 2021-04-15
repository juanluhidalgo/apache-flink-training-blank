package com.apache.flink.training.key;

import org.apache.flink.api.java.functions.KeySelector;

public class MyKey implements KeySelector<Integer, Integer> {

    @Override
    public Integer getKey(Integer integer) throws Exception {
        return integer % 2 == 0 ? 1 : 2;
    }
}
