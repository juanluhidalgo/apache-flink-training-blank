package com.apache.flink.training;

import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class StreamOperationsSample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        Collection<Integer> numbers = Arrays.asList(10,
                                                    2,
                                                    3,
                                                    4,
                                                    7);

        SingleOutputStreamOperator<Integer> oddOrEven = (SingleOutputStreamOperator<Integer>) env.fromCollection(numbers)
                .name("Collection of Numbers");

        oddOrEven.addSink(new PrintSinkFunction<>());

        env.execute("Streaming App");
    }
}
