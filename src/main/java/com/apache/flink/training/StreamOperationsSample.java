package com.apache.flink.training;

import com.apache.flink.training.filter.JustEven;
import com.apache.flink.training.key.MyKey;
import com.apache.flink.training.mappers.Double;
import com.apache.flink.training.mappers.OddOrEven;
import com.apache.flink.training.mappers.RepeatDouble;
import com.apache.flink.training.reduce.SumNumbers;
import com.apache.flink.training.sink.JsonOutputFormat;
import com.apache.flink.training.sink.MySink;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

public class StreamOperationsSample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        Collection<Integer> numbers = Arrays.asList(10,
                                                    2,
                                                    3,
                                                    4,
                                                    7);

        Collection<Integer> numbersOther = Arrays.asList(1,
                                                         2);

        SingleOutputStreamOperator<Integer> oddOrEven = (SingleOutputStreamOperator<Integer>) env.fromCollection(numbers)
                .name("Collection of Numbers");

        DataStream<Integer> other = env.fromCollection(numbersOther)
                .name("Collection of Numbers");

        SingleOutputStreamOperator<Integer> pares = other.union(oddOrEven).process(new OddOrEven());

        DataStream<Integer> impares = pares.getSideOutput(new OutputTag<Integer>("impares") {
        });

        //impares.addSink(new PrintSinkFunction<>());

        impares.addSink(new MySink());
        //pares.addSink(new PrintSinkFunction<>());

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
