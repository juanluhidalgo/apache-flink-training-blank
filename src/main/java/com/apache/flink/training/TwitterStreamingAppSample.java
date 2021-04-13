package com.apache.flink.training;

import com.apache.flink.training.mappers.FromTweetToTuple;
import com.apache.flink.training.source.TwitterExampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

/**
 * Lee los datos usando @TwitterExampleData Obten la palabra más repetida en Ingles
 * Obten la palabra más repetida en idiomas que no sean
 * Ingles
 * Obten el maximo número de palabras en tweets en Ingles y no Ingles
 */

public class TwitterStreamingAppSample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = streamSource.process(new FromTweetToTuple());
        //words.keyBy(a -> a.f0).max(1).addSink(new PrintSinkFunction<>());

        words.getSideOutput(new OutputTag<Tuple2<String, Integer>>("no_english") {

        }).keyBy(a -> a.f0).sum(1).addSink(new PrintSinkFunction<>());

        env.execute("Twitter Sample");

    }


}
