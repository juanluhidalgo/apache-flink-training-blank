package com.apache.flink.training;

import com.apache.flink.training.source.TwitterExampleData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Lee los datos usando @TwitterExampleData
 * Obten la palabra más repetida en Ingles
 * Obten la palabra más repetida en idiomas que no sean Ingles
 * Obten el maximo número de palabras en tweets en Ingles y no Ingles
 */

public class TwitterStreamingAppSample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);

        env.execute("Twitter Sample");

    }


}
