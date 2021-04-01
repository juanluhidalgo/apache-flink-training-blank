package com.apache.flink.training;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class StreamKafkaSample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "broker:29092");
        properties.setProperty("group.id",
                               "test");

        Properties outputProperties = new Properties();

        outputProperties.setProperty("bootstrap.servers",
                               "broker:29092");


        DataStream<String> messages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                          new SimpleStringSchema(),
                                                          properties)).name("Read messages");


        messages.map(m -> "output ===> " + m).addSink(new FlinkKafkaProducer<>("myOutputTopic", new SimpleStringSchema(),
                                                                               outputProperties)).name("Produce messages");

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
