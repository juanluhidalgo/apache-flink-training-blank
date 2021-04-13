package com.apache.flink.training;


import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class StreamKafkaSampleEventTimeWindow {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "localhost:9092");
        properties.setProperty("group.id",
                               "test");

        Properties outputProperties = new Properties();

        outputProperties.setProperty("bootstrap.servers",
                                     "localhost:9092");

        DataStream<EventMessage> messages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                   new EventMessageDeserializer(),
                                                                                   properties)).name("Read messages");


        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
