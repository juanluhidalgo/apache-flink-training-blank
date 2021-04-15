package com.apache.flink.training;

import com.apache.flink.training.filter.OnlyError;
import com.apache.flink.training.mappers.FromEventMessageToMessage;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

public class StreamKafkaSample {


    public static void main(String[] args) throws Exception {

        final OutputTag<String> errors = new OutputTag<String>("errors") {
        };
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "broker:29092");
        properties.setProperty("group.id",
                               "test");

        Properties outputProperties = new Properties();

        outputProperties.setProperty("bootstrap.servers",
                                     "broker:29092");

        DataStream<EventMessage> messages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                   new EventMessageDeserializer(),
                                                                                   properties)).name("Read messages");

        SingleOutputStreamOperator<String> noErrors = messages.process(new FromEventMessageToMessage()).name("Map no Errors to Message")
                .uid("map_no_error_to_message");
        noErrors
                .addSink(new FlinkKafkaProducer("myOutputTopic",
                                                new SimpleStringSchema(),
                                                outputProperties))
                .name("Produce No Error Messages");

        noErrors.getSideOutput(errors).addSink(new FlinkKafkaProducer("myErrorTopic",
                                                                      new SimpleStringSchema(),
                                                                      outputProperties))
                .name("Produce Error messages");

        env.enableCheckpointing(60000);

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
