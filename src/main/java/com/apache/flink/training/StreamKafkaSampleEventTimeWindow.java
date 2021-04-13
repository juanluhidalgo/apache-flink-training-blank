package com.apache.flink.training;


import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import com.apache.flink.training.watermark.EventMessageWatermarkStrategy;
import com.apache.flink.training.window.ReorderMessage;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

public class StreamKafkaSampleEventTimeWindow {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final OutputTag<EventMessage> late = new OutputTag<EventMessage>("late") {
        };

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

        DataStream<EventMessage> eventMessageWithWatermark = messages.assignTimestampsAndWatermarks(new EventMessageWatermarkStrategy());

        SingleOutputStreamOperator<EventMessage> eventMessages = eventMessageWithWatermark.keyBy(em -> em.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(late)
                .process(new ReorderMessage());

        eventMessages.addSink(
                new FlinkKafkaProducer<EventMessage>("myOutputTopic",
                                                     new EventMessageDeserializer(),
                                                     properties)).name("Produce sorted messages");

        eventMessages.getSideOutput(late).addSink(
                new FlinkKafkaProducer<EventMessage>("myErrorTopic",
                                                     new EventMessageDeserializer(),
                                                     properties)).name("Produce late messages");

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
