package com.apache.flink.training;

import com.apache.flink.training.mappers.AggregateIPRequestsProcessFunction;
import com.apache.flink.training.model.IPRequest;
import com.apache.flink.training.serialiazer.IPRequestDeserializer;
import com.apache.flink.training.window.IPReportRequests;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class WindowSampleIPs {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "localhost:9092");
        properties.setProperty("group.id",
                               "test");

        DataStream<IPRequest> ipRequests = env.addSource(new FlinkKafkaConsumer<IPRequest>("myInputTopic",
                                                                                           new IPRequestDeserializer(),
                                                                                           properties));

        ipRequests.keyBy(i -> i.getIp()).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).process(new IPReportRequests())
                .addSink(new PrintSinkFunction<>());

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
