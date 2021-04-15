package com.apache.flink.training;

import com.apache.flink.training.mappers.AggregateIPRequestsFlatMap;
import com.apache.flink.training.mappers.AggregateIPRequestsProcessFunction;
import com.apache.flink.training.mappers.CountConsecutiveGoals;
import com.apache.flink.training.mappers.FromStringToGoalTeam;
import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.GoalTeam;
import com.apache.flink.training.model.IPRequest;
import com.apache.flink.training.serialiazer.CommandReportTeamDeserializer;
import com.apache.flink.training.serialiazer.IPRequestDeserializer;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class StreamValueStateSampleIPs {


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

        ipRequests.keyBy(i -> i.getIp()).process(new AggregateIPRequestsProcessFunction()).addSink(new PrintSinkFunction<>());

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
