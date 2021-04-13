package com.apache.flink.training;

import com.apache.flink.training.mappers.CountConsecutiveGoals;
import com.apache.flink.training.mappers.FromEventMessageToMessage;
import com.apache.flink.training.mappers.FromStringToGoalTeam;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.GoalTeam;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

public class StreamValueStateSample {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "localhost:9092");
        properties.setProperty("group.id",
                               "test");

        DataStream<String> messages = env.addSource(new FlinkKafkaConsumer<String>("myInputTopic",
                                                                                   new SimpleStringSchema(),
                                                                                   properties));

        DataStream<GoalTeam> goals = messages.map(new FromStringToGoalTeam());

        goals.keyBy(gt -> gt.getTeamName()).flatMap(new CountConsecutiveGoals()).addSink(new PrintSinkFunction<>());

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
