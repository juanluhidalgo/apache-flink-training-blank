package com.apache.flink.training;

import com.apache.flink.training.mappers.CountConsecutiveGoals;
import com.apache.flink.training.mappers.FromStringToGoalTeam;
import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.GoalTeam;
import com.apache.flink.training.pattern.FromEventMessageToAlert;
import com.apache.flink.training.serialiazer.CommandReportTeamDeserializer;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class StreamCEP {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "localhost:9092");
        properties.setProperty("group.id",
                               "test");

        DataStream<EventMessage> events = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                 new EventMessageDeserializer(),
                                                                                 properties)).name("Read messages");

        Pattern<EventMessage, ?> pattern = Pattern.<EventMessage>begin("errors").where(new SimpleCondition<EventMessage>() {
            @Override
            public boolean filter(EventMessage eventMessage) throws Exception {
                return "ERROR".equals(eventMessage.getSeverity());
            }
        }).times(3).followedBy("fatal").where(new SimpleCondition<EventMessage>() {
            @Override
            public boolean filter(EventMessage eventMessage) throws Exception {
                return "FATAL".equals(eventMessage.getSeverity());
            }
        }).optional().within(Time.seconds(10));

        PatternStream<EventMessage> patternStream = CEP.pattern(events,
                                                                pattern).inProcessingTime();

        patternStream.process(new FromEventMessageToAlert()).addSink(new PrintSinkFunction<>());

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
