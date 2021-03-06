package com.apache.flink.training;

import com.apache.flink.training.mappers.AggregateAlertsCoFlatMap;
import com.apache.flink.training.mappers.EvaluateReportProcessFunction;
import com.apache.flink.training.model.AlertReport;
import com.apache.flink.training.model.CommandReportAlerts;
import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.pattern.FromEventMessageToAlertEventMessage;
import com.apache.flink.training.serialiazer.CommandReportAlertsDeserializer;
import com.apache.flink.training.serialiazer.CommandReportTeamDeserializer;
import com.apache.flink.training.serialiazer.EventMessageDeserializer;
import java.util.Properties;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

public class StreamCEPValueState {


    public static void main(String[] args) throws Exception {

        final OutputTag<EventMessage> fatalEventMessages = new OutputTag<EventMessage>("fatal") {
        };

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "broker:29092");
        properties.setProperty("group.id",
                               "test");

        DataStream<EventMessage> events = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                 new EventMessageDeserializer(),
                                                                                 properties)).name("Read events");

        DataStream<CommandReportAlerts> commands = env.addSource(new FlinkKafkaConsumer<CommandReportAlerts>("myCommandTopic",
                                                                                                             new CommandReportAlertsDeserializer(),
                                                                                                             properties))
                .name("Read commands");

        PatternStream<EventMessage> patternStream = CEP.pattern(events,
                                                                getEventMessagePattern()).inProcessingTime();

        DataStream<EventMessage> alerts = patternStream.process(new FromEventMessageToAlertEventMessage()).name("From Events to Alerts");

        DataStream<AlertReport> alertReportDataStream = alerts.keyBy(a -> a.getId()).connect(commands.keyBy(c -> c.getId()))
                .flatMap(new AggregateAlertsCoFlatMap()).name("Generate Report");

        SingleOutputStreamOperator<EventMessage> eventMessagesEvaluated = alertReportDataStream.process(new EvaluateReportProcessFunction())
                .name("Evaluate Alerts");

        eventMessagesEvaluated.addSink(new FlinkKafkaProducer<EventMessage>(
                "myOutputTopic",
                new EventMessageDeserializer(),
                properties)).name("Write Errors");

        eventMessagesEvaluated.getSideOutput(fatalEventMessages).addSink(new FlinkKafkaProducer<EventMessage>(
                "myErrorTopic",
                new EventMessageDeserializer(),
                properties)).name("Write Fatal");

        env.disableOperatorChaining();

        env.enableCheckpointing(60000,
                                CheckpointingMode.EXACTLY_ONCE);

        env.execute("Streaming App");
    }

    private static Pattern<EventMessage, ?> getEventMessagePattern() {
        return Pattern.<EventMessage>begin("errors").where(new SimpleCondition<EventMessage>() {
            @Override
            public boolean filter(EventMessage eventMessage) throws Exception {
                return "ERROR".equals(eventMessage.getSeverity());
            }
        }).times(3).consecutive().followedBy("fatal").where(new SimpleCondition<EventMessage>() {
            @Override
            public boolean filter(EventMessage eventMessage) throws Exception {
                return "FATAL".equals(eventMessage.getSeverity());
            }
        }).within(Time.seconds(30));
    }
}
