package com.apache.flink.training.mappers;

import com.apache.flink.training.model.AlertReport;
import com.apache.flink.training.model.EventMessage;
import java.util.Objects;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class EvaluateReportProcessFunction extends ProcessFunction<AlertReport, EventMessage> {

    final OutputTag<EventMessage> fatalEventMessages = new OutputTag<EventMessage>("fatal") {
    };

    @Override
    public void processElement(AlertReport alertReport, Context context, Collector<EventMessage> collector) throws Exception {

        if (Objects.nonNull(alertReport.getEventMessages())) {
            for (EventMessage eventMessage : alertReport.getEventMessages()) {
                if ("ERROR".equals(eventMessage.getSeverity())) {
                    collector.collect(eventMessage);
                } else if ("FATAL".equals(eventMessage.getSeverity())) {
                    context.output(fatalEventMessages,
                                   eventMessage);
                }
            }
        }
    }
}
