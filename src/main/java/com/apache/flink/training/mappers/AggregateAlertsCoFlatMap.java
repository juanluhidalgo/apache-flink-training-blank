package com.apache.flink.training.mappers;

import com.apache.flink.training.model.AlertReport;
import com.apache.flink.training.model.CommandReportAlerts;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.IPRequestReport;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class AggregateAlertsCoFlatMap extends RichCoFlatMapFunction<EventMessage, CommandReportAlerts, AlertReport> {

    private transient ValueState<AlertReport> alertReportValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<AlertReport> descriptor = new ValueStateDescriptor<AlertReport>("alertReport",
                                                                                             TypeInformation
                                                                                                     .of(new TypeHint<AlertReport>() {
                                                                                                     }),
                                                                                             new AlertReport());
        alertReportValueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap1(EventMessage eventMessage, Collector<AlertReport> collector) throws Exception {

        AlertReport alertReport = alertReportValueState.value();

        addEventToReport(eventMessage,
                         alertReport);

        alertReportValueState.update(alertReport);

    }

    private void addEventToReport(EventMessage eventMessage, AlertReport alertReport) {
        List<EventMessage> eventMessages = alertReport.getEventMessages();

        if (Objects.isNull(eventMessages)) {
            eventMessages = Lists.newArrayList();
        }

        eventMessages.add(eventMessage);

        alertReport.setEventMessages(eventMessages);
        alertReport.setId(eventMessage.getId());
        alertReport.setUpdatedAt(System.currentTimeMillis());
    }

    @Override
    public void flatMap2(CommandReportAlerts commandReportAlerts, Collector<AlertReport> collector) throws Exception {

        AlertReport alertReport = alertReportValueState.value();

        if ("REPORT".equals(commandReportAlerts.getAction())) {
            collector.collect(alertReport);
        } else if ("CLEAN".equals(commandReportAlerts.getAction())) {
            alertReportValueState.clear();
        } else {
            log.warn("Action not supported");
        }

    }
}
