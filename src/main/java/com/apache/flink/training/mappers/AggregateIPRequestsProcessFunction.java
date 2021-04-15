package com.apache.flink.training.mappers;

import com.apache.flink.training.model.IPRequest;
import com.apache.flink.training.model.IPRequestReport;
import java.util.List;
import java.util.Objects;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class AggregateIPRequestsProcessFunction extends ProcessFunction<IPRequest, IPRequestReport> {

    private transient ValueState<IPRequestReport> ipRequestReport;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<IPRequestReport> descriptor = new ValueStateDescriptor<IPRequestReport>("ipRequestReport",
                                                                                                     TypeInformation
                                                                                                             .of(new TypeHint<IPRequestReport>() {
                                                                                                             }),
                                                                                                     new IPRequestReport());
        ipRequestReport = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IPRequestReport> out) throws Exception {
        IPRequestReport currentReport = ipRequestReport.value();

        if (Objects.nonNull(currentReport)) {
            out.collect(currentReport);
        }

        ipRequestReport.clear();

    }

    @Override
    public void processElement(IPRequest ipRequest, Context context, Collector<IPRequestReport> collector) throws Exception {

        IPRequestReport currentReport = ipRequestReport.value();

        List<Long> timestamps = currentReport.getTimestamps();

        long updatedAt = System.currentTimeMillis();

        if (Objects.isNull(timestamps)) {
            context.timerService().registerProcessingTimeTimer(updatedAt + 10000);
            currentReport.setUpdatedAt(updatedAt);
            timestamps = Lists.newArrayList();
        } else {
            context.timerService().deleteProcessingTimeTimer(currentReport.getUpdatedAt() + 10000);
            currentReport.setUpdatedAt(updatedAt);
            context.timerService().registerProcessingTimeTimer(currentReport.getUpdatedAt() + 10000);
        }

        timestamps.add(ipRequest.getTimestamp());

        IPRequestReport updatedReport = IPRequestReport.builder().ip(ipRequest.getIp()).timestamps(timestamps)
                .updatedAt(currentReport.getUpdatedAt()).build();

        ipRequestReport.update(updatedReport);

        if (timestamps.size() > 5) {
            collector.collect(updatedReport);
        }
    }
}
