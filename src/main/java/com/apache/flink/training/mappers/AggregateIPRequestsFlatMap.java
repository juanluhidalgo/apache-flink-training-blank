package com.apache.flink.training.mappers;

import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.GoalTeam;
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
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class AggregateIPRequestsFlatMap extends RichFlatMapFunction<IPRequest, IPRequestReport> {

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
    public void flatMap(IPRequest ipRequest, Collector<IPRequestReport> collector) throws Exception {

        IPRequestReport currentReport = ipRequestReport.value();

        List<Long> timestamps = currentReport.getTimestamps();

        if (Objects.isNull(timestamps)) {
            timestamps = Lists.newArrayList();
        }

        timestamps.add(ipRequest.getTimestamp());

        IPRequestReport updatedReport = currentReport.builder().ip(ipRequest.getIp()).timestamps(timestamps).build();
        ipRequestReport.update(updatedReport);

        if (timestamps.size() > 5) {
            collector.collect(updatedReport);
        }

    }
}
