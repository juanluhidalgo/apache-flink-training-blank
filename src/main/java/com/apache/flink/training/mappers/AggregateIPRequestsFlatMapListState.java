package com.apache.flink.training.mappers;

import com.apache.flink.training.model.IPRequest;
import com.apache.flink.training.model.IPRequestReport;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AggregateIPRequestsFlatMapListState extends RichFlatMapFunction<IPRequest, IPRequest> {

    private transient ListState<IPRequest> ipRequests;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<IPRequest> descriptor = new ListStateDescriptor<IPRequest>("ipRequest",
                                                                                       TypeInformation
                                                                                               .of(new TypeHint<IPRequest>() {
                                                                                               }));
        ipRequests = getRuntimeContext().getListState(descriptor);
    }


    @Override
    public void flatMap(IPRequest ipRequest, Collector<IPRequest> collector) throws Exception {

        ipRequests.add(ipRequest);

        List<IPRequest> ipRequestList = StreamSupport.stream(ipRequests.get().spliterator(),
                                                             false).collect(Collectors.toList());
        if (ipRequestList.size() > 5) {
            for (IPRequest request : ipRequestList) {
                collector.collect(request);
            }
        }

    }
}
