package com.apache.flink.training.window;

import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.IPRequest;
import com.apache.flink.training.model.IPRequestReport;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IPReportRequests extends ProcessWindowFunction<IPRequest, IPRequestReport, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<IPRequest> ipRequests, Collector<IPRequestReport> collector) throws Exception {
        List<Long> timestamps = Lists.newArrayList();

        ipRequests.forEach(ir -> timestamps.add(ir.getTimestamp()));

        if (timestamps.size() > 5) {
            collector.collect(IPRequestReport.builder().timestamps(timestamps).ip(ipRequests.iterator().next().getIp()).build());
        }
    }
}
