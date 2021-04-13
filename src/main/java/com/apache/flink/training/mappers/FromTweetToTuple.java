package com.apache.flink.training.mappers;

import com.apache.flink.training.model.Tweet;
import java.util.StringTokenizer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FromTweetToTuple extends ProcessFunction<String, Tuple2<String, Integer>> {

    final OutputTag<Tuple2<String, Integer>> noEnglish = new OutputTag<Tuple2<String, Integer>>("no_english") {
    };

    @Override
    public void processElement(String s, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        Tweet tweet = objectMapper.readValue(s,
                                             Tweet.class);

        dividiendoFrasesEnPalabras(context,
                                   collector,
                                   tweet);


    }

    private void dividiendoFrasesEnPalabras(Context context, Collector<Tuple2<String, Integer>> collector, Tweet tweet) {
        StringTokenizer st = new StringTokenizer(tweet.getText());
        if ("en".equals(tweet.getUser().getLang())) {
            while (st.hasMoreElements()) {
                collector.collect(Tuple2.of(st.nextToken(),
                                            1));
            }
        } else {
            while (st.hasMoreElements()) {
                context.output(noEnglish,
                               Tuple2.of(st.nextToken(),
                                         1));
            }
        }
    }
}
