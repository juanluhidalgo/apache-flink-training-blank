package com.apache.flink.training;

import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;

public class BatchTransformationsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Collection<Integer> numbers = Arrays.asList(1,
                                                    2,
                                                    3,
                                                    4);

        env.fromCollection(numbers).output(new PrintingOutputFormat<>()).name("Writing");
        env.execute("Batch App");
    }
}
