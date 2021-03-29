package com.apache.flink.training;

import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import com.apache.flink.training.mappers.Double;

public class BatchIterationSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Collection<Integer> numbers = Arrays.asList(2);

        IterativeDataSet<Integer> initial = env.fromCollection(numbers).iterate(10);
        DataSet<Integer> repeatedDouble = initial.map(new Double());
        initial.closeWith(repeatedDouble).output(new PrintingOutputFormat<>()).name("Writing");
        env.execute("Batch App");
    }
}
