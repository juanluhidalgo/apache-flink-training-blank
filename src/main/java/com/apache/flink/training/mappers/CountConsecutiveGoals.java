package com.apache.flink.training.mappers;

import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.GoalTeam;
import java.util.Objects;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CountConsecutiveGoals extends RichCoFlatMapFunction<CommandReportTeam, GoalTeam, GoalTeam> {

    private transient ValueState<GoalTeam> totalGoals;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<GoalTeam> descriptor = new ValueStateDescriptor<GoalTeam>("goals",
                                                                                       TypeInformation.of(new TypeHint<GoalTeam>() {
                                                                                       }));
        totalGoals = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap1(CommandReportTeam commandReportTeam, Collector<GoalTeam> collector) throws Exception {

        if ("REPORT".equals(commandReportTeam.getCommand())) {
            GoalTeam value = totalGoals.value();

            collector.collect(
                    Objects.nonNull(value) ? value : GoalTeam.builder().teamName(commandReportTeam.getTeamName()).goals(0).build());
        }

    }

    @Override
    public void flatMap2(GoalTeam goalTeam, Collector<GoalTeam> collector) throws Exception {
        GoalTeam currentGoalTeam = totalGoals.value();

        if (Objects.nonNull(currentGoalTeam)) {
            currentGoalTeam.setGoals(currentGoalTeam.getGoals() + goalTeam.getGoals());
        } else {
            currentGoalTeam = goalTeam;
        }

        totalGoals.update(currentGoalTeam);

        if (goalTeam.getGoals() == 0) {
            collector.collect(currentGoalTeam);
            totalGoals.clear();
        }

    }
}
