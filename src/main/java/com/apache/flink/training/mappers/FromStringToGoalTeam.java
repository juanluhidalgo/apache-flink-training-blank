package com.apache.flink.training.mappers;

import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FromStringToGoalTeam implements MapFunction<String, GoalTeam> {

    @Override
    public GoalTeam map(String s) throws Exception {
        return new ObjectMapper().readValue(s,
                                            GoalTeam.class);
    }
}
