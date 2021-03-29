package com.apache.flink.training.join;

import com.apache.flink.training.model.CoachGoalTeam;
import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.util.Collector;

public class FlatJoinGoalsCoachTeam implements FlatJoinFunction<GoalTeam, CoachTeam, CoachGoalTeam> {


    @Override
    public void join(GoalTeam goalTeam, CoachTeam coachTeam, Collector<CoachGoalTeam> collector) throws Exception {

    }
}
