package com.apache.flink.training.join;

import com.apache.flink.training.model.CoachGoalTeam;
import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinGoalsCoachTeam implements JoinFunction<GoalTeam, CoachTeam, CoachGoalTeam> {

    @Override
    public CoachGoalTeam join(GoalTeam goalTeam, CoachTeam coachTeam) throws Exception {
        return null;
    }
}
