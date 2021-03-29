package com.apache.flink.training;

import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import com.apache.flink.training.reduce.GoalCounter;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;

public class BatchJoinsSample {

    // Contar goles por equipo
    // Unir entrenador con equipo y goles
    // Leer datos de fichero

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CoachTeam> coachTeams = env.fromCollection(getCoachTeams());

        coachTeams.output(new PrintingOutputFormat<>());

        DataSet<GoalTeam> goalTeams = env.fromCollection(getGoalsTeams());

        goalTeams.output(new PrintingOutputFormat<>());

        env.execute("Batch App");
    }

    private static Collection<CoachTeam> getCoachTeams() {
        return Arrays.asList(CoachTeam.builder().teamName("Real Madrid").coach("Zinedine Zidane").build(),
                             CoachTeam.builder().teamName("Barcelona").coach("Ronald Koeman").build(),
                             CoachTeam.builder().teamName("Atletico Madrid").coach("Diego Pablo Simeone").build());
    }

    private static Collection<GoalTeam> getGoalsTeams() {
        return Arrays.asList(GoalTeam.builder().teamName("Real Madrid").goals(10).build(),
                             GoalTeam.builder().teamName("Barcelona").goals(2).build(),
                             GoalTeam.builder().teamName("Atletico Madrid").goals(2).build(),
                             GoalTeam.builder().teamName("Real Madrid").goals(5).build(),
                             GoalTeam.builder().teamName("Real Madrid").goals(1).build(),
                             GoalTeam.builder().teamName("Atletico Madrid").goals(1).build(),
                             GoalTeam.builder().teamName("Barcelona").goals(3).build());
    }

}
