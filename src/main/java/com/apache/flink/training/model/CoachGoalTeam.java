package com.apache.flink.training.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CoachGoalTeam {

    String teamName;

    String coach;

    Integer goals;

}
