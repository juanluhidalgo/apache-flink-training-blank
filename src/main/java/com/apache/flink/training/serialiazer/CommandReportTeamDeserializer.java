package com.apache.flink.training.serialiazer;

import com.apache.flink.training.model.CommandReportTeam;
import com.apache.flink.training.model.EventMessage;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class CommandReportTeamDeserializer implements DeserializationSchema<CommandReportTeam>, SerializationSchema<CommandReportTeam> {

    @Override
    public CommandReportTeam deserialize(byte[] bytes) throws IOException {
        try {
            return new ObjectMapper().readValue(bytes,
                                                CommandReportTeam.class);
        } catch (Exception e) {
            return CommandReportTeam.builder().teamName("No team").build();
        }
    }

    @Override
    public boolean isEndOfStream(CommandReportTeam commandReportTeam) {
        return false;
    }

    @Override
    public TypeInformation<CommandReportTeam> getProducedType() {
        return TypeInformation.of(CommandReportTeam.class);
    }

    @Override
    public byte[] serialize(CommandReportTeam commandReportTeam) {
        try {
            return new ObjectMapper().writeValueAsBytes(commandReportTeam);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "".getBytes();
        }
    }
}
