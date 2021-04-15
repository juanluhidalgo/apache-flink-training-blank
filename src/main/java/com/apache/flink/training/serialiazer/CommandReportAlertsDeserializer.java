package com.apache.flink.training.serialiazer;

import com.apache.flink.training.model.CommandReportAlerts;
import com.apache.flink.training.model.CommandReportTeam;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class CommandReportAlertsDeserializer implements DeserializationSchema<CommandReportAlerts>,
        SerializationSchema<CommandReportAlerts> {

    @Override
    public CommandReportAlerts deserialize(byte[] bytes) throws IOException {
        try {
            return new ObjectMapper().readValue(bytes,
                                                CommandReportAlerts.class);
        } catch (Exception e) {
            return CommandReportAlerts.builder().id("No id").build();
        }
    }

    @Override
    public boolean isEndOfStream(CommandReportAlerts commandReportAlerts) {
        return false;
    }

    @Override
    public TypeInformation<CommandReportAlerts> getProducedType() {
        return TypeInformation.of(CommandReportAlerts.class);
    }

    @Override
    public byte[] serialize(CommandReportAlerts commandReportAlerts) {
        try {
            return new ObjectMapper().writeValueAsBytes(commandReportAlerts);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "".getBytes();
        }
    }
}
