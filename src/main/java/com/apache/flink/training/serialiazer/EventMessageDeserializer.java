package com.apache.flink.training.serialiazer;

import com.apache.flink.training.model.EventMessage;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class EventMessageDeserializer implements DeserializationSchema<EventMessage>, SerializationSchema<EventMessage> {

    @Override
    public EventMessage deserialize(byte[] bytes) throws IOException {
        try {
            return new ObjectMapper().readValue(bytes,
                                                EventMessage.class);
        } catch (Exception e) {
            return EventMessage.builder().message("Bad format").severity("ERROR").timestamp(System.currentTimeMillis()).build();
        }
    }

    @Override
    public boolean isEndOfStream(EventMessage eventMessage) {
        return false;
    }

    @Override
    public TypeInformation<EventMessage> getProducedType() {
        return TypeInformation.of(EventMessage.class);
    }

    @Override
    public byte[] serialize(EventMessage eventMessage) {
        try {
            return new ObjectMapper().writeValueAsBytes(eventMessage);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "".getBytes();
        }
    }
}
