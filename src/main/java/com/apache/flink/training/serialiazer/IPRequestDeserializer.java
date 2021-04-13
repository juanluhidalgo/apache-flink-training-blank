package com.apache.flink.training.serialiazer;

import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.IPRequest;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class IPRequestDeserializer implements DeserializationSchema<IPRequest>, SerializationSchema<IPRequest> {

    @Override
    public IPRequest deserialize(byte[] bytes) throws IOException {
        try {
            return new ObjectMapper().readValue(bytes,
                                                IPRequest.class);
        } catch (Exception e) {
            return IPRequest.builder().ip("Bad ip").timestamp(System.currentTimeMillis()).build();
        }
    }

    @Override
    public boolean isEndOfStream(IPRequest ipRequest) {
        return false;
    }

    @Override
    public TypeInformation<IPRequest> getProducedType() {
        return TypeInformation.of(IPRequest.class);
    }

    @Override
    public byte[] serialize(IPRequest ipRequest) {
        try {
            return new ObjectMapper().writeValueAsBytes(ipRequest);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "".getBytes();
        }
    }
}
