package com.notification_system.config;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    public JsonSerde(Class<T> clazz) {
        super(new JsonSerializer<>(), new JsonDeserializer<>(clazz));
    }
}
