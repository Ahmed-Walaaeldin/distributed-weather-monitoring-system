package com.netcentric.weather.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Tiny JSON helper shared by all services (one ObjectMapper, it is thread-safe). */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize " + value, e);
        }
    }

    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot deserialize: " + json, e);
        }
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
