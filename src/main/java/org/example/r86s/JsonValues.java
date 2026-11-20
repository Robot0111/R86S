package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;

final class JsonValues {
    private JsonValues() {
    }

    static Instant instant(JsonNode node) {
        if (node.isTextual()) {
            try {
                return Instant.parse(node.textValue());
            } catch (DateTimeParseException ignored) {
                // The collector may have recorded an invalid timestamp.
            }
        }
        return null;
    }

    static Long integer(JsonNode node) {
        return node.isIntegralNumber() && node.canConvertToLong() ? node.longValue() : null;
    }

    static Double number(JsonNode node) {
        if (node.isNumber()) {
            double value = node.doubleValue();
            return Double.isFinite(value) ? value : null;
        }
        return null;
    }

    static Boolean bool(JsonNode node) {
        return node.isBoolean() ? node.booleanValue() : null;
    }
}
