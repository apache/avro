package com.commercehub.gradle.plugin.avro;

import java.util.Arrays;

class Enums {
    static <T extends Enum> T parseCaseInsensitive(String label, T[] values, String input) {
        for (T value : values) {
            if (value.name().equalsIgnoreCase(input)) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid %s '%s'.  Value values are: %s",
                label, input, Arrays.asList(values)));
    }
}
