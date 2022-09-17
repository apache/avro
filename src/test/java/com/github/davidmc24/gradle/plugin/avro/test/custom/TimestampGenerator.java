package com.github.davidmc24.gradle.plugin.avro.test.custom;


public class TimestampGenerator {

    public static final String MESSAGE_PREFIX = "Current timestamp is";

    public String generateTimestampMessage() {
        return String.format("%s %s", MESSAGE_PREFIX,
                java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ISO_DATE_TIME));
    }

}
