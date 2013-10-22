package com.commercehub.gradle.plugin.avro;

public class DefaultAvroExtension implements AvroExtension {
    private String stringType;

    @Override
    public String getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType = stringType;
    }
}
