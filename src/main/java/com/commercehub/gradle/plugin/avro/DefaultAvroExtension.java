package com.commercehub.gradle.plugin.avro;

public class DefaultAvroExtension implements AvroExtension {
    private String encoding;
    private String stringType;

    @Override
    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public String getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType = stringType;
    }
}
