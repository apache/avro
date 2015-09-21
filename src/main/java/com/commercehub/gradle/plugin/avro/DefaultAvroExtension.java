package com.commercehub.gradle.plugin.avro;

public class DefaultAvroExtension implements AvroExtension {
    private String encoding;
    private String stringType;
    private String fieldVisibility;
    private String templateDirectory;
    private boolean createSetters;

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

    @Override
    public String getFieldVisibility() {
        return fieldVisibility;
    }
    
    public void setFieldVisibility(String fieldVisibility) {
        this.fieldVisibility = fieldVisibility;
    }

    @Override
    public String getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory = templateDirectory;
    }

    @Override
    public Boolean isCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(boolean createSetters) {
        this.createSetters = createSetters;
    }

    public void setCreateSetters(String createSetters) {
        this.createSetters = Boolean.parseBoolean(createSetters);
    }
}
