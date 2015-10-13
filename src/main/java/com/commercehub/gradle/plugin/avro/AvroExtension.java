package com.commercehub.gradle.plugin.avro;

public interface AvroExtension {
    String getOutputCharacterEncoding();
    String getStringType();
    String getFieldVisibility();
    String getTemplateDirectory();
    boolean isCreateSetters();
}
