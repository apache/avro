package com.commercehub.gradle.plugin.avro;

public interface AvroExtension {
    String getEncoding();
    String getStringType();
    String getFieldVisibility();
    String getTemplateDirectory();
    boolean isCreateSetters();
    boolean isRetryDuplicateTypes();
}
