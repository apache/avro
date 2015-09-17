package com.commercehub.gradle.plugin.avro;

import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility;
import org.apache.avro.generic.GenericData.StringType;

/**
 * Various constants needed by the plugin.
 *
 * <p>The default values from {@code avro-compiler} aren't exposed in a way that's easily accessible, so even default
 * values that we want to match are still reproduced here.</p>
 */
class Constants {
    static final String UTF8_ENCODING = "UTF-8";

    static final String DEFAULT_ENCODING = UTF8_ENCODING;
    static final String DEFAULT_STRING_TYPE = StringType.String.name();
    static final String DEFAULT_FIELD_VISIBILITY = FieldVisibility.PUBLIC_DEPRECATED.name();
    static final String DEFAULT_TEMPLATE_DIR = System.getProperty("org.apache.avro.specific.templates",
                         "/org/apache/avro/compiler/specific/templates/java/classic/");

    static final String SCHEMA_EXTENSION = "avsc";
    static final String PROTOCOL_EXTENSION = "avpr";
    static final String IDL_EXTENSION = "avdl";
    static final String JAVA_EXTENSION = "java";

    static final String GROUP_SOURCE_GENERATION = "Source Generation";

    static final String AVRO_EXTENSION_NAME = "avro";
}
