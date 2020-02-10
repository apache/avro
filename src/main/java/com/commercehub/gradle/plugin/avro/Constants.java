/**
 * Copyright © 2013-2015 Commerce Technologies, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.commercehub.gradle.plugin.avro;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.compiler.specific.SpecificCompiler;
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

    static final String DEFAULT_STRING_TYPE = StringType.String.name();
    static final String DEFAULT_FIELD_VISIBILITY = FieldVisibility.PUBLIC_DEPRECATED.name();
    static final boolean DEFAULT_CREATE_SETTERS = true;
    static final boolean DEFAULT_CREATE_OPTIONAL_GETTERS = false;
    static final boolean DEFAULT_GETTERS_RETURN_OPTIONAL = false;
    static final boolean DEFAULT_ENABLE_DECIMAL_LOGICAL_TYPE = true;
    static final String DEFAULT_DATE_TIME_LOGICAL_TYPE = SpecificCompiler.DateTimeLogicalTypeImplementation.DEFAULT.name();
    @SuppressWarnings("rawtypes")
    static final Map<String, Class> DEFAULT_LOGICAL_TYPE_FACTORIES = Collections.emptyMap();
    @SuppressWarnings("rawtypes")
    static final List<Class> DEFAULT_CUSTOM_CONVERSIONS = Collections.emptyList();

    static final String SCHEMA_EXTENSION = "avsc";
    static final String PROTOCOL_EXTENSION = "avpr";
    static final String IDL_EXTENSION = "avdl";
    static final String JAVA_EXTENSION = "java";

    static final String GROUP_SOURCE_GENERATION = "Source Generation";

    static final String AVRO_EXTENSION_NAME = "avro";

    static final String OPTION_FIELD_VISIBILITY = "fieldVisibility";
    static final String OPTION_STRING_TYPE = "stringType";
    static final String OPTION_DATE_TIME_LOGICAL_TYPE = "dateTimeLogicalType";
}
