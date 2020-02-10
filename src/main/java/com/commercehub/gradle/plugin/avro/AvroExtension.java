/**
 * Copyright © 2013-2019 Commerce Technologies, LLC.
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

import java.util.List;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.gradle.api.provider.Property;

public interface AvroExtension {
    Property<String> getOutputCharacterEncoding();
    Property<String> getStringType();
    Property<String> getFieldVisibility();
    Property<String> getTemplateDirectory();
    Property<Boolean> isCreateSetters();
    Property<Boolean> isCreateOptionalGetters();
    Property<Boolean> isGettersReturnOptional();
    Property<Boolean> isEnableDecimalLogicalType();
    Property<String> getDateTimeLogicalType();
    // When we require Gradle 5.1+, we could use MapProperty here.
    @SuppressWarnings("rawtypes")
    Property<Map> getLogicalTypeFactories();
    // When we require Gradle 4.5+, we could use ListProperty here.
    @SuppressWarnings("rawtypes")
    Property<List> getCustomConversions();
    AvroExtension logicalTypeFactory(String typeName, Class<? extends LogicalTypes.LogicalTypeFactory> typeFactoryClass);
    AvroExtension customConversion(Class<? extends Conversion<?>> conversionClass);
}
