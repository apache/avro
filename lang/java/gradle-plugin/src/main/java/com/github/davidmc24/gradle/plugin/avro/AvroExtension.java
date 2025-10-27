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
package com.github.davidmc24.gradle.plugin.avro;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

@SuppressWarnings("unused")
public interface AvroExtension {
    Property<String> getOutputCharacterEncoding();
    Property<String> getStringType();
    Property<String> getFieldVisibility();
    Property<String> getTemplateDirectory();
    ListProperty<String> getAdditionalVelocityToolClasses();
    Property<Boolean> isCreateSetters();
    Property<Boolean> isCreateOptionalGetters();
    Property<Boolean> isGettersReturnOptional();
    Property<Boolean> isOptionalGettersForNullableFieldsOnly();
    Property<Boolean> isEnableDecimalLogicalType();
    ConfigurableFileCollection getConversionsAndTypeFactoriesClasspath();

    /**
     * @deprecated use {@link #getLogicalTypeFactoryClassNames()} instead
     */
    @Deprecated
    MapProperty<String, Class<? extends LogicalTypes.LogicalTypeFactory>> getLogicalTypeFactories();
    MapProperty<String, String> getLogicalTypeFactoryClassNames();

    /**
     * @deprecated use {@link #getCustomConversionClassNames()} instead
     */
    @Deprecated
    ListProperty<Class<? extends Conversion<?>>> getCustomConversions();
    ListProperty<String> getCustomConversionClassNames();

    /**
     * @deprecated use {@link #logicalTypeFactory(String, String)}
     */
    @Deprecated
    AvroExtension logicalTypeFactory(String typeName, Class<? extends LogicalTypes.LogicalTypeFactory> typeFactoryClass);
    AvroExtension logicalTypeFactory(String typeName, String typeFactoryClassName);

    /**
     * @deprecated use {@link #customConversion(String)} instead
     */
    @Deprecated
    AvroExtension customConversion(Class<? extends Conversion<?>> conversionClass);
    AvroExtension customConversion(String conversionClassName);
}
