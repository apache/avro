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

import java.nio.charset.Charset;
import java.util.Map;
import javax.inject.Inject;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_CREATE_OPTIONAL_GETTERS;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_CREATE_SETTERS;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_CUSTOM_CONVERSIONS;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_DATE_TIME_LOGICAL_TYPE;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_ENABLE_DECIMAL_LOGICAL_TYPE;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_FIELD_VISIBILITY;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_GETTERS_RETURN_OPTIONAL;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_LOGICAL_TYPE_FACTORIES;
import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_STRING_TYPE;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configureListPropertyConvention;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configureMapPropertyConvention;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configurePropertyConvention;

public class DefaultAvroExtension implements AvroExtension {
    private final Property<String> outputCharacterEncoding;
    private final Property<String> stringType;
    private final Property<String> fieldVisibility;
    private final Property<String> templateDirectory;
    private final Property<Boolean> createSetters;
    private final Property<Boolean> createOptionalGetters;
    private final Property<Boolean> gettersReturnOptional;
    private final Property<Boolean> enableDecimalLogicalType;
    private final Property<String> dateTimeLogicalType;
    @SuppressWarnings("rawtypes")
    private final MapProperty<String, Class> logicalTypeFactories;
    @SuppressWarnings("rawtypes")
    private final ListProperty<Class> customConversions;

    @Inject
    public DefaultAvroExtension(ObjectFactory objects) {
        this.outputCharacterEncoding = objects.property(String.class);
        this.stringType = configurePropertyConvention(objects.property(String.class), DEFAULT_STRING_TYPE);
        this.fieldVisibility = configurePropertyConvention(objects.property(String.class), DEFAULT_FIELD_VISIBILITY);
        this.templateDirectory = objects.property(String.class);
        this.createSetters = configurePropertyConvention(objects.property(Boolean.class), DEFAULT_CREATE_SETTERS);
        this.createOptionalGetters = configurePropertyConvention(objects.property(Boolean.class), DEFAULT_CREATE_OPTIONAL_GETTERS);
        this.gettersReturnOptional = configurePropertyConvention(objects.property(Boolean.class), DEFAULT_GETTERS_RETURN_OPTIONAL);
        this.enableDecimalLogicalType = configurePropertyConvention(objects.property(Boolean.class), DEFAULT_ENABLE_DECIMAL_LOGICAL_TYPE);
        this.dateTimeLogicalType = configurePropertyConvention(objects.property(String.class), DEFAULT_DATE_TIME_LOGICAL_TYPE);
        this.logicalTypeFactories = configureMapPropertyConvention(
            objects.mapProperty(String.class, Class.class), DEFAULT_LOGICAL_TYPE_FACTORIES);
        this.customConversions = configureListPropertyConvention(objects.listProperty(Class.class), DEFAULT_CUSTOM_CONVERSIONS);
    }

    @Override
    public Property<String> getOutputCharacterEncoding() {
        return outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(String outputCharacterEncoding) {
        this.outputCharacterEncoding.set(outputCharacterEncoding);
    }

    public void setOutputCharacterEncoding(Charset outputCharacterEncoding) {
        setOutputCharacterEncoding(outputCharacterEncoding.name());
    }

    @Override
    public Property<String> getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType.set(stringType);
    }

    public void setStringType(GenericData.StringType stringType) {
        setStringType(stringType.name());
    }

    @Override
    public Property<String> getFieldVisibility() {
        return fieldVisibility;
    }

    public void setFieldVisibility(String fieldVisibility) {
        this.fieldVisibility.set(fieldVisibility);
    }

    public void setFieldVisibility(SpecificCompiler.FieldVisibility fieldVisibility) {
        setFieldVisibility(fieldVisibility.name());
    }

    @Override
    public Property<String> getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory.set(templateDirectory);
    }

    @Override
    public Property<Boolean> isCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(String createSetters) {
        setCreateSetters(Boolean.parseBoolean(createSetters));
    }

    public void setCreateSetters(boolean createSetters) {
        this.createSetters.set(createSetters);
    }

    @Override
    public Property<Boolean> isCreateOptionalGetters() {
        return createOptionalGetters;
    }

    public void setCreateOptionalGetters(String createOptionalGetters) {
        setCreateOptionalGetters(Boolean.parseBoolean(createOptionalGetters));
    }

    public void setCreateOptionalGetters(boolean createOptionalGetters) {
        this.createOptionalGetters.set(createOptionalGetters);
    }

    @Override
    public Property<Boolean> isGettersReturnOptional() {
        return gettersReturnOptional;
    }

    public void setGettersReturnOptional(String gettersReturnOptional) {
        setGettersReturnOptional(Boolean.parseBoolean(gettersReturnOptional));
    }

    public void setGettersReturnOptional(boolean gettersReturnOptional) {
        this.gettersReturnOptional.set(gettersReturnOptional);
    }

    @Override
    public Property<Boolean> isEnableDecimalLogicalType() {
        return enableDecimalLogicalType;
    }

    public void setEnableDecimalLogicalType(String enableDecimalLogicalType) {
        setEnableDecimalLogicalType(Boolean.parseBoolean(enableDecimalLogicalType));
    }

    public void setEnableDecimalLogicalType(boolean enableDecimalLogicalType) {
        this.enableDecimalLogicalType.set(enableDecimalLogicalType);
    }

    @Override
    public Property<String> getDateTimeLogicalType() {
        return dateTimeLogicalType;
    }

    public void setDateTimeLogicalType(String dateTimeLogicalType) {
        this.dateTimeLogicalType.set(dateTimeLogicalType);
    }

    public void setDateTimeLogicalType(SpecificCompiler.DateTimeLogicalTypeImplementation dateTimeLogicalType) {
        setDateTimeLogicalType(dateTimeLogicalType.name());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public MapProperty<String, Class> getLogicalTypeFactories() {
        return logicalTypeFactories;
    }

    @SuppressWarnings("rawtypes")
    public void setLogicalTypeFactories(Provider<? extends Map<? extends String, ? extends Class>> provider) {
        this.logicalTypeFactories.set(provider);
    }

    @SuppressWarnings("rawtypes")
    public void setLogicalTypeFactories(Map<? extends String, ? extends Class> logicalTypeFactories) {
        this.logicalTypeFactories.set(logicalTypeFactories);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ListProperty<Class> getCustomConversions() {
        return customConversions;
    }

    @SuppressWarnings("rawtypes")
    public void setCustomConversions(Provider<Iterable<Class>> provider) {
        this.customConversions.set(provider);
    }

    @SuppressWarnings("rawtypes")
    public void setCustomConversions(Iterable<Class> customConversions) {
        this.customConversions.set(customConversions);
    }
}
