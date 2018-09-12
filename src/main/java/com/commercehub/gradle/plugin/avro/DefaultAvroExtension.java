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

import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;

import java.nio.charset.Charset;

public class DefaultAvroExtension implements AvroExtension {
    private String outputCharacterEncoding;
    private String stringType;
    private String fieldVisibility;
    private String templateDirectory;
    private boolean createSetters;
    private boolean enableDecimalLogicalType;
    private boolean validateDefaults;

    @Override
    public String getOutputCharacterEncoding() {
        return outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(String outputCharacterEncoding) {
        this.outputCharacterEncoding = outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(Charset outputCharacterEncoding) {
        setOutputCharacterEncoding(outputCharacterEncoding.name());
    }

    @Override
    public String getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType = stringType;
    }

    public void setStringType(GenericData.StringType stringType) {
        setStringType(stringType.name());
    }

    @Override
    public String getFieldVisibility() {
        return fieldVisibility;
    }

    public void setFieldVisibility(String fieldVisibility) {
        this.fieldVisibility = fieldVisibility;
    }

    public void setFieldVisibility(SpecificCompiler.FieldVisibility fieldVisibility) {
        setFieldVisibility(fieldVisibility.name());
    }

    @Override
    public String getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory = templateDirectory;
    }

    @Override
    public boolean isCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(String createSetters) {
        setCreateSetters(Boolean.parseBoolean(createSetters));
    }

    public void setCreateSetters(boolean createSetters) {
        this.createSetters = createSetters;
    }

    @Override
    public boolean isEnableDecimalLogicalType() {
        return enableDecimalLogicalType;
    }

    public void setEnableDecimalLogicalType(String enableDecimalLogicalType) {
        setEnableDecimalLogicalType(Boolean.parseBoolean(enableDecimalLogicalType));
    }

    public void setEnableDecimalLogicalType(boolean enableDecimalLogicalType) {
        this.enableDecimalLogicalType = enableDecimalLogicalType;
    }

    @Override
    public boolean isValidateDefaults() {
        return validateDefaults;
    }

    public void setValidateDefaults(String validateDefaults) {
        setValidateDefaults(Boolean.parseBoolean(validateDefaults));
    }

    public void setValidateDefaults(boolean validateDefaults) {
        this.validateDefaults = validateDefaults;
    }
}
