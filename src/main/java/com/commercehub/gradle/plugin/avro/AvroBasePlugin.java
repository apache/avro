/**
 * Copyright © 2014-2019 Commerce Technologies, LLC.
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

import org.gradle.api.Plugin;
import org.gradle.api.Project;

import static com.commercehub.gradle.plugin.avro.Constants.AVRO_EXTENSION_NAME;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configureListPropertyConvention;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configureMapPropertyConvention;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.configurePropertyConvention;
import static com.commercehub.gradle.plugin.avro.GradleCompatibility.createExtensionWithObjectFactory;

public class AvroBasePlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        configureExtension(project);
    }

    private static void configureExtension(final Project project) {
        final AvroExtension avroExtension = createExtensionWithObjectFactory(project, AVRO_EXTENSION_NAME, DefaultAvroExtension.class);
        project.getTasks().withType(GenerateAvroJavaTask.class).all(task -> {
            configurePropertyConvention(task.getOutputCharacterEncoding(), avroExtension.getOutputCharacterEncoding());
            configurePropertyConvention(task.getStringType(), avroExtension.getStringType());
            configurePropertyConvention(task.getFieldVisibility(), avroExtension.getFieldVisibility());
            configurePropertyConvention(task.getTemplateDirectory(), avroExtension.getTemplateDirectory());
            configurePropertyConvention(task.isCreateSetters(), avroExtension.isCreateSetters());
            configurePropertyConvention(task.isCreateOptionalGetters(), avroExtension.isCreateOptionalGetters());
            configurePropertyConvention(task.isGettersReturnOptional(), avroExtension.isGettersReturnOptional());
            configurePropertyConvention(task.isEnableDecimalLogicalType(), avroExtension.isEnableDecimalLogicalType());
            configurePropertyConvention(task.getDateTimeLogicalType(), avroExtension.getDateTimeLogicalType());
            configureMapPropertyConvention(task.getLogicalTypeFactories(), avroExtension.getLogicalTypeFactories());
            configureListPropertyConvention(task.getCustomConversions(), avroExtension.getCustomConversions());
        });
    }
}
