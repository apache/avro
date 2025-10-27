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
package com.github.davidmc24.gradle.plugin.avro;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class AvroBasePlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        configureExtension(project);
    }

    @SuppressWarnings("deprecation")
    private static void configureExtension(final Project project) {
        final AvroExtension avroExtension =
            GradleCompatibility.createExtensionWithObjectFactory(project, Constants.AVRO_EXTENSION_NAME, DefaultAvroExtension.class);
        project.getTasks().withType(GenerateAvroJavaTask.class).configureEach(task -> {
            task.getOutputCharacterEncoding().convention(avroExtension.getOutputCharacterEncoding());
            task.getStringType().convention(avroExtension.getStringType());
            task.getFieldVisibility().convention(avroExtension.getFieldVisibility());
            task.getTemplateDirectory().convention(avroExtension.getTemplateDirectory());
            task.getAdditionalVelocityToolClasses().convention(avroExtension.getAdditionalVelocityToolClasses());
            task.isCreateSetters().convention(avroExtension.isCreateSetters());
            task.isCreateOptionalGetters().convention(avroExtension.isCreateOptionalGetters());
            task.isGettersReturnOptional().convention(avroExtension.isGettersReturnOptional());
            task.isOptionalGettersForNullableFieldsOnly().convention(avroExtension.isOptionalGettersForNullableFieldsOnly());
            task.isEnableDecimalLogicalType().convention(avroExtension.isEnableDecimalLogicalType());
            task.getConversionsAndTypeFactoriesClasspath().from(avroExtension.getConversionsAndTypeFactoriesClasspath());
            task.getLogicalTypeFactories().convention(avroExtension.getLogicalTypeFactories());
            task.getLogicalTypeFactoryClassNames().convention(avroExtension.getLogicalTypeFactoryClassNames());
            task.getCustomConversions().convention(avroExtension.getCustomConversions());
            task.getCustomConversionClassNames().convention(avroExtension.getCustomConversionClassNames());
        });
    }
}
