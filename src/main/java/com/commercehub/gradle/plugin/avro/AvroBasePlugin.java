/**
 * Copyright © 2014-2015 Commerce Technologies, LLC.
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

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.internal.ConventionMapping;
import org.gradle.api.internal.IConventionAware;

import java.util.concurrent.Callable;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class AvroBasePlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        configureExtension(project);
    }

    private static void configureExtension(final Project project) {
        final AvroExtension avroExtension = project.getExtensions().create(AVRO_EXTENSION_NAME, DefaultAvroExtension.class);
        ConventionMapping extensionMapping = conventionMapping(avroExtension);
        extensionMapping.map(OPTION_STRING_TYPE, new Callable<String>() {
            @Override
            public String call() throws Exception {
                return DEFAULT_STRING_TYPE;
            }
        });
        extensionMapping.map(OPTION_FIELD_VISIBILITY, new Callable<String>() {
            @Override
            public String call() throws Exception {
                return DEFAULT_FIELD_VISIBILITY;
            }
        });
        extensionMapping.map(OPTION_CREATE_SETTERS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return DEFAULT_CREATE_SETTERS;
            }
        });
        project.getTasks().withType(GenerateAvroJavaTask.class).all(new Action<GenerateAvroJavaTask>() {
            @Override
            public void execute(GenerateAvroJavaTask task) {
                ConventionMapping taskMapping = conventionMapping(task);
                taskMapping.map(OPTION_OUTPUT_CHARACTER_ENCODING, new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getOutputCharacterEncoding();
                    }
                });
                taskMapping.map(OPTION_STRING_TYPE, new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getStringType();
                    }
                });
                taskMapping.map(OPTION_FIELD_VISIBILITY, new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getFieldVisibility();
                    }
                });
                taskMapping.map(OPTION_TEMPLATE_DIRECTORY, new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getTemplateDirectory();
                    }
                });
                taskMapping.map(OPTION_CREATE_SETTERS, new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return avroExtension.isCreateSetters();
                    }
                });
            }
        });
    }

    private static ConventionMapping conventionMapping(Object conventionAware) {
        // TODO: try other alternatives to convention mapping
        // Convention mapping is an internal API.
        // Other options here:
        // http://forums.gradle.org/gradle/topics/how_can_i_do_convention_mappings_from_java_without_depending_on_an_internal_api
        return ((IConventionAware) conventionAware).getConventionMapping();
    }
}
