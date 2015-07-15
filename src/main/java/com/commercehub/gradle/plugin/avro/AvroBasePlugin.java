package com.commercehub.gradle.plugin.avro;

import org.apache.avro.generic.GenericData;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.internal.ConventionMapping;
import org.gradle.api.internal.IConventionAware;

import java.util.concurrent.Callable;

import static com.commercehub.gradle.plugin.avro.Constants.AVRO_EXTENSION_NAME;

public class AvroBasePlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        configureExtension(project);
    }

    private static void configureExtension(final Project project) {
        final AvroExtension avroExtension = project.getExtensions().create(AVRO_EXTENSION_NAME, DefaultAvroExtension.class);
        ConventionMapping extensionMapping = conventionMapping(avroExtension);
        extensionMapping.map("encoding", new Callable<String>() {
            @Override
            public String call() throws Exception {
                return Constants.UTF8_ENCONDING;
            }
        });
        extensionMapping.map("stringType", new Callable<String>() {
            @Override
            public String call() throws Exception {
                return GenericData.StringType.String.name();
            }
        });
        extensionMapping.map("fieldVisibility", new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "PUBLIC_DEPRECATED";
            }
        });
        project.getTasks().withType(GenerateAvroJavaTask.class).all(new Action<GenerateAvroJavaTask>() {
            @Override
            public void execute(GenerateAvroJavaTask task) {
                ConventionMapping taskMapping = conventionMapping(task);
                taskMapping.map("encoding", new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getEncoding();
                    }
                });
                taskMapping.map("stringType", new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getStringType();
                    }
                });
                taskMapping.map("fieldVisibility", new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getFieldVisibility();
                    }
                });
            }
        });
    }

    private static ConventionMapping conventionMapping(Object conventionAware) {
        // TODO: try other alternatives to convention mapping
        // Convention mapping is an internal API.
        // Other options here: http://forums.gradle.org/gradle/topics/how_can_i_do_convention_mappings_from_java_without_depending_on_an_internal_api
        return ((IConventionAware) conventionAware).getConventionMapping();
    }
}
