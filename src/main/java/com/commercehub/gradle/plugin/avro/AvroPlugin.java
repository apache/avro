package com.commercehub.gradle.plugin.avro;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceTask;

import java.io.File;

public class AvroPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(JavaPlugin.class);
        project.getConvention().getPlugin(JavaPluginConvention.class).getSourceSets().all(new Action<SourceSet>() {
            public void execute(SourceSet sourceSet) {
                // TODO: use an extension?
                GenerateAvroProtocolTask protoTask = configureProtocolGenerationTask(project, sourceSet);
                configureJavaGenerationTask(project, sourceSet, protoTask);
            }
        });
    }

    private static GenerateAvroProtocolTask configureProtocolGenerationTask(Project project, SourceSet sourceSet) {
        File outputDir = getGeneratedOutputDir(project, sourceSet, Constants.PROTOCOL_EXTENSION);
        String taskName = sourceSet.getTaskName("generate", "avroProtocol");
        GenerateAvroProtocolTask task = project.getTasks().create(taskName, GenerateAvroProtocolTask.class);
        task.setDescription(String.format("Generates %s Avro protocol definition files from IDL files.", sourceSet.getName()));
        // TODO: group name?
        task.source(project.file(String.format("src/%s/avro", sourceSet.getName())));
        task.include("*." + Constants.IDL_EXTENSION);
        task.setOutputDir(outputDir);
        return task;
    }

    private static GenerateAvroJavaTask configureJavaGenerationTask(Project project, SourceSet sourceSet, GenerateAvroProtocolTask protoTask) {
        File outputDir = getGeneratedOutputDir(project, sourceSet, "java");
        String taskName = sourceSet.getTaskName("generate", "avroJava");
        GenerateAvroJavaTask task = project.getTasks().create(taskName, GenerateAvroJavaTask.class);
        task.setDescription(String.format("Generates %s Avro Java source files from schema/protocol definition files.", sourceSet.getName()));
        // TODO: group name?
        task.source(project.file(String.format("src/%s/avro", sourceSet.getName())));
        task.source(protoTask.getOutputs());
        task.include("*." + Constants.SCHEMA_EXTENSION, "*." + Constants.PROTOCOL_EXTENSION);
        task.setOutputDir(outputDir);
        getCompileJavaTask(project, sourceSet).source(task.getOutputs());
        return task;
    }

    private static File getGeneratedOutputDir(Project project, SourceSet sourceSet, String extension) {
        return new File(project.getBuildDir(), String.format("generated-%s-avro-%s", sourceSet.getName(), extension));
    }

    private static SourceTask getCompileJavaTask(Project project, SourceSet sourceSet) {
        return (SourceTask) project.getTasks().getByName(sourceSet.getCompileJavaTaskName());
    }
}
