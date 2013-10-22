package com.commercehub.gradle.plugin.avro;

import com.google.common.collect.ImmutableSet;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.SourceTask;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.plugins.ide.idea.model.IdeaModule;

import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.Set;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class AvroPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(JavaPlugin.class);
        configureTasks(project);
        configureIntelliJ(project);
    }

    private static void configureTasks(final Project project) {
        getSourceSets(project).all(new Action<SourceSet>() {
            public void execute(SourceSet sourceSet) {
                // TODO: use an extension?
                GenerateAvroProtocolTask protoTask = configureProtocolGenerationTask(project, sourceSet);
                configureJavaGenerationTask(project, sourceSet, protoTask);
            }
        });
    }

    private static void configureIntelliJ(final Project project) {
        project.getPlugins().withType(IdeaPlugin.class).all(new Action<IdeaPlugin>() {
            @Override
            public void execute(IdeaPlugin ideaPlugin) {
                SourceSetContainer sourceSets = getSourceSets(project);
                SourceSet mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
                SourceSet testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME);
                IdeaModule module = ideaPlugin.getModel().getModule();
                module.setSourceDirs(new ImmutableSet.Builder<File>()
                        .addAll(module.getSourceDirs())
                        .add(getAvroSourceDir(project, mainSourceSet))
                        .add(getGeneratedOutputDir(project, mainSourceSet, JAVA_EXTENSION))
                        .build());
                module.setTestSourceDirs(new ImmutableSet.Builder<File>()
                        .addAll(module.getTestSourceDirs())
                        .add(getAvroSourceDir(project, testSourceSet))
                        .add(getGeneratedOutputDir(project, testSourceSet, JAVA_EXTENSION))
                        .build());
                // IntelliJ doesn't allow source directories beneath an excluded directory.
                // Thus, we remove the build directory exclude and add all non-generated sub-directories as excludes.
                Set<File> excludeDirs = new HashSet<>(module.getExcludeDirs());
                excludeDirs.remove(project.getBuildDir());
                for (File dir : project.getBuildDir().listFiles(new NonGeneratedDirectoryFileFilter())) {
                    excludeDirs.add(dir);
                }
                module.setExcludeDirs(excludeDirs);
            }
        });
    }

    private static GenerateAvroProtocolTask configureProtocolGenerationTask(Project project, SourceSet sourceSet) {
        File outputDir = getGeneratedOutputDir(project, sourceSet, PROTOCOL_EXTENSION);
        String taskName = sourceSet.getTaskName("generate", "avroProtocol");
        GenerateAvroProtocolTask task = project.getTasks().create(taskName, GenerateAvroProtocolTask.class);
        task.setDescription(
                String.format("Generates %s Avro protocol definition files from IDL files.", sourceSet.getName()));
        task.setGroup(GROUP_SOURCE_GENERATION);
        task.source(getAvroSourceDir(project, sourceSet));
        task.include("*." + IDL_EXTENSION);
        task.setOutputDir(outputDir);
        return task;
    }

    private static GenerateAvroJavaTask configureJavaGenerationTask(Project project, SourceSet sourceSet,
                                                                    GenerateAvroProtocolTask protoTask) {
        File outputDir = getGeneratedOutputDir(project, sourceSet, JAVA_EXTENSION);
        String taskName = sourceSet.getTaskName("generate", "avroJava");
        GenerateAvroJavaTask task = project.getTasks().create(taskName, GenerateAvroJavaTask.class);
        task.setDescription(String.format("Generates %s Avro Java source files from schema/protocol definition files.",
                sourceSet.getName()));
        task.setGroup(GROUP_SOURCE_GENERATION);
        task.source(getAvroSourceDir(project, sourceSet));
        task.source(protoTask.getOutputs());
        task.include("*." + SCHEMA_EXTENSION, "*." + PROTOCOL_EXTENSION);
        task.setOutputDir(outputDir);
        getCompileJavaTask(project, sourceSet).source(task.getOutputs());
        return task;
    }

    private static File getAvroSourceDir(Project project, SourceSet sourceSet) {
        return project.file(String.format("src/%s/avro", sourceSet.getName()));
    }

    private static File getGeneratedOutputDir(Project project, SourceSet sourceSet, String extension) {
        return new File(project.getBuildDir(), String.format("generated-%s-avro-%s", sourceSet.getName(), extension));
    }

    private static SourceTask getCompileJavaTask(Project project, SourceSet sourceSet) {
        return (SourceTask) project.getTasks().getByName(sourceSet.getCompileJavaTaskName());
    }

    private static SourceSetContainer getSourceSets(Project project) {
        return project.getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
    }

    private static class NonGeneratedDirectoryFileFilter implements FileFilter {
        @Override
        public boolean accept(File file) {
            return file.isDirectory() && !file.getName().startsWith("generated-");
        }
    }
}
