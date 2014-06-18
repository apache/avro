package com.commercehub.gradle.plugin.avro;

import org.apache.avro.generic.GenericData;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.internal.ConventionMapping;
import org.gradle.api.internal.IConventionAware;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.SourceTask;
import org.gradle.plugins.ide.idea.GenerateIdeaModule;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.plugins.ide.idea.model.IdeaModule;

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.Callable;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class AvroPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(JavaPlugin.class);
        configureExtension(project);
        configureTasks(project);
        configureIntelliJ(project);
    }

    private static void configureExtension(final Project project) {
        final AvroExtension avroExtension = project.getExtensions().create(AVRO_EXTENSION_NAME, DefaultAvroExtension.class);
        conventionMapping(avroExtension).map("stringType", new Callable<String>() {
            @Override
            public String call() throws Exception {
                return GenericData.StringType.String.name();
            }
        });
        project.getTasks().withType(GenerateAvroJavaTask.class).all(new Action<GenerateAvroJavaTask>() {
            @Override
            public void execute(GenerateAvroJavaTask task) {
                conventionMapping(task).map("stringType", new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return avroExtension.getStringType();
                    }
                });
            }
        });
    }

    private static void configureTasks(final Project project) {
        getSourceSets(project).all(new Action<SourceSet>() {
            public void execute(SourceSet sourceSet) {
                GenerateAvroProtocolTask protoTask = configureProtocolGenerationTask(project, sourceSet);
                configureJavaGenerationTask(project, sourceSet, protoTask);
            }
        });
    }

    private static void configureIntelliJ(final Project project) {
        project.getPlugins().withType(IdeaPlugin.class).all(new Action<IdeaPlugin>() {
            @Override
            public void execute(IdeaPlugin ideaPlugin) {
                SourceSet mainSourceSet = getMainSourceSet(project);
                SourceSet testSourceSet = getTestSourceSet(project);
                final File mainGeneratedOutputDir = getGeneratedOutputDir(project, mainSourceSet, JAVA_EXTENSION);
                final File testGeneratedOutputDir = getGeneratedOutputDir(project, testSourceSet, JAVA_EXTENSION);
                project.getTasks().withType(GenerateIdeaModule.class).all(new Action<GenerateIdeaModule>() {
                    @Override
                    public void execute(GenerateIdeaModule generateIdeaModule) {
                        generateIdeaModule.doFirst(new Action<Task>() {
                            @Override
                            public void execute(Task task) {
                                project.mkdir(mainGeneratedOutputDir);
                                project.mkdir(testGeneratedOutputDir);
                            }
                        });
                    }
                });
                IdeaModule module = ideaPlugin.getModel().getModule();
                module.setSourceDirs(new SetBuilder<File>()
                        .addAll(module.getSourceDirs())
                        .add(getAvroSourceDir(project, mainSourceSet))
                        .add(mainGeneratedOutputDir)
                        .build());
                module.setTestSourceDirs(new SetBuilder<File>()
                        .addAll(module.getTestSourceDirs())
                        .add(getAvroSourceDir(project, testSourceSet))
                        .add(testGeneratedOutputDir)
                        .build());
                // IntelliJ doesn't allow source directories beneath an excluded directory.
                // Thus, we remove the build directory exclude and add all non-generated sub-directories as excludes.
                SetBuilder<File> excludeDirs = new SetBuilder<>();
                excludeDirs.addAll(module.getExcludeDirs()).remove(project.getBuildDir());
                File buildDir = project.getBuildDir();
                if (buildDir.isDirectory()) {
                    excludeDirs.addAll(project.getBuildDir().listFiles(new NonGeneratedDirectoryFileFilter()));
                }
                module.setExcludeDirs(excludeDirs.build());
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

    private static SourceSet getMainSourceSet(Project project) {
        return getSourceSets(project).getByName(SourceSet.MAIN_SOURCE_SET_NAME);
    }

    private static SourceSet getTestSourceSet(Project project) {
        return getSourceSets(project).getByName(SourceSet.TEST_SOURCE_SET_NAME);
    }

    private static class NonGeneratedDirectoryFileFilter implements FileFilter {
        @Override
        public boolean accept(File file) {
            return file.isDirectory() && !file.getName().startsWith("generated-");
        }
    }

    private static ConventionMapping conventionMapping(Object conventionAware) {
        // TODO: try other alternatives to convention mapping
        // Convention mapping is an internal API.
        // Other options here: http://forums.gradle.org/gradle/topics/how_can_i_do_convention_mappings_from_java_without_depending_on_an_internal_api
        return ((IConventionAware) conventionAware).getConventionMapping();
    }
}
