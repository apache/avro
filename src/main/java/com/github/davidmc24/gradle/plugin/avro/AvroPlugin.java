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

import java.io.File;
import java.io.FileFilter;
import java.nio.charset.Charset;
import java.util.Optional;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.SourceTask;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.jvm.tasks.Jar;
import org.gradle.plugins.ide.idea.GenerateIdeaModule;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.plugins.ide.idea.model.IdeaModule;

import static org.gradle.api.plugins.JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME;

public class AvroPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(JavaPlugin.class);
        project.getPlugins().apply(AvroBasePlugin.class);
        configureTasks(project);
        configureIntelliJ(project);
    }

    private static void configureTasks(final Project project) {
        getSourceSets(project).configureEach(sourceSet -> {
            TaskProvider<GenerateAvroProtocolTask> protoTaskProvider = configureProtocolGenerationTask(project, sourceSet);
            TaskProvider<GenerateAvroJavaTask> javaTaskProvider = configureJavaGenerationTask(project, sourceSet, protoTaskProvider);
            configureTaskDependencies(project, sourceSet, javaTaskProvider);
        });
    }

    private static void configureIntelliJ(final Project project) {
        project.getPlugins().withType(IdeaPlugin.class).configureEach(ideaPlugin -> {
            SourceSet mainSourceSet = getMainSourceSet(project);
            SourceSet testSourceSet = getTestSourceSet(project);
            IdeaModule module = ideaPlugin.getModel().getModule();
            module.setSourceDirs(new SetBuilder<File>()
                .addAll(module.getSourceDirs())
                .add(getAvroSourceDir(project, mainSourceSet))
                .add(getGeneratedOutputDir(project, mainSourceSet, Constants.JAVA_EXTENSION).map(Directory::getAsFile).get())
                .build());
            module.setTestSourceDirs(new SetBuilder<File>()
                .addAll(module.getTestSourceDirs())
                .add(getAvroSourceDir(project, testSourceSet))
                .add(getGeneratedOutputDir(project, testSourceSet, Constants.JAVA_EXTENSION).map(Directory::getAsFile).get())
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
        });
        project.getTasks().withType(GenerateIdeaModule.class).configureEach(generateIdeaModule ->
            generateIdeaModule.doFirst(task ->
                project.getTasks().withType(GenerateAvroJavaTask.class, generateAvroJavaTask ->
                    project.mkdir(generateAvroJavaTask.getOutputDir().get()))));
    }

    private static TaskProvider<GenerateAvroProtocolTask> configureProtocolGenerationTask(final Project project,
                                                                                          final SourceSet sourceSet) {
        String taskName = sourceSet.getTaskName("generate", "avroProtocol");
        return project.getTasks().register(taskName, GenerateAvroProtocolTask.class, task -> {
            task.setDescription(
                String.format("Generates %s Avro protocol definition files from IDL files.", sourceSet.getName()));
            task.setGroup(Constants.GROUP_SOURCE_GENERATION);
            task.source(getAvroSourceDir(project, sourceSet));
            task.include("**/*." + Constants.IDL_EXTENSION);
            task.setClasspath(project.getConfigurations().getByName(RUNTIME_CLASSPATH_CONFIGURATION_NAME));
            task.getOutputDir().convention(getGeneratedOutputDir(project, sourceSet, Constants.PROTOCOL_EXTENSION));
        });
    }

    private static TaskProvider<GenerateAvroJavaTask> configureJavaGenerationTask(final Project project, final SourceSet sourceSet,
                                                                    TaskProvider<GenerateAvroProtocolTask> protoTaskProvider) {
        String taskName = sourceSet.getTaskName("generate", "avroJava");
        TaskProvider<GenerateAvroJavaTask> javaTaskProvider = project.getTasks().register(taskName, GenerateAvroJavaTask.class, task -> {
            task.setDescription(String.format("Generates %s Avro Java source files from schema/protocol definition files.",
                sourceSet.getName()));
            task.setGroup(Constants.GROUP_SOURCE_GENERATION);
            task.source(getAvroSourceDir(project, sourceSet));
            task.source(protoTaskProvider);
            task.include("**/*." + Constants.SCHEMA_EXTENSION, "**/*." + Constants.PROTOCOL_EXTENSION);
            task.getOutputDir().convention(getGeneratedOutputDir(project, sourceSet, Constants.JAVA_EXTENSION));

            sourceSet.getJava().srcDir(task.getOutputDir());

            JavaCompile compileJavaTask = project.getTasks().named(sourceSet.getCompileJavaTaskName(), JavaCompile.class).get();
            task.getOutputCharacterEncoding().convention(project.provider(() ->
                Optional.ofNullable(compileJavaTask.getOptions().getEncoding()).orElse(Charset.defaultCharset().name())));
        });
        project.getTasks().named(sourceSet.getCompileJavaTaskName(), JavaCompile.class, compileJavaTask -> {
            compileJavaTask.source(javaTaskProvider);
        });
        // When the Gradle's JVM plugin's withSourcesJar capability is used, it automatically includes all directories listed in the SourceSet's `allSource`.
        // However, in Gradle 7.1, they started including a warning that execution optimizations are disabled unless you explicitly declare what task produced the directory you're using.
        // Gradle doesn't currently have a way to declare a source directory and the task that creates it, so for now we need to manually declare the task dependency.
        project.getTasks().withType(Jar.class).matching(task -> sourceSet.getSourcesJarTaskName().equals(task.getName())).all(sourcesJarTask -> {
            sourcesJarTask.dependsOn(javaTaskProvider);
        });
        return javaTaskProvider;
    }

    private static void configureTaskDependencies(final Project project, final SourceSet sourceSet,
                                                  final TaskProvider<GenerateAvroJavaTask> javaTaskProvider) {
        project.getPluginManager().withPlugin("org.jetbrains.kotlin.jvm", appliedPlugin ->
            project.getTasks()
                .withType(SourceTask.class)
                .matching(task -> sourceSet.getCompileTaskName("kotlin").equals(task.getName()))
                .configureEach(task ->
                    task.source(javaTaskProvider.get().getOutputs())
                )
        );
    }

    private static File getAvroSourceDir(Project project, SourceSet sourceSet) {
        return project.file(String.format("src/%s/avro", sourceSet.getName()));
    }

    private static Provider<Directory> getGeneratedOutputDir(Project project, SourceSet sourceSet, String extension) {
        String generatedOutputDirName = String.format("generated-%s-avro-%s", sourceSet.getName(), extension);
        return project.getLayout().getBuildDirectory().dir(generatedOutputDirName);
    }

    private static SourceSetContainer getSourceSets(Project project) {
        return project.getExtensions().getByType(SourceSetContainer.class);
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
}
