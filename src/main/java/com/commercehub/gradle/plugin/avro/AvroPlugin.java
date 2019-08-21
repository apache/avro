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

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.Callable;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.internal.ConventionMapping;
import org.gradle.api.internal.IConventionAware;
import org.gradle.api.plugins.AppliedPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.SourceTask;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.plugins.ide.idea.GenerateIdeaModule;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.plugins.ide.idea.model.IdeaModule;

import static com.commercehub.gradle.plugin.avro.Constants.GROUP_SOURCE_GENERATION;
import static com.commercehub.gradle.plugin.avro.Constants.IDL_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.JAVA_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.OPTION_OUTPUT_CHARACTER_ENCODING;
import static com.commercehub.gradle.plugin.avro.Constants.PROTOCOL_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.SCHEMA_EXTENSION;

public class AvroPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(JavaPlugin.class);
        project.getPlugins().apply(AvroBasePlugin.class);
        configureTasks(project);
        configureIntelliJ(project);
    }

    private static void configureTasks(final Project project) {
        getSourceSets(project).all(new Action<SourceSet>() {
            public void execute(SourceSet sourceSet) {
                GenerateAvroProtocolTask protoTask = configureProtocolGenerationTask(project, sourceSet);
                GenerateAvroJavaTask javaTask = configureJavaGenerationTask(project, sourceSet, protoTask);
                configureTaskDependencies(project, sourceSet, javaTask);
            }
        });
    }

    private static void configureIntelliJ(final Project project) {
        project.getPlugins().withType(IdeaPlugin.class).all(new Action<IdeaPlugin>() {
            @Override
            public void execute(IdeaPlugin ideaPlugin) {
                SourceSet mainSourceSet = getMainSourceSet(project);
                SourceSet testSourceSet = getTestSourceSet(project);
                project.getTasks().withType(GenerateIdeaModule.class).all(new Action<GenerateIdeaModule>() {
                    @Override
                    public void execute(final GenerateIdeaModule generateIdeaModule) {
                        project.getTasks().withType(GenerateAvroJavaTask.class).all(new Action<GenerateAvroJavaTask>() {
                            @Override
                            public void execute(final GenerateAvroJavaTask generateAvroJavaTask) {
                                generateIdeaModule.doFirst(new Action<Task>() {
                                    @Override
                                    public void execute(Task task) {
                                        project.mkdir(generateAvroJavaTask.getOutputDir());
                                    }
                                });
                            }
                        });
                    }
                });
                IdeaModule module = ideaPlugin.getModel().getModule();
                module.setSourceDirs(new SetBuilder<File>()
                    .addAll(module.getSourceDirs())
                    .add(getAvroSourceDir(project, mainSourceSet))
                    .build());
                module.setTestSourceDirs(new SetBuilder<File>()
                    .addAll(module.getTestSourceDirs())
                    .add(getAvroSourceDir(project, testSourceSet))
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

    private static GenerateAvroProtocolTask configureProtocolGenerationTask(final Project project, final SourceSet sourceSet) {
        String taskName = sourceSet.getTaskName("generate", "avroProtocol");
        GenerateAvroProtocolTask task = project.getTasks().create(taskName, GenerateAvroProtocolTask.class);
        task.setDescription(
            String.format("Generates %s Avro protocol definition files from IDL files.", sourceSet.getName()));
        task.setGroup(GROUP_SOURCE_GENERATION);
        task.source(getAvroSourceDir(project, sourceSet));
        task.include("**/*." + IDL_EXTENSION);
        task.getConventionMapping().map("outputDir", new Callable<File>() {
            @Override
            public File call() throws Exception {
                return getGeneratedOutputDir(project, sourceSet, PROTOCOL_EXTENSION);
            }
        });
        return task;
    }

    private static GenerateAvroJavaTask configureJavaGenerationTask(final Project project, final SourceSet sourceSet,
                                                                    GenerateAvroProtocolTask protoTask) {
        String taskName = sourceSet.getTaskName("generate", "avroJava");
        GenerateAvroJavaTask task = project.getTasks().create(taskName, GenerateAvroJavaTask.class);
        task.setDescription(String.format("Generates %s Avro Java source files from schema/protocol definition files.",
            sourceSet.getName()));
        task.setGroup(GROUP_SOURCE_GENERATION);
        task.source(getAvroSourceDir(project, sourceSet));
        task.source(protoTask.getOutputDir());
        task.source(protoTask.getOutputs());
        task.include("**/*." + SCHEMA_EXTENSION, "**/*." + PROTOCOL_EXTENSION);
        task.getConventionMapping().map("outputDir", new Callable<File>() {
            @Override
            public File call() throws Exception {
                return getGeneratedOutputDir(project, sourceSet, JAVA_EXTENSION);
            }
        });

        sourceSet.getJava().srcDir(task.getOutputDir());

        final JavaCompile compileJavaTask = getCompileJavaTask(project, sourceSet);
        compileJavaTask.source(task.getOutputDir());
        compileJavaTask.source(task.getOutputs());

        final AvroExtension avroExtension = project.getExtensions().findByType(AvroExtension.class);
        ConventionMapping taskMapping = conventionMapping(task);
        taskMapping.map(OPTION_OUTPUT_CHARACTER_ENCODING, new Callable<String>() {
            @Override
            public String call() throws Exception {
                String compilationEncoding = compileJavaTask.getOptions().getEncoding();
                String extensionEncoding = avroExtension.getOutputCharacterEncoding();
                return compilationEncoding != null ? compilationEncoding : extensionEncoding;
            }
        });
        return task;
    }

    private static void configureTaskDependencies(final Project project, final SourceSet sourceSet, final GenerateAvroJavaTask javaTask) {
        project.getPluginManager().withPlugin("org.jetbrains.kotlin.jvm", new Action<AppliedPlugin>() {
            @Override
            public void execute(AppliedPlugin appliedPlugin) {
                project.getTasks().matching(new Spec<Task>() {
                    @Override
                    public boolean isSatisfiedBy(Task task) {
                        String compilationTaskName = sourceSet.getCompileTaskName("kotlin");
                        return compilationTaskName.equals(task.getName());
                    }
                })
                    .all(new Action<Task>() {
                        @Override
                        public void execute(Task task) {
                            if (task instanceof SourceTask) {
                                ((SourceTask) task).source(javaTask.getOutputs());
                            } else {
                                task.dependsOn(javaTask);
                            }
                        }
                    });
            }
        });
    }

    private static File getAvroSourceDir(Project project, SourceSet sourceSet) {
        return project.file(String.format("src/%s/avro", sourceSet.getName()));
    }

    private static File getGeneratedOutputDir(Project project, SourceSet sourceSet, String extension) {
        return new File(project.getBuildDir(), String.format("generated-%s-avro-%s", sourceSet.getName(), extension));
    }

    private static JavaCompile getCompileJavaTask(Project project, SourceSet sourceSet) {
        return (JavaCompile) project.getTasks().getByName(sourceSet.getCompileJavaTaskName());
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

    private static ConventionMapping conventionMapping(Object conventionAware) {
        // TODO: try other alternatives to convention mapping
        // Convention mapping is an internal API.
        // Other options here:
        // http://forums.gradle.org/gradle/topics/how_can_i_do_convention_mappings_from_java_without_depending_on_an_internal_api
        return ((IConventionAware) conventionAware).getConventionMapping();
    }

    private static class NonGeneratedDirectoryFileFilter implements FileFilter {
        @Override
        public boolean accept(File file) {
            return file.isDirectory() && !file.getName().startsWith("generated-");
        }
    }
}
