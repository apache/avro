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
package com.commercehub.gradle.plugin.avro;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.UnknownConfigurationException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.TaskAction;

import static com.commercehub.gradle.plugin.avro.Constants.IDL_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.PROTOCOL_EXTENSION;

/**
 * Task to convert Avro IDL files into Avro protocol files using {@link Idl}.
 */
@CacheableTask
public class GenerateAvroProtocolTask extends OutputDirTask {
    @TaskAction
    protected void process() {
        getLogger().info("Found {} files", getSource().getFiles().size());
        failOnUnsupportedFiles();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<>(new FileExtensionSpec(IDL_EXTENSION)));
        if (!unsupportedFiles.isEmpty()) {
            throw new GradleException(
                    String.format("Unsupported file extension for the following files: %s", unsupportedFiles));
        }
    }

    private void processFiles() {
        int processedFileCount = 0;
        ClassLoader loader = getRuntimeClassLoader(getProject());
        for (File sourceFile : filterSources(new FileExtensionSpec(IDL_EXTENSION))) {
            processIDLFile(sourceFile, loader);
            processedFileCount++;
        }
        setDidWork(processedFileCount > 0);
    }

    private void processIDLFile(File idlFile, ClassLoader loader) {
        getLogger().info("Processing {}", idlFile);
        File protoFile = new File(getOutputDir().get().getAsFile(),
                FilenameUtils.getBaseName(idlFile.getName()) + "." + PROTOCOL_EXTENSION);
        Idl idl = null;
        try {
            idl = new Idl(idlFile, loader);
            String protoJson = idl.CompilationUnit().toString(true);
            FileUtils.writeJsonFile(protoFile, protoJson);
            getLogger().debug("Wrote {}", protoFile.getPath());
        } catch (IOException | ParseException ex) {
            throw new GradleException(String.format("Failed to compile IDL file %s", idlFile), ex);
        } finally {
            if (idl != null) {
                try {
                    idl.close();
                } catch (IOException ioe) {
                    // ignore
                }
            }
        }
    }

    private ClassLoader getRuntimeClassLoader(Project project) {
        List<URL> urls = new LinkedList<>();
        String configurationName = JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME;
        try {
            Configuration configuration = project.getConfigurations().getByName(configurationName);
            for (File file : configuration) {
                try {
                    urls.add(file.toURI().toURL());
                } catch (MalformedURLException e) {
                    getLogger().debug(e.getMessage());
                }
            }
        } catch (UnknownConfigurationException ex) {
            getLogger().debug("No configuration found with name {}; defaulting to system classloader", configurationName);
        }
        return urls.isEmpty() ? ClassLoader.getSystemClassLoader()
                : new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader());
    }
}
