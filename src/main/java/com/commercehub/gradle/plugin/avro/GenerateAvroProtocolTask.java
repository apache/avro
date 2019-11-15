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
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.TaskAction;

import static com.commercehub.gradle.plugin.avro.Constants.IDL_EXTENSION;
import static com.commercehub.gradle.plugin.avro.Constants.PROTOCOL_EXTENSION;
import static org.gradle.api.tasks.PathSensitivity.RELATIVE;

/**
 * Task to convert Avro IDL files into Avro protocol files using {@link Idl}.
 */
@CacheableTask
public class GenerateAvroProtocolTask extends OutputDirTask {
    @InputFiles
    @PathSensitive(value = RELATIVE)
    private FileCollection classpath;

    public GenerateAvroProtocolTask() {
        super();
        this.classpath = GradleCompatibility.createConfigurableFileCollection(getProject());
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    public void classpath(Object... paths) {
        this.classpath.plus(getProject().files(paths));
    }

    public FileCollection getClasspath() {
        return this.classpath;
    }

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
        ClassLoader loader = assembleClassLoader();
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
        try (Idl idl = new Idl(idlFile, loader)) {
            String protoJson = idl.CompilationUnit().toString(true);
            FileUtils.writeJsonFile(protoFile, protoJson);
            getLogger().debug("Wrote {}", protoFile.getPath());
        } catch (IOException | ParseException ex) {
            throw new GradleException(String.format("Failed to compile IDL file %s", idlFile), ex);
        }
    }

    private ClassLoader assembleClassLoader() {
        List<URL> urls = new LinkedList<>();
        for (File file : classpath) {
            try {
                urls.add(file.toURI().toURL());
            } catch (MalformedURLException e) {
                getLogger().debug(e.getMessage());
            }
        }
        if (urls.isEmpty()) {
            getLogger().debug("No classpath configured; defaulting to system classloader");
        }
        return urls.isEmpty() ? ClassLoader.getSystemClassLoader()
                : new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader());
    }
}
