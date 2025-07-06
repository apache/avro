/**
 * Copyright © 2013-2019 Commerce Technologies, LLC.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.davidmc24.gradle.plugin.avro;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.avro.Protocol;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.TaskAction;

import static com.github.davidmc24.gradle.plugin.avro.Constants.IDL_EXTENSION;

/**
 * Task to convert Avro IDL files into Avro protocol files using {@link Idl}.
 */
@CacheableTask
public class GenerateAvroProtocolTask extends OutputDirTask {

    private FileCollection classpath;
    private Set<String> processedFiles;

    public GenerateAvroProtocolTask() {
        super();
        this.classpath = GradleCompatibility.createConfigurableFileCollection(getProject());
        this.processedFiles = new HashSet<String>();
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    public void classpath(Object... paths) {
        this.classpath.plus(getProject().files(paths));
    }

    @Classpath
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
        try (Idl idl = new Idl(idlFile, loader)) {
            File outputDir = getOutputDir().get().getAsFile();
            Protocol protocol = idl.CompilationUnit();
            String filePath = AvroUtils.assemblePath(protocol);
            if (!processedFiles.add(filePath)) {
                throw new GradleException("File already processed with same namespace and protocol name.");
            }
            File protoFile = new File(outputDir, filePath);
            String protoJson = protocol.toString(true);
            FileUtils.writeJsonFile(protoFile, protoJson);
            getLogger().debug("Wrote {}", protoFile.getPath());
        } catch (IOException | ParseException | GradleException ex) {
            throw new GradleException(String.format("Failed to compile IDL file %s", idlFile), ex);
        }
    }

    private ClassLoader assembleClassLoader() {
        getLogger().debug("Using classpath: {}", classpath.getFiles());
        List<URL> urls = new LinkedList<>();
        for (File file : classpath) {
            try {
                urls.add(file.toURI().toURL());
            } catch (MalformedURLException e) {
                getLogger().debug(e.getMessage());
            }
        }
        // No parent classloader; either it's in the specified classpath or it shouldn't be resolved.
        return new URLClassLoader(urls.toArray(new URL[0]), null);
    }
}
