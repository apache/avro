package com.commercehub.gradle.plugin.avro;

import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.SourceTask;

import java.io.File;

class OutputDirTask extends SourceTask {
    private File outputDir;

    public void setOutputDir(File outputDir) {
        this.outputDir = outputDir;
        getOutputs().dir(outputDir);
    }

    protected File getOutputDir() {
        return outputDir;
    }

    protected FileCollection filterSources(Spec<? super File> spec) {
        return getInputs().getSourceFiles().filter(spec);
    }
}
